import asyncio
import signal
from typing import Dict, List, Optional

from .constants import log
from .irc_server import EmbeddedIRCServer
from .irc_client import IRCClient
from .lora_serial import LoRaSerial


_NOISE_PREFIXES = ("[replying]", "(lora)")


def _is_upstream_noise(text: str) -> bool:
    t = text.strip().lower()
    for p in _NOISE_PREFIXES:
        if t.startswith(p): return True
    if t.startswith("["):
        end = t.find("]")
        if end != -1 and "/" in t[1:end] and ":" in t[1:end]: return True
    return False


def _normalize_channel(raw: Optional[str]) -> Optional[str]:
    if not raw: return None
    s = raw.strip()
    i = 0
    while i < len(s) and s[i] in "#:": i += 1
    name = s[i:]
    for sep in (" ", "\t", "|", ":"):
        pos = name.find(sep)
        if pos != -1: name = name[:pos]
    return ("#" + name) if name else None


class IRCLoRaBridge:
    def __init__(self, cfg):
        self.cfg = cfg
        self._pending_upstream_msgs: Dict[str, List[str]] = {}

        self.local_srv = EmbeddedIRCServer(
            host=cfg.local_host, port=cfg.local_port,
            server_name=cfg.local_server_name, motd=cfg.local_motd,
            on_message=self._on_local_message, password=cfg.local_password,
            on_join=self._on_local_join, on_part=self._on_local_part,
            bridge_nick=cfg.irc_nick)

        self.irc = IRCClient(
            host=cfg.irc_server, port=cfg.irc_port, nick=cfg.irc_nick,
            password=cfg.irc_password, tls=cfg.irc_tls,
            on_message=self._on_upstream_message,
            on_connected=self._on_upstream_connected,
            on_disconnected=self._on_upstream_disconnected,
            on_channel_join=self._on_upstream_channel_joined,
            upstream_enabled=cfg.upstream_enabled,
            max_retries=cfg.upstream_max_retries,
            tor=cfg.irc_tor, tor_host=cfg.irc_tor_host, tor_port=cfg.irc_tor_port,
            sasl_username=cfg.irc_sasl_username,
            sasl_password=cfg.irc_sasl_password)

        self.lora = LoRaSerial(
            device=cfg.serial_device, baud=cfg.serial_baud,
            on_message=self._on_lora_message,
            node_id=cfg.mesh_node_id,
            collision_avoidance_ms=cfg.collision_avoidance_ms,
            send_delay_ms=cfg.send_delay_ms,
            send_jitter_ms=cfg.send_jitter_ms,
            send_retries=cfg.send_retries,
            encryption_key=cfg.encryption_key,
            on_mesh_packet=self._on_mesh_packet_debug,
            mesh_ttl=cfg.mesh_ttl,
            safe_json=cfg.lora_safe_json) if cfg.serial_enabled else None

        self._stats = {k: 0 for k in (
            "local_rx", "upstream_rx", "lora_rx",
            "local_tx", "upstream_tx", "lora_tx", "mesh_fwd")}

    async def run(self):
        lora_enabled = self.cfg.serial_enabled
        node_id = self.lora.node_id if lora_enabled else "N/A"
        if not self.cfg.upstream_enabled and not lora_enabled:
            mode = "local IRC server only"
        elif not self.cfg.upstream_enabled:
            mode = "local IRC server + LoRa"
        elif not lora_enabled:
            mode = "IRC↔IRC (no LoRa)"
        else:
            mode = "three-way IRC↔LoRa↔IRC"
        log.info(f"[BRIDGE] Starting {mode} bridge  "
                 f"(mesh node={node_id}, sasl={bool(self.cfg.irc_sasl_username)})")
        await self.local_srv.start()
        if lora_enabled:
            await self.lora.start()
        irc_task   = asyncio.create_task(self.irc.run(),     name="upstream-irc")
        stats_task = asyncio.create_task(self._stats_loop(), name="stats")
        route_task = asyncio.create_task(self._route_loop(), name="route-maintenance") if lora_enabled else None
        loop = asyncio.get_running_loop(); done = asyncio.Event()

        def _sig(sig, _=None):
            log.info(f"[BRIDGE] {sig.name}"); loop.call_soon_threadsafe(done.set)

        for sig in (signal.SIGINT, signal.SIGTERM):
            try: loop.add_signal_handler(sig, _sig, sig)
            except: signal.signal(sig, lambda s, f: _sig(signal.Signals(s)))

        await done.wait()
        log.info("[BRIDGE] Stopping …")
        for t in (irc_task, stats_task):
            t.cancel()
        if route_task:
            route_task.cancel()
        await self.irc.stop()
        if lora_enabled:
            await self.lora.stop()
        await self.local_srv.stop()
        log.info("[BRIDGE] Done")


    async def _on_local_message(self, nick: str, channel: str, text: str):
        self._stats["local_rx"] += 1
        clean_ch = "#" + channel.lstrip("#")
        payload  = f"{clean_ch}|{nick}: {text}"
        if self.irc.connected and clean_ch.lower() in self.irc.channels:
            if await self.irc.send_message(payload, clean_ch): self._stats["upstream_tx"] += 1
        if self.lora and await self.lora.mesh_broadcast(payload): self._stats["lora_tx"] += 1

    async def _on_local_join(self, nick: str, channel: str):
        if not self.cfg.upstream_enabled or channel.lower() in self.irc.channels: return
        try:
            if await self.irc.join_channel(channel): log.info(f"[BRIDGE] upstream JOIN {channel}")
            else: log.warning(f"[BRIDGE] failed JOIN {channel}")
        except Exception: log.exception("Error requesting upstream JOIN")

    async def _on_local_part(self, nick: str, channel: str):
        if not self.cfg.upstream_enabled or channel.lower() not in self.irc.channels: return
        try:
            if await self.irc.part_channel(channel): log.info(f"[BRIDGE] upstream PART {channel}")
            else: log.warning(f"[BRIDGE] failed PART {channel}")
        except Exception: log.exception("Error requesting upstream PART")


    async def _on_upstream_message(self, nick: str, channel: Optional[str], text: str):
        self._stats["upstream_rx"] += 1
        if channel is None:
            await self.local_srv.broadcast_message(nick, self.irc.nick, text)
            self._stats["local_tx"] += 1; return
        await self.local_srv.broadcast_message(nick, channel, text)
        self._stats["local_tx"] += 1
        if _is_upstream_noise(text): return
        clean_ch = "#" + channel.lstrip("#")
        if self.lora and await self.lora.mesh_broadcast(f"{clean_ch}|{nick}: {text}"):
            self._stats["lora_tx"] += 1

    async def _on_upstream_connected(self):
        log.info("[BRIDGE] Upstream IRC connected"
                 + (" (SASL authenticated)" if self.cfg.irc_sasl_username else ""))

    async def _on_upstream_disconnected(self):
        log.info("[BRIDGE] Upstream IRC disconnected")

    async def _on_upstream_channel_joined(self, channel: str):
        pending = self._pending_upstream_msgs.pop(channel.lower(), [])
        if not pending: return
        log.info(f"[BRIDGE] Flushing {len(pending)} queued LoRa message(s) → upstream {channel}")
        for msg in pending:
            try:
                if await self.irc.send_message(msg, channel): self._stats["upstream_tx"] += 1
            except Exception: log.exception("Error flushing queued LoRa message")


    async def _on_lora_message(self, text: str):
        self._stats["lora_rx"] += 1
        pipe = text.find("|")
        if pipe != -1 and text.startswith("#"):
            display_ch = _normalize_channel(text[:pipe])
            body       = text[pipe+1:]
            target_ch  = display_ch.lower() if display_ch else None
        else:
            display_ch = target_ch = None; body = text

        if target_ch and self.cfg.upstream_enabled and target_ch not in self.irc.channels:
            try:
                if await self.irc.join_channel(display_ch):
                    log.info(f"[BRIDGE] requested upstream JOIN {display_ch} (LoRa)")
            except Exception: log.exception("Error requesting upstream JOIN (LoRa)")

        async with self.local_srv._lock:
            active = list(self.local_srv._channels.keys())

        if active:
            delivered = False
            for lch in active:
                if target_ch is None or lch == target_ch:
                    dch = self.local_srv._ch_disp.get(lch, lch)
                    await self.local_srv.broadcast_message("LoRa", dch, body)
                    delivered = True
            if not delivered:
                log.warning(f"[BRIDGE] LoRa target {target_ch!r} not active; broadcasting all")
                for lch in active:
                    await self.local_srv.broadcast_message(
                        "LoRa", self.local_srv._ch_disp.get(lch, lch), body)
            self._stats["local_tx"] += 1
        elif target_ch and self.cfg.upstream_enabled:
            if self.irc.connected and target_ch in self.irc.channels:
                if await self.irc.send_message(body, display_ch): self._stats["upstream_tx"] += 1
            else:
                self._pending_upstream_msgs.setdefault(target_ch, []).append(body)
                log.info(f"[BRIDGE] Queued LoRa message for upstream {display_ch}")
        else:
            log.warning("[BRIDGE] LoRa→local: no active local clients – dropped")

    async def _on_mesh_packet_debug(self, pkt: dict):
        if pkt.get("src") != self.lora.node_id:
            self._stats["mesh_fwd"] += 1


    async def _stats_loop(self):
        while True:
            await asyncio.sleep(300); s = self._stats
            log.info(
                f"[BRIDGE] Stats local-rx={s['local_rx']} up-rx={s['upstream_rx']} "
                f"lora-rx={s['lora_rx']} local-tx={s['local_tx']} up-tx={s['upstream_tx']} "
                f"lora-tx={s['lora_tx']} mesh-fwd={s['mesh_fwd']}"
            )
            if self.lora:
                log.info(self.lora.routing_table_summary())

    async def _route_loop(self):
        while True:
            await asyncio.sleep(60)
            if self.lora:
                self.lora._routes.purge_expired()