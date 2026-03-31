import asyncio
import atexit
import hashlib
import logging
import os
import sqlite3
import uuid
from typing import Dict, List, Optional, Set

from .constants import SERVER_VERSION, SERVER_CREATED, log


_NONCE_LEN = 5


def _make_nonce() -> str:
    import uuid
    return uuid.uuid4().hex[:_NONCE_LEN]


def _read_nonce(text: str) -> str | None:
    tail_len = 2 + _NONCE_LEN + 1
    if (len(text) > tail_len
            and text[-1] == "]"
            and text[-(tail_len):-(tail_len - 2)] == " ["
            and text[-(1 + _NONCE_LEN):-1].isalnum()):
        return text[-(1 + _NONCE_LEN):-1]
    return None


def _irc_parse(line: str):
    prefix = ""
    if line.startswith(":"):
        sp = line.find(" ")
        if sp == -1: return "", line[1:], []
        prefix = line[1:sp]; line = line[sp+1:]
    params: List[str] = []
    while line:
        if line.startswith(":"):
            params.append(line[1:]); break
        sp = line.find(" ")
        if sp == -1: params.append(line); break
        params.append(line[:sp]); line = line[sp+1:].lstrip(" ")
    cmd = params.pop(0).upper() if params else ""
    return prefix, cmd, params


def _nick_from_prefix(prefix: str) -> str:
    bang = prefix.find("!")
    return prefix[:bang] if bang != -1 else prefix


def _valid_nick(nick: str) -> bool:
    if not nick or len(nick) > 30: return False
    LEAD = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz[]\\`_^{|}")
    BODY = LEAD | set("0123456789-")
    return nick[0] in LEAD and all(c in BODY for c in nick[1:])


class IRCClientConn:
    def __init__(self, reader, writer, server):
        self.reader, self.writer, self.server = reader, writer, server
        self.nick = self.user = None
        self.realname = ""
        self.channels: Dict[str, str] = {}
        self.registered = self._stop = False
        self._send_lock = asyncio.Lock()
        addr = writer.get_extra_info("peername")
        self.addr = f"{addr[0]}:{addr[1]}" if addr else "?"

    @property
    def prefix(self): return f"{self.nick}!{self.user or 'user'}@bridge"

    async def send(self, line: str):
        async with self._send_lock:
            try: self.writer.write((line + "\r\n").encode()); await self.writer.drain()
            except Exception: self._stop = True

    async def num(self, code: int, *args: str, trailing: str = ""):
        parts = [self.server.name, f"{code:03d}", self.nick or "*"] + list(args)
        await self.send(" ".join(parts) + (f" :{trailing}" if trailing else ""))

    async def run(self):
        log.info(f"[SRV] Client connected: {self.addr}")
        try:
            while not self._stop:
                try: raw = await asyncio.wait_for(self.reader.readline(), timeout=120.0)
                except asyncio.TimeoutError: await self.send(f"PING :{self.server.name}"); continue
                except Exception: break
                if not raw: break
                line = raw.decode(errors="replace").rstrip("\r\n")
                if line: await self.server.handle(self, line)
        finally:
            await self.server.remove_client(self)
            try: self.writer.close()
            except Exception: pass
            log.info(f"[SRV] Client disconnected: {self.addr}")


class EmbeddedIRCServer:
    def __init__(self, host="0.0.0.0", port=6667, server_name="archaeon",
                 motd="Archaeon", on_message=None, password=None,
                 on_join=None, on_part=None, bridge_nick=None,
                 echo_store_path: Optional[str] = None):
        self.host, self.port, self.name = host, port, server_name
        self.motd, self.on_message = motd, on_message
        self.password, self.on_join, self.on_part = password, on_join, on_part
        self.bridge_nick = bridge_nick.lower() if bridge_nick else None
        self._clients:   Dict[str, IRCClientConn] = {}
        self._nick_disp: Dict[str, str]           = {}
        self._channels:  Dict[str, Set[str]]      = {}
        self._ch_disp:   Dict[str, str]           = {}
        self._lock = asyncio.Lock()
        self._echo_lock = asyncio.Lock()
        self._bridged_fingerprints: Set[str] = set()
        self.echo_store_path = echo_store_path or os.path.join(os.getcwd(), f"embedded_irc_echoes_{self.name}.sqlite3")
        self._init_echo_store()
        self._load_echo_store()
        atexit.register(self._close_echo_store)

    def _norm_channel(self, channel: str) -> str:
        return channel.lower()

    def _norm_text(self, text: str) -> str:
        return text.strip()

    def _message_fingerprint(self, nonce: str) -> str:
        return nonce

    def _init_echo_store(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.echo_store_path) or ".", exist_ok=True)
            self._echo_db = sqlite3.connect(self.echo_store_path)
            self._echo_db.execute("""
                CREATE TABLE IF NOT EXISTS server_echo_fingerprints (
                    fingerprint TEXT PRIMARY KEY
                )
            """)
            self._echo_db.commit()
        except Exception:
            log.warning(f"[SRV] Failed to initialize echo store {self.echo_store_path}", exc_info=True)
            self._echo_db = None

    def _load_echo_store(self) -> None:
        if not getattr(self, "_echo_db", None):
            return
        try:
            rows = self._echo_db.execute(
                "SELECT fingerprint FROM server_echo_fingerprints"
            ).fetchall()
            self._bridged_fingerprints = {row[0] for row in rows if row and row[0]}
        except Exception:
            log.warning(f"[SRV] Failed to load echo store {self.echo_store_path}", exc_info=True)

    def _store_fingerprint(self, fingerprint: str) -> None:
        if not getattr(self, "_echo_db", None):
            return
        try:
            self._echo_db.execute(
                "INSERT OR IGNORE INTO server_echo_fingerprints (fingerprint) VALUES (?)",
                (fingerprint,),
            )
            self._echo_db.commit()
        except Exception:
            log.warning(f"[SRV] Failed to persist echo fingerprint to {self.echo_store_path}", exc_info=True)

    def _close_echo_store(self) -> None:
        db = getattr(self, "_echo_db", None)
        if not db:
            return
        try:
            db.commit()
            db.close()
        except Exception:
            log.warning(f"[SRV] Failed to close echo store {self.echo_store_path}", exc_info=True)

    async def _remember_bridged(self, nonce: str) -> None:
        async with self._echo_lock:
            fingerprint = self._message_fingerprint(nonce)
            self._bridged_fingerprints.add(fingerprint)
            self._store_fingerprint(fingerprint)

    async def _is_known_bridged(self, nonce: str) -> bool:
        async with self._echo_lock:
            return self._message_fingerprint(nonce) in self._bridged_fingerprints

    async def start(self):
        self._srv = await asyncio.start_server(self._accept, self.host, self.port)
        log.info(f"[SRV] Listening on {self.host}:{self.port}")

    async def stop(self):
        self._srv.close(); await self._srv.wait_closed()
        for c in list(self._clients.values()):
            try: await c.send(f":{self.name} ERROR :Server shutting down"); c.writer.close()
            except Exception: pass

    async def _accept(self, reader, writer):
        asyncio.create_task(IRCClientConn(reader, writer, self).run())

    async def remove_client(self, conn: IRCClientConn):
        ln = conn.nick.lower() if conn.nick else None
        async with self._lock:
            if ln and self._clients.get(ln) is conn:
                del self._clients[ln]; self._nick_disp.pop(ln, None)
            for lch in list(conn.channels):
                self._channels.get(lch, set()).discard(ln)
                if lch in self._channels and not self._channels[lch]:
                    del self._channels[lch]; self._ch_disp.pop(lch, None)
        for lch in list(conn.channels):
            await self._bcast(lch, f":{conn.prefix} QUIT :Connection closed", exclude=ln)
        conn.channels.clear()

    async def _bcast(self, lch: str, line: str, exclude: Optional[str] = None):
        for ln in list(self._channels.get(lch, set())):
            if ln == exclude: continue
            c = self._clients.get(ln)
            if c: await c.send(line)

    async def broadcast_message(self, src: str, channel: str, text: str):
        lch = channel.lower(); dch = self._ch_disp.get(lch, channel)
        nonce = _read_nonce(text)
        if not nonce:
            nonce = _make_nonce()
            text = f"{text} [{nonce}]"
        await self._remember_bridged(nonce)
        await self._bcast(lch, f":{src}!bridge@lora PRIVMSG {dch} :{text}")

    async def handle(self, conn: IRCClientConn, line: str):
        prefix, cmd, params = _irc_parse(line)
        if cmd == "CAP":
            if params and params[0].upper() == "LS": await conn.send(f":{self.name} CAP * LS :")
            return
        if cmd == "PASS":
            if self.password and (params[0] if params else "") != self.password:
                await conn.num(464, trailing="Password incorrect"); conn._stop = True
            return
        if cmd == "NICK":  await self._do_nick(conn, params[0] if params else ""); return
        if cmd == "USER":
            conn.user = params[0] if params else "user"
            conn.realname = params[3] if len(params) > 3 else ""
            await self._try_register(conn); return
        if cmd == "PING":
            await conn.send(f":{self.name} PONG {self.name} :{params[0] if params else self.name}"); return
        if cmd == "PONG": return
        if not conn.registered:
            await conn.num(451, trailing="You have not registered"); return
        if   cmd == "JOIN":    await self._do_join(conn, params)
        elif cmd == "PART":    await self._do_part(conn, params)
        elif cmd == "PRIVMSG": await self._do_privmsg(conn, params)
        elif cmd == "NOTICE":  pass
        elif cmd == "QUIT":
            await conn.send(f":{conn.prefix} QUIT :{params[0] if params else 'Quit'}"); conn._stop = True
        elif cmd == "WHO":     await self._do_who(conn, params)
        elif cmd == "WHOIS":   await self._do_whois(conn, params)
        elif cmd == "NAMES":
            if params:
                lch = params[0].lower(); dch = self._ch_disp.get(lch, params[0])
                await self._send_names(conn, dch); await conn.num(366, dch, trailing="End of /NAMES list")
        elif cmd == "LIST":    await self._do_list(conn)
        elif cmd == "MOTD":    await self._send_motd(conn)
        elif cmd == "MODE":
            tgt = params[0] if params else conn.nick
            if tgt and tgt.startswith("#"): await conn.send(f":{self.name} 324 {conn.nick} {tgt} +")
            else: await conn.send(f":{self.name} 221 {conn.nick} +i")
        elif cmd == "TOPIC":
            await conn.num(331, params[0] if params else "", trailing="No topic set")
        elif cmd == "AWAY":
            await conn.num(305, trailing="You are no longer marked as being away")
        elif cmd == "ISON":
            online = [self._nick_disp[n.lower()] for n in params if n.lower() in self._clients]
            await conn.num(303, trailing=" ".join(online))
        else:
            await conn.num(421, cmd, trailing="Unknown command")

    async def _do_nick(self, conn: IRCClientConn, new_nick: str):
        if not _valid_nick(new_nick):
            await conn.num(432, new_nick or "*", trailing="Erroneous nickname"); return
        new_lower = new_nick.lower()
        async with self._lock:
            old_nick = conn.nick; old_lower = old_nick.lower() if old_nick else None
            if new_lower in self._clients and self._clients[new_lower] is not conn:
                await conn.num(433, new_nick, trailing="Nickname already in use"); return
            if old_lower and old_lower in self._clients:
                del self._clients[old_lower]; del self._nick_disp[old_lower]
            conn.nick = new_nick
            self._clients[new_lower] = conn; self._nick_disp[new_lower] = new_nick
            for lch in conn.channels:
                s = self._channels.get(lch)
                if s: s.discard(old_lower or ""); s.add(new_lower)
        if old_nick:
            ln = f":{old_nick}!{conn.user or 'user'}@bridge NICK :{new_nick}"
            await self._bcast_visible(conn, ln); await conn.send(ln)
        await self._try_register(conn)

    async def _try_register(self, conn: IRCClientConn):
        if conn.registered or not conn.nick or not conn.user: return
        conn.registered = True; n = conn.nick
        await conn.send(f":{self.name} 001 {n} :Welcome to Archaeon, {conn.prefix}")
        await conn.send(f":{self.name} 002 {n} :Your host is {self.name}, running {SERVER_VERSION}")
        await conn.send(f":{self.name} 003 {n} :This server was created {SERVER_CREATED}")
        await conn.send(f":{self.name} 004 {n} {self.name} {SERVER_VERSION} io beiklmnoOpqstv")
        await conn.send(f":{self.name} 005 {n} CHANTYPES=# CASEMAPPING=ascii :are supported")
        await self._send_motd(conn)

    async def _send_motd(self, conn: IRCClientConn):
        await conn.num(375, trailing=f"- {self.name} Message of the day -")
        for line in self.motd.splitlines(): await conn.num(372, trailing=f"- {line}")
        await conn.num(376, trailing="End of /MOTD command")

    async def _do_join(self, conn: IRCClientConn, params: List[str]):
        for ch in (params[0].split(",") if params else []):
            ch = ch.strip()
            if not ch: continue
            if not ch.startswith("#"): ch = "#" + ch
            lch = ch.lower(); ln = conn.nick.lower()
            async with self._lock:
                first = lch not in self._channels or not self._channels.get(lch)
                self._channels.setdefault(lch, set()).add(ln)
                self._ch_disp[lch] = ch; conn.channels[lch] = ch
            await self._bcast(lch, f":{conn.prefix} JOIN :{ch}")
            await self._send_names(conn, ch); await conn.num(366, ch, trailing="End of /NAMES list")
            if first and self.on_join:
                try: asyncio.create_task(self.on_join(conn.nick, ch))
                except Exception: log.exception("on_join failed")

    async def _do_part(self, conn: IRCClientConn, params: List[str]):
        if not params: return
        ch = params[0]; reason = params[1] if len(params) > 1 else "Leaving"
        lch = ch.lower(); ln = conn.nick.lower(); dch = self._ch_disp.get(lch, ch)
        async with self._lock:
            self._channels.get(lch, set()).discard(ln); conn.channels.pop(lch, None)
            removed = lch in self._channels and not self._channels[lch]
            if removed: del self._channels[lch]; self._ch_disp.pop(lch, None)
        await self._bcast(lch, f":{conn.prefix} PART {dch} :{reason}")
        if removed and self.on_part:
            try: asyncio.create_task(self.on_part(conn.nick, dch))
            except Exception: log.exception("on_part failed")

    async def _do_privmsg(self, conn: IRCClientConn, params: List[str]):
        if len(params) < 2: return
        target, wire_text = params[0], params[1]; ln = conn.nick.lower()
        nonce = _read_nonce(wire_text)
        if target.startswith("#"):
            lch = target.lower(); dch = self._ch_disp.get(lch, target)
            if lch not in conn.channels:
                await conn.num(404, target, trailing="Cannot send to channel"); return
            await self._bcast(lch, f":{conn.prefix} PRIVMSG {dch} :{wire_text}", exclude=ln)
            if self.on_message and ln != self.bridge_nick:
                if nonce and await self._is_known_bridged(nonce):
                    log.debug(f"[SRV] Ignored bridged echo on {dch}: {wire_text!r}")
                    return
                asyncio.create_task(self.on_message(conn.nick, dch, wire_text))
        else:
            lt = target.lower(); dest = self._clients.get(lt)
            if dest: await dest.send(f":{conn.prefix} PRIVMSG {self._nick_disp.get(lt, target)} :{wire_text}")
            else: await conn.num(401, target, trailing="No such nick/channel")

    async def _do_who(self, conn: IRCClientConn, params: List[str]):
        mask = params[0] if params else "*"; lmask = mask.lower()
        nicks = list(self._channels.get(lmask, set())) if lmask.startswith("#") else list(self._clients)
        for ln in nicks:
            c = self._clients.get(ln)
            if c:
                dn = self._nick_disp.get(ln, ln)
                await conn.send(f":{self.name} 352 {conn.nick} {mask} {c.user or 'user'} bridge {self.name} {dn} H :0 {c.realname or dn}")
        await conn.num(315, mask, trailing="End of WHO list")

    async def _do_whois(self, conn: IRCClientConn, params: List[str]):
        target = params[-1] if params else ""; lt = target.lower(); c = self._clients.get(lt)
        if not c: await conn.num(401, target, trailing="No such nick"); return
        dn = self._nick_disp.get(lt, target)
        await conn.num(311, dn, c.user or "user", "bridge", "*", trailing=c.realname)
        chs = " ".join(c.channels.values())
        if chs: await conn.num(319, dn, trailing=chs)
        await conn.num(318, dn, trailing="End of /WHOIS list")

    async def _do_list(self, conn: IRCClientConn):
        await conn.num(321, "Channel", trailing="Users  Name")
        async with self._lock:
            for lch, members in self._channels.items():
                await conn.send(f":{self.name} 322 {conn.nick} {self._ch_disp.get(lch, lch)} {len(members)} :")
        await conn.num(323, trailing="End of /LIST")

    async def _send_names(self, conn: IRCClientConn, channel: str):
        lch = channel.lower(); dch = self._ch_disp.get(lch, channel)
        members = [self._nick_disp.get(n, n) for n in self._channels.get(lch, set())]
        batch: List[str] = []
        for nick in members:
            batch.append(nick)
            if sum(len(n)+1 for n in batch) > 400:
                await conn.send(f":{self.name} 353 {conn.nick} = {dch} :{' '.join(batch[:-1])}"); batch = [nick]
        if batch: await conn.send(f":{self.name} 353 {conn.nick} = {dch} :{' '.join(batch)}")

    async def _bcast_visible(self, conn: IRCClientConn, line: str):
        seen: Set[str] = set(); ln = conn.nick.lower() if conn.nick else ""
        for lch in conn.channels:
            for nick in self._channels.get(lch, set()):
                if nick != ln and nick not in seen:
                    seen.add(nick); c = self._clients.get(nick)
                    if c: await c.send(line)