#!/usr/bin/env python3

import asyncio
import logging

from archaeon.config import build_config, _parse_args
from archaeon.bridge import IRCLoRaBridge
from archaeon.constants import log


def main():
    args = _parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    cfg  = build_config(args)

    up_label   = ("DISABLED" if not cfg.upstream_enabled
                  else f"{cfg.irc_server}:{cfg.irc_port} tls={cfg.irc_tls}")
    tor_label  = f" via Tor {cfg.irc_tor_host}:{cfg.irc_tor_port}" if cfg.irc_tor else ""
    node_label = cfg.mesh_node_id or "(auto)"
    lora_label = (f"{cfg.serial_device} @ {cfg.serial_baud} baud"
                  if cfg.serial_enabled else "DISABLED")

    log.info("═" * 65)
    log.info("  Archaeon Server")
    log.info(f"  Local IRC  : {cfg.local_host}:{cfg.local_port}"
             + (" pw=yes" if cfg.local_password else ""))
    log.info(f"  Upstream   : {up_label}{tor_label}")
    log.info(f"  LoRa       : {lora_label}")
    log.info(f"  Encryption : {'XChaCha20-Poly1305' if cfg.encryption_key else 'none'}")
    log.info(f"  Mesh TTL   : {cfg.mesh_ttl}  Node ID: {node_label}")
    log.info(f"  Frame limit: {cfg.lora_safe_json}B "
             f"(hardware MAX_SEND_LEN={cfg.lora_safe_json+1}B)")
    log.info("═" * 65)

    try:
        asyncio.run(IRCLoRaBridge(cfg).run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()