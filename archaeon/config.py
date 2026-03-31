import logging
from dataclasses import dataclass
from typing import Optional

from .constants import LORA_SAFE_JSON, MESH_DEFAULT_TTL, MESH_NODE_ID_LEN, XCHACHA_KEY_LEN


@dataclass
class BridgeConfig:
    local_host:        str           = "0.0.0.0"
    local_port:        int           = 6667
    local_server_name: str           = "archaeon"
    local_motd:        str           = "Archaeon – messages are bridged between IRC-LoRa."
    local_password:    Optional[str] = None
    upstream_enabled:     bool       = True
    upstream_max_retries: int        = 0
    irc_server:        str           = "irc.libera.chat"
    irc_port:          int           = 6697
    irc_nick:          str           = "LoRa"
    irc_password:      Optional[str] = None
    irc_tls:           bool          = True
    irc_tor:           bool          = False
    irc_tor_host:      str           = "127.0.0.1"
    irc_tor_port:      int           = 9050
    irc_sasl_username: Optional[str] = None
    irc_sasl_password: Optional[str] = None
    serial_enabled:         bool = True
    serial_device:          str = "/dev/ttyACM0"
    serial_baud:            int = 115200
    collision_avoidance_ms: int = 1000
    send_delay_ms:          int = 3000
    send_jitter_ms:         int = 500
    send_retries:           int = 2
    encryption_key: Optional[bytes] = None
    mesh_node_id:   Optional[str]   = None
    mesh_ttl:       int             = MESH_DEFAULT_TTL
    lora_safe_json: int             = LORA_SAFE_JSON


def _parse_key(raw: str) -> bytes:
    if raw.startswith("@"):
        key = open(raw[1:], "rb").read()
        if len(key) != XCHACHA_KEY_LEN:
            raise ValueError(f"Key file must be {XCHACHA_KEY_LEN} bytes")
        return key
    raw = raw.strip()
    if len(raw) != XCHACHA_KEY_LEN * 2:
        raise ValueError(f"Hex key must be {XCHACHA_KEY_LEN*2} chars")
    return bytes.fromhex(raw)


def _load_toml(path: str) -> dict:
    try: import tomllib
    except ImportError:
        try: import tomli as tomllib
        except ImportError: raise RuntimeError("pip install tomli  (or Python 3.11+)")
    with open(path, "rb") as f: return tomllib.load(f)


def build_config(args) -> BridgeConfig:
    cfg = BridgeConfig()
    if args.config:
        d    = _load_toml(args.config)
        loc  = d.get("local",      {})
        up   = d.get("upstream",   {})
        irc  = d.get("irc",        {})
        ser  = d.get("serial",     {})
        tune = d.get("tuning",     {})
        enc  = d.get("encryption", {})
        mesh = d.get("mesh",       {})
        cfg.local_host        = loc.get("host",        cfg.local_host)
        cfg.local_port        = loc.get("port",        cfg.local_port)
        cfg.local_server_name = loc.get("server_name", cfg.local_server_name)
        cfg.local_motd        = loc.get("motd",        cfg.local_motd)
        cfg.local_password    = loc.get("password",    cfg.local_password)
        cfg.upstream_enabled     = up.get("enabled",     cfg.upstream_enabled)
        cfg.upstream_max_retries = up.get("max_retries", cfg.upstream_max_retries)
        cfg.irc_server        = irc.get("server",        cfg.irc_server)
        cfg.irc_port          = irc.get("port",          cfg.irc_port)
        cfg.irc_nick          = irc.get("nick",          cfg.irc_nick)
        cfg.irc_password      = irc.get("password",      cfg.irc_password)
        cfg.irc_tls           = irc.get("tls",           cfg.irc_tls)
        cfg.irc_tor           = irc.get("tor",           cfg.irc_tor)
        cfg.irc_tor_host      = irc.get("tor_host",      cfg.irc_tor_host)
        cfg.irc_tor_port      = irc.get("tor_port",      cfg.irc_tor_port)
        cfg.irc_sasl_username = irc.get("sasl_username", cfg.irc_sasl_username)
        cfg.irc_sasl_password = irc.get("sasl_password", cfg.irc_sasl_password)
        cfg.serial_device = ser.get("device", cfg.serial_device)
        cfg.serial_baud   = ser.get("baud",   cfg.serial_baud)
        cfg.serial_enabled = ser.get("enabled", cfg.serial_enabled)
        cfg.collision_avoidance_ms = tune.get("collision_avoidance_ms", cfg.collision_avoidance_ms)
        cfg.send_delay_ms          = tune.get("send_delay_ms",          cfg.send_delay_ms)
        cfg.send_jitter_ms         = tune.get("send_jitter_ms",         cfg.send_jitter_ms)
        cfg.send_retries           = tune.get("send_retries",           cfg.send_retries)
        if enc.get("key"): cfg.encryption_key = _parse_key(enc["key"])
        cfg.mesh_node_id   = mesh.get("node_id",  cfg.mesh_node_id)
        cfg.mesh_ttl       = mesh.get("ttl",       cfg.mesh_ttl)
        cfg.lora_safe_json = mesh.get("safe_json", cfg.lora_safe_json)

    if args.no_upstream:          cfg.upstream_enabled     = False
    if args.upstream_max_retries: cfg.upstream_max_retries = args.upstream_max_retries
    if args.local_host         != "0.0.0.0":         cfg.local_host        = args.local_host
    if args.local_port         != 6667:               cfg.local_port        = args.local_port
    if args.local_server_name  != "archaeon":         cfg.local_server_name = args.local_server_name
    if args.local_password:                           cfg.local_password    = args.local_password
    if args.irc_server         != "irc.libera.chat":  cfg.irc_server        = args.irc_server
    if args.irc_port           != 6697:               cfg.irc_port          = args.irc_port
    if args.irc_nick           != "LoRa":             cfg.irc_nick          = args.irc_nick
    if args.irc_password:                             cfg.irc_password      = args.irc_password
    if args.irc_tls is False:             cfg.irc_tls      = False
    if args.irc_tor:                      cfg.irc_tor      = True
    if args.irc_tor_host != "127.0.0.1":  cfg.irc_tor_host = args.irc_tor_host
    if args.irc_tor_port != 9050:         cfg.irc_tor_port = args.irc_tor_port
    if args.irc_sasl_username:            cfg.irc_sasl_username = args.irc_sasl_username
    if args.irc_sasl_password:            cfg.irc_sasl_password = args.irc_sasl_password
    if args.no_serial:               cfg.serial_enabled    = False
    if args.serial_device != "/dev/ttyACM0": cfg.serial_device = args.serial_device
    if args.serial_baud   != 115200:         cfg.serial_baud   = args.serial_baud
    cfg.collision_avoidance_ms = args.collision_avoidance_ms
    cfg.send_delay_ms          = args.send_delay_ms
    cfg.send_jitter_ms         = args.send_jitter_ms
    cfg.send_retries           = args.send_retries
    if args.encryption_key: cfg.encryption_key = _parse_key(args.encryption_key)
    if args.mesh_node_id:                     cfg.mesh_node_id   = args.mesh_node_id
    if args.mesh_ttl != MESH_DEFAULT_TTL:     cfg.mesh_ttl       = args.mesh_ttl
    if args.lora_safe_json != LORA_SAFE_JSON: cfg.lora_safe_json = args.lora_safe_json
    return cfg


def _parse_args():
    import argparse
    p = argparse.ArgumentParser(description="A bridge that connects IRC to LoRa mesh network for off-grid, low-bandwidth chat.")
    g = p.add_argument_group("Embedded IRC server")
    g.add_argument("--local-host", default="0.0.0.0")
    g.add_argument("--local-port", type=int, default=6667)
    g.add_argument("--local-server-name", default="archaeon")
    g.add_argument("--local-password", default=None)
    g = p.add_argument_group("Upstream IRC")
    g.add_argument("--no-upstream", action="store_true")
    g.add_argument("--upstream-max-retries", type=int, default=0)
    g.add_argument("--irc-server", default="irc.libera.chat")
    g.add_argument("--irc-port", type=int, default=6697)
    g.add_argument("--irc-nick", default="LoRa")
    g.add_argument("--irc-password", default=None)
    g.add_argument("--irc-tls", action="store_true", default=True)
    g.add_argument("--no-irc-tls", action="store_false", dest="irc_tls")
    g.add_argument("--tor", action="store_true", default=False, dest="irc_tor",
                   help="Route upstream IRC through Tor SOCKS5 proxy")
    g.add_argument("--tor-host", default="127.0.0.1", dest="irc_tor_host")
    g.add_argument("--tor-port", type=int, default=9050, dest="irc_tor_port")
    g.add_argument("--irc-sasl-username", default=None, metavar="ACCOUNT",
                   help="SASL PLAIN account name for upstream IRC authentication")
    g.add_argument("--irc-sasl-password", default=None, metavar="PASSWORD",
                   help="SASL PLAIN password for upstream IRC authentication")
    g = p.add_argument_group("LoRa serial")
    g.add_argument("--no-serial", action="store_true",
                   help="Disable LoRa serial entirely (IRC-only mode)")
    g.add_argument("--serial-device", default="/dev/ttyACM0")
    g.add_argument("--serial-baud", type=int, default=115200)
    g = p.add_argument_group("Tuning")
    g.add_argument("--collision-avoidance-ms", type=int, default=100)
    g.add_argument("--send-delay-ms", type=int, default=3000)
    g.add_argument("--send-jitter-ms", type=int, default=30)
    g.add_argument("--send-retries", type=int, default=2)
    g = p.add_argument_group("Encryption")
    g.add_argument("--encryption-key", default=None, metavar="HEX_OR_FILE")
    g = p.add_argument_group("Mesh")
    g.add_argument("--mesh-node-id", default=None, metavar="8HEX",
                   help=f"Fixed {MESH_NODE_ID_LEN*2}-char hex node ID (auto-generated if omitted)")
    g.add_argument("--mesh-ttl", type=int, default=MESH_DEFAULT_TTL,
                   help=f"Max hop count for mesh packets (default {MESH_DEFAULT_TTL})")
    g.add_argument("--lora-safe-json", type=int, default=LORA_SAFE_JSON,
                   help=f"Max frame bytes, must be hardware MAX_SEND_LEN-1 (default {LORA_SAFE_JSON})")
    p.add_argument("--config")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()