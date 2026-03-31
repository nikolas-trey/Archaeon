import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("archaeon")

IRC_MAX_MSG         = 400
IRC_RECONNECT_DELAY = 10
IRC_RECONNECT_MAX   = 300
IRC_PING_INTERVAL   = 90
IRC_PING_TIMEOUT    = 30

LORA_SAFE_JSON      = 199
LORA_MAX_CHUNKS     = 32
COMPRESS_THRESHOLD  = 60
XCHACHA_NONCE_LEN   = 24
XCHACHA_KEY_LEN     = 32

REASSEMBLY_TTL           = 30
REASSEMBLY_NACK_INTERVAL = 5.0
REASSEMBLY_NACK_MAX      = 15

ECHO_TTL    = 10.0
ECHO_MAX    = 64
TX_DONE_TTL = 60.0
RX_DONE_TTL = 60.0

SERVER_VERSION = "archaeon-1.0"
SERVER_CREATED = time.strftime("%Y-%m-%d")

MESH_DEFAULT_TTL = 7
MESH_SEEN_TTL    = 60.0
MESH_ROUTE_TTL   = 300.0
MESH_BCAST_DST   = "*"
MESH_NODE_ID_LEN = 4
