from __future__ import annotations

import asyncio
import base64
import json
import logging
import math
import os
import random
import re
import time
import zlib
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Tuple

from .constants import (
    COMPRESS_THRESHOLD, ECHO_MAX, ECHO_TTL, LORA_MAX_CHUNKS, LORA_SAFE_JSON,
    MESH_BCAST_DST, MESH_DEFAULT_TTL, MESH_NODE_ID_LEN, MESH_ROUTE_TTL,
    REASSEMBLY_NACK_INTERVAL, REASSEMBLY_NACK_MAX, REASSEMBLY_TTL,
    RX_DONE_TTL, TX_DONE_TTL, XCHACHA_KEY_LEN, XCHACHA_NONCE_LEN, log,
)
from .crypto import HAS_XCHACHA, XChaCha20Poly1305
from .mesh import MeshRoutingTable, SeenCache

HAS_SERIAL_ASYNCIO = HAS_PYSERIAL = False
try:
    import serial_asyncio
    HAS_SERIAL_ASYNCIO = True
except ImportError:
    try:
        import serial
        HAS_PYSERIAL = True
    except ImportError:
        pass

try:
    import lz4.frame as _lz4
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False


_TAG_ZLIB = b"Z"
_TAG_LZ4  = b"L"
_TAG_RAW  = b"R"

_SIGIL_B85_COMP = "B:"
_SIGIL_Z_LEGACY = "Z:"
_SIGIL_ENC      = "E:"

_COMPRESS_MIN = 32


def _compress_best(data: bytes) -> bytes:
    best_tag, best_body = _TAG_RAW, data
    try:
        z = zlib.compress(data, 9)
        if len(z) < len(best_body):
            best_tag, best_body = _TAG_ZLIB, z
    except zlib.error:
        pass
    if HAS_LZ4:
        try:
            lz = _lz4.compress(
                data,
                compression_level=_lz4.COMPRESSIONLEVEL_MAX,
                store_size=False,
            )
            if len(lz) < len(best_body):
                best_tag, best_body = _TAG_LZ4, lz
        except Exception:
            pass
    return best_tag + best_body


def _decompress_tagged(data: bytes) -> bytes:
    if not data:
        raise ValueError("empty payload")
    tag, body = data[:1], data[1:]
    if tag == _TAG_RAW:
        return body
    if tag == _TAG_ZLIB:
        return zlib.decompress(body)
    if tag == _TAG_LZ4:
        if not HAS_LZ4:
            raise RuntimeError("lz4 not installed")
        return _lz4.decompress(body)
    raise ValueError(f"unknown compression tag: {tag!r}")

_PRIO_BURST  = 0
_PRIO_NORMAL = 1
_PRIO_NACK   = 2

_NACK_BACKOFF_BASE = 0.5
_NACK_JITTER_MAX   = 0.8
_NACK_ECHO_TTL     = 10.0

_MAX_REASSEMBLY_SESSIONS = 64



_GOSSIP_SUPPRESS_K = 2


_LQ_ALPHA       = 0.25   
_LQ_RSSI_FLOOR  = -130.0
_LQ_RSSI_REF    = -50.0
_LQ_SNR_FLOOR   = -20.0
_LQ_SNR_REF     = 10.0


_CHAN_BUSY_THRESHOLD = 0.4
_CHAN_BUSY_EXTRA_S   = 0.5
_CHAN_WINDOW_S       = 5.0  


_NEIGHBOR_TTL_S = 120.0

@dataclass(order=True)
class _TxJob:
    priority:       int
    seq:            int
    frame:          str            = field(compare=False)
    future:         asyncio.Future = field(compare=False)
    is_burst_start: bool           = field(compare=False, default=False)
    is_burst_end:   bool           = field(compare=False, default=False)


@dataclass
class _NeighborEntry:
    node_id:    str
    last_heard: float = field(default_factory=time.time)
    rssi_ema:   float = -80.0
    snr_ema:    float = 5.0
    rx_count:   int   = 0

    def update(self, rssi: Optional[float], snr: Optional[float]) -> None:
        self.last_heard = time.time()
        self.rx_count  += 1
        if rssi is not None:
            self.rssi_ema = (1 - _LQ_ALPHA) * self.rssi_ema + _LQ_ALPHA * rssi
        if snr is not None:
            self.snr_ema  = (1 - _LQ_ALPHA) * self.snr_ema  + _LQ_ALPHA * snr

    @property
    def score(self) -> float:
        r = (self.rssi_ema - _LQ_RSSI_FLOOR) / (_LQ_RSSI_REF - _LQ_RSSI_FLOOR)
        s = (self.snr_ema  - _LQ_SNR_FLOOR)  / (_LQ_SNR_REF  - _LQ_SNR_FLOOR)
        return max(0.0, min(1.0, 0.6 * r + 0.4 * s))

    @property
    def is_alive(self) -> bool:
        return time.time() - self.last_heard < _NEIGHBOR_TTL_S


class _LRUDict(OrderedDict):
    def __init__(self, maxsize: int) -> None:
        super().__init__()
        self._maxsize = maxsize

    def __setitem__(self, key, value) -> None:
        super().__setitem__(key, value)
        self.move_to_end(key)
        while len(self) > self._maxsize:
            self.popitem(last=False)

    def touch(self, key) -> None:
        if key in self:
            self.move_to_end(key)


class _ChannelMonitor:
    def __init__(self, window_s: float = _CHAN_WINDOW_S) -> None:
        self._window = window_s
        self._events: List[float] = []   

    def record_rx(self) -> None:
        self._events.append(time.time())
        
        if len(self._events) > 200:
            cutoff = time.time() - self._window
            self._events = [t for t in self._events if t >= cutoff]

    def busy_fraction(self) -> float:
        now    = time.time()
        cutoff = now - self._window
        recent = [t for t in self._events if t >= cutoff]
        self._events = recent
        if not recent:
            return 0.0
        
        occupied = min(len(recent) * 0.1, self._window)
        return occupied / self._window

class _ForwardedCache:
    def __init__(self, maxsize: int = 512) -> None:
        self._cache: _LRUDict = _LRUDict(maxsize)

    def mark(self, pkt_id: str) -> None:
        self._cache[pkt_id] = time.time()

    def was_forwarded(self, pkt_id: str) -> bool:
        return pkt_id in self._cache


class _GossipTracker:
    def __init__(self, ttl: float = 30.0, maxsize: int = 512) -> None:
        self._ttl  = ttl
        self._data: Dict[str, Tuple[float, Set[str]]] = {}
        self._max  = maxsize

    def record_forward(self, pkt_id: str, via: str) -> int:
        now = time.time()
        
        if len(self._data) >= self._max:
            expired = [k for k, (ts, _) in self._data.items()
                       if now - ts > self._ttl]
            for k in expired:
                del self._data[k]

        if pkt_id not in self._data:
            self._data[pkt_id] = (now, set())
        ts, forwarders = self._data[pkt_id]
        forwarders.add(via)
        self._data[pkt_id] = (now, forwarders)
        return len(forwarders)

    def forwarder_count(self, pkt_id: str) -> int:
        entry = self._data.get(pkt_id)
        if entry is None:
            return 0
        return len(entry[1])


class LoRaSerial:

    def __init__(
        self,
        device: str,
        baud: int,
        on_message: Callable,
        node_id: Optional[str] = None,
        collision_avoidance_ms: int = 1000,
        send_delay_ms: int = 3000,
        send_jitter_ms: int = 500,
        send_retries: int = 10,
        encryption_key: Optional[bytes] = None,
        on_mesh_packet: Optional[Callable] = None,
        mesh_ttl: int = MESH_DEFAULT_TTL,
        safe_json: int = LORA_SAFE_JSON,
        
        gossip_suppress_k: int = _GOSSIP_SUPPRESS_K,
        adaptive_backoff: bool = True,
    ) -> None:

        self.device            = device
        self.baud              = baud
        self.on_message        = on_message
        self._col_ms           = collision_avoidance_ms
        self._delay_ms         = send_delay_ms
        self._jitter_ms        = send_jitter_ms
        self._retries          = send_retries
        self.on_mesh_packet    = on_mesh_packet
        self._mesh_ttl         = mesh_ttl
        self._safe_json        = safe_json
        self._gossip_k         = gossip_suppress_k
        self._adaptive_backoff = adaptive_backoff

        
        if node_id:
            expected_len = MESH_NODE_ID_LEN * 2
            if len(node_id) != expected_len or not all(
                    c in "0123456789abcdefABCDEF" for c in node_id):
                raise ValueError(
                    f"node_id must be {expected_len} hex chars, got {node_id!r}")
            self.node_id = node_id.lower()
        else:
            self.node_id = os.urandom(MESH_NODE_ID_LEN).hex()
        log.info(f"[MESH] Node ID: {self.node_id}")

        
        self._routes    = MeshRoutingTable()
        self._seen      = SeenCache()
        self._forwarded = _ForwardedCache()
        self._gossip    = _GossipTracker()

        
        self._neighbors: Dict[str, _NeighborEntry] = {}

        
        self._channel = _ChannelMonitor()

        
        self._xchacha: Optional[XChaCha20Poly1305] = None
        if encryption_key is not None:
            if not HAS_XCHACHA:
                raise RuntimeError(
                    "Encryption requested but 'cryptography' package not installed.")
            if len(encryption_key) != XCHACHA_KEY_LEN:
                raise ValueError(
                    f"Encryption key must be {XCHACHA_KEY_LEN} bytes, "
                    f"got {len(encryption_key)}")
            self._xchacha = XChaCha20Poly1305(encryption_key)
            log.info("[SERIAL] XChaCha20-Poly1305 encryption enabled")

        
        self._reader = self._writer = self._ser = None

        
        self._task:      Optional[asyncio.Task] = None
        self._nack_task: Optional[asyncio.Task] = None
        self._tx_task:   Optional[asyncio.Task] = None
        self._stop = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        
        self._reassembly: Dict[str, dict]              = {}
        self._tx_cache:   Dict[Tuple[str, int], tuple] = {}
        self._rx_done:    _LRUDict                     = _LRUDict(512)
        self._last_rx: float = 0.0
        self._last_tx: float = 0.0

        
        self._tx_queue:          Optional[asyncio.PriorityQueue] = None
        self._serial_write_lock: Optional[asyncio.Lock]          = None
        self._tx_seq:            int                             = 0
        self._burst_depth:       int                             = 0
        self._pending_nacks:     List[_TxJob]                   = []

        
        self._echo_suppress: _LRUDict                     = _LRUDict(ECHO_MAX)
        self._sent_nacks:    Dict[Tuple[str, int], float] = {}

        
        import threading
        self._write_lock = threading.Lock()

        
        self._stats: Dict[str, int] = {
            "tx_frames":          0,
            "rx_frames":          0,
            "tx_chunks":          0,
            "rx_chunks":          0,
            "reassembled_msgs":   0,
            "nacks_sent":         0,
            "nacks_recv":         0,
            "nacks_coalesced":    0,
            "decrypt_errors":     0,
            "crc_errors":         0,
            "dropped_duplicates": 0,
            "gossip_suppressed":  0,
            "unicast_forwarded":  0,
            "bcast_forwarded":    0,
            "adaptive_backoffs":  0,
        }


    async def start(self) -> None:
        if not self.device:
            log.warning("[SERIAL] No device configured – LoRa disabled")
            return
        self._loop              = asyncio.get_running_loop()
        self._tx_queue          = asyncio.PriorityQueue()
        self._serial_write_lock = asyncio.Lock()
        self._stop              = False

        self._tx_task = asyncio.create_task(
            self._tx_dispatch_loop(), name="lora-tx-dispatch")

        if HAS_SERIAL_ASYNCIO:
            try:
                self._reader, self._writer = (
                    await serial_asyncio.open_serial_connection(
                        url=self.device, baudrate=self.baud))
                self._task = asyncio.create_task(
                    self._async_read_loop(), name="lora-rx-async")
                self._nack_task = asyncio.create_task(
                    self._reassembly_nack_loop(), name="lora-nack")
                log.info(f"[SERIAL] Opened (asyncio) {self.device} @ {self.baud}")
                return
            except Exception as e:
                log.error(f"[SERIAL] serial_asyncio open failed: {e}")

        if HAS_PYSERIAL:
            self._task = asyncio.create_task(
                asyncio.to_thread(self._blocking_read_loop), name="lora-rx-blocking")
            self._nack_task = asyncio.create_task(
                self._reassembly_nack_loop(), name="lora-nack")
            log.info(f"[SERIAL] Opened (blocking) {self.device} @ {self.baud}")
            return

        log.error(
            "[SERIAL] No serial library available. "
            "Install: pip install pyserial  OR  pip install serial-asyncio")

    async def stop(self) -> None:
        self._stop = True
        if self._tx_queue:
            try:
                fut = asyncio.get_running_loop().create_future()
                fut.set_result(False)
                self._tx_queue.put_nowait(
                    _TxJob(priority=999, seq=999999, frame="", future=fut))
            except Exception:
                pass

        tasks = [t for t in (self._task, self._nack_task, self._tx_task) if t]
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=3.0)
            for t in pending:
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

        for obj in (self._writer, self._ser):
            if obj:
                try:
                    obj.close()
                except Exception:
                    pass

        log.info(f"[SERIAL] Stopped. Final stats: {self._stats}")



    def stats(self) -> Dict[str, int]:
        return dict(self._stats)

    def neighbor_table(self) -> Dict[str, dict]:
        now = time.time()
        return {
            nid: {
                "rssi_ema":   round(e.rssi_ema, 1),
                "snr_ema":    round(e.snr_ema,  1),
                "score":      round(e.score,    3),
                "rx_count":   e.rx_count,
                "age_s":      round(now - e.last_heard, 1),
                "alive":      e.is_alive,
            }
            for nid, e in self._neighbors.items()
        }

    
    def _update_neighbor(
            self,
            node_id: str,
            rssi: Optional[float] = None,
            snr:  Optional[float] = None,
    ) -> None:
        if node_id == self.node_id:
            return
        entry = self._neighbors.get(node_id)
        if entry is None:
            entry = _NeighborEntry(node_id=node_id)
            self._neighbors[node_id] = entry
            log.info(f"[NEIGHBOR] New neighbor discovered: {node_id}")
        entry.update(rssi, snr)

        
        if len(self._neighbors) > 128:
            dead = [nid for nid, e in self._neighbors.items() if not e.is_alive]
            for nid in dead:
                del self._neighbors[nid]
                log.info(f"[NEIGHBOR] Expired: {nid}")

    def _live_neighbor_count(self) -> int:
        return sum(1 for e in self._neighbors.values() if e.is_alive)
    

    def _register_sent(self, text: str) -> None:
        self._echo_suppress[text] = time.time() + ECHO_TTL

    def _is_echo(self, text: str) -> bool:
        exp = self._echo_suppress.get(text)
        if exp is None:
            return False
        if time.time() < exp:
            self._echo_suppress.touch(text)
            return True
        del self._echo_suppress[text]
        return False

    def _register_nack_sent(self, msg_id: str, idx: int) -> None:
        self._sent_nacks[(msg_id, idx)] = time.time() + _NACK_ECHO_TTL
        if len(self._sent_nacks) > 512:
            now     = time.time()
            expired = [k for k, v in self._sent_nacks.items() if v < now]
            for k in expired:
                del self._sent_nacks[k]

    
    def _encrypt(self, pt: str) -> str:
        nonce = os.urandom(XCHACHA_NONCE_LEN)
        ct    = self._xchacha.encrypt(nonce, pt.encode("utf-8"), None)
        return _SIGIL_ENC + base64.b64encode(nonce + ct).decode("ascii")

    def _decrypt(self, token: str) -> Optional[str]:
        try:
            raw = base64.b64decode(token[len(_SIGIL_ENC):])
            pt  = self._xchacha.decrypt(
                raw[:XCHACHA_NONCE_LEN], raw[XCHACHA_NONCE_LEN:], None)
            return pt.decode("utf-8")
        except Exception as e:
            log.warning(f"[SERIAL] Decrypt failed: {e}")
            self._stats["decrypt_errors"] += 1
            return None

    
    def _encode_payload(self, text: str) -> str:
        raw_bytes = text.encode("utf-8")
        if len(raw_bytes) >= _COMPRESS_MIN:
            compressed = _compress_best(raw_bytes)
            b85        = base64.b85encode(compressed).decode("ascii")
            candidate  = _SIGIL_B85_COMP + b85
            encoded    = candidate if len(candidate) < len(text) else text
        else:
            encoded = text
        if self._xchacha:
            encoded = self._encrypt(encoded)
        return encoded

    def _decode_payload(self, token: str) -> Optional[str]:
        if token.startswith(_SIGIL_ENC):
            if not self._xchacha:
                log.error("[SERIAL] Encrypted frame but no key configured")
                return None
            inner = self._decrypt(token)
            return self._decode_payload(inner) if inner is not None else None

        if token.startswith(_SIGIL_B85_COMP):
            try:
                raw = base64.b85decode(token[len(_SIGIL_B85_COMP):])
                return _decompress_tagged(raw).decode("utf-8")
            except Exception as e:
                log.error(f"[SERIAL] B85 decompress failed: {e}")
                return None

        if token.startswith(_SIGIL_Z_LEGACY):
            try:
                return zlib.decompress(
                    base64.b64decode(token[len(_SIGIL_Z_LEGACY):])).decode("utf-8")
            except Exception as e:
                log.error(f"[SERIAL] Legacy Z: decompress failed: {e}")
                return None

        return token

    
    def _enqueue(
        self,
        frame: str,
        *,
        priority:       int  = _PRIO_NORMAL,
        is_burst_start: bool = False,
        is_burst_end:   bool = False,
    ) -> "asyncio.Future[bool]":
        loop = asyncio.get_running_loop()
        fut  = loop.create_future()
        self._tx_seq += 1
        job = _TxJob(
            priority       = priority,
            seq            = self._tx_seq,
            frame          = frame,
            future         = fut,
            is_burst_start = is_burst_start,
            is_burst_end   = is_burst_end,
        )
        self._tx_queue.put_nowait(job)
        return fut

    async def _hd_send(
        self,
        frame: str,
        *,
        is_nack:        bool = False,
        is_burst_start: bool = False,
        is_burst_end:   bool = False,
    ) -> bool:
        if is_nack:
            priority = _PRIO_NACK
        elif is_burst_start or is_burst_end:
            priority = _PRIO_BURST
        else:
            priority = _PRIO_NORMAL

        fut = self._enqueue(
            frame,
            priority       = priority,
            is_burst_start = is_burst_start,
            is_burst_end   = is_burst_end,
        )
        try:
            return await asyncio.wait_for(fut, timeout=30.0)
        except asyncio.TimeoutError:
            log.error(f"[TX] Frame timed out in dispatch queue: {frame[:60]!r}")
            return False

    
    async def _tx_dispatch_loop(self) -> None:
        while not self._stop:
            try:
                job: _TxJob = await asyncio.wait_for(
                    self._tx_queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.error(f"[TX] Queue get error: {e}")
                continue

            if self._stop and not job.frame:
                self._tx_queue.task_done()
                break

            if job.is_burst_start:
                self._burst_depth += 1

            if job.priority == _PRIO_NACK and self._burst_depth > 0:
                log.debug(
                    f"[TX] NACK deferred (burst depth={self._burst_depth}): "
                    f"{job.frame[:60]!r}")
                self._pending_nacks.append(job)
                self._tx_queue.task_done()
                if job.is_burst_end:
                    self._burst_depth = max(0, self._burst_depth - 1)
                    self._flush_pending_nacks()
                continue

            ok = await self._physical_send(job.frame)
            if not job.future.done():
                job.future.set_result(ok)
            self._tx_queue.task_done()

            if job.is_burst_end:
                self._burst_depth = max(0, self._burst_depth - 1)
                if self._burst_depth == 0:
                    self._flush_pending_nacks()

    def _flush_pending_nacks(self) -> None:
        if not self._pending_nacks:
            return
        log.debug(f"[TX] Flushing {len(self._pending_nacks)} deferred NACK(s)")
        for job in self._pending_nacks:
            self._tx_queue.put_nowait(job)
        self._pending_nacks.clear()


    async def _physical_send(self, frame: str) -> bool:
        b = frame.encode("utf-8") + b"\n"

        if len(b) > self._safe_json + 1:
            log.error(
                f"[SERIAL TX] Refusing oversized frame "
                f"({len(b)}B > {self._safe_json + 1}B). Head: {frame[:60]!r}")
            return False

        col_s = self._col_ms  / 1000.0
        tx_s  = self._delay_ms / 1000.0

        
        while True:
            remaining = col_s - (time.time() - self._last_rx)
            if remaining <= 0:
                break
            await asyncio.sleep(min(remaining, 0.05))

        
        gap = tx_s - (time.time() - self._last_tx)
        if gap > 0:
            await asyncio.sleep(gap)

        async with self._serial_write_lock:
            
            gap = tx_s - (time.time() - self._last_tx)
            if gap > 0:
                await asyncio.sleep(gap)

            
            if self._adaptive_backoff:
                bf = self._channel.busy_fraction()
                if bf > _CHAN_BUSY_THRESHOLD:
                    extra = _CHAN_BUSY_EXTRA_S * bf + random.uniform(0, 0.3)
                    log.debug(
                        f"[TX] Adaptive back-off {extra:.2f}s "
                        f"(channel busy={bf:.0%})")
                    await asyncio.sleep(extra)
                    self._stats["adaptive_backoffs"] += 1

            self._last_tx = time.time()
            self._stats["tx_frames"] += 1

            if HAS_SERIAL_ASYNCIO and self._writer:
                try:
                    self._writer.write(b)
                    await asyncio.wait_for(self._writer.drain(), timeout=5.0)
                    return True
                except asyncio.TimeoutError:
                    log.error("[SERIAL] drain() timed out")
                    return False
                except Exception as e:
                    log.error(f"[SERIAL] Async write error: {e}")
                    return False

            if HAS_PYSERIAL and self._ser:
                def _write() -> bool:
                    try:
                        with self._write_lock:
                            if not self._ser:
                                return False
                            self._ser.write(b)
                            self._ser.flush()
                            return True
                    except Exception as e:
                        log.error(f"[SERIAL] Blocking write error: {e}")
                        return False
                return await asyncio.to_thread(_write)

        log.error("[SERIAL] No serial backend available for write")
        return False


    async def _send_nack(
        self,
        missing_indices: List[int],
        msg_id: str,
        src: Optional[str],
    ) -> None:
        if not missing_indices:
            return

        nack: dict = {"id": msg_id, "ii": missing_indices}
        if src:
            nack["src"] = src

        for idx in missing_indices:
            self._register_nack_sent(msg_id, idx)

        coalesced = len(missing_indices)
        self._stats["nacks_sent"]     += coalesced
        self._stats["nacks_coalesced"] += max(0, coalesced - 1)

        
        jitter = random.uniform(0, _NACK_JITTER_MAX)
        await asyncio.sleep(jitter)

        asyncio.create_task(
            self._hd_send(json.dumps({"nack": nack}), is_nack=True),
            name=f"nack-{msg_id}",
        )


    async def mesh_broadcast(self, text: str) -> bool:
        return await self._mesh_originate(text, MESH_BCAST_DST)

    async def mesh_send(self, text: str, dst: str) -> bool:
        return await self._mesh_originate(text, dst.lower())

    async def _mesh_originate(self, text: str, dst: str) -> bool:
        pkt_id  = f"{self.node_id}:{os.urandom(2).hex()}"
        payload = self._encode_payload(text)
        pkt = {
            "src":  self.node_id,
            "dst":  dst,
            "id":   pkt_id,
            "ttl":  self._mesh_ttl,
            "hops": 0,
            "via":  self.node_id,
            "p":    payload,
        }
        self._register_sent(text)
        self._seen.seen(pkt_id)
        self._forwarded.mark(pkt_id)
        return await self._mesh_tx(pkt)

    async def _mesh_tx(self, pkt: dict) -> bool:
        frame = json.dumps({"m": pkt}, separators=(",", ":"))
        if len(frame.encode("utf-8")) <= self._safe_json:
            return await self._hd_send(frame)
        return await self._send_split_mesh(pkt)


    def _should_forward_broadcast(self, pkt_id: str, via: str) -> bool:
        k = self._gossip.forwarder_count(pkt_id)
        if k == 0:
            return True
        if k >= self._gossip_k:
            return False
        p_forward = 1.0 - k / self._gossip_k
        return random.random() < p_forward


    async def _handle_mesh(
        self,
        pkt: dict,
        rssi: Optional[float] = None,
        snr:  Optional[float] = None,
    ) -> None:
        try:
            src    = str(pkt["src"]).lower()
            dst    = str(pkt["dst"]).lower()
            pkt_id = str(pkt["id"])
            ttl    = int(pkt["ttl"])
            hops   = int(pkt["hops"])
            via    = str(pkt["via"]).lower()
            raw_p  = str(pkt["p"])
        except (KeyError, ValueError, TypeError) as e:
            log.warning(f"[MESH] Malformed packet fields: {e}")
            return

        if ttl < 0 or hops < 0:
            log.warning(f"[MESH] Packet {pkt_id} invalid ttl={ttl} hops={hops}")
            return

        if src == self.node_id:
            log.debug(f"[MESH] Dropped own bounced packet: {pkt_id}")
            return

        
        self._update_neighbor(via, rssi=rssi, snr=snr)
        self._routes.learn(src=src, via=via, hops=hops)

        
        is_broadcast = (dst == MESH_BCAST_DST)
        if is_broadcast and via != src:
            self._gossip.record_forward(pkt_id, via)

        
        if self.on_mesh_packet:
            try:
                asyncio.create_task(
                    self.on_mesh_packet(dict(pkt)),
                    name=f"mesh-pkt-cb-{pkt_id}",
                )
            except Exception as e:
                log.error(f"[MESH] on_mesh_packet callback error: {e}")

        log.debug(
            f"[MESH] RX id={pkt_id} src={src} dst={dst} "
            f"ttl={ttl} hops={hops} via={via}"
            + (f" rssi={rssi}" if rssi is not None else "")
        )

        
        if self._seen.seen(pkt_id):
            log.debug(f"[MESH] Duplicate dropped: {pkt_id}")
            self._stats["dropped_duplicates"] += 1
            return

        
        payload = self._decode_payload(raw_p)
        if payload is None:
            log.warning(f"[MESH] Could not decode payload for {pkt_id}")
            return

        
        is_for_us = is_broadcast or (dst == self.node_id)
        if is_for_us:
            if self._is_echo(payload):
                log.debug(f"[MESH] Echo suppressed: {pkt_id}")
            else:
                log.debug(f"[MESH] Delivering to app: {pkt_id}")
                try:
                    await self.on_message(payload)
                except Exception as e:
                    log.error(f"[MESH] on_message callback raised: {e}")

        
        if ttl <= 1:
            log.debug(f"[MESH] TTL exhausted, not forwarding {pkt_id}")
            return
        if dst == self.node_id:
            return  
        if self._forwarded.was_forwarded(pkt_id):
            log.debug(f"[MESH] Already forwarded {pkt_id} – skipping")
            return

        fwd = dict(pkt)
        fwd["ttl"]  = ttl - 1
        fwd["hops"] = hops + 1
        fwd["via"]  = self.node_id

        if is_broadcast:
            if not self._should_forward_broadcast(pkt_id, via):
                log.debug(
                    f"[MESH] Gossip-suppressed broadcast {pkt_id} "
                    f"(forwarders={self._gossip.forwarder_count(pkt_id)})")
                self._stats["gossip_suppressed"] += 1
                return
            self._forwarded.mark(pkt_id)
            log.debug(f"[MESH] Rebroadcasting {pkt_id} (ttl→{fwd['ttl']})")
            self._stats["bcast_forwarded"] += 1
        else:
            
            route = self._routes.lookup(dst)
            if route:
                log.debug(
                    f"[MESH] Forwarding unicast {pkt_id} → {dst} via {route}")
            else:
                log.debug(
                    f"[MESH] Forwarding unicast {pkt_id} → {dst} (flooding)")
            self._forwarded.mark(pkt_id)
            self._stats["unicast_forwarded"] += 1

        asyncio.create_task(
            self._mesh_tx(fwd),
            name=f"mesh-fwd-{pkt_id}",
        )

    
    async def _send_split_mesh(self, pkt: dict) -> bool:
        payload_bytes = pkt["p"].encode("utf-8")
        msg_id        = os.urandom(4).hex()
        mesh_header   = {
            "s": pkt["src"], "d": pkt["dst"], "i": pkt["id"],
            "t": pkt["ttl"], "h": pkt["hops"], "v": pkt["via"],
        }

        def _probe(n_raw: int) -> str:
            return json.dumps(
                {"k": {"i": LORA_MAX_CHUNKS, "n": LORA_MAX_CHUNKS,
                       "id": msg_id, "crc": 0xFFFFFFFF,
                       "d":  base64.b64encode(b"x" * n_raw).decode(),
                       "mh": mesh_header}},
                separators=(",", ":"),
            )

        lo, hi, max_raw = 1, len(payload_bytes), 0
        while lo <= hi:
            mid = (lo + hi) // 2
            if len(_probe(mid).encode("utf-8")) <= self._safe_json:
                max_raw = mid
                lo = mid + 1
            else:
                hi = mid - 1

        if max_raw < 1:
            log.error("[MESH TX] Payload too large to fit in one chunk")
            return False

        slices = [
            payload_bytes[i:i + max_raw]
            for i in range(0, len(payload_bytes), max_raw)
        ]
        total = len(slices)
        if total > LORA_MAX_CHUNKS:
            log.error(
                f"[MESH TX] Payload requires {total} chunks > max {LORA_MAX_CHUNKS}")
            return False

        log.info(
            f"[MESH TX] Sending {total} chunks "
            f"(msg_id={msg_id}, {len(payload_bytes)}B, {max_raw}B/chunk)")

        now    = time.time()
        frames: List[Tuple[int, str]] = []
        for i, raw_slice in enumerate(slices):
            idx   = i + 1
            d     = base64.b64encode(raw_slice).decode("ascii")
            crc   = zlib.crc32(raw_slice) & 0xFFFFFFFF
            frame = json.dumps(
                {"k": {"i": idx, "n": total, "id": msg_id,
                       "crc": crc, "d": d, "mh": mesh_header}},
                separators=(",", ":"),
            )
            frames.append((idx, frame))
            self._tx_cache[(msg_id, idx)] = (d, crc, total, mesh_header, now)
            self._stats["tx_chunks"] += 1

        send_timeout = (self._col_ms + self._delay_ms) / 1000.0 + 5.0
        failed: List[int] = list(range(1, total + 1))

        for attempt in range(self._retries + 1):
            if self._stop or not failed:
                break

            to_send = [(idx, frm) for idx, frm in frames if idx in set(failed)]

            if attempt > 0:
                backoff = _NACK_BACKOFF_BASE * (2 ** (attempt - 1))
                wait    = backoff + random.uniform(0, _NACK_JITTER_MAX)
                log.info(
                    f"[MESH TX] Retry {attempt}/{self._retries} for "
                    f"{len(to_send)} chunk(s) of {msg_id} "
                    f"(back-off {wait:.2f}s): {[i for i, _ in to_send]}")
                await asyncio.sleep(wait)

            futs: List[Tuple[int, asyncio.Future]] = []
            for pos, (idx, frm) in enumerate(to_send):
                fut = self._enqueue(
                    frm,
                    priority       = _PRIO_BURST,
                    is_burst_start = (pos == 0),
                    is_burst_end   = (pos == len(to_send) - 1),
                )
                futs.append((idx, fut))

            round_failed: List[int] = []
            for idx, fut in futs:
                try:
                    ok = await asyncio.wait_for(fut, timeout=send_timeout)
                    if not ok:
                        round_failed.append(idx)
                except (asyncio.TimeoutError, Exception) as e:
                    log.warning(
                        f"[MESH TX] Chunk {idx}/{total} attempt "
                        f"{attempt + 1} failed: {e}")
                    round_failed.append(idx)

            failed = round_failed

        burst_end = time.time()
        for k in list(self._tx_cache):
            if k[0] == msg_id:
                d, c, t, h, _ = self._tx_cache[k]
                self._tx_cache[k] = (d, c, t, h, burst_end)

        if failed:
            log.error(
                f"[MESH TX] {msg_id}: {len(failed)}/{total} chunks failed "
                f"after {self._retries} retries: {failed}")
            return False

        log.info(f"[MESH TX] All {total} chunks sent (msg_id={msg_id})")
        return True


    async def _async_read_loop(self) -> None:
        while not self._stop:
            try:
                raw_bytes = await self._reader.readline()
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._stop:
                    log.error(f"[SERIAL] readline error: {e}")
                    await asyncio.sleep(0.1)
                continue
            if not raw_bytes:
                continue
            line = raw_bytes.decode("utf-8", errors="replace").rstrip("\r\n")
            await self._process_raw_line(line)

    def _blocking_read_loop(self) -> None:
        import serial as sl
        try:
            ser       = sl.Serial(self.device, self.baud, timeout=0.1)
            self._ser = ser
        except Exception as e:
            log.error(f"[SERIAL] Failed to open {self.device}: {e}")
            return

        while not self._stop:
            try:
                raw = ser.readline()
            except Exception as e:
                if not self._stop:
                    log.error(f"[SERIAL] Read error: {e}")
                    time.sleep(0.1)
                continue
            if raw and self._loop:
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                asyncio.run_coroutine_threadsafe(
                    self._process_raw_line(line), self._loop)
            elif not raw:
                time.sleep(0.01)

        try:
            ser.close()
        except Exception:
            pass
        self._ser = None


    async def _process_raw_line(self, line: str) -> None:
        if not line:
            return

        self._last_rx = time.time()
        self._stats["rx_frames"] += 1
        self._channel.record_rx()

        if "Truncated message to MAX_SEND_LEN" in line:
            log.warning(
                "[SERIAL] Firmware truncated an inbound LoRa packet – "
                "reduce --lora-safe-json on the remote node. Discarding.")
            return

        
        
        rssi = snr = None
        rssi_m = re.search(r"RSSI=(-?\d+(?:\.\d+)?)", line, re.IGNORECASE)
        snr_m  = re.search(r"SNR=(-?\d+(?:\.\d+)?)",  line, re.IGNORECASE)
        if rssi_m:
            try:
                rssi = float(rssi_m.group(1))
            except ValueError:
                pass
        if snr_m:
            try:
                snr = float(snr_m.group(1))
            except ValueError:
                pass

        m = re.search(r"\{.*\}", line)
        if not m:
            idx, msg_id = self._try_extract_corrupt_chunk(line)
            if idx is not None and msg_id is not None:
                log.warning(
                    f"[SERIAL RX] No JSON but chunk fields recoverable "
                    f"– NACKing i={idx} id={msg_id!r}")
                await self._send_nack([idx], msg_id, src=None)
            else:
                log.debug(f"[SERIAL RX] Ignored non-JSON line: {line!r}")
            return

        fragment = m.group(0)
        try:
            obj = json.loads(fragment)
        except json.JSONDecodeError:
            idx, msg_id = self._try_extract_corrupt_chunk(fragment)
            if idx is not None and msg_id is not None:
                log.warning(
                    f"[SERIAL RX] Corrupt JSON – NACKing chunk i={idx} "
                    f"id={msg_id!r}: {fragment[:80]!r}")
                self._stats["crc_errors"] += 1
                await self._send_nack([idx], msg_id, src=None)
            else:
                log.warning(
                    f"[SERIAL RX] Corrupt JSON, fields not recoverable: "
                    f"{fragment[:80]!r}")
            return

        await self._dispatch(obj, rssi=rssi, snr=snr)

    async def _dispatch(
        self,
        obj: dict,
        rssi: Optional[float] = None,
        snr:  Optional[float] = None,
    ) -> None:
        if not isinstance(obj, dict):
            return

        if "nack" in obj:
            n = obj["nack"]
            if not isinstance(n, dict):
                return
            
            try:
                msg_id   = str(n["id"])
                
                if "ii" in n:
                    indices = [int(x) for x in n["ii"]]
                else:
                    indices = [int(n["i"])]
                filtered = []
                for idx in indices:
                    key = (msg_id, idx)
                    exp = self._sent_nacks.get(key)
                    if exp is not None:
                        if time.time() < exp:
                            log.debug(
                                f"[SERIAL] Self-echoed NACK dropped: "
                                f"{msg_id}:{idx}")
                            continue
                        del self._sent_nacks[key]
                    filtered.append(idx)
                if not filtered:
                    return
                self._stats["nacks_recv"] += len(filtered)
                for idx in filtered:
                    await self._handle_nack_single(idx, msg_id, n.get("src"))
            except (KeyError, ValueError, TypeError):
                log.warning("[SERIAL] Received malformed NACK")
            return

        if "k" in obj:
            if isinstance(obj["k"], dict):
                self._stats["rx_chunks"] += 1
                await self._handle_chunk(obj["k"])
            return

        if "m" in obj:
            if isinstance(obj["m"], dict):
                await self._handle_mesh(obj["m"], rssi=rssi, snr=snr)
            return

        if "c" in obj:
            log.warning(
                '[SERIAL] Dropping legacy {"c":...} frame – upgrade remote node')
            return

        for val in obj.values():
            if isinstance(val, dict) and all(k in val for k in ("i", "n", "id", "d")):
                try:
                    idx    = int(val["i"])
                    msg_id = str(val["id"])
                    log.warning(
                        f"[SERIAL] Chunk under unexpected key – "
                        f"NACKing i={idx} id={msg_id!r}")
                    await self._send_nack([idx], msg_id, src=None)
                except (ValueError, TypeError) as e:
                    log.warning(f"[SERIAL] Could not extract chunk fields: {e}")
                return

        log.debug(f"[SERIAL] Unrecognised frame keys {list(obj.keys())} – ignored")


    async def _handle_chunk(self, k: dict) -> None:
        try:
            idx    = int(k["i"])
            total  = int(k["n"])
            msg_id = str(k["id"])
            crc    = int(k["crc"])
            d_b64  = str(k["d"])
        except (KeyError, ValueError, TypeError) as e:
            log.warning(f"[SERIAL] Malformed chunk header: {e}")
            return

        if not (1 <= idx <= total <= LORA_MAX_CHUNKS):
            log.warning(
                f"[SERIAL] Chunk bounds invalid: idx={idx} total={total}")
            return

        self._last_rx = time.time()

        chunk_src: Optional[str] = None
        try:
            chunk_src = str(k["mh"]["s"]).lower()
        except (KeyError, TypeError):
            if ":" in msg_id:
                chunk_src = msg_id.split(":")[0]

        try:
            raw = base64.b64decode(d_b64)
        except Exception:
            log.warning(f"[SERIAL] Bad base64 in chunk {idx}/{total} of {msg_id}")
            await self._send_nack([idx], msg_id, src=chunk_src)
            self._stats["crc_errors"] += 1
            return

        if (zlib.crc32(raw) & 0xFFFFFFFF) != crc:
            log.warning(
                f"[SERIAL] CRC mismatch in chunk {idx}/{total} of {msg_id}")
            await self._send_nack([idx], msg_id, src=chunk_src)
            self._stats["crc_errors"] += 1
            return

        if msg_id in self._rx_done:
            log.debug(
                f"[SERIAL] Duplicate chunk {idx}/{total} for completed "
                f"{msg_id} – ignored")
            self._stats["dropped_duplicates"] += 1
            return

        buf = self._reassembly.get(msg_id)
        if buf is None or buf["n"] != total:
            if len(self._reassembly) >= _MAX_REASSEMBLY_SESSIONS:
                oldest = min(
                    self._reassembly,
                    key=lambda mid: self._reassembly[mid]["ts"])
                evicted = self._reassembly.pop(oldest)
                log.warning(
                    f"[SERIAL] Reassembly table full – evicting {oldest} "
                    f"({len(evicted['parts'])}/{evicted['n']} chunks)")

            buf = {
                "n":              total,
                "parts":          {},
                "ts":             time.time(),
                "first_chunk_ts": time.time(),
                "mh":             None,
                "nack_attempts":  {},
                "src":            chunk_src,
            }
            self._reassembly[msg_id] = buf

        if buf["mh"] is None and "mh" in k:
            mh = k["mh"]
            try:
                buf["mh"] = {
                    "src":  str(mh["s"]),
                    "dst":  str(mh["d"]),
                    "id":   str(mh["i"]),
                    "ttl":  int(mh["t"]),
                    "hops": int(mh["h"]),
                    "via":  str(mh["v"]),
                }
                buf["src"] = buf["mh"]["src"]
            except (KeyError, ValueError, TypeError) as e:
                log.warning(f"[SERIAL] Malformed mesh header in chunk: {e}")

        buf["parts"][idx] = raw
        buf["ts"] = time.time()
        buf["nack_attempts"].pop(idx, None)

        log.debug(
            f"[SERIAL] Chunk {idx}/{total} OK for {msg_id} "
            f"({len(buf['parts'])}/{total} received)")

        if len(buf["parts"]) < total:
            
            now   = time.time()
            stale = [
                mid for mid, b in list(self._reassembly.items())
                if now - b["ts"] > REASSEMBLY_TTL
            ]
            for mid in stale:
                b = self._reassembly.pop(mid, None)
                if b:
                    log.warning(
                        f"[SERIAL] Stale reassembly session {mid} pruned "
                        f"({len(b['parts'])}/{b['n']})")
            return

        
        assembled_bytes = b"".join(buf["parts"][i] for i in range(1, total + 1))
        mh_full         = buf.get("mh")
        del self._reassembly[msg_id]

        self._rx_done[msg_id] = time.time() + RX_DONE_TTL
        self._stats["reassembled_msgs"] += 1

        assembled = self._decode_payload(
            assembled_bytes.decode("utf-8", errors="replace"))
        if assembled is None:
            log.error(
                f"[SERIAL] Payload decode failed for reassembled {msg_id}")
            return

        log.info(
            f"[SERIAL] Reassembled {total} chunks → "
            f"{len(assembled_bytes)}B (msg_id={msg_id})")

        if mh_full:
            pkt = dict(mh_full)
            pkt["p"] = assembled
            await self._handle_mesh(pkt)
        else:
            if not self._is_echo(assembled):
                try:
                    await self.on_message(assembled)
                except Exception as e:
                    log.error(f"[SERIAL] on_message callback raised: {e}")

    

    async def _reassembly_nack_loop(self) -> None:
        while not self._stop:
            await asyncio.sleep(REASSEMBLY_NACK_INTERVAL)
            now = time.time()

            
            self._tx_cache = {
                k: v for k, v in self._tx_cache.items()
                if now - v[4] < TX_DONE_TTL
            }

            for msg_id, buf in list(self._reassembly.items()):
                if now - buf["ts"] > REASSEMBLY_TTL:
                    log.warning(
                        f"[SERIAL] Reassembly timeout: {msg_id} "
                        f"({len(buf['parts'])}/{buf['n']}) – discarding")
                    del self._reassembly[msg_id]
                    continue

                if now - buf["first_chunk_ts"] < REASSEMBLY_NACK_INTERVAL:
                    continue

                total   = buf["n"]
                present = set(buf["parts"].keys())
                missing = [i for i in range(1, total + 1) if i not in present]
                if not missing:
                    continue

                if msg_id in self._rx_done:
                    del self._reassembly[msg_id]
                    continue

                nack_attempts = buf["nack_attempts"]
                actionable = [
                    i for i in missing
                    if nack_attempts.get(i, 0) < REASSEMBLY_NACK_MAX
                ]
                exhausted = [i for i in missing if i not in actionable]

                if exhausted and not actionable:
                    log.warning(
                        f"[SERIAL] Giving up on {msg_id}: chunks "
                        f"{exhausted} did not respond after "
                        f"{REASSEMBLY_NACK_MAX} NACKs")
                    del self._reassembly[msg_id]
                    continue

                if actionable:
                    msg_src = buf.get("src")
                    log.info(
                        f"[SERIAL] NACKing {msg_id} (src={msg_src!r}): "
                        f"missing={actionable} "
                        f"({len(present)}/{total} received)")
                    for idx in actionable:
                        nack_attempts[idx] = nack_attempts.get(idx, 0) + 1
                    buf["ts"] = time.time()
                    
                    asyncio.create_task(
                        self._send_nack(actionable, msg_id, src=msg_src),
                        name=f"nack-loop-{msg_id}",
                    )


    async def _handle_nack_single(
        self,
        idx: int,
        msg_id: str,
        nack_src: Optional[str],
    ) -> None:
        if nack_src and str(nack_src).lower() != self.node_id:
            log.debug(
                f"[SERIAL] NACK {msg_id}:{idx} addressed to "
                f"{nack_src} – not us, ignoring")
            return

        entry = self._tx_cache.get((msg_id, idx))
        if not entry:
            log.debug(
                f"[SERIAL] NACK {msg_id}:{idx} – not in TX cache")
            return

        d_b64, crc, total, mh, _ = entry
        frame = json.dumps(
            {"k": {"i": idx, "n": total, "id": msg_id,
                   "crc": crc, "d": d_b64, "mh": mh}},
            separators=(",", ":"),
        )
        log.info(
            f"[SERIAL] Retransmitting chunk {msg_id}:{idx}/{total} "
            f"({len(frame.encode())}B) at NACK priority")

        async def _do_retransmit() -> None:
            ok = await self._hd_send(frame, is_nack=True)
            if ok:
                now = time.time()
                for k in list(self._tx_cache):
                    if k[0] == msg_id:
                        d, c, t, h, _ = self._tx_cache[k]
                        self._tx_cache[k] = (d, c, t, h, now)
            else:
                log.warning(
                    f"[SERIAL] Retransmit of chunk {msg_id}:{idx} failed")

        asyncio.create_task(
            _do_retransmit(), name=f"retx-{msg_id}:{idx}")


    @staticmethod
    def _try_extract_corrupt_chunk(
            text: str) -> Tuple[Optional[int], Optional[str]]:
        i_m  = re.search(r'"i"\s*:\s*(\d+)', text)
        id_m = re.search(r'"id"\s*:\s*"([^"\x00-\x1f]{1,32})"', text)
        if i_m and id_m:
            return int(i_m.group(1)), id_m.group(1)
        return None, None

    def routing_table_summary(self) -> str:
        routes = self._routes.all_known()
        if not routes:
            return "Routing table: empty"
        lines = [f"Routing table ({len(routes)} entries):"]
        for dst, entry in sorted(routes.items()):
            age = int(MESH_ROUTE_TTL - (entry.expires - time.time()))
            lines.append(
                f"  {dst} -> via {entry.next_hop}  "
                f"hops={entry.hops}  age={age}s")
        return "\n".join(lines)

    def neighbor_summary(self) -> str:
        nt = self.neighbor_table()
        if not nt:
            return "Neighbors: none"
        lines = [f"Neighbors ({len(nt)}):"]
        for nid, info in sorted(nt.items(), key=lambda x: -x[1]["score"]):
            status = "alive" if info["alive"] else "STALE"
            lines.append(
                f"  {nid}  score={info['score']:.2f}  "
                f"rssi={info['rssi_ema']}dBm  snr={info['snr_ema']}dB  "
                f"rx={info['rx_count']}  age={info['age_s']}s  [{status}]")
        return "\n".join(lines)

    def reassembly_summary(self) -> str:
        if not self._reassembly:
            return "Reassembly: no active sessions"
        lines = [f"Reassembly ({len(self._reassembly)} sessions):"]
        now = time.time()
        for mid, buf in sorted(self._reassembly.items()):
            age     = now - buf["first_chunk_ts"]
            missing = [
                i for i in range(1, buf["n"] + 1)
                if i not in buf["parts"]
            ]
            lines.append(
                f"  {mid}: {len(buf['parts'])}/{buf['n']} chunks  "
                f"age={age:.1f}s  missing={missing}  "
                f"src={buf.get('src')!r}")
        return "\n".join(lines)
