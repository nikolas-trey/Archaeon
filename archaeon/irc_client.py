import asyncio
import atexit
import base64
import hashlib
import os
import sqlite3
import time
import traceback
import uuid
from typing import Callable, List, Optional, Set

from collections import defaultdict

_NONCE_LEN = 5


def _make_nonce() -> str:
    return uuid.uuid4().hex[:_NONCE_LEN]


def _read_nonce(text: str) -> str | None:
    tail_len = 2 + _NONCE_LEN + 1
    if (len(text) > tail_len
            and text[-1] == "]"
            and text[-(tail_len):-(tail_len - 2)] == " ["
            and text[-(1 + _NONCE_LEN):-1].isalnum()):
        return text[-(1 + _NONCE_LEN):-1]
    return None

from .constants import (
    IRC_MAX_MSG, IRC_RECONNECT_DELAY, IRC_RECONNECT_MAX,
    IRC_PING_INTERVAL, IRC_PING_TIMEOUT, log,
)
from .irc_server import _irc_parse, _nick_from_prefix


class IRCClient:
    def __init__(self, host, port, nick, password=None, tls=False,
                 on_message=None, on_connected=None, on_disconnected=None,
                 on_channel_join=None, upstream_enabled=True, max_retries=0,
                 tor=False, tor_host="127.0.0.1", tor_port=9050,
                 sasl_username=None, sasl_password=None,
                 echo_store_path: Optional[str] = None):
        self.host, self.port, self.nick = host, port, nick
        self.password, self.tls = password, tls
        self.tor, self.tor_host, self.tor_port = tor, tor_host, tor_port
        self.on_message, self.on_connected = on_message, on_connected
        self.on_disconnected, self.on_channel_join = on_disconnected, on_channel_join
        self.upstream_enabled, self.max_retries = upstream_enabled, max_retries
        self._reader = self._writer = None
        self._stop = False
        self.channels: Set[str] = set()
        self._last_pong = time.time()
        self._send_lock = asyncio.Lock()
        self.connected = False

        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self._sasl_done = asyncio.Event()
        self._sasl_success = False

        self._outgoing_fingerprints: Set[str] = set()
        self._echo_lock = asyncio.Lock()
        self.echo_store_path = echo_store_path or os.path.join(os.getcwd(), f"irc_echoes_{self.nick}.sqlite3")
        self._init_echo_store()
        self._load_echo_store()
        atexit.register(self._close_echo_store)

    def _norm_nick(self, nick: Optional[str]) -> Optional[str]:
        return nick.lower() if nick else nick

    def _norm_channel(self, channel: str) -> str:
        return channel.lower()

    def _norm_text(self, text: str) -> str:
        return text.strip()

    def _message_fingerprint(self, nonce: str) -> str:
        return nonce

    def _content_fingerprint(self, channel: str, text: str) -> str:
        payload = f"{channel.lower()}\0{text.strip()}".encode("utf-8", errors="ignore")
        return hashlib.sha256(payload).hexdigest()

    def _init_echo_store(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.echo_store_path) or ".", exist_ok=True)
            self._echo_db = sqlite3.connect(self.echo_store_path)
            self._echo_db.execute("""
                CREATE TABLE IF NOT EXISTS client_echo_fingerprints (
                    fingerprint TEXT PRIMARY KEY
                )
            """)
            self._echo_db.commit()
        except Exception:
            log.warning(f"[IRC] Failed to initialize echo store {self.echo_store_path}", exc_info=True)
            self._echo_db = None

    def _load_echo_store(self) -> None:
        if not getattr(self, "_echo_db", None):
            return
        try:
            rows = self._echo_db.execute(
                "SELECT fingerprint FROM client_echo_fingerprints"
            ).fetchall()
            self._outgoing_fingerprints = {row[0] for row in rows if row and row[0]}
        except Exception:
            log.warning(f"[IRC] Failed to load echo store {self.echo_store_path}", exc_info=True)

    def _store_fingerprint(self, fingerprint: str) -> None:
        if not getattr(self, "_echo_db", None):
            return
        try:
            self._echo_db.execute(
                "INSERT OR IGNORE INTO client_echo_fingerprints (fingerprint) VALUES (?)",
                (fingerprint,),
            )
            self._echo_db.commit()
        except Exception:
            log.warning(f"[IRC] Failed to persist echo fingerprint to {self.echo_store_path}", exc_info=True)

    def _close_echo_store(self) -> None:
        db = getattr(self, "_echo_db", None)
        if not db:
            return
        try:
            db.commit()
            db.close()
        except Exception:
            log.warning(f"[IRC] Failed to close echo store {self.echo_store_path}", exc_info=True)

    async def _remember_outgoing(self, nonce: str) -> None:
        async with self._echo_lock:
            fingerprint = self._message_fingerprint(nonce)
            self._outgoing_fingerprints.add(fingerprint)
            self._store_fingerprint(fingerprint)

    async def _is_known_outgoing(self, nonce: str) -> bool:
        async with self._echo_lock:
            return self._message_fingerprint(nonce) in self._outgoing_fingerprints

    async def run(self):
        if not self.upstream_enabled:
            log.info("[IRC] Upstream disabled")
            return
        attempt, delay = 0, IRC_RECONNECT_DELAY
        while not self._stop:
            attempt += 1
            if self.max_retries and attempt > self.max_retries:
                log.warning(f"[IRC] Giving up after {self.max_retries} attempt(s)")
                return
            try:
                await self._connect()
                delay = IRC_RECONNECT_DELAY
                await self._loop()
            except asyncio.CancelledError:
                break
            except OSError as e:
                log.warning(f"[IRC] Network error: {e}")
            except Exception:
                log.error(f"[IRC]\n{traceback.format_exc()}")
            finally:
                self.connected = False
                if self.on_disconnected:
                    asyncio.create_task(self.on_disconnected())
            if self._stop:
                break
            log.info(f"[IRC] Reconnecting in {delay}s …")
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break
            delay = min(delay * 2, IRC_RECONNECT_MAX)

    async def stop(self):
        self._stop = True
        if self._writer:
            await self._raw("QUIT :Archaeon shutting down")
            try:
                self._writer.close()
            except Exception:
                pass

    async def send_message(self, text: str, channel: str) -> bool:
        if channel.lower() not in self.channels or not self.connected:
            return False
        ok = True
        for chunk in self._split(text):
            nonce = _make_nonce()
            tagged = f"{chunk} [{nonce}]"
            ok = ok and await self._raw(f"PRIVMSG {channel} :{tagged}")
            await self._remember_outgoing(nonce)
            await self._remember_outgoing(self._content_fingerprint(channel, tagged))
        return ok

    async def join_channel(self, channel: str) -> bool:
        if not channel.startswith("#"):
            channel = "#" + channel
        return await self._raw(f"JOIN {channel}")

    async def part_channel(self, channel: str) -> bool:
        if not channel.startswith("#"):
            channel = "#" + channel
        return await self._raw(f"PART {channel}")

    async def _connect(self):
        if self.tor:
            log.info(f"[IRC] Connecting via Tor {self.tor_host}:{self.tor_port} "
                     f"→ {self.host}:{self.port} tls={self.tls}")
            self._reader, self._writer = await self._socks5_connect()
        else:
            log.info(f"[IRC] Connecting to {self.host}:{self.port} tls={self.tls}")
            if self.tls:
                import ssl
                ctx = ssl.create_default_context()
                self._reader, self._writer = await asyncio.open_connection(
                    self.host, self.port, ssl=ctx)
            else:
                self._reader, self._writer = await asyncio.open_connection(
                    self.host, self.port)

        self.channels.clear()
        self._last_pong = time.time()

        if self.password:
            await self._raw(f"PASS {self.password}")

        if not (self.sasl_username and self.sasl_password):
            await self._raw(f"NICK {self.nick}")
            await self._raw(f"USER {self.nick} 0 * :{self.nick} (Archaeon)")
            return

        self._sasl_done.clear()
        self._sasl_success = False

        async def _read_until_sasl_done():
            while not self._sasl_done.is_set():
                try:
                    raw = await asyncio.wait_for(self._reader.readline(), timeout=35.0)
                except asyncio.TimeoutError:
                    break
                if not raw:
                    break
                await self._handle(raw.decode(errors="replace").rstrip("\r\n"))

        reader_task = asyncio.create_task(_read_until_sasl_done())
        try:
            await self._raw("CAP REQ :sasl")
            await self._raw(f"NICK {self.nick}")
            await self._raw(f"USER {self.nick} 0 * :{self.nick} (Archaeon)")

            try:
                await asyncio.wait_for(self._sasl_done.wait(), timeout=30)
            except asyncio.TimeoutError:
                log.warning("[IRC] SASL timed out")
        finally:
            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass

        if not self._sasl_success:
            raise ConnectionError("SASL authentication failed")

    async def _socks5_connect(self):
        import ssl, socket, struct as _s

        host_b = self.host.encode()
        if len(host_b) > 255:
            raise ValueError(f"Hostname too long for SOCKS5: {self.host!r}")

        def _recvexact(s: socket.socket, n: int) -> bytes:
            buf = b""
            while len(buf) < n:
                chunk = s.recv(n - len(buf))
                if not chunk:
                    raise ConnectionError("SOCKS5: connection closed during handshake")
                buf += chunk
            return buf

        def _handshake() -> socket.socket:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30)
            try:
                sock.connect((self.tor_host, self.tor_port))
                sock.sendall(b"\x05\x01\x00")
                resp = _recvexact(sock, 2)
                if resp[0] != 5 or resp[1] != 0:
                    raise ConnectionError(f"SOCKS5 auth failed: {resp!r}")
                req = (bytes([5, 1, 0, 3, len(host_b)])
                       + host_b
                       + _s.pack(">H", self.port))
                sock.sendall(req)
                hdr = _recvexact(sock, 4)
                if hdr[0] != 5 or hdr[1] != 0:
                    _ERRORS = {
                        1: "general failure", 2: "not allowed", 3: "net unreachable",
                        4: "host unreachable", 5: "connection refused", 6: "TTL expired",
                        7: "command not supported", 8: "address type not supported",
                    }
                    raise ConnectionError(
                        f"SOCKS5 CONNECT failed: {_ERRORS.get(hdr[1], hdr[1])}")
                atyp = hdr[3]
                if   atyp == 1: _recvexact(sock, 4)
                elif atyp == 3: _recvexact(sock, _recvexact(sock, 1)[0])
                elif atyp == 4: _recvexact(sock, 16)
                _recvexact(sock, 2)
                sock.settimeout(None)
                return sock
            except Exception:
                sock.close()
                raise

        sock = await asyncio.to_thread(_handshake)
        if self.tls:
            import ssl
            ctx = ssl.create_default_context()
            r, w = await asyncio.open_connection(
                server_hostname=self.host, ssl=ctx, sock=sock)
        else:
            r, w = await asyncio.open_connection(sock=sock)
        return r, w

    async def _loop(self):
        ping_task = asyncio.create_task(self._ping_loop())
        try:
            while not self._stop:
                try:
                    raw = await asyncio.wait_for(self._reader.readline(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    log.error(f"[IRC] readline: {e}")
                    break
                if not raw:
                    log.warning("[IRC] Server closed")
                    break
                await self._handle(raw.decode(errors="replace").rstrip("\r\n"))
        finally:
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass

    async def _ping_loop(self):
        while not self._stop:
            await asyncio.sleep(IRC_PING_INTERVAL)
            await self._raw(f"PING :{self.host}")
            await asyncio.sleep(IRC_PING_TIMEOUT)
            if time.time() - self._last_pong > IRC_PING_INTERVAL + IRC_PING_TIMEOUT:
                log.warning("[IRC] PING timeout")
                self._writer and self._writer.close()
                return


    def _sasl_plain_blob(self) -> str:
        payload = f"\0{self.sasl_username}\0{self.sasl_password}".encode()
        return base64.b64encode(payload).decode()

    async def _handle_cap(self, params: List[str]) -> None:
        if len(params) < 3:
            return
        subcmd = params[1].upper()
        caps = params[2].lstrip(":")

        if subcmd == "ACK" and "sasl" in caps.split():
            log.info("[IRC] SASL capability acknowledged, authenticating (PLAIN)")
            await self._raw("AUTHENTICATE PLAIN")

        elif subcmd == "NAK":
            log.warning(f"[IRC] CAP NAK – server rejected: {caps}")
            self._sasl_success = False
            self._sasl_done.set()
            await self._raw("CAP END")

    async def _handle(self, line: str):
        if not line:
            return
        if line.startswith("PING"):
            rest = line[5:].lstrip(":")
            await self._raw(f"PONG :{rest or self.host}")
            return

        prefix, cmd, params = _irc_parse(line)
        nick = _nick_from_prefix(prefix)
        l_nick = self._norm_nick(nick)

        if cmd == "PONG":
            self._last_pong = time.time()
            return

        if cmd == "001":
            log.info(f"[IRC] Registered as {self.nick}")
            return

        if cmd == "433":
            self.nick += "_"
            await self._raw(f"NICK {self.nick}")
            return

        if cmd == "CAP":
            await self._handle_cap(params)
            return

        if cmd == "AUTHENTICATE":
            if params and params[0] == "+":
                await self._raw(f"AUTHENTICATE {self._sasl_plain_blob()}")
            return

        if cmd in ("900", "903"):
            log.info(f"[IRC] SASL authentication succeeded (numeric {cmd})")
            self._sasl_success = True
            self._sasl_done.set()
            await self._raw("CAP END")
            return

        if cmd in ("902", "904", "905", "906"):
            log.warning(f"[IRC] SASL authentication failed (numeric {cmd}): "
                        f"{' '.join(params)}")
            self._sasl_success = False
            self._sasl_done.set()
            await self._raw("CAP END")
            return

        if cmd == "JOIN":
            ch = params[0] if params else ""
            lch = ch.lower()
            if l_nick == self._norm_nick(self.nick):
                already = lch in self.channels
                self.channels.add(lch)
                self.connected = True
                log.info(f"[IRC] Joined {ch}")
                if self.on_connected and not already:
                    asyncio.create_task(self.on_connected())
                if self.on_channel_join:
                    try:
                        asyncio.create_task(self.on_channel_join(ch))
                    except Exception:
                        log.exception("on_channel_join failed")
            return

        if cmd == "PART":
            ch = params[0] if params else ""
            if l_nick == self._norm_nick(self.nick):
                self.channels.discard(ch.lower())
                log.info(f"[IRC] Parted {ch}")
            return

        if cmd == "PRIVMSG":
            if len(params) < 2:
                return
            target, raw_text = params[0], params[1]
            nonce = _read_nonce(raw_text)
            if target.startswith("#"):
                if target.lower() in self.channels:
                    if l_nick == self._norm_nick(self.nick):
                        is_echo = (
                            (nonce and await self._is_known_outgoing(nonce))
                            or await self._is_known_outgoing(self._content_fingerprint(target, raw_text))
                        )
                        if is_echo:
                            log.debug(f"[IRC] Ignored incoming echo on {target}: {raw_text!r}")
                        return
                    if self.on_message:
                        asyncio.create_task(self.on_message(nick, target, raw_text))
            elif target.lower() == self.nick.lower():
                if l_nick != self._norm_nick(self.nick) and self.on_message:
                    asyncio.create_task(self.on_message(nick, None, raw_text))
            return

    async def _raw(self, msg: str) -> bool:
        if not self._writer:
            return False
        async with self._send_lock:
            try:
                self._writer.write((msg + "\r\n").encode())
                await self._writer.drain()
                return True
            except Exception as e:
                log.error(f"[IRC] send: {e}")
                return False

    @staticmethod
    def _split(text: str) -> List[str]:
        chunks = []
        while len(text.encode()) > IRC_MAX_MSG:
            chunk = text.encode()[:IRC_MAX_MSG].decode(errors="ignore")
            chunks.append(chunk)
            text = text[len(chunk):]
        if text:
            chunks.append(text)
        return chunks