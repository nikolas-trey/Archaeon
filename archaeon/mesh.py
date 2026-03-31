import time
from dataclasses import dataclass
from typing import Dict, Optional

from .constants import MESH_ROUTE_TTL, MESH_SEEN_TTL, log


@dataclass
class RouteEntry:
    next_hop: str
    expires:  float
    hops:     int = 0


class MeshRoutingTable:

    def __init__(self):
        self._routes: Dict[str, RouteEntry] = {}

    def learn(self, src: str, via: str, hops: int = 0) -> None:
        existing = self._routes.get(src)
        expiry = time.time() + MESH_ROUTE_TTL
        if existing is None or hops <= existing.hops:
            self._routes[src] = RouteEntry(next_hop=via, expires=expiry, hops=hops)
            log.debug(f"[MESH] Route learned: {src} via {via} ({hops} hops)")

    def lookup(self, dst: str) -> Optional[str]:
        entry = self._routes.get(dst)
        if entry is None: return None
        if time.time() > entry.expires:
            del self._routes[dst]; return None
        return entry.next_hop

    def purge_expired(self) -> None:
        now = time.time()
        expired = [k for k, v in self._routes.items() if now > v.expires]
        for k in expired:
            del self._routes[k]
        if expired:
            log.debug(f"[MESH] Purged {len(expired)} expired route(s)")

    def all_known(self) -> Dict[str, RouteEntry]:
        self.purge_expired()
        return dict(self._routes)


class SeenCache:
    def __init__(self, ttl: float = MESH_SEEN_TTL):
        self._ttl = ttl
        self._seen: Dict[str, float] = {}

    def seen(self, pkt_id: str) -> bool:
        now = time.time()
        self._evict(now)
        if pkt_id in self._seen:
            return True
        self._seen[pkt_id] = now + self._ttl
        return False

    def _evict(self, now: float) -> None:
        expired = [k for k, v in self._seen.items() if v < now]
        for k in expired:
            del self._seen[k]
