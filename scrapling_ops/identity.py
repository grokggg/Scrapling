"""Identity pool: several independent browser/session profiles, health-scored.

A single long-lived session is the fastest way to get a whole crawl blocked: one
bad fingerprint poisons the IP, the cookie jar and the TLS profile together. The
pool lets a run rotate across profiles, and — more importantly — *learns* which
ones are burnt so it stops wasting requests on them.

Health model (0..1):
    success            +0.05
    block signal       -0.35
    hard ban (403/429) -0.60, cooldown 30s × 2^n (cap 30min)
A profile at health < 0.2 is only used when nothing else is left.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

BLOCK_STATUS = frozenset({403, 429, 503})
BAN_HINTS = (
    "captcha",
    "verify you are human",
    "are you a robot",
    "access denied",
    "unusual traffic",
    "too many requests",
    "人机验证",
    "访问过于频繁",
    "请输入验证码",
)


@dataclass
class Identity:
    name: str
    headers: dict[str, str] = field(default_factory=dict)
    proxy: Optional[str] = None
    weight: float = 1.0
    health: float = 1.0
    strikes: int = 0
    cooldown_until: float = 0.0
    last_used: float = 0.0
    requests: int = 0
    blocks: int = 0

    def available(self, now: Optional[float] = None) -> bool:
        return (now or time.monotonic()) >= self.cooldown_until

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "health": round(self.health, 3),
            "strikes": self.strikes,
            "cooldown_s": round(max(0.0, self.cooldown_until - time.monotonic()), 1),
            "requests": self.requests,
            "blocks": self.blocks,
            "proxy": bool(self.proxy),
        }


class NoIdentityAvailable(RuntimeError):
    pass


class IdentityPool:
    def __init__(self, identities: list[Identity], *, cooldown_base: float = 30.0, cooldown_cap: float = 1800.0):
        if not identities:
            raise ValueError("IdentityPool needs at least one identity")
        self._ids = identities
        self._by_name = {i.name: i for i in identities}
        self._lock = threading.Lock()
        self.cooldown_base = cooldown_base
        self.cooldown_cap = cooldown_cap
        self._rr = 0

    # ------------------------------------------------------------------ pick
    def pick(self, *, domain: str = "") -> Identity:
        """Weighted pick among healthy profiles; degrades to the least-burnt one."""
        now = time.monotonic()
        with self._lock:
            usable = [i for i in self._ids if i.available(now) and i.health > 0.2]
            if not usable:
                usable = [i for i in self._ids if i.available(now)]
            if not usable:
                raise NoIdentityAvailable(f"all identities cooling down (domain={domain or '-'})")
            total = sum(i.weight * max(i.health, 0.05) for i in usable)
            needle = random.random() * total
            acc = 0.0
            chosen = usable[-1]
            for ident in usable:
                acc += ident.weight * max(ident.health, 0.05)
                if needle <= acc:
                    chosen = ident
                    break
            chosen.requests += 1
            chosen.last_used = now
            return chosen

    # ---------------------------------------------------------------- report
    def report(self, identity: Identity | str, *, ok: bool, status: Optional[int] = None, body: str = "") -> Identity:
        ident = self._by_name[identity] if isinstance(identity, str) else identity
        now = time.monotonic()
        with self._lock:
            blocked = (not ok) or status in BLOCK_STATUS or self.looks_blocked(body)
            if not blocked:
                ident.health = min(1.0, ident.health + 0.05)
                ident.strikes = max(0, ident.strikes - 1)
                return ident

            ident.blocks += 1
            ident.strikes += 1
            penalty = 0.60 if status in BLOCK_STATUS else 0.35
            ident.health = max(0.0, ident.health - penalty)
            cooldown = min(self.cooldown_base * (2 ** (ident.strikes - 1)), self.cooldown_cap)
            ident.cooldown_until = now + cooldown
            return ident

    @staticmethod
    def looks_blocked(body: str) -> bool:
        if not body:
            return False
        sample = body[:20000].lower()
        return any(hint in sample for hint in BAN_HINTS)

    # ----------------------------------------------------------------- stats
    def stats(self) -> dict:
        with self._lock:
            return {
                "size": len(self._ids),
                "healthy": sum(1 for i in self._ids if i.health > 0.2 and i.available()),
                "identities": [i.as_dict() for i in self._ids],
            }

    def add(self, identity: Identity) -> None:
        with self._lock:
            self._ids.append(identity)
            self._by_name[identity.name] = identity


def profiles_from_specs(specs: list[dict]) -> list[Identity]:
    """Build identities from `[{"name": ..., "headers": {...}, "proxy": ...}, ...]`."""
    out = []
    for item in specs:
        out.append(
            Identity(
                name=item["name"],
                headers=dict(item.get("headers") or {}),
                proxy=item.get("proxy"),
                weight=float(item.get("weight", 1.0)),
            )
        )
    return out
