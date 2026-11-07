"""Content-level de-duplication for crawls that revisit the same page in many URLs.

URL dedupe (the frontier's job) misses the common cases:
* `?page=1` vs `/`
* print views, AMP mirrors, locale aliases
* the same article syndicated across paths

So we fingerprint the *text* with SimHash (64-bit) and look for near duplicates
by Hamming distance. The index is banded (4×16 bits), which turns the search into
four dictionary lookups instead of a scan.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional

_TOKEN_RE = re.compile(r"[a-z0-9]+|[\u4e00-\u9fff]", re.I)
_WS_RE = re.compile(r"\s+")
MASK64 = (1 << 64) - 1


def normalize_text(text: str) -> str:
    return _WS_RE.sub(" ", text or "").strip().lower()


def features(text: str, *, shingle: int = 3) -> Iterator[str]:
    """Word 3-shingles for latin text, character 4-grams for CJK runs."""
    tokens = _TOKEN_RE.findall(normalize_text(text))
    if not tokens:
        return iter(())
    cjk = [t for t in tokens if "\u4e00" <= t <= "\u9fff"]
    if cjk and len(cjk) > len(tokens) // 2:
        joined = "".join(tokens)
        grams = [joined[i : i + 4] for i in range(max(1, len(joined) - 3))]
        return iter(grams)
    if len(tokens) < shingle:
        return iter(tokens)
    return iter(" ".join(tokens[i : i + shingle]) for i in range(len(tokens) - shingle + 1))


def simhash64(text: str, *, shingle: int = 3) -> int:
    """64-bit SimHash. Empty input yields 0 so callers can treat it as "no signal"."""
    weights = [0] * 64
    seen = 0
    for feat in features(text, shingle=shingle):
        digest = int.from_bytes(hashlib.blake2b(feat.encode("utf-8"), digest_size=8).digest(), "big")
        seen += 1
        for bit in range(64):
            weights[bit] += 1 if (digest >> bit) & 1 else -1
    if not seen:
        return 0
    value = 0
    for bit in range(64):
        if weights[bit] > 0:
            value |= 1 << bit
    return value


def hamming64(a: int, b: int) -> int:
    return bin((a ^ b) & MASK64).count("1")


def content_hash(text: str) -> str:
    return hashlib.sha256(normalize_text(text).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class DupHit:
    key: str
    simhash: int
    distance: int


class NearDupIndex:
    """Banded SimHash index.

    Recall guarantee (pigeonhole): if two hashes differ in `d` bits and we cut the
    64 bits into `b` bands, they must share at least one identical band whenever
    `d < b`. So `bands` must exceed the threshold, or some near-duplicates are
    missed without any warning — that is why the constructor rejects
    `threshold >= bands` instead of silently degrading.

    Default `bands=8` (8-bit bands) guarantees recall up to distance 7.
    Candidates are always verified with the exact Hamming distance before use.
    """

    def __init__(self, *, threshold: int = 6, bands: int = 8, max_bucket: int = 4096):
        if not 0 <= threshold < 32:
            raise ValueError("threshold must be in [0, 32)")
        if bands < 1 or 64 % bands:
            raise ValueError("bands must divide 64")
        if threshold >= bands:
            raise ValueError(f"threshold={threshold} >= bands={bands}: recall is no longer guaranteed")
        self.threshold = threshold
        self.bands = bands
        self.max_bucket = max_bucket
        self._buckets: dict[tuple[int, int], dict[str, int]] = {}
        self._hashes: dict[str, int] = {}
        self._sizes: dict[tuple[int, int], int] = {}

    def _band_keys(self, value: int) -> Iterable[tuple[int, int]]:
        width = 64 // self.bands
        mask = (1 << width) - 1
        for i in range(self.bands):
            yield i, (value >> (i * width)) & mask

    def add(self, key: str, value: int) -> Optional[DupHit]:
        """Register `key`. Returns the existing near-duplicate if one is found."""
        hit = self.find(value)
        if hit is not None:
            return hit
        self._hashes[key] = value
        for band in self._band_keys(value):
            bucket = self._buckets.setdefault(band, {})
            bucket[key] = value
            self._sizes[band] = len(bucket)
        return None

    def find(self, value: int) -> Optional[DupHit]:
        if value == 0:
            return None
        best: Optional[DupHit] = None
        for band in self._band_keys(value):
            bucket = self._buckets.get(band)
            if not bucket:
                continue
            for key, other in bucket.items():
                distance = hamming64(value, other)
                if distance <= self.threshold and (best is None or distance < best.distance):
                    best = DupHit(key, other, distance)
                    if distance == 0:
                        return best
        return best

    def __len__(self) -> int:
        return len(self._hashes)

    def __contains__(self, key: str) -> bool:
        return key in self._hashes

    def stats(self) -> dict:
        return {
            "items": len(self._hashes),
            "bands": self.bands,
            "threshold": self.threshold,
            "max_bucket": max((self._sizes.values() or [0])),
        }
