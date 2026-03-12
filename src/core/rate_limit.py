from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from threading import Lock
import time


@dataclass
class _Bucket:
    timestamps: deque[float]


class InMemoryRateLimiter:
    def __init__(self) -> None:
        self._lock = Lock()
        self._buckets: dict[tuple[str, str], _Bucket] = {}

    def allow(
        self,
        *,
        scope: str,
        key: str,
        max_requests: int,
        window_seconds: int,
    ) -> bool:
        now = time.monotonic()
        bucket_key = (scope, key)
        with self._lock:
            bucket = self._buckets.get(bucket_key)
            if bucket is None:
                bucket = _Bucket(timestamps=deque())
                self._buckets[bucket_key] = bucket
            cutoff = now - float(window_seconds)
            while bucket.timestamps and bucket.timestamps[0] <= cutoff:
                bucket.timestamps.popleft()
            if len(bucket.timestamps) >= max_requests:
                return False
            bucket.timestamps.append(now)
            return True

    def reset(self) -> None:
        with self._lock:
            self._buckets.clear()


rate_limiter = InMemoryRateLimiter()
