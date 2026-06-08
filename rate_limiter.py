"""Token-bucket rate limiter shared across primary and secondary agents.

Used to throttle calls to Apollo / Lusha / SEMrush so the primary pipeline's
ThreadPoolExecutor and the SecondaryAgent cannot burst past each API's
documented rate cap. Thread-safe: every acquire() is guarded by a lock.

Design: per-API bucket with a target rate (tokens/sec). When empty, callers
block until a token refills — no busy-wait; uses Condition.wait_for with a
computed timeout based on refill rate.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Dict


# API-specific caps. Tuned to the documented plan limits with a safety buffer.
# Apollo paid: ~240 req/min → 4.0/s. We sit slightly below.
# Lusha paid: ~100 req/min → 1.66/s. We sit well below at 1.3.
# SEMrush: typically 10 req/sec on paid plans.
_DEFAULTS: Dict[str, tuple[float, float]] = {
    # service: (capacity, refill_per_sec)
    "apollo":  (10.0, 3.3),   # up to 10 burst, sustained 3.3/s = 198/min
    "lusha":   ( 5.0, 1.3),   # up to  5 burst, sustained 1.3/s =  78/min
    "semrush": (20.0, 8.0),   # up to 20 burst, sustained 8/s (below 10/s cap)
    "serpapi": ( 5.0, 1.0),   # conservative — SerpAPI has its own plan
    "openai":  (10.0, 2.0),   # OpenAI tier-1 headroom
}


@dataclass
class _Bucket:
    capacity: float
    refill_rate: float  # tokens per second
    tokens: float
    last_refill: float
    lock: threading.Lock
    cond: threading.Condition


class RateLimiter:
    """Per-API token bucket. Call .acquire(service) before each API call.

    Example:
        limiter = RateLimiter()
        limiter.acquire("apollo")         # blocks if bucket empty
        requests.post(apollo_url, ...)
    """

    def __init__(self, overrides: Dict[str, tuple[float, float]] | None = None) -> None:
        cfg = dict(_DEFAULTS)
        if overrides:
            cfg.update(overrides)
        now = time.monotonic()
        self._buckets: Dict[str, _Bucket] = {}
        for svc, (cap, rate) in cfg.items():
            lk = threading.Lock()
            self._buckets[svc] = _Bucket(
                capacity=cap,
                refill_rate=rate,
                tokens=cap,
                last_refill=now,
                lock=lk,
                cond=threading.Condition(lk),
            )

    def _refill(self, b: _Bucket, now: float) -> None:
        elapsed = now - b.last_refill
        if elapsed <= 0:
            return
        b.tokens = min(b.capacity, b.tokens + elapsed * b.refill_rate)
        b.last_refill = now

    def acquire(self, service: str, tokens: float = 1.0, timeout: float | None = 30.0) -> bool:
        """Block until `tokens` are available on the given service bucket.

        Returns True on success, False if `timeout` elapsed. An unknown service
        name is a no-op (returns True immediately) so callers can probe new
        APIs without having to register them up-front.
        """
        if service not in self._buckets:
            return True
        b = self._buckets[service]
        deadline = None if timeout is None else time.monotonic() + timeout
        with b.cond:
            while True:
                now = time.monotonic()
                self._refill(b, now)
                if b.tokens >= tokens:
                    b.tokens -= tokens
                    return True
                # Compute how long until we WOULD have enough tokens.
                deficit = tokens - b.tokens
                wait = deficit / b.refill_rate if b.refill_rate > 0 else 1.0
                if deadline is not None:
                    remaining = deadline - now
                    if remaining <= 0:
                        return False
                    wait = min(wait, remaining)
                # Cap single-wait at 1s so Python can process signals.
                b.cond.wait(timeout=min(wait, 1.0))

    def snapshot(self) -> Dict[str, Dict[str, float]]:
        """Return current token levels. Debug/observability only."""
        out: Dict[str, Dict[str, float]] = {}
        now = time.monotonic()
        for svc, b in self._buckets.items():
            with b.lock:
                self._refill(b, now)
                out[svc] = {
                    "tokens": round(b.tokens, 2),
                    "capacity": b.capacity,
                    "refill_rate": b.refill_rate,
                }
        return out


# Module-level singleton. The pipeline imports `GLOBAL_LIMITER` and every
# API client calls `GLOBAL_LIMITER.acquire("apollo")` before issuing a
# request. Overridable per-process by replacing the binding before first use.
GLOBAL_LIMITER = RateLimiter()


if __name__ == "__main__":
    # Quick smoke test: acquire 3 semrush tokens in quick succession, then
    # confirm the 4th call blocks briefly until refill.
    rl = RateLimiter(overrides={"semrush": (3.0, 2.0)})
    start = time.monotonic()
    for _ in range(3):
        assert rl.acquire("semrush")
    # 4th should block ~0.5s (need 1 token at 2/s)
    assert rl.acquire("semrush", timeout=2.0)
    elapsed = time.monotonic() - start
    assert 0.3 < elapsed < 1.5, f"unexpected wait: {elapsed}"
    print(f"rate_limiter smoke ok (4th acquire after {elapsed:.2f}s)")
