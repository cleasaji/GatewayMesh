"""Rate limiting and circuit breaking. Clocks are injectable so tests are deterministic."""
from __future__ import annotations

import threading
import time
from typing import Callable, Dict, Tuple


class TokenBucket:
    """Classic token bucket: ``rate`` tokens/second, holding at most ``burst``."""

    def __init__(self, rate: float, burst: float, clock: Callable[[], float] = time.monotonic):
        if rate <= 0 or burst < 1:
            raise ValueError("rate must be > 0 and burst >= 1")
        self.rate, self.burst, self.clock = rate, float(burst), clock
        self.tokens, self.last = float(burst), clock()

    def allow(self, cost: float = 1.0) -> Tuple[bool, float]:
        """Returns (allowed, seconds_until_allowed)."""
        now = self.clock()
        self.tokens = min(self.burst, self.tokens + (now - self.last) * self.rate)
        self.last = now
        if self.tokens >= cost:
            self.tokens -= cost
            return True, 0.0
        return False, (cost - self.tokens) / self.rate


class RateLimiter:
    """One bucket per client key; idle (full) buckets are purged to bound memory."""

    def __init__(self, rate: float, burst: float, clock=time.monotonic, max_keys: int = 10_000):
        self.rate, self.burst, self.clock, self.max_keys = rate, burst, clock, max_keys
        self.buckets: Dict[str, TokenBucket] = {}
        self.lock = threading.Lock()

    def _idle(self, b: TokenBucket) -> bool:
        """Fully refilled by now (tokens are only updated on use, so account for elapsed time)."""
        return b.tokens + (self.clock() - b.last) * b.rate >= b.burst

    def allow(self, key: str) -> Tuple[bool, float]:
        with self.lock:
            b = self.buckets.get(key)
            if b is None:
                if len(self.buckets) >= self.max_keys:
                    self.buckets = {k: v for k, v in self.buckets.items() if not self._idle(v)}
                    while len(self.buckets) >= self.max_keys:        # still full of active keys: evict the stalest
                        del self.buckets[min(self.buckets, key=lambda k: self.buckets[k].last)]
                b = self.buckets[key] = TokenBucket(self.rate, self.burst, self.clock)
            return b.allow()


class CircuitBreaker:
    """CLOSED -> (N consecutive failures) -> OPEN -> (timeout) -> HALF_OPEN -> probe -> CLOSED/OPEN."""
    CLOSED, OPEN, HALF_OPEN = "closed", "open", "half-open"

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 10.0,
                 clock: Callable[[], float] = time.monotonic):
        self.threshold, self.timeout, self.clock = failure_threshold, recovery_timeout, clock
        self.state, self.failures, self.opened_at, self._probing = self.CLOSED, 0, 0.0, False
        self.lock = threading.Lock()

    def peek(self) -> bool:
        """Would a request be admitted right now? (does not consume the half-open probe)"""
        with self.lock:
            if self.state == self.CLOSED:
                return True
            if self.state == self.OPEN:
                return self.clock() - self.opened_at >= self.timeout
            return not self._probing

    def allow(self) -> bool:
        with self.lock:
            if self.state == self.OPEN and self.clock() - self.opened_at >= self.timeout:
                self.state, self._probing = self.HALF_OPEN, False
            if self.state == self.CLOSED:
                return True
            if self.state == self.HALF_OPEN and not self._probing:
                self._probing = True            # exactly one trial request at a time
                return True
            return False

    def record_success(self):
        with self.lock:
            self.state, self.failures, self._probing = self.CLOSED, 0, False

    def record_failure(self):
        with self.lock:
            self._probing = False
            if self.state == self.HALF_OPEN:
                self.state, self.opened_at = self.OPEN, self.clock()
                return
            self.failures += 1
            if self.failures >= self.threshold:
                self.state, self.opened_at = self.OPEN, self.clock()
