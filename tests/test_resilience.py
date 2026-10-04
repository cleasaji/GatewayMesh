import pytest
from gatewaymesh import TokenBucket, CircuitBreaker, RateLimiter


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_token_bucket_burst_then_deny_then_refill():
    c = Clock()
    tb = TokenBucket(rate=2, burst=3, clock=c)
    assert [tb.allow()[0] for _ in range(3)] == [True, True, True]
    ok, wait = tb.allow()
    assert not ok and wait == pytest.approx(0.5)
    c.t += 0.5
    assert tb.allow()[0] and not tb.allow()[0]


def test_token_bucket_never_exceeds_burst():
    c = Clock()
    tb = TokenBucket(rate=100, burst=2, clock=c)
    c.t += 1000
    assert [tb.allow()[0] for _ in range(3)] == [True, True, False]


def test_token_bucket_validation():
    with pytest.raises(ValueError):
        TokenBucket(0, 1)


def test_rate_limiter_isolates_clients_and_purges_idle():
    c = Clock()
    rl = RateLimiter(1, 1, clock=c, max_keys=3)
    assert rl.allow("a")[0] and not rl.allow("a")[0] and rl.allow("b")[0]
    c.t += 5
    for k in ("c", "d", "e"):
        rl.allow(k)
    assert len(rl.buckets) <= 3


def test_rate_limiter_memory_is_hard_capped_even_with_all_keys_active():
    c = Clock()
    rl = RateLimiter(1, 1, clock=c, max_keys=50)
    for i in range(5000):
        rl.allow(f"attacker-{i}")          # rotating identities, none idle
    assert len(rl.buckets) <= 50


def test_breaker_full_lifecycle():
    c = Clock()
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=10, clock=c)
    for _ in range(2):
        cb.record_failure()
    assert cb.state == "closed" and cb.allow()
    cb.record_failure()
    assert cb.state == "open" and not cb.allow() and not cb.peek()
    c.t += 9.9
    assert not cb.allow()
    c.t += 0.2
    assert cb.peek() and cb.allow() and cb.state == "half-open"
    assert not cb.allow()                       # only ONE probe at a time
    cb.record_failure()
    assert cb.state == "open"                   # failed probe re-opens immediately
    c.t += 10
    assert cb.allow()
    cb.record_success()
    assert cb.state == "closed" and cb.allow()


def test_success_resets_failure_count():
    cb = CircuitBreaker(failure_threshold=3)
    cb.record_failure()
    cb.record_failure()
    cb.record_success()
    cb.record_failure()
    cb.record_failure()
    assert cb.state == "closed"
