import http.client
import json
import threading
import time
from collections import Counter

import pytest
from conftest import dead_url
from gatewaymesh import Gateway, ConfigError


def call(gw, method="GET", path="/api/x", body=None, headers=None):
    c = http.client.HTTPConnection(*gw.addr, timeout=10)
    c.request(method, path, body=body, headers=headers or {})
    r = c.getresponse()
    data = r.read()
    out = (r.status, data, {k.lower(): v for k, v in r.getheaders()})
    c.close()
    return out


def cfg(urls, **route):
    return {"routes": [{"prefix": "/api", "upstreams": urls, **route}]}


def test_round_robin_is_even(fakes, start_gw):
    fs = [fakes(n) for n in "abc"]
    gw = start_gw(cfg([f.url for f in fs]))
    for _ in range(30):
        assert call(gw)[0] == 200
    assert [f.hits for f in fs] == [10, 10, 10]


def test_least_conn_avoids_busy_backend(fakes, start_gw):
    slow, fast = fakes("slow"), fakes("fast")
    slow.slow_paths["/api/slow"] = 0.8
    gw = start_gw(cfg([slow.url, fast.url], strategy="least_conn"))
    t = threading.Thread(target=call, args=(gw,), kwargs={"path": "/api/slow"})
    t.start()
    time.sleep(0.25)
    served = Counter(json.loads(call(gw)[1])["backend"] for _ in range(6))
    t.join()
    assert served == {"fast": 6}


def test_p2c_strategy_serves_everything(fakes, start_gw):
    fs = [fakes(n) for n in "ab"]
    gw = start_gw(cfg([f.url for f in fs], strategy="p2c"))
    assert all(call(gw)[0] == 200 for _ in range(20))
    assert sum(f.hits for f in fs) == 20


def test_get_fails_over_from_dead_backend(fakes, start_gw):
    live = fakes("live")
    gw = start_gw(cfg([dead_url(), live.url], retries=2))
    assert [call(gw)[0] for _ in range(10)] == [200] * 10


def test_non_idempotent_post_is_not_retried(fakes, start_gw):
    live = fakes("live")
    gw = start_gw(cfg([dead_url(), live.url], retries=3))
    assert call(gw, "POST", body=b"x")[0] == 502          # first pick is the dead one, no retry
    assert live.hits == 0
    assert call(gw, "POST", body=b"x")[0] == 200


def test_5xx_is_retried_on_another_backend(fakes, start_gw):
    bad, good = fakes("bad", status=500), fakes("good")
    gw = start_gw(cfg([bad.url, good.url], retries=2))
    assert [call(gw)[0] for _ in range(8)] == [200] * 8


def test_all_5xx_returns_last_upstream_response(fakes, start_gw):
    a, b = fakes("a", status=503), fakes("b", status=503)
    gw = start_gw(cfg([a.url, b.url], retries=2))
    assert call(gw)[0] == 503


def test_circuit_opens_then_probes_after_recovery(fakes, start_gw):
    bad, good = fakes("bad", status=500), fakes("good")
    gw = start_gw({**cfg([bad.url, good.url], retries=2),
                   "breaker": {"failure_threshold": 3, "recovery_timeout": 0.4}})
    for _ in range(20):
        assert call(gw)[0] == 200
    assert bad.hits == 3                                   # breaker opened after exactly 3 failures
    assert gw.routes[0]["pool"].backends[0].breaker.state == "open"
    time.sleep(0.5)
    for _ in range(6):
        call(gw)
    assert bad.hits == 4                                   # exactly one half-open probe, which failed


def test_circuit_closes_when_backend_recovers(fakes, start_gw):
    flaky, good = fakes("flaky", status=500), fakes("good")
    gw = start_gw({**cfg([flaky.url, good.url], retries=2),
                   "breaker": {"failure_threshold": 2, "recovery_timeout": 0.3}})
    for _ in range(6):
        call(gw)
    flaky.status = 200
    time.sleep(0.4)
    for _ in range(6):
        call(gw)
    assert gw.routes[0]["pool"].backends[0].breaker.state == "closed"


def test_timeout_gives_504(fakes, start_gw):
    slow = fakes("slow", delay=0.6)
    gw = start_gw(cfg([slow.url], timeout=0.15, retries=0))
    assert call(gw)[0] == 504


def test_all_unreachable_gives_502_then_503_once_marked_unhealthy(start_gw):
    gw = start_gw({**cfg([dead_url(), dead_url()], retries=2),
                   "breaker": {"failure_threshold": 99}, "health": {"fall": 1}})
    assert call(gw)[0] == 502
    gw.check_once()
    assert call(gw)[0] == 503


def test_health_check_removes_and_restores_backend(fakes, start_gw):
    a, b = fakes("a"), fakes("b")
    gw = start_gw({**cfg([a.url, b.url]), "health": {"path": "/health", "fall": 1, "rise": 1}})
    be = gw.routes[0]["pool"].backends
    b.stop()
    gw.check_once()
    assert [x.healthy for x in be] == [True, False]
    assert {json.loads(call(gw)[1])["backend"] for _ in range(6)} == {"a"}


def test_longest_prefix_routing_and_strip_prefix(fakes, start_gw):
    api, users = fakes("api"), fakes("users")
    gw = start_gw({"routes": [{"prefix": "/api", "upstreams": [api.url]},
                              {"prefix": "/api/users", "upstreams": [users.url], "strip_prefix": True}]})
    assert json.loads(call(gw, path="/api/users/42?x=1")[1])["backend"] == "users"
    assert users.last["path"] == "/42?x=1"                 # prefix stripped, query kept
    assert json.loads(call(gw, path="/api/other")[1])["backend"] == "api"
    assert api.last["path"] == "/api/other"
    assert call(gw, path="/nothing")[0] == 404
    assert call(gw, path="/apix")[0] == 404                # '/api' must not match '/apix'


def test_headers_and_body_forwarding(fakes, start_gw):
    f = fakes("a")
    gw = start_gw(cfg([f.url]))
    st, data, h = call(gw, "POST", body=b"hello=1", headers={
        "Connection": "keep-alive, X-Secret", "X-Secret": "s", "X-Custom": "yes", "Content-Type": "text/plain"})
    assert st == 200 and f.last["body"] == b"hello=1"
    sent = {k.lower(): v for k, v in f.last["headers"].items()}
    assert sent["x-forwarded-for"] == "127.0.0.1" and "x-request-id" in sent
    assert sent["x-custom"] == "yes" and "x-secret" not in sent       # listed in Connection => dropped
    assert h["x-upstream"] == f"127.0.0.1:{f.port}" and h["x-request-id"] == sent["x-request-id"]


def test_existing_xff_is_appended(fakes, start_gw):
    f = fakes("a")
    gw = start_gw(cfg([f.url]))
    call(gw, headers={"X-Forwarded-For": "9.9.9.9"})
    assert {k.lower(): v for k, v in f.last["headers"].items()}["x-forwarded-for"] == "9.9.9.9, 127.0.0.1"


def test_head_has_no_body(fakes, start_gw):
    gw = start_gw(cfg([fakes("a").url]))
    st, data, _ = call(gw, "HEAD")
    assert st == 200 and data == b""


def test_rate_limit_429_with_retry_after(fakes, start_gw):
    gw = start_gw({**cfg([fakes("a").url]), "rate_limit": {"rate": 1, "burst": 3}})
    codes = [call(gw)[0] for _ in range(5)]
    assert codes[:3] == [200] * 3 and codes[3:] == [429, 429]
    _, _, h = call(gw)
    assert int(h["retry-after"]) >= 1
    assert gw.metrics.rate_limited >= 3


def test_api_key_auth_and_per_key_limits(fakes, start_gw):
    gw = start_gw({**cfg([fakes("a").url]), "auth_keys": ["k1", "k2"], "rate_limit": {"rate": 1, "burst": 2}})
    assert call(gw)[0] == 401
    assert call(gw, headers={"X-API-Key": "nope"})[0] == 401
    assert [call(gw, headers={"X-API-Key": "k1"})[0] for _ in range(3)] == [200, 200, 429]
    assert call(gw, headers={"X-API-Key": "k2"})[0] == 200          # different key, own bucket


def test_internal_endpoints_bypass_auth_and_report_metrics(fakes, start_gw):
    gw = start_gw({**cfg([fakes("a").url]), "auth_keys": ["k"]})
    assert call(gw, path="/_mesh/health")[0] == 200
    call(gw, headers={"X-API-Key": "k"})
    st, data, _ = call(gw, path="/_mesh/metrics")
    m = json.loads(data)
    assert st == 200 and m["metrics"]["requests"] >= 2 and m["routes"][0]["backends"][0]["requests"] == 1


def test_body_size_limit_and_chunked_rejected(fakes, start_gw):
    gw = start_gw({**cfg([fakes("a").url]), "max_body_bytes": 10})
    assert call(gw, "POST", body=b"x" * 11)[0] == 413
    c = http.client.HTTPConnection(*gw.addr)
    c.putrequest("POST", "/api/x")
    c.putheader("Transfer-Encoding", "chunked")
    c.endheaders()
    c.send(b"1\r\nx\r\n0\r\n\r\n")
    assert c.getresponse().status == 411


@pytest.mark.parametrize("bad", [
    {},
    {"routes": [{"prefix": "api", "upstreams": ["http://x:1"]}]},
    {"routes": [{"prefix": "/a", "upstreams": []}]},
    {"routes": [{"prefix": "/a", "upstreams": ["http://x:1"], "strategy": "magic"}]},
    {"routes": [{"prefix": "/a", "upstreams": ["ftp://x:1"]}]},
])
def test_config_validation(bad):
    with pytest.raises(ConfigError):
        Gateway(bad)


def test_concurrent_load_no_errors_and_balanced(fakes, start_gw):
    fs = [fakes(n) for n in "abc"]
    gw = start_gw(cfg([f.url for f in fs], strategy="least_conn"))
    results = []

    def worker():
        for _ in range(25):
            results.append(call(gw)[0])
    ts = [threading.Thread(target=worker) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert results.count(200) == 200
    assert min(f.hits for f in fs) > 40
