# GatewayMesh

A resilient **API gateway / reverse proxy** in pure Python (standard library only): routing, three load-balancing
strategies, retries with failover, per-backend **circuit breakers**, **token-bucket rate limiting**, API-key auth,
active health checks and a metrics endpoint.

```
$ python -m gatewaymesh demo          # 3 backends; backend B is killed at request 100
-- request 100: backend B killed --
status codes : {200: 300}
served by    : {'backend A': 135, 'backend B': 33, 'backend C': 132}
  breaker=closed    requests=135  errors=0
  breaker=open      requests=36   errors=3        <- tripped after exactly 3 failures
  breaker=closed    requests=132  errors=0
retries used : 3
```
All 300 requests succeeded across a backend failure: the only visible effect was 3 retried requests.

## Features

| Capability | How it works |
|---|---|
| **Routing** | longest-prefix match (`/api/users` beats `/api`), optional `strip_prefix`, query strings preserved, `/apix` never matches `/api` |
| **Load balancing** | `round_robin`, `least_conn` (fewest in-flight requests), `p2c` (power-of-two-choices weighted by load x latency EWMA) |
| **Retries** | only for idempotent methods (GET/HEAD/OPTIONS/PUT/DELETE); never retries POST/PATCH; each retry goes to a *different* backend; 5xx responses are retried too |
| **Circuit breaker** | per backend: CLOSED -> OPEN after N consecutive failures -> HALF-OPEN after a timeout, letting exactly **one** probe through; a failed probe re-opens immediately |
| **Rate limiting** | token bucket per API key (or client IP), `429` + `Retry-After`; bucket table is memory-bounded |
| **Auth** | optional `X-API-Key` allow-list, constant-time comparison |
| **Health checks** | active `GET /health` with `fall`/`rise` thresholds; unhealthy backends leave rotation, no healthy backend gives `503` |
| **Proxy hygiene** | strips hop-by-hop headers (and any header named in `Connection:`), adds `X-Forwarded-For/Host/Proto` and `X-Request-Id`, tags responses with `X-Upstream` |
| **Errors** | `502` unreachable, `504` timeout, `503` no healthy upstream, `413` oversize body, `411` chunked request bodies |
| **Observability** | `/_mesh/metrics` (JSON): status classes, retries, rate-limited count, p50/p95/p99, per-backend breaker state and EWMA latency; `/_mesh/health` |

## Run it

```json
{
  "listen": {"host": "0.0.0.0", "port": 8080},
  "routes": [
    {"prefix": "/api", "upstreams": ["http://10.0.0.5:9000", "http://10.0.0.6:9000"],
     "strategy": "least_conn", "retries": 2, "timeout": 5, "strip_prefix": false}
  ],
  "rate_limit": {"rate": 50, "burst": 100},
  "auth_keys": ["key-one", "key-two"],
  "breaker": {"failure_threshold": 5, "recovery_timeout": 10},
  "health": {"path": "/health", "interval": 5, "fall": 2, "rise": 1}
}
```
```
python -m gatewaymesh run config.json
```

## Measured (localhost, Python 3.12, 8 threads x 250 requests, new connection per request)

| path | req/s | p50 ms | p95 ms | errors |
|---|---|---|---|---|
| direct to one backend | 1,708 | 1.85 | 2.53 | 0 |
| via gateway (3 backends, least_conn) | 832 | 5.91 | 9.14 | 0 |

The gateway adds about **4 ms at p50** and halves throughput on this micro-benchmark. Most of that cost is opening a fresh TCP
connection to the upstream for every request, plus Python thread-per-request handling. `benchmarks/bench.py` reproduces it.
This is a correct, readable resilience layer, not a high-throughput proxy; it is not competing with nginx/Envoy.

## What the tests prove (33 tests, real HTTP servers on ephemeral ports)

Even round-robin split (10/10/10), least-conn dodging a busy backend, failover from a dead backend, POST *not* retried,
5xx retried elsewhere, breaker opening after exactly 3 failures and admitting exactly one probe after recovery,
recovery closing the breaker, 504 on timeout, 502 vs 503 behaviour, health-check removal and restore, prefix routing,
header hygiene, rate limiting + `Retry-After`, per-key limits, auth, body limits, config validation, and 200 concurrent
requests across 8 threads with zero errors. Breaker and bucket logic use an injected clock for deterministic unit tests.

The tests also caught a real bug in my own code: the rate limiter's idle-client purge compared stale token counts, so
rotating client identities could grow memory without bound. It now accounts for elapsed refill time and has a hard cap (regression tests included).

## Limitations

- HTTP/1.1 only, upstreams must be plain `http://`; no TLS termination, HTTP/2, WebSockets or streaming (bodies are buffered).
- No upstream connection pooling (see the benchmark); no weighted backends; no request queueing.
- Rate limits and breaker state are per process (not shared across gateway instances).
- Chunked *request* bodies are rejected with `411`.

## Development
```
pip install -e .[dev] && pytest -q       # ~25 s (several tests wait on timeouts)
python benchmarks/bench.py
```
MIT licensed.
