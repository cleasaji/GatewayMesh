"""Reverse-proxy API gateway: routing, load balancing, retries, circuit breaking,
rate limiting, API-key auth, health checks and metrics. Standard library only."""
from __future__ import annotations

import hmac
import http.client
import itertools
import json
import random
import socket
import threading
import time
import uuid
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional
from urllib.parse import urlsplit

from .resilience import CircuitBreaker, RateLimiter

HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te",
              "trailers", "transfer-encoding", "upgrade", "proxy-connection"}
IDEMPOTENT = {"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}
STRATEGIES = {"round_robin", "least_conn", "p2c"}


class ConfigError(ValueError):
    pass


class Backend:
    def __init__(self, url: str, breaker: CircuitBreaker):
        u = urlsplit(url)
        if u.scheme != "http" or not u.hostname:
            raise ConfigError(f"upstream must be an http:// URL, got {url!r}")
        self.url, self.host, self.port = url, u.hostname, u.port or 80
        self.breaker = breaker
        self.healthy = True
        self.active = 0
        self.total = self.errors = 0
        self.ewma_ms = 1.0
        self._ok = self._bad = 0
        self.lock = threading.Lock()

    @property
    def name(self) -> str:
        return f"{self.host}:{self.port}"

    def usable(self) -> bool:
        return self.healthy and self.breaker.peek()

    def snapshot(self) -> dict:
        return {"url": self.url, "healthy": self.healthy, "breaker": self.breaker.state,
                "active": self.active, "requests": self.total, "errors": self.errors,
                "ewma_ms": round(self.ewma_ms, 2)}


class Pool:
    def __init__(self, urls: List[str], strategy: str, breaker_cfg: dict):
        self.backends = [Backend(u, CircuitBreaker(**breaker_cfg)) for u in urls]
        self.strategy = strategy
        self._rr = itertools.count()
        self._lock = threading.Lock()

    def pick(self, exclude: set) -> Optional[Backend]:
        cands = [b for b in self.backends if b not in exclude and b.usable()]
        if self.strategy == "least_conn":
            order = sorted(cands, key=lambda b: (b.active, b.total))
        elif self.strategy == "p2c":                       # power of two choices, by load x latency
            pair = random.sample(cands, min(2, len(cands)))
            order = sorted(pair, key=lambda b: (b.active + 1) * b.ewma_ms) + \
                [b for b in cands if b not in pair]
        else:
            with self._lock:
                start = next(self._rr)
            order = [cands[(start + i) % len(cands)] for i in range(len(cands))] if cands else []
        for b in order:
            if b.breaker.allow():                          # claims the half-open probe if needed
                return b
        return None


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.requests = 0
        self.by_status: Dict[str, int] = {}
        self.latency = deque(maxlen=4096)
        self.rate_limited = self.retries = 0

    def record(self, status: int, ms: float):
        with self.lock:
            self.requests += 1
            k = f"{status // 100}xx"
            self.by_status[k] = self.by_status.get(k, 0) + 1
            self.latency.append(ms)

    def summary(self) -> dict:
        with self.lock:
            lat = sorted(self.latency)
            pick = lambda p: round(lat[min(len(lat) - 1, int(p * len(lat)))], 2) if lat else None
            return {"requests": self.requests, "by_status": dict(self.by_status),
                    "rate_limited": self.rate_limited, "retries": self.retries,
                    "latency_ms": {"p50": pick(0.50), "p95": pick(0.95), "p99": pick(0.99)}}


class Gateway:
    def __init__(self, config: dict):
        routes = config.get("routes") or []
        if not routes:
            raise ConfigError("config needs at least one route")
        br = {"failure_threshold": 5, "recovery_timeout": 10.0, **config.get("breaker", {})}
        self.routes = []
        for r in routes:
            if not str(r.get("prefix", "")).startswith("/") or not r.get("upstreams"):
                raise ConfigError("each route needs a '/prefix' and a non-empty 'upstreams' list")
            strat = r.get("strategy", "round_robin")
            if strat not in STRATEGIES:
                raise ConfigError(f"unknown strategy {strat!r}; choose from {sorted(STRATEGIES)}")
            self.routes.append({"prefix": r["prefix"].rstrip("/") or "/", "strip": r.get("strip_prefix", False),
                                "retries": int(r.get("retries", 2)), "timeout": float(r.get("timeout", 5)),
                                "pool": Pool(r["upstreams"], strat, br)})
        self.routes.sort(key=lambda r: -len(r["prefix"]))          # longest prefix wins
        rl = config.get("rate_limit")
        self.limiter = RateLimiter(rl["rate"], rl.get("burst", rl["rate"])) if rl else None
        self.auth_keys = [k.encode() for k in config.get("auth_keys", [])]
        self.max_body = int(config.get("max_body_bytes", 10 * 1024 * 1024))
        self.health = {"path": "/health", "interval": 5.0, "fall": 2, "rise": 1, **config.get("health", {})}
        self.metrics = Metrics()
        self.listen = config.get("listen", {"host": "127.0.0.1", "port": 8080})
        self.server: Optional[ThreadingHTTPServer] = None
        self._stop = threading.Event()

    # -- routing helpers ---------------------------------------------------
    def match(self, path: str):
        for r in self.routes:
            p = r["prefix"]
            if p == "/" or path == p or path.startswith(p + "/") or path.startswith(p + "?"):
                return r
        return None

    def backends(self):
        return [b for r in self.routes for b in r["pool"].backends]

    def authorized(self, key: Optional[str]) -> bool:
        if not self.auth_keys:
            return True
        return key is not None and any(hmac.compare_digest(key.encode(), k) for k in self.auth_keys)

    # -- health checking ---------------------------------------------------
    def check_once(self):
        for b in self.backends():
            ok = False
            try:
                c = http.client.HTTPConnection(b.host, b.port, timeout=1.0)
                c.request("GET", self.health["path"])
                ok = c.getresponse().status < 500
                c.close()
            except (OSError, http.client.HTTPException):
                pass
            if ok:
                b._ok, b._bad = b._ok + 1, 0
                if not b.healthy and b._ok >= self.health["rise"]:
                    b.healthy = True
            else:
                b._bad, b._ok = b._bad + 1, 0
                if b.healthy and b._bad >= self.health["fall"]:
                    b.healthy = False

    def _health_loop(self):
        while not self._stop.wait(self.health["interval"]):
            self.check_once()

    # -- lifecycle ---------------------------------------------------------
    def start(self, health_checks: bool = True):
        gw = self

        class Handler(_Handler):
            gateway = gw
        self.server = ThreadingHTTPServer((self.listen["host"], self.listen["port"]), Handler)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        if health_checks:
            threading.Thread(target=self._health_loop, daemon=True).start()
        return self.server.server_address

    def stop(self):
        self._stop.set()
        if self.server:
            self.server.shutdown()
            self.server.server_close()

    def snapshot(self) -> dict:
        return {"metrics": self.metrics.summary(),
                "routes": [{"prefix": r["prefix"], "strategy": r["pool"].strategy,
                            "backends": [b.snapshot() for b in r["pool"].backends]} for r in self.routes]}


class _Handler(BaseHTTPRequestHandler):
    gateway: Gateway
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send(self, status: int, body: bytes = b"", headers=None, head: bool = False):
        self.send_response(status)
        for k, v in (headers or []):
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head and status not in (204, 304):
            self.wfile.write(body)

    def _error(self, status: int, msg: str, rid: str, extra=None):
        body = json.dumps({"error": msg, "request_id": rid}).encode()
        self._send(status, body, [("Content-Type", "application/json"), ("X-Request-Id", rid)] + (extra or []))
        return status

    def _proxy(self):
        gw, t0 = self.gateway, time.perf_counter()
        rid = self.headers.get("X-Request-Id") or uuid.uuid4().hex[:16]
        status = self._route(gw, rid)
        gw.metrics.record(status, (time.perf_counter() - t0) * 1000)

    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = do_OPTIONS = _proxy

    def _route(self, gw: Gateway, rid: str) -> int:
        path = self.path
        if path == "/_mesh/health":
            self._send(200, b'{"status":"ok"}', [("Content-Type", "application/json")])
            return 200
        if path == "/_mesh/metrics":
            self._send(200, json.dumps(gw.snapshot()).encode(), [("Content-Type", "application/json")])
            return 200
        api_key = self.headers.get("X-API-Key")
        if not gw.authorized(api_key):
            return self._error(401, "missing or invalid API key", rid)
        if gw.limiter:
            ok, wait = gw.limiter.allow(api_key or self.client_address[0])
            if not ok:
                gw.metrics.rate_limited += 1
                return self._error(429, "rate limit exceeded", rid, [("Retry-After", str(max(1, round(wait + 0.499))))])
        route = gw.match(path)
        if route is None:
            return self._error(404, "no route for this path", rid)
        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            return self._error(411, "chunked request bodies are not supported", rid)
        n = int(self.headers.get("Content-Length") or 0)
        if n > gw.max_body:
            return self._error(413, "request body too large", rid)
        body = self.rfile.read(n) if n else b""

        upstream_path = path
        if route["strip"] and route["prefix"] != "/":
            upstream_path = path[len(route["prefix"]):] or "/"
            if not upstream_path.startswith("/"):
                upstream_path = "/" + upstream_path
        return self._forward(gw, route, upstream_path, body, rid)

    def _forward(self, gw, route, upath, body, rid) -> int:
        method = self.command
        attempts = 1 + (route["retries"] if method in IDEMPOTENT else 0)
        tried, fallback, last_err = set(), None, (503, "no healthy upstream")
        drop = {h.strip().lower() for h in self.headers.get("Connection", "").split(",") if h.strip()}
        fwd = [(k, v) for k, v in self.headers.items()
               if k.lower() not in HOP_BY_HOP and k.lower() not in drop and k.lower() not in ("host", "content-length")]
        xff = self.headers.get("X-Forwarded-For")
        fwd += [("X-Forwarded-For", f"{xff}, {self.client_address[0]}" if xff else self.client_address[0]),
                ("X-Forwarded-Host", self.headers.get("Host", "")), ("X-Forwarded-Proto", "http"),
                ("X-Request-Id", rid)]
        for attempt in range(attempts):
            b = route["pool"].pick(tried)
            if b is None:
                break
            tried.add(b)
            if attempt:
                gw.metrics.retries += 1
            with b.lock:
                b.active += 1
                b.total += 1
            t0 = time.perf_counter()
            try:
                conn = http.client.HTTPConnection(b.host, b.port, timeout=route["timeout"])
                conn.request(method, upath, body=body or None, headers=dict(fwd))
                resp = conn.getresponse()
                data = resp.read()
                conn.close()
            except (OSError, http.client.HTTPException) as e:
                b.breaker.record_failure()
                b.errors += 1
                last_err = (504, "upstream timed out") if isinstance(e, (socket.timeout, TimeoutError)) \
                    else (502, "upstream unreachable")
                continue
            finally:
                with b.lock:
                    b.active -= 1
            ms = (time.perf_counter() - t0) * 1000
            b.ewma_ms = 0.8 * b.ewma_ms + 0.2 * ms
            if resp.status >= 500:
                b.breaker.record_failure()
                b.errors += 1
                fallback = (resp, data, b)
                continue
            b.breaker.record_success()
            return self._relay(resp, data, b, rid)
        if fallback:                                            # every attempt returned 5xx: pass the last one on
            return self._relay(*fallback, rid)
        return self._error(*last_err, rid)

    def _relay(self, resp, data, b, rid) -> int:
        out = [(k, v) for k, v in resp.getheaders()
               if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"]
        out += [("X-Upstream", b.name), ("X-Request-Id", rid)]
        self._send(resp.status, data, out, head=self.command == "HEAD")
        return resp.status
