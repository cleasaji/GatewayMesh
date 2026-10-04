"""Direct-to-backend vs through-gateway latency and throughput (localhost, new connection per request)."""
import http.client
import statistics
import sys
import threading
import time
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from gatewaymesh import Gateway
from gatewaymesh.cli import make_backend

THREADS, PER = 8, 250


def run(host, port, path):
    lat, errs = [], [0]

    def worker():
        for _ in range(PER):
            t = time.perf_counter()
            try:
                c = http.client.HTTPConnection(host, port, timeout=10)
                c.request("GET", path)
                r = c.getresponse()
                r.read()
                c.close()
                if r.status != 200:
                    errs[0] += 1
            except OSError:
                errs[0] += 1
            lat.append((time.perf_counter() - t) * 1000)
    t0 = time.perf_counter()
    ts = [threading.Thread(target=worker) for _ in range(THREADS)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    wall = time.perf_counter() - t0
    lat.sort()
    return len(lat) / wall, statistics.median(lat), lat[int(len(lat) * 0.95)], errs[0]


def main():
    backs = [make_backend(n) for n in "ABC"]
    urls = [f"http://127.0.0.1:{b.server_address[1]}" for b in backs]
    gw = Gateway({"listen": {"host": "127.0.0.1", "port": 0},
                  "routes": [{"prefix": "/api", "upstreams": urls, "strategy": "least_conn"}]})
    host, port = gw.start(health_checks=False)
    run("127.0.0.1", backs[0].server_address[1], "/api/x")          # warm up
    d = run("127.0.0.1", backs[0].server_address[1], "/api/x")
    g = run(host, port, "/api/x")
    print(f"{THREADS} threads x {PER} requests, new connection per request")
    print(f"| path | req/s | p50 ms | p95 ms | errors |\n|---|---|---|---|---|")
    print(f"| direct to one backend | {d[0]:,.0f} | {d[1]:.2f} | {d[2]:.2f} | {d[3]} |")
    print(f"| via gateway (3 backends, least_conn) | {g[0]:,.0f} | {g[1]:.2f} | {g[2]:.2f} | {g[3]} |")
    print(f"gateway overhead: +{g[1]-d[1]:.2f} ms p50, +{g[2]-d[2]:.2f} ms p95")
    gw.stop()


if __name__ == "__main__":
    main()
