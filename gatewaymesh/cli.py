"""CLI: run <config.json> | demo"""
import argparse
import collections
import http.client
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .gateway import Gateway


def make_backend(name: str):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def do_GET(self):
            body = json.dumps({"backend": name, "path": self.path}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def demo() -> int:
    backs = [make_backend(n) for n in "ABC"]
    urls = [f"http://127.0.0.1:{b.server_address[1]}" for b in backs]
    gw = Gateway({"listen": {"host": "127.0.0.1", "port": 0},
                  "routes": [{"prefix": "/api", "upstreams": urls, "strategy": "round_robin",
                              "retries": 2, "timeout": 2}],
                  "breaker": {"failure_threshold": 3, "recovery_timeout": 30}})
    host, port = gw.start(health_checks=False)
    seen, codes = collections.Counter(), collections.Counter()
    for i in range(300):
        if i == 100:
            backs[1].shutdown()
            backs[1].server_close()
            print("-- request 100: backend B killed --")
        c = http.client.HTTPConnection(host, port, timeout=5)
        c.request("GET", "/api/ping")
        r = c.getresponse()
        r.read()
        seen[r.getheader("X-Upstream")] += 1
        codes[r.status] += 1
        c.close()
    snap = gw.snapshot()
    gw.stop()
    print("status codes :", dict(codes))
    print("served by    :", {f"backend {n}": seen.get(u.split('//')[1], 0) for n, u in zip('ABC', urls)})
    for b in snap["routes"][0]["backends"]:
        print(f"  {b['url']}  breaker={b['breaker']:<9} requests={b['requests']:<4} errors={b['errors']}")
    print("retries used :", snap["metrics"]["retries"])
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="gatewaymesh")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("config")
    sub.add_parser("demo")
    a = p.parse_args(argv)
    if a.cmd == "demo":
        return demo()
    gw = Gateway(json.load(open(a.config)))
    host, port = gw.start()
    print(f"GatewayMesh listening on http://{host}:{port}  (metrics: /_mesh/metrics)")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        gw.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
