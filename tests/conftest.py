import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


class Fake:
    """Controllable upstream server that records what it receives."""

    def __init__(self, name="x", status=200, delay=0.0):
        self.name, self.status, self.delay = name, status, delay
        self.hits, self.last = 0, {}
        self.slow_paths = {}
        outer = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _do(self):
                outer.hits += 1
                n = int(self.headers.get("Content-Length") or 0)
                outer.last = {"path": self.path, "method": self.command, "headers": dict(self.headers.items()),
                              "body": self.rfile.read(n) if n else b""}
                time.sleep(outer.slow_paths.get(self.path.split("?")[0], outer.delay))
                body = json.dumps({"backend": outer.name}).encode()
                self.send_response(outer.status)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.send_header("X-Backend-Hop", "should-not-leak")
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)

            do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = _do

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.srv.daemon_threads = True
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.port = self.srv.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"

    def stop(self):
        self.srv.shutdown()
        self.srv.server_close()


def dead_url() -> str:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"http://127.0.0.1:{port}"


@pytest.fixture
def fakes():
    made = []

    def make(*a, **k):
        f = Fake(*a, **k)
        made.append(f)
        return f
    yield make
    for f in made:
        try:
            f.stop()
        except Exception:
            pass


@pytest.fixture
def start_gw():
    from gatewaymesh import Gateway
    gws = []

    def start(config, health=False):
        config.setdefault("listen", {"host": "127.0.0.1", "port": 0})
        gw = Gateway(config)
        gw.addr = gw.start(health_checks=health)
        gws.append(gw)
        return gw
    yield start
    for g in gws:
        g.stop()
