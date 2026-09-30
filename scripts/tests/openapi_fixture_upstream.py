#!/usr/bin/env python3
"""Fixture upstream for scripts/tests/openapi-edge-smoke.

Stands in for the API behind the committed Caddyfile. /openapi.json serves the
checked-in generated document with CORS and conditional caching. Other paths
answer 404 with a distinct fixture body and no CORS header. Binds to loopback and writes the port it got to --port-file.
"""

import argparse
import http.server
import hashlib
from pathlib import Path
import os

OK_BODY = (Path(__file__).resolve().parents[2] / 'apps/api/openapi.json').read_bytes().replace(b'__BIGNAME_VERSION__', b'edge-fixture').replace(b'__BIGNAME_BUILD_SHA__', b'edge-fixture-sha')
ETAG = 'W/"' + hashlib.sha256(OK_BODY).hexdigest() + '"'
MISSING_BODY = b"openapi-fixture-upstream-404"


class Handler(http.server.BaseHTTPRequestHandler):
    def _answer(self, head):
        if self.path == "/openapi.json":
            candidates = self.headers.get("If-None-Match", "").split(",")
            matched = any(tag.strip() in ("*", ETAG, ETAG[2:]) for tag in candidates)
            status, body, ctype, cors = (304, b"", "application/json", True) if matched else (200, OK_BODY, "application/json", True)
        else:
            status, body, ctype, cors = 404, MISSING_BODY, "text/plain", False
        self.send_response(status)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(body)))
        self.send_header("x-openapi-fixture", "1")
        if cors:
            self.send_header("access-control-allow-origin", "*")
            self.send_header("etag", ETAG)
            self.send_header("cache-control", "public, max-age=300")
        self.end_headers()
        if not head:
            self.wfile.write(body)

    def do_GET(self):
        self._answer(head=False)

    def do_HEAD(self):
        self._answer(head=True)

    def log_message(self, *args):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--port-file", required=True)
    args = parser.parse_args()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    tmp = args.port_file + ".tmp"
    with open(tmp, "w") as out:
        out.write(str(server.server_address[1]))
    os.replace(tmp, args.port_file)
    server.serve_forever()


if __name__ == "__main__":
    main()
