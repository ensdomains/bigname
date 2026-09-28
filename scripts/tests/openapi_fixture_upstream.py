#!/usr/bin/env python3
"""Fixture upstream for scripts/tests/openapi-edge-smoke.

Stands in for the API behind the committed Caddyfile. /openapi.json?case=ok
answers 200 with a JSON body and its own wildcard CORS header; every other
GET or HEAD answers 404 with a body no other part of the stack produces and
no CORS header. Binds to loopback and writes the port it got to --port-file.
"""

import argparse
import http.server
import os

OK_BODY = b'{"openapi":"3.1.0","fixture":"openapi-edge-smoke"}'
MISSING_BODY = b"openapi-fixture-upstream-404"


class Handler(http.server.BaseHTTPRequestHandler):
    def _answer(self, head):
        if self.path == "/openapi.json?case=ok":
            status, body, ctype, cors = 200, OK_BODY, "application/json", True
        else:
            status, body, ctype, cors = 404, MISSING_BODY, "text/plain", False
        self.send_response(status)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(body)))
        self.send_header("x-openapi-fixture", "1")
        if cors:
            self.send_header("access-control-allow-origin", "*")
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
