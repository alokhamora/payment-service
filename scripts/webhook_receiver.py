"""Локальный приёмник webhook-ов для демонстрации.

    python scripts/webhook_receiver.py            # отвечает 200
    python scripts/webhook_receiver.py --status 500   # всегда 500 → проверка retry/DLQ
"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


def make_handler(status: int):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length) or b"{}")
            print(f"--> webhook (reply {status}):", json.dumps(body, indent=2, ensure_ascii=False), flush=True)
            self.send_response(status)
            self.end_headers()

        def log_message(self, *args) -> None:
            pass

    return Handler


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--status", type=int, default=200)
    args = parser.parse_args()
    print(f"Listening on :{args.port}, replying {args.status}")
    HTTPServer(("0.0.0.0", args.port), make_handler(args.status)).serve_forever()
