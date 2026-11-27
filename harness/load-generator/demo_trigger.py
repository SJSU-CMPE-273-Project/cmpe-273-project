"""Internal-only, fixed three-event smoke trigger used by the public console."""
from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MAX_EVENTS = 3
COOLDOWN_SECONDS = 30
RUN_TIMEOUT_SECONDS = 30


def command(target):
    if target not in ("adapter", "iggy"):
        raise ValueError("DEMO_TARGET must be adapter or iggy")
    # All traffic parameters are fixed. Network credentials and destinations come
    # only from deployment environment variables, never from the HTTP request.
    return [sys.executable, "-m", "loadgen", "run", "--target", target,
            "--phase", "3s@1", "--accounts", "3", "--mix", "deposit=1",
            "--seed", "273", "--max-attempts", "1"]


def run_smoke(target):
    result = subprocess.run(command(target), capture_output=True, text=True,
                            timeout=RUN_TIMEOUT_SECONDS, check=False)
    if result.returncode != 0:
        raise RuntimeError("smoke_test_failed")
    try:
        summary = json.loads(result.stdout.strip().splitlines()[-1])["summary"]
        events = summary["events"]
    except (ValueError, KeyError, IndexError, TypeError):
        raise RuntimeError("invalid_smoke_result") from None
    if events != MAX_EVENTS:
        raise RuntimeError("unexpected_event_count")
    # Do not expose subprocess stderr, credentials, generated bodies or recipients.
    sink = summary.get("sink", {})
    return {"events": events, "accepted": sink.get("accepted", 0),
            "acked": sink.get("acked", 0), "target": target}


class Trigger:
    def __init__(self, target="adapter", *, runner=run_smoke, clock=time.monotonic):
        command(target)  # Validate configuration at startup.
        self.target = target
        self.runner = runner
        self.clock = clock
        self.lock = threading.Lock()
        self.active = False
        self.last_started = None
        self.last_result = None
        self.last_error = None

    def _status(self):
        remaining = 0 if self.last_started is None else max(
            0, math.ceil(COOLDOWN_SECONDS - (self.clock() - self.last_started)))
        return {"active": self.active, "cooldown_seconds": remaining,
                "max_events": MAX_EVENTS, "target": self.target,
                "scope": "adapter" if self.target == "adapter" else "ingest",
                "last_result": self.last_result, "last_error": self.last_error}

    def status(self):
        with self.lock:
            return self._status()

    def start(self):
        with self.lock:
            status = self._status()
            if self.active or status["cooldown_seconds"]:
                return 429, {**status, "error": "smoke test active or cooling down"}
            self.active = True
            self.last_started = self.clock()
            self.last_result = None
            self.last_error = None
            threading.Thread(target=self._run, daemon=True).start()
            return 202, {**self._status(), "status": "started"}

    def _run(self):
        try:
            result = self.runner(self.target)
            error = None
        except subprocess.TimeoutExpired:
            result, error = None, "smoke_test_timeout"
        except Exception:
            result, error = None, "smoke_test_failed"
        with self.lock:
            self.last_result, self.last_error = result, error
            self.active = False


def handler(trigger):
    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, body):
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path != "/status":
                return self.reply(404, {"error": "not found"})
            self.reply(200, trigger.status())

        def do_POST(self):
            if self.path != "/run":
                return self.reply(404, {"error": "not found"})
            if self.headers.get("Content-Length", "0") != "0" or self.headers.get("Transfer-Encoding"):
                self.close_connection = True
                return self.reply(400, {"error": "request input is not accepted"})
            self.reply(*trigger.start())

        def log_message(self, *_args):
            pass

    return Handler


if __name__ == "__main__":
    trigger = Trigger(os.environ.get("DEMO_TARGET", "adapter"))
    server = ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("DEMO_TRIGGER_PORT", "8099"))), handler(trigger))
    server.serve_forever()
