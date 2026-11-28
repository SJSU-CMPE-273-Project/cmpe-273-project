import concurrent.futures
import http.client
import json
import subprocess
import threading
import unittest
from unittest.mock import patch

from demo_trigger import Trigger, command, handler, run_smoke, ThreadingHTTPServer


class DemoTriggerTests(unittest.TestCase):
    def test_fixed_command_rejects_other_targets(self):
        for target in ("adapter", "iggy"):
            args = command(target)
            self.assertIn("3s@1", args)
            self.assertEqual(args[args.index("--accounts") + 1], "3")
            self.assertNotIn("--reset-oracle", args)
        with self.assertRaises(ValueError):
            command("arbitrary-command")

    def test_concurrent_runs_and_cooldown(self):
        release = threading.Event()
        finished = threading.Event()
        now = [100.0]

        def runner(_target):
            release.wait(2)
            finished.set()
            return {"events": 3}

        trigger = Trigger(runner=runner, clock=lambda: now[0])
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda _: trigger.start()[0], range(8)))
        self.assertEqual(results.count(202), 1)
        self.assertEqual(results.count(429), 7)
        release.set()
        self.assertTrue(finished.wait(2))
        # Synchronize with the worker's final state update.
        for _ in range(100):
            if not trigger.status()["active"]:
                break
            threading.Event().wait(0.001)
        self.assertFalse(trigger.status()["active"])
        self.assertEqual(trigger.start()[0], 429)
        now[0] += 31
        self.assertEqual(trigger.start()[0], 202)

    def test_result_is_bounded_and_raw_output_is_not_exposed(self):
        with patch("demo_trigger.subprocess.run", return_value=subprocess.CompletedProcess(
                [], 0, json.dumps({"summary": {"events": 3, "body": "secret",
                "sink": {"accepted": 3, "recipient": "secret"}}}), "secret")) as run:
            result = run_smoke("adapter")
            self.assertEqual(result, {"events": 3, "accepted": 3, "acked": 0, "target": "adapter"})
            self.assertEqual(run.call_args.kwargs["timeout"], 30)
            self.assertNotIn("shell", run.call_args.kwargs)

    def test_failure_and_timeout_are_sanitized(self):
        for error, expected in [(RuntimeError("secret"), "smoke_test_failed"),
                                (subprocess.TimeoutExpired("secret", 30), "smoke_test_timeout")]:
            def fail(_target):
                raise error
            trigger = Trigger(runner=fail)
            trigger._run()
            self.assertEqual(trigger.status()["last_error"], expected)
            self.assertNotIn("secret", json.dumps(trigger.status()))

    def test_http_routes_accept_no_custom_workload(self):
        calls = []
        trigger = Trigger(runner=lambda target: calls.append(target) or {"events": 3})
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler(trigger))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for method, route, body, expected in [
                ("GET", "/status", None, 200),
                ("POST", "/run", '{"events":1000}', 400),
                ("POST", "/run?events=1000", None, 404),
                ("POST", "/run", None, 202),
                ("POST", "/run", None, 429),
            ]:
                conn = http.client.HTTPConnection(*server.server_address)
                conn.request(method, route, body)
                response = conn.getresponse()
                self.assertEqual(response.status, expected)
                json.loads(response.read())
                conn.close()
            self.assertLessEqual(len(calls), 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
