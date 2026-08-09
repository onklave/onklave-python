# Copyright 2025-2026 Onklave (Pty) Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the Onklave Python SDK. Stdlib unittest, no network."""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import onklave  # noqa: E402
from onklave import errors as errors_mod  # noqa: E402


class RecordingTransport:
    """Injectable transport that records calls and returns canned statuses."""

    def __init__(self, statuses=None):
        self.calls = []
        self._statuses = list(statuses or [])

    def __call__(self, url, headers, body):
        self.calls.append((url, headers, json.loads(body.decode("utf-8"))))
        return self._statuses.pop(0) if self._statuses else 200


class ExplodingTransport:
    def __init__(self):
        self.calls = 0

    def __call__(self, url, headers, body):
        self.calls += 1
        raise ConnectionError("boom")


class SdkTestCase(unittest.TestCase):
    def setUp(self):
        for var in ("ONKLAVE_ERRORS_INGEST_KEY", "ONKLAVE_COMMIT_SHA", "ONKLAVE_ENV"):
            os.environ.pop(var, None)

    def tearDown(self):
        errors_mod._client = errors_mod.NoopClient()


class TestCaptureException(SdkTestCase):
    def test_payload_and_auth_header(self):
        transport = RecordingTransport()
        onklave.init(
            "test-key",
            service_name="unit-svc",
            release="1.2.3",
            environment="test",
            transport=transport,
        )
        try:
            raise ValueError("kaboom")
        except ValueError:
            onklave.capture_exception(context={"job": "nightly"}, tags={"a": "b"})
        self.assertTrue(onklave.flush(timeout=5))

        self.assertEqual(len(transport.calls), 1)
        url, headers, payload = transport.calls[0]
        self.assertEqual(url, "https://errors.onklave.app/api/v1/events/errors")
        self.assertEqual(headers["Authorization"], "Bearer test-key")
        self.assertEqual(headers["Content-Type"], "application/json")

        self.assertEqual(payload["serviceName"], "unit-svc")
        self.assertEqual(payload["release"], "1.2.3")
        self.assertEqual(payload["environment"], "test")
        self.assertEqual(payload["context"], {"job": "nightly"})
        self.assertEqual(payload["tags"], {"a": "b"})
        self.assertIn("timestamp", payload)

        err = payload["error"]
        self.assertEqual(err["type"], "ValueError")
        self.assertEqual(err["message"], "kaboom")
        self.assertEqual(err["level"], "error")
        self.assertIn("ValueError: kaboom", err["stack"])
        self.assertGreaterEqual(len(err["frames"]), 1)
        frame = err["frames"][-1]
        for field in ("function", "file", "line", "raw"):
            self.assertIn(field, frame)

        # Origin = innermost non-SDK frame: the raise site in this test file.
        origin = err["origin"]
        self.assertEqual(origin["function"], "test_payload_and_auth_header")
        self.assertTrue(origin["file"].endswith("test_sdk.py"))

    def test_explicit_exception_and_request(self):
        transport = RecordingTransport()
        onklave.init("k", service_name="s", transport=transport)
        exc = RuntimeError("no traceback")
        onklave.capture_exception(
            exc, request={"method": "GET", "path": "/x", "statusCode": 500}
        )
        onklave.flush(timeout=5)
        _, _, payload = transport.calls[0]
        self.assertEqual(payload["error"]["type"], "RuntimeError")
        self.assertEqual(payload["request"]["statusCode"], 500)
        # No traceback: frames/origin omitted, not null.
        self.assertNotIn("frames", payload["error"])
        self.assertNotIn("origin", payload["error"])

    def test_no_current_exception_is_noop(self):
        transport = RecordingTransport()
        onklave.init("k", service_name="s", transport=transport)
        onklave.capture_exception()  # nothing being handled
        onklave.flush(timeout=5)
        self.assertEqual(transport.calls, [])

    def test_env_defaults(self):
        os.environ["ONKLAVE_ERRORS_INGEST_KEY"] = "env-key"
        os.environ["ONKLAVE_COMMIT_SHA"] = "abc123"
        os.environ["ONKLAVE_ENV"] = "staging"
        transport = RecordingTransport()
        onklave.init(service_name="s", transport=transport)
        onklave.capture_message("hi")
        onklave.flush(timeout=5)
        _, headers, payload = transport.calls[0]
        self.assertEqual(headers["Authorization"], "Bearer env-key")
        self.assertEqual(payload["release"], "abc123")
        self.assertEqual(payload["commitSha"], "abc123")
        self.assertEqual(payload["environment"], "staging")

    def test_custom_endpoint(self):
        transport = RecordingTransport()
        onklave.init(
            "k",
            service_name="s",
            endpoint="https://errors.example.com/",
            transport=transport,
        )
        onklave.capture_message("hi")
        onklave.flush(timeout=5)
        url, _, _ = transport.calls[0]
        self.assertEqual(url, "https://errors.example.com/api/v1/events/errors")

    def test_capture_never_raises_when_transport_explodes(self):
        transport = ExplodingTransport()
        onklave.init("k", service_name="s", transport=transport)
        try:
            raise ValueError("bad")
        except ValueError:
            onklave.capture_exception()  # must not raise
        self.assertTrue(onklave.flush(timeout=5))
        # One retry after the network error, then gave up.
        self.assertEqual(transport.calls, 2)
        # Worker survived — subsequent captures still processed.
        onklave.capture_message("still alive")
        self.assertTrue(onklave.flush(timeout=5))
        self.assertEqual(transport.calls, 4)


class TestCaptureMessage(SdkTestCase):
    def test_message_level_default_and_override(self):
        transport = RecordingTransport()
        onklave.init("k", service_name="s", transport=transport)
        onklave.capture_message("plain")
        onklave.capture_message("scary", level="warning")
        onklave.flush(timeout=5)
        self.assertEqual(transport.calls[0][2]["error"]["type"], "Message")
        self.assertEqual(transport.calls[0][2]["error"]["level"], "info")
        self.assertEqual(transport.calls[1][2]["error"]["level"], "warning")


class TestNoopWithoutKey(SdkTestCase):
    def test_init_without_key_is_silent_noop(self):
        client = onklave.init(service_name="s")
        self.assertIsInstance(client, onklave.NoopClient)
        try:
            raise ValueError("bad")
        except ValueError:
            onklave.capture_exception()
        onklave.capture_message("hi")
        self.assertFalse(onklave.submit_feedback("t", "d"))
        self.assertTrue(onklave.flush(timeout=1))

    def test_module_calls_before_init_are_noops(self):
        onklave.capture_message("before init")
        self.assertFalse(onklave.submit_feedback("t", "d"))
        self.assertTrue(onklave.flush())


class TestFeedback(SdkTestCase):
    def test_success_payload_and_true(self):
        transport = RecordingTransport(statuses=[201])
        onklave.init("fk", service_name="s", transport=transport)
        ok = onklave.submit_feedback(
            "Title here",
            "Longer description",
            category="ui_ux",
            priority="high",
            route="/settings",
            page_url="https://app.example.com/settings",
            reporter={"name": "Ada", "email": "ada@example.com"},
            context={"theme": "dark"},
        )
        self.assertTrue(ok)
        url, headers, payload = transport.calls[0]
        self.assertEqual(url, "https://errors.onklave.app/api/v1/events/feedback")
        self.assertEqual(headers["Authorization"], "Bearer fk")
        self.assertEqual(
            payload,
            {
                "title": "Title here",
                "description": "Longer description",
                "category": "ui_ux",
                "priority": "high",
                "route": "/settings",
                "pageUrl": "https://app.example.com/settings",
                "reporter": {"name": "Ada", "email": "ada@example.com"},
                "context": {"theme": "dark"},
            },
        )

    def test_optional_fields_omitted(self):
        transport = RecordingTransport()
        onklave.init("fk", service_name="s", transport=transport)
        self.assertTrue(onklave.submit_feedback("t", "d"))
        _, _, payload = transport.calls[0]
        self.assertEqual(payload, {"title": "t", "description": "d"})

    def test_4xx_returns_false_without_retry(self):
        transport = RecordingTransport(statuses=[400])
        onklave.init("fk", service_name="s", transport=transport)
        self.assertFalse(onklave.submit_feedback("t", "d"))
        self.assertEqual(len(transport.calls), 1)

    def test_5xx_retries_then_false(self):
        transport = RecordingTransport(statuses=[500, 503])
        onklave.init("fk", service_name="s", transport=transport)
        self.assertFalse(onklave.submit_feedback("t", "d"))
        self.assertEqual(len(transport.calls), 2)

    def test_5xx_then_2xx_returns_true(self):
        transport = RecordingTransport(statuses=[500, 200])
        onklave.init("fk", service_name="s", transport=transport)
        self.assertTrue(onklave.submit_feedback("t", "d"))

    def test_exploding_transport_returns_false(self):
        onklave.init("fk", service_name="s", transport=ExplodingTransport())
        self.assertFalse(onklave.submit_feedback("t", "d"))


if __name__ == "__main__":
    unittest.main()
