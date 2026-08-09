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

"""Onklave error-tracking SDK for Python — framework-agnostic core.

Thin client that posts errors to the Onklave errors service ingest endpoint
(``POST /api/v1/events/errors``), authenticated with a per-project ingest key.

Vendor-neutral: the only coupling is HTTP-to-ingest. The server resolves
org + project from the ingest key, so no trusted project id is ever sent in
the body. Capture is fire-and-forget and never raises into the host app.
"""

from __future__ import annotations

import json
import os
import queue
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Optional

#: Default ingest base URL; the ingest/feedback paths are appended to this.
#: Dedicated public host that routes straight to the errors service — error
#: telemetry must not depend on the component whose failures it captures.
DEFAULT_ENDPOINT_BASE = "https://errors.onklave.app"

#: Path appended to the base to form the full error-ingest URL.
INGEST_PATH = "/api/v1/events/errors"

#: Path appended to the base to form the feedback submission URL.
FEEDBACK_PATH = "/api/v1/events/feedback"

#: Pluggable transport: ``(url, headers, body_bytes) -> HTTP status code``.
#: May raise on network failure; the SDK treats a raise as a failed attempt.
Transport = Callable[[str, dict[str, str], bytes], int]

_SDK_DIR = os.path.dirname(os.path.abspath(__file__))

# Max events buffered for background sending; further events are dropped
# silently rather than blocking or growing without bound.
_QUEUE_MAX = 100

# Max send attempts per event (1 = no retry).
_MAX_ATTEMPTS = 2


def _default_transport(url: str, headers: dict[str, str], body: bytes) -> int:
    """Stdlib transport built on ``urllib.request``. Returns the HTTP status."""
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as res:  # noqa: S310
            return int(res.status)
    except urllib.error.HTTPError as err:
        # Non-2xx responses arrive as HTTPError; the status is still an answer.
        return int(err.code)


def _frames_from_traceback(tb: Any) -> list[dict[str, Any]]:
    """Normalize a traceback into structured frames (outermost first).

    Mirrors the JS SDK's ``StackFrame`` shape: ``{function, file, line, raw}``.
    """
    frames: list[dict[str, Any]] = []
    for summary in traceback.extract_tb(tb):
        frames.append(
            {
                "function": summary.name,
                "file": summary.filename,
                "line": summary.lineno,
                "raw": 'File "{0}", line {1}, in {2}'.format(
                    summary.filename, summary.lineno, summary.name
                ),
            }
        )
    return frames


def _pick_origin(frames: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Pick the originating ("culprit") frame — where the error was raised.

    Python tracebacks run outermost → innermost, so the raise site is the
    *last* application frame. Frames inside this SDK are skipped so the SDK
    never blames itself; falls back to the innermost frame if nothing else
    qualifies.
    """
    if not frames:
        return None
    chosen = None
    for frame in reversed(frames):
        file = frame.get("file")
        if not file:
            continue
        if os.path.abspath(file).startswith(_SDK_DIR):
            continue  # never blame the error SDK itself
        chosen = frame
        break
    if chosen is None:
        chosen = frames[-1]
    return {
        "function": chosen.get("function"),
        "file": chosen.get("file"),
        "line": chosen.get("line"),
    }


def _prune(obj: dict[str, Any]) -> dict[str, Any]:
    """Drop ``None`` values so optional fields are omitted, not nulled."""
    return {k: v for k, v in obj.items() if v is not None}


class Client:
    """The error-tracking client.

    Most callers use the module-level facade via :func:`init`, but a
    standalone client can be constructed directly for tests or
    multi-project usage. All capture methods are fire-and-forget: events are
    handed to a bounded queue drained by a daemon background thread, and no
    SDK failure ever raises into the host application.
    """

    def __init__(
        self,
        key: str,
        service_name: str,
        release: str,
        environment: str,
        endpoint: Optional[str] = None,
        transport: Optional[Transport] = None,
        commit_sha: Optional[str] = None,
    ) -> None:
        base = (endpoint or DEFAULT_ENDPOINT_BASE).rstrip("/")
        self._key = key
        self._service_name = service_name
        self._release = release
        self._environment = environment
        self._commit_sha = commit_sha
        self._ingest_url = base + INGEST_PATH
        self._feedback_url = base + FEEDBACK_PATH
        self._transport: Transport = transport or _default_transport
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=_QUEUE_MAX)
        self._cond = threading.Condition()
        self._pending = 0
        self._worker: Optional[threading.Thread] = None
        self._worker_lock = threading.Lock()

    # -- capture ---------------------------------------------------------

    def capture_exception(
        self,
        exc: Optional[BaseException] = None,
        *,
        request: Optional[dict[str, Any]] = None,
        context: Optional[dict[str, Any]] = None,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Capture an exception. Fire-and-forget; never raises.

        When ``exc`` is ``None`` the exception currently being handled
        (``sys.exc_info()``) is captured; a no-op if there isn't one.
        """
        try:
            if exc is None:
                exc = sys.exc_info()[1]
            if exc is None:
                return
            frames = _frames_from_traceback(exc.__traceback__)
            stack = "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            )
            error = _prune(
                {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "stack": stack,
                    "frames": frames or None,
                    "origin": _pick_origin(frames),
                    "level": "error",
                }
            )
            self._enqueue(self._build_payload(error, request, context, tags))
        except Exception:
            pass  # Capture must never raise into the host app.

    def capture_message(
        self,
        message: str,
        level: str = "info",
        *,
        request: Optional[dict[str, Any]] = None,
        context: Optional[dict[str, Any]] = None,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Capture a message at the given level. Fire-and-forget; never raises."""
        try:
            error = {"type": "Message", "message": message, "level": level}
            self._enqueue(self._build_payload(error, request, context, tags))
        except Exception:
            pass  # Capture must never raise into the host app.

    def submit_feedback(
        self,
        title: str,
        description: str,
        *,
        category: Optional[str] = None,
        priority: Optional[str] = None,
        route: Optional[str] = None,
        page_url: Optional[str] = None,
        reporter: Optional[dict[str, str]] = None,
        context: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Submit end-user feedback to the owning org/project's triage.

        Unlike error capture this is synchronous — the app usually wants to
        confirm receipt to the user. Returns ``True`` on acceptance (2xx),
        ``False`` on any failure; never raises.
        """
        try:
            body = _prune(
                {
                    "title": title,
                    "description": description,
                    "category": category,
                    "priority": priority,
                    "route": route,
                    "pageUrl": page_url,
                    "reporter": reporter,
                    "context": context,
                }
            )
            return self._post(self._feedback_url, body)
        except Exception:
            return False

    def flush(self, timeout: Optional[float] = None) -> bool:
        """Wait for queued sends to drain. Returns ``True`` when drained."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while self._pending > 0:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._cond.wait(remaining)
        return True

    # -- internals -------------------------------------------------------

    def _build_payload(
        self,
        error: dict[str, Any],
        request: Optional[dict[str, Any]],
        context: Optional[dict[str, Any]],
        tags: Optional[dict[str, str]],
    ) -> dict[str, Any]:
        return _prune(
            {
                "serviceName": self._service_name,
                "environment": self._environment,
                "release": self._release,
                "commitSha": self._commit_sha,
                "timestamp": datetime.now(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                "error": error,
                "request": request,
                "context": context,
                "tags": tags,
            }
        )

    def _enqueue(self, payload: dict[str, Any]) -> None:
        self._ensure_worker()
        with self._cond:
            self._pending += 1
        try:
            self._queue.put_nowait(payload)
        except queue.Full:
            # Bounded buffer: drop rather than block or grow without bound.
            with self._cond:
                self._pending -= 1
                self._cond.notify_all()

    def _ensure_worker(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return
        with self._worker_lock:
            if self._worker is not None and self._worker.is_alive():
                return
            self._worker = threading.Thread(
                target=self._run, name="onklave-errors", daemon=True
            )
            self._worker.start()

    def _run(self) -> None:
        while True:
            payload = self._queue.get()
            try:
                self._post(self._ingest_url, payload)
            except Exception:
                pass  # Sending must never take down the worker.
            finally:
                with self._cond:
                    self._pending -= 1
                    self._cond.notify_all()

    def _post(self, url: str, body: dict[str, Any]) -> bool:
        """POST JSON with a tiny retry loop. True on 2xx; never raises."""
        data = json.dumps(body).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self._key,
        }
        for _attempt in range(_MAX_ATTEMPTS):
            try:
                status = self._transport(url, headers, data)
            except Exception:
                continue  # Network error — retry / give up.
            if 200 <= status < 300:
                return True
            if 400 <= status < 500 and status != 429:
                return False  # Won't succeed on retry — stop.
        return False


class NoopClient:
    """Silent stand-in used when no ingest key is configured (local dev)."""

    def capture_exception(self, exc: Optional[BaseException] = None, **_: Any) -> None:
        return None

    def capture_message(self, message: str, level: str = "info", **_: Any) -> None:
        return None

    def submit_feedback(self, title: str, description: str, **_: Any) -> bool:
        return False

    def flush(self, timeout: Optional[float] = None) -> bool:
        return True


_client: Any = NoopClient()


def init(
    key: Optional[str] = None,
    *,
    service_name: str,
    release: Optional[str] = None,
    environment: Optional[str] = None,
    endpoint: Optional[str] = None,
    transport: Optional[Transport] = None,
) -> Any:
    """Initialize the module-level client. Call once at startup.

    ``key`` defaults to ``$ONKLAVE_ERRORS_INGEST_KEY``, ``release`` to
    ``$ONKLAVE_COMMIT_SHA`` (falling back to ``"dev"``) and ``environment``
    to ``$ONKLAVE_ENV`` (falling back to ``"development"``).

    When no key is available a :class:`NoopClient` is installed and returned:
    every capture is a silent no-op and ``submit_feedback`` returns ``False``.
    That is the local-dev story — apps call ``init()`` unconditionally and
    only ship telemetry where an ingest key is provisioned.
    """
    global _client
    key = key or os.environ.get("ONKLAVE_ERRORS_INGEST_KEY")
    if not key:
        _client = NoopClient()
        return _client
    commit_sha = os.environ.get("ONKLAVE_COMMIT_SHA")
    _client = Client(
        key=key,
        service_name=service_name,
        release=release or commit_sha or "dev",
        environment=environment or os.environ.get("ONKLAVE_ENV") or "development",
        endpoint=endpoint,
        transport=transport,
        commit_sha=commit_sha,
    )
    return _client


def capture_exception(
    exc: Optional[BaseException] = None,
    *,
    request: Optional[dict[str, Any]] = None,
    context: Optional[dict[str, Any]] = None,
    tags: Optional[dict[str, str]] = None,
) -> None:
    """Capture an exception via the module client. No-op before :func:`init`."""
    _client.capture_exception(exc, request=request, context=context, tags=tags)


def capture_message(
    message: str,
    level: str = "info",
    *,
    request: Optional[dict[str, Any]] = None,
    context: Optional[dict[str, Any]] = None,
    tags: Optional[dict[str, str]] = None,
) -> None:
    """Capture a message via the module client. No-op before :func:`init`."""
    _client.capture_message(message, level, request=request, context=context, tags=tags)


def submit_feedback(
    title: str,
    description: str,
    *,
    category: Optional[str] = None,
    priority: Optional[str] = None,
    route: Optional[str] = None,
    page_url: Optional[str] = None,
    reporter: Optional[dict[str, str]] = None,
    context: Optional[dict[str, Any]] = None,
) -> bool:
    """Submit feedback via the module client. ``False`` before :func:`init`."""
    return _client.submit_feedback(
        title,
        description,
        category=category,
        priority=priority,
        route=route,
        page_url=page_url,
        reporter=reporter,
        context=context,
    )


def flush(timeout: Optional[float] = None) -> bool:
    """Drain pending sends via the module client."""
    return _client.flush(timeout)
