# onklave

Onklave error tracking and end-user feedback for Python apps.

A thin, zero-dependency (stdlib-only) client that posts errors and feedback to
the Onklave errors service, authenticated with a per-project ingest key. The
org and project are resolved **server-side from the key** — the payload never
carries a trusted project id.

- **Fire-and-forget capture.** Errors are sent from a daemon background
  thread with a bounded queue; no SDK failure ever raises into your app.
- **Silent in local dev.** Without an ingest key, `init()` installs a no-op
  client — every capture is a silent no-op, so you call `init()`
  unconditionally and only ship telemetry where a key is provisioned.
- **Zero runtime dependencies.** Python >= 3.10, `urllib.request` only.

## Install

```bash
pip install onklave
```

## Quickstart

```python
import onklave

# Key defaults to $ONKLAVE_ERRORS_INGEST_KEY; release to $ONKLAVE_COMMIT_SHA
# ("dev" fallback); environment to $ONKLAVE_ENV ("development" fallback).
onklave.init(service_name="my-service")

try:
    do_work()
except Exception:
    # No argument: captures the exception currently being handled.
    onklave.capture_exception()

onklave.capture_message("cache warmed", level="info")

# Optionally drain pending sends before process exit (e.g. short-lived jobs).
onklave.flush(timeout=5)
```

`capture_exception` / `capture_message` accept optional keyword-only
`request={"method", "path", "statusCode"}`, `context={...}` and
`tags={...}` extras that are attached to the event.

## Feedback

Feedback submission is synchronous (apps usually want to confirm receipt to
the user) and returns `True` on acceptance:

```python
accepted = onklave.submit_feedback(
    "Dark mode contrast",                 # title, 1-200 chars
    "The sidebar text is unreadable...",  # description, 1-5000 chars
    category="dark_light",
    priority="medium",
    route="/settings/appearance",
    reporter={"name": "Ada", "email": "ada@example.com"},
)
```

It lands in the owning org/project's feedback triage in the Onklave portal —
again, org and project come from the ingest key, not the body.

## Configuration

```python
onklave.init(
    key="ok_ingest_...",                  # default: $ONKLAVE_ERRORS_INGEST_KEY
    service_name="my-service",            # required
    release="1.4.2",                      # default: $ONKLAVE_COMMIT_SHA or "dev"
    environment="production",             # default: $ONKLAVE_ENV or "development"
    endpoint="https://errors.onklave.app",  # default shown
    transport=None,                       # injectable for tests:
                                          #   (url, headers, body_bytes) -> status
)
```

## License

Apache-2.0. Copyright 2025-2026 Onklave (Pty) Ltd.
