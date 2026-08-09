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

"""Onklave — error tracking and end-user feedback for Python apps.

Quickstart::

    import onklave

    onklave.init(service_name="my-service")  # key from $ONKLAVE_ERRORS_INGEST_KEY

    try:
        do_work()
    except Exception:
        onklave.capture_exception()
"""

from .errors import (
    DEFAULT_ENDPOINT_BASE,
    FEEDBACK_PATH,
    INGEST_PATH,
    Client,
    NoopClient,
    capture_exception,
    capture_message,
    flush,
    init,
    submit_feedback,
)

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_ENDPOINT_BASE",
    "FEEDBACK_PATH",
    "INGEST_PATH",
    "Client",
    "NoopClient",
    "__version__",
    "capture_exception",
    "capture_message",
    "flush",
    "init",
    "submit_feedback",
]
