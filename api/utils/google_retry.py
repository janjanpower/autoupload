from __future__ import annotations

import logging
import random
import time
from typing import Iterable, Optional

try:
    from googleapiclient.errors import HttpError  # type: ignore
except Exception:  # pragma: no cover
    class HttpError(Exception):
        pass  # fallback for type checkers


_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_RETRYABLE_REASONS = {
    "userratelimitexceeded",
    "ratelimitexceeded",
    "backenderror",
    "quotaexceeded",
    "internalerror",
    "badgateway",
    "serviceunavailable",
    "gatewaytimeout",
}


def _extract_status_and_reason(e: HttpError) -> tuple[Optional[int], str]:
    status = None
    reason = ""

    # status
    try:
        status = int(getattr(e, "status_code", None) or 0)
    except Exception:
        status = None
    if not status:
        try:
            status = int(getattr(e, "resp", {}).get("status", 0) or 0)  # type: ignore
        except Exception:
            status = None

    # reason
    try:
        details = getattr(e, "error_details", None) or []
        if details and isinstance(details, Iterable):
            reason = (details[0].get("reason") or "").lower()
    except Exception:
        pass
    if not reason:
        try:
            reason = (e._get_reason() or "").lower()  # type: ignore
        except Exception:
            reason = ""
    return status, reason


def google_execute(
    request,
    *,
    max_attempts: int = 5,
    base_sleep: float = 1.0,
    max_sleep: float = 8.0,
    jitter: float = 0.25,
):
    """
    Safely execute Google API request.execute() with exponential backoff.
    Usage:
        resp = google_execute(sheets.spreadsheets().values().append(...))
    """
    attempt = 0
    while True:
        try:
            return request.execute()
        except HttpError as e:  # pragma: no cover - network path
            status, reason = _extract_status_and_reason(e)
            if (status in _RETRYABLE_STATUS) or (reason in _RETRYABLE_REASONS):
                attempt += 1
                if attempt >= max_attempts:
                    logging.error(
                        "google.execute.giveup",
                        extra={"status": status, "reason": reason, "attempts": attempt},
                    )
                    raise
                sleep = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
                sleep = sleep * (1 - jitter + 2 * jitter * random.random())
                logging.warning(
                    "google.execute.retry",
                    extra={
                        "attempt": attempt,
                        "status": status,
                        "reason": reason,
                        "sleep": round(sleep, 2),
                    },
                )
                time.sleep(sleep)
                continue
            raise
