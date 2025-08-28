# api/utils/google_retry.py
from __future__ import annotations

import logging
import random
import time
from typing import Iterable, Optional

try:
    # googleapiclient 的錯誤型別
    from googleapiclient.errors import HttpError
except Exception:  # pragma: no cover
    class HttpError(Exception):
        pass  # 讓型別檢查過得去


# 會重試的 HTTP 狀態碼與 reason
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
    """盡可能從不同版本的 HttpError 取出 status 與 reason。"""
    status = None
    reason = ""
    # 不同版本可能放在不同屬性
    try:
        status = int(getattr(e, "status_code", None) or 0)
    except Exception:
        status = None
    if not status:
        try:
            status = int(getattr(e, "resp", {}).get("status", 0) or 0)
        except Exception:
            status = None

    # reason 嘗試多種來源
    try:
        # 新版有 error_details
        details = getattr(e, "error_details", None) or []
        if details and isinstance(details, Iterable):
            reason = (details[0].get("reason") or "").lower()
    except Exception:
        pass
    if not reason:
        try:
            reason = (e._get_reason() or "").lower()  # 舊版
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
    安全執行 Google API request.execute()，對 429/5xx 與常見 reason 自動退避重試。
    用法：
        req = sheets.spreadsheets().values().append(...);
        resp = google_execute(req)
    """
    attempt = 0
    while True:
        try:
            return request.execute()
        except HttpError as e:
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
                # 加一點抖動，避免雪崩重試
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
            # 非可重試錯誤，直接拋出
            raise
