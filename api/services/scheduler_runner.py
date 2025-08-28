from __future__ import annotations

import os
import time
import logging
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import text
from pytz import timezone

from api.db import engine

_LEADER_LOCK_KEY = 9132025


def _try_acquire_leader() -> bool:
    with engine.begin() as conn:
        ok = conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": _LEADER_LOCK_KEY}).scalar()
        return bool(ok)


def start_scheduler() -> Optional[BackgroundScheduler]:
    logger = logging.getLogger("scheduler")

    if os.getenv("RUN_SCHEDULER", "0") != "1":
        logger.info("scheduler.disabled (RUN_SCHEDULER!=1) — skip start")
        return None

    if not _try_acquire_leader():
        logger.info("scheduler.another_leader_exists — skip start")
        return None

    tz = timezone(os.getenv("TZ", "Asia/Taipei"))
    scheduler = BackgroundScheduler(timezone=tz)

    # Import here to avoid circular
    try:
        from api.services.auto_scheduler import (
            scan_and_schedule_uploads,
            reconcile_youtube_deletions_and_sheet,
        )
    except Exception as e:
        logger.exception("scheduler.import_failed: %s", e)
        return None

    scheduler.add_job(
        scan_and_schedule_uploads,
        "interval",
        minutes=5,
        id="scan_and_schedule",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        reconcile_youtube_deletions_and_sheet,
        "interval",
        minutes=10,
        id="reconcile",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()
    logger.info("scheduler.started")
    return scheduler


if __name__ == "__main__":
    os.environ.setdefault("RUN_SCHEDULER", "1")
    start_scheduler()
    while True:
        time.sleep(3600)
