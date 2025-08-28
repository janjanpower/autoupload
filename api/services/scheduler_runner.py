# api/services/scheduler_runner.py
from __future__ import annotations

import os
import time
import logging
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import text
from pytz import timezone

# 你專案內已有 engine；若路徑不同請自行調整
from api.db import engine

# 建議固定一個整數作為 Advisory Lock 的 key（專案專用即可）
_LEADER_LOCK_KEY = 9132025


def _try_acquire_leader() -> bool:
    """
    嘗試取得 PostgreSQL 的 '進程級' 顧問鎖（advisory lock）。
    只有拿到鎖的那個進程可以啟動 scheduler，避免多副本重覆跑。
    """
    with engine.begin() as conn:
        # 注意：這把鎖會在連線關閉或進程結束時自動釋放
        ok = conn.execute(
            text("SELECT pg_try_advisory_lock(:k)"),
            {"k": _LEADER_LOCK_KEY},
        ).scalar()
        return bool(ok)


def start_scheduler() -> Optional[BackgroundScheduler]:
    """
    啟動 APScheduler（僅在 RUN_SCHEDULER=1 且成功取得 leader 鎖時才啟動）。
    回傳 scheduler 物件；若不啟動則回傳 None。
    """
    logger = logging.getLogger("scheduler")

    # 1) 只在指定的容器/進程啟動（例如 docker-compose 內專用的 scheduler 服務）
    if os.getenv("RUN_SCHEDULER", "0") != "1":
        logger.info("scheduler.disabled (RUN_SCHEDULER!=1) — skip start")
        return None

    # 2) 透過 DB 顧問鎖，確保只有一台是 leader
    if not _try_acquire_leader():
        logger.info("scheduler.another_leader_exists — skip start")
        return None

    # 3) 建立 Scheduler（使用你目前的時區）
    tz = timezone(os.getenv("TZ", "Asia/Taipei"))
    scheduler = BackgroundScheduler(timezone=tz)

    # === 4) 在這裡掛你的工作 ===
    # 你專案裡面的任務函式（請依實際檔案路徑調整 import）
    from api.services.auto_scheduler import (
        scan_and_schedule_uploads,
        reconcile_youtube_deletions_and_sheet,
    )

    # 每 5 分鐘掃一次資料夾 & 安排上傳
    scheduler.add_job(
        scan_and_schedule_uploads,
        "interval",
        minutes=5,
        id="scan_and_schedule",
        replace_existing=True,
        max_instances=1,  # 避免同時併發
        coalesce=True,
    )

    # 每 10 分鐘對帳（YouTube/SHEET 刪除/補寫等）
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
    # 讓你可用「獨立進程」方式啟動：python -m api.services.scheduler_runner
    os.environ.setdefault("RUN_SCHEDULER", "1")
    start_scheduler()
    # 阻塞主執行緒，避免進程退出
    while True:
        time.sleep(3600)
