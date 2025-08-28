# api/services/scheduler_runner.py
from __future__ import annotations
import os, time, logging, inspect, asyncio
from typing import Optional, Callable
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import text
from pytz import timezone
from api.db import engine

# 全域鎖，避免多個 scheduler 同時跑
_LEADER_LOCK_KEY = 9132025
log = logging.getLogger("scheduler")


def _try_acquire_leader() -> bool:
    """用 PostgreSQL 顧問鎖判斷自己是不是 leader"""
    with engine.begin() as conn:
        ok = conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": _LEADER_LOCK_KEY}).scalar()
        return bool(ok)


def _resolve_job(mod, candidates: list[str]) -> Optional[Callable]:
    """嘗試用候選名稱或關鍵字找到合適的函式"""
    for name in candidates:
        if hasattr(mod, name):
            fn = getattr(mod, name)
            if callable(fn):
                return fn
    # fallback：找名稱裡有關鍵字的
    funcs = {n: f for n, f in inspect.getmembers(mod, inspect.isfunction)}
    for n, f in funcs.items():
        ln = n.lower()
        if any(k in ln for k in ("scan", "schedule", "upload")):
            return f
        if any(k in ln for k in ("reconcile", "sync", "sheet", "youtube")):
            return f
    return None


def _wrap(fn: Callable) -> Callable:
    """把 async 函式包裝成同步呼叫"""
    if inspect.iscoroutinefunction(fn):
        def _runner():
            asyncio.run(fn())
        return _runner
    return fn


def start_scheduler() -> Optional[BackgroundScheduler]:
    if os.getenv("RUN_SCHEDULER", "0") != "1":
        log.info("scheduler.disabled (RUN_SCHEDULER!=1) — skip start")
        return None
    if not _try_acquire_leader():
        log.info("scheduler.another_leader_exists — skip start")
        return None

    tz = timezone(os.getenv("TZ", "Asia/Taipei"))
    scheduler = BackgroundScheduler(timezone=tz)

    try:
        import api.services.auto_scheduler as m
    except Exception as e:
        log.exception("scheduler.import_module_failed: %s", e)
        return None

    # 如果 auto_scheduler 自己有 start_scheduler，就直接交給它
    if hasattr(m, "start_scheduler") and callable(getattr(m, "start_scheduler")):
        log.info("scheduler.compat_mode: delegating to auto_scheduler.start_scheduler()")
        try:
            return m.start_scheduler()  # type: ignore[attr-defined]
        except Exception as e:
            log.exception("scheduler.compat_mode_failed: %s", e)
            return None

    # 自動找兩個主要任務
    scan_candidates = [
        "scan_and_schedule_uploads", "scan_and_schedule",
        "schedule_uploads", "plan_and_schedule", "enqueue_uploads"
    ]
    recon_candidates = [
        "reconcile_youtube_deletions_and_sheet", "reconcile_sheets_and_youtube",
        "reconcile_deletions", "reconcile", "sync_sheet_and_youtube"
    ]

    scan_fn = _resolve_job(m, scan_candidates)
    recon_fn = _resolve_job(m, recon_candidates)

    if not scan_fn and not recon_fn:
        names = [n for n, f in inspect.getmembers(m, inspect.isfunction)]
        log.error("scheduler.no_jobs_found: auto_scheduler 沒有可辨識的工作函式；available=%s", names)
        return None

    if scan_fn:
        scheduler.add_job(
            _wrap(scan_fn),
            "interval", minutes=5,
            id="scan_and_schedule",
            replace_existing=True, max_instances=1, coalesce=True
        )
        log.info("scheduler.job_added: %s", scan_fn.__name__)

    if recon_fn:
        scheduler.add_job(
            _wrap(recon_fn),
            "interval", minutes=10,
            id="reconcile",
            replace_existing=True, max_instances=1, coalesce=True
        )
        log.info("scheduler.job_added: %s", recon_fn.__name__)

    scheduler.start()
    log.info("scheduler.started")
    return scheduler


if __name__ == "__main__":
    os.environ.setdefault("RUN_SCHEDULER", "1")
    start_scheduler()
    while True:
        time.sleep(3600)
