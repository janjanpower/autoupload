# api/services/scheduler_runner.py
from __future__ import annotations
import os, time, logging, inspect, asyncio
from typing import Optional, Callable
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import text
from pytz import timezone
from api.db import engine

_LEADER_LOCK_KEY = 9132025
log = logging.getLogger("scheduler")

def _try_acquire_leader() -> bool:
    with engine.begin() as conn:
        ok = conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": _LEADER_LOCK_KEY}).scalar()
        return bool(ok)

def _resolve_job(mod, candidates: list[str]) -> Optional[Callable]:
    for name in candidates:
        if hasattr(mod, name):
            fn = getattr(mod, name)
            if callable(fn):
                return fn
    # 嘗試用關鍵字 heuristics 找最像的函式
    funcs = {n: f for n, f in inspect.getmembers(mod, inspect.isfunction)}
    for n, f in funcs.items():
        ln = n.lower()
        if any(k in ln for k in ("scan", "schedule", "upload")):
            return f
    return None

def _wrap(fn: Callable) -> Callable:
    # 若是 async 函式，包成同步呼叫
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

    # 盡量相容你現有的 auto_scheduler
    try:
        import api.services.auto_scheduler as m
    except Exception as e:
        log.exception("scheduler.import_module_failed: %s", e)
        return None

    # 若你的檔案本來就有 start_scheduler()，直接交給它（相容模式）
    if hasattr(m, "start_scheduler") and callable(getattr(m, "start_scheduler")):
        log.info("scheduler.compat_mode: delegating to api.services.auto_scheduler.start_scheduler()")
        try:
            return m.start_scheduler()  # type: ignore[attr-defined]
        except Exception as e:
            log.exception("scheduler.compat_mode_failed: %s", e)
            return None

    # 否則就嘗試解析兩個工作函式
    scan_candidates = [
        "scan_and_schedule_uploads", "scan_and_schedule", "schedule_uploads",
        "plan_and_schedule", "enqueue_uploads", "scan_and_enqueue",
    ]
    recon_candidates = [
        "reconcile_youtube_deletions_and_sheet", "reconcile_sheets_and_youtube",
        "reconcile_deletions", "reconcile", "sync_sheet_and_youtube",
    ]

    scan_fn = _resolve_job(m, scan_candidates)
    recon_fn = _resolve_job(m, recon_candidates)

    if not scan_fn and not recon_fn:
        # 列出可用函式名，方便你比對
        names = [n for n, f in inspect.getmembers(m, inspect.isfunction)]
        log.error("scheduler.no_jobs_found: auto_scheduler 沒有可辨識的工作函式；請確認函式命名。available=%s", names)
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
