# api/main.py
from fastapi import FastAPI, BackgroundTasks
import os

from .db import init_tables
from .routers.webhook_line import router as line_router
from .routers.n8n_misc import router as n8n_router
from api.services import scheduler_repo

# 從 auto_scheduler 匯入正確的函式名稱（依你的程式）
from api.services.auto_scheduler import (
    start_scheduler,
    scan_and_schedule_from_mother,
    run_due_uploads,
    promote_published_and_move,
    reconcile_sheet_and_drive_for_published,
    reconcile_youtube_deletions_and_sheet
)

app = FastAPI(title="LINE Menu + Drive + Scheduler (Modularized)")

# 建表等初始化
init_tables()

# 啟動時掛上排程器（你的檔案已實作 start_scheduler）
if os.getenv("ENABLE_SCHEDULER", "1") == "1":
    @app.on_event("startup")
    async def _on_startup():
        start_scheduler() # 內部會建立每日/每3分/每5分等排程

# 既有路由
app.include_router(line_router)
app.include_router(n8n_router)

# ---- 測試/運維用 API：全部改為背景執行，避免 H12 ----

# 1) 立即觸發：掃描母資料夾 -> 配檔位 -> 直接上傳 -> 寫入 Sheet(已排程)
@app.post("/api/scheduler/scan")
def scan_now(background_tasks: BackgroundTasks):
    background_tasks.add_task(scan_and_schedule_from_mother)
    return {"status": "accepted", "msg": "已在背景觸發掃描，請查看 logs 追蹤進度"}

# 2) 立即觸發：到點上傳（掃描已在 DB 的排程，時間到就上傳）
@app.post("/api/scheduler/upload-now")
def upload_now(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_due_uploads)
    return {"status": "accepted", "msg": "已在背景觸發到點上傳，請查看 logs"}

# 3) 立即觸發：已公開 -> 搬移到已發布資料夾 + 更新 Sheet 狀態
@app.post("/api/scheduler/promote-now")
async def promote_now():
    res = promote_published_and_move(dry_run=False)
    return {"status": "ok", **res}

@app.post("/api/scheduler/reconcile-sheet-now")
async def reconcile_sheet_now():
    res = reconcile_sheet_and_drive_for_published(dry_run=False)
    return {"status": "ok", **res}

@app.post("/api/scheduler/reconcile-ytsched-now")
async def reconcile_ytsched_now():
    from api.services.auto_scheduler import reconcile_youtube_schedule_drift
    try:
        res = reconcile_youtube_schedule_drift()
        return {"ok": True, **res}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/api/scheduler/reconcile-ytdel-sheet-now")
def reconcile_ytdel_sheet_now():
    res = reconcile_youtube_deletions_and_sheet(dry_run=False)
    return res

@app.get("/api/scheduler/ready-dump")
async def ready_dump():
    rows = scheduler_repo.debug_ready_snapshot(limit=100)
    summary = {
        "total_rows_sampled": len(rows),
        "has_video_id": sum(1 for r in rows if r["has_video_id"]),
        "is_due":       sum(1 for r in rows if r["is_due"]),
        "status_ok":    sum(1 for r in rows if r["status_ok"]),
        "would_be_picked": sum(1 for r in rows if r["has_video_id"] and r["is_due"] and r["status_ok"]),
    }
    return {"summary": summary, "rows": rows}


@app.get("/")
def root():
    return {"ok": True}

@app.get("/diag/health")
def health():
    return {"status": "ok"}
