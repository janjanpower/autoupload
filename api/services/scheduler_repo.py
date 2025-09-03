from typing import Dict, List,Optional
from sqlalchemy import text as sql_text
from ..db import engine
import json

from typing import List, Dict
from sqlalchemy import text as sql_text
from api.db import engine


def list_future_uploaded() -> List[Dict]:
    """抓出未來時間點，且狀態是 uploaded/scheduled、且有 YT video id 的排程"""
    with engine.begin() as conn:
        rows = conn.execute(sql_text("""
            SELECT id, youtube_video_id
            FROM video_schedules
            WHERE (status='uploaded' OR status='scheduled')
              AND youtube_video_id IS NOT NULL
              AND schedule_time > now()
        """)).mappings().all()
    return [dict(r) for r in rows]

def mark_deleted(sid: int):
    with engine.begin() as conn:
        conn.execute(sql_text(
            "UPDATE video_schedules SET status='deleted' WHERE id=:id"
        ), {"id": sid})


def update_title(schedule_id: int, new_title: str):
    # 若你把 title 存在 meta_text 裡就不需要這個；若有獨立欄位可同步一下
    with engine.begin() as conn:
        conn.execute(sql_text(
            "UPDATE video_schedules SET meta_text = :t WHERE id = :id"
        ), {"t": new_title, "id": schedule_id})

def get_state(line_user_id: str):
    with engine.begin() as conn:
        row = conn.execute(
            sql_text("SELECT stage, COALESCE(data,'{}') FROM line_states WHERE line_user_id=:u"),
            {"u": line_user_id}
        ).fetchone()
    if not row:
        return "IDLE", {}
    return row[0] or "IDLE", row[1] or {}

def set_state(line_user_id: str, stage: str, data: Dict):
    with engine.begin() as conn:
        conn.execute(sql_text("""
            INSERT INTO line_states (line_user_id, stage, data, updated_at)
            VALUES (:u, :s, CAST(:d AS JSONB), now())
            ON CONFLICT (line_user_id)
            DO UPDATE SET stage=:s, data=CAST(:d AS JSONB), updated_at=now();
        """), {"u": line_user_id, "s": stage, "d": __import__("json").dumps(data, ensure_ascii=False)})

def reset_state(line_user_id: str):
    set_state(line_user_id, "IDLE", {})

def insert_schedule(line_user_id, folder_id, folder_name, video_type, meta_file_id, meta_text, dt_utc):
    from sqlalchemy import text as sql_text
    with engine.begin() as conn:
        row = conn.execute(sql_text("""
            INSERT INTO video_schedules (
                line_user_id, folder_id, folder_name, video_type,
                meta_file_id, meta_text, schedule_time, status
            ) VALUES (
                :u, :fid, :fname, :vt,
                :mid, :mt, :t, 'scheduled'
            )
            ON CONFLICT (folder_id) DO UPDATE SET
                line_user_id   = EXCLUDED.line_user_id,
                folder_name    = EXCLUDED.folder_name,
                video_type     = EXCLUDED.video_type,
                meta_file_id   = EXCLUDED.meta_file_id,
                meta_text      = EXCLUDED.meta_text,
                schedule_time  = EXCLUDED.schedule_time,
                status         = 'scheduled',
                last_error     = NULL
            RETURNING id
        """), {
            "u": line_user_id, "fid": folder_id, "fname": folder_name, "vt": video_type,
            "mid": meta_file_id, "mt": meta_text, "t": dt_utc
        }).fetchone()
    return int(row[0]) if row else 0


def list_scheduled(line_user_id):
    with engine.begin() as conn:
        return conn.execute(sql_text("""
            SELECT id, folder_name, video_type, schedule_time AT TIME ZONE 'Asia/Taipei' AS t
            FROM video_schedules
            WHERE line_user_id=:u AND status='scheduled'
            ORDER BY schedule_time ASC
        """), {"u": line_user_id}).fetchall()

def list_all(line_user_id):
    with engine.begin() as conn:
        return conn.execute(sql_text("""
            SELECT id, folder_name, video_type, schedule_time AT TIME ZONE 'Asia/Taipei' AS t, status
            FROM video_schedules
            WHERE line_user_id=:u
            ORDER BY schedule_time ASC
        """), {"u": line_user_id}).fetchall()

def update_uploaded(line_user_id, folder_id, dt_utc, video_id):
    with engine.begin() as c:
        c.execute(sql_text("""
            UPDATE video_schedules
            SET status='uploaded', youtube_video_id=:vid, last_error=NULL
            WHERE line_user_id=:u AND folder_id=:fid AND schedule_time=:t
        """), {"vid": video_id, "u": line_user_id, "fid": folder_id, "t": dt_utc})

def update_error(line_user_id, folder_id, dt_utc, err):
    with engine.begin() as c:
        c.execute(sql_text("""
            UPDATE video_schedules
            SET status='error', last_error=:err
            WHERE line_user_id=:u AND folder_id=:fid AND schedule_time=:t
        """), {"err": f"{err}", "u": line_user_id, "fid": folder_id, "t": dt_utc})

def cancel_schedule(sid: int):
    with engine.begin() as conn:
        conn.execute(sql_text("UPDATE video_schedules SET status='canceled' WHERE id=:id"), {"id": sid})

def update_schedule_time(sid: int, dt_utc):
    with engine.begin() as conn:
        conn.execute(sql_text("UPDATE video_schedules SET schedule_time=:t WHERE id=:id AND status='scheduled'"),
                     {"t": dt_utc, "id": sid})


def get_schedule_by_id(sid: int):
    with engine.begin() as conn:
        return conn.execute(sql_text("""
            SELECT id, line_user_id, folder_id, folder_name, video_type, meta_text, schedule_time, status, youtube_video_id
            FROM video_schedules
            WHERE id=:id
        """), {"id": sid}).fetchone()

def update_schedule_meta(sid: int, meta_text: dict):
    with engine.begin() as conn:
        conn.execute(sql_text("""
            UPDATE video_schedules
            SET meta_text=CAST(:mt AS JSONB)
            WHERE id=:id
        """), {"id": sid, "mt": json.dumps(meta_text, ensure_ascii=False)})

def get_status_and_video_id(sid: int) -> Optional[tuple]:
    with engine.begin() as conn:
        row = conn.execute(sql_text("""
            SELECT status, youtube_video_id
            FROM video_schedules
            WHERE id=:id
        """), {"id": sid}).fetchone()
    if not row:
        return None
    return row[0], row[1]


# == Add to api/services/scheduler_repo.py ==

from typing import Optional, Dict, List
from sqlalchemy import text as sql_text
from ..db import engine
from datetime import datetime
import json

def is_folder_scheduled(folder_id: str) -> bool:
    with engine.begin() as conn:
        row = conn.execute(sql_text("SELECT 1 FROM video_schedules WHERE folder_id=:fid"), {"fid": folder_id}).fetchone()
    return bool(row)

def insert_schedule_basic(folder_id: str, folder_name: str, video_type: str, schedule_time_utc: datetime, meta_text: Dict) -> int:
    with engine.begin() as conn:
        row = conn.execute(sql_text("""
            INSERT INTO video_schedules (folder_id, folder_name, video_type, schedule_time, meta_text, status)
            VALUES (:fid, :fname, :vtype, :t, CAST(:mt AS JSONB), 'scheduled')
            ON CONFLICT (folder_id) DO NOTHING
            RETURNING id
        """), {"fid": folder_id, "fname": folder_name, "vtype": video_type, "t": schedule_time_utc, "mt": json.dumps(meta_text, ensure_ascii=False)}).fetchone()
    return int(row[0]) if row else 0

def get_due_for_upload(now_utc: datetime) -> List[Dict]:
    with engine.begin() as conn:
        rows = conn.execute(sql_text("""
            SELECT id, folder_id, folder_name, video_type, schedule_time, meta_text
            FROM video_schedules
            WHERE status='scheduled' AND schedule_time <= :now
            ORDER BY schedule_time ASC
            LIMIT 10
        """), {"now": now_utc}).mappings().all()
    return [dict(r) for r in rows]

def mark_uploaded(sid: int, video_id: str):
    with engine.begin() as conn:
        conn.execute(sql_text("""
            UPDATE video_schedules
            SET status='uploaded', youtube_video_id=:vid
            WHERE id=:id
        """), {"id": sid, "vid": video_id})

def mark_failed(sid: int, error: str):
    with engine.begin() as conn:
        conn.execute(sql_text("""
            UPDATE video_schedules
            SET status='failed', last_error=:e
            WHERE id=:id
        """), {"id": sid, "e": error[:800] if error else None})

def get_due_for_publish(now_utc: datetime) -> List[Dict]:
    with engine.begin() as conn:
        rows = conn.execute(sql_text("""
            SELECT id, folder_id, folder_name, video_type, schedule_time, meta_text, youtube_video_id
            FROM video_schedules
            WHERE status='uploaded' AND schedule_time <= :now
            ORDER BY schedule_time ASC
            LIMIT 20
        """), {"now": now_utc}).mappings().all()
    return [dict(r) for r in rows]

def mark_published(sid: int):
    with engine.begin() as conn:
        conn.execute(sql_text("UPDATE video_schedules SET status='published' WHERE id=:id"), {"id": sid})

def get_all_published_with_video_id() -> List[Dict]:
    with engine.begin() as conn:
        rows = conn.execute(sql_text("""
            SELECT id, folder_id, folder_name, video_type, schedule_time, youtube_video_id
            FROM video_schedules
            WHERE status='published' AND youtube_video_id IS NOT NULL
        """)).mappings().all()
    return [dict(r) for r in rows]

# PostgreSQL Advisory lock，避免多 worker 併發
def acquire_lock(lock_key: int) -> bool:
    with engine.begin() as conn:
        row = conn.execute(sql_text("SELECT pg_try_advisory_lock(:k)"), {"k": lock_key}).fetchone()
    return bool(row and row[0])

def release_lock(lock_key: int):
    with engine.begin() as conn:
        conn.execute(sql_text("SELECT pg_advisory_unlock(:k)"), {"k": lock_key})

def get_by_video_id(video_id: str):
    with engine.begin() as conn:
        row = conn.execute(sql_text("""
            SELECT id, sheet_row
            FROM video_schedules
            WHERE youtube_video_id = :vid
            ORDER BY schedule_time DESC
            LIMIT 1
        """), {"vid": video_id}).mappings().first()
    return row

from sqlalchemy import text as sql_text
from api.db import engine

def _has_column(table: str, column: str) -> bool:
    sql = """
      SELECT 1
        FROM information_schema.columns
       WHERE table_schema='public' AND table_name=:t AND column_name=:c
       LIMIT 1
    """
    with engine.begin() as conn:
        row = conn.execute(sql_text(sql), {"t": table, "c": column}).first()
    return bool(row)

def _pick_video_id_col() -> str:
    sql = """
      SELECT column_name
        FROM information_schema.columns
       WHERE table_schema='public'
         AND table_name='video_schedules'
         AND column_name IN ('youtube_video_id','youtube_id','video_id')
    """
    with engine.begin() as conn:
        cols = {r[0] for r in conn.execute(sql_text(sql)).fetchall()}
    for name in ("youtube_video_id", "youtube_id", "video_id"):
        if name in cols:
            return name
    raise RuntimeError("video_schedules 缺少影片 id 欄位（預期 youtube_video_id / youtube_id / video_id）")

def list_published_for_reconcile(limit: int = 300):
    """
    撈出 DB 已標示 published 的資料，用於補帳（Sheet/Drive）。
    動態處理 published_at 欄位：有才選、有才排序。
    """
    col = _pick_video_id_col()
    has_pub_at = _has_column("video_schedules", "published_at")
    select_pub_at = "published_at," if has_pub_at else "NULL::timestamp AS published_at,"
    order_by = "COALESCE(published_at, schedule_time)" if has_pub_at else "schedule_time"

    sql = f"""
        SELECT id, folder_id, sheet_row, {select_pub_at} schedule_time, {col} AS video_id
          FROM public.video_schedules
         WHERE {col} IS NOT NULL
           AND COALESCE(status,'') = 'published'
         ORDER BY {order_by} DESC
         LIMIT :limit
    """
    with engine.begin() as conn:
        rows = conn.execute(sql_text(sql), {"limit": limit}).mappings().all()
    return [dict(r) for r in rows]

def mark_published(schedule_id: int):
    """
    設為 published；若有 published_at 欄位則一併寫入現在時間。
    """
    has_pub_at = _has_column("video_schedules", "published_at")
    if has_pub_at:
        sql = """
            UPDATE public.video_schedules
               SET status='published', published_at=now()
             WHERE id=:id
        """
    else:
        sql = """
            UPDATE public.video_schedules
               SET status='published'
             WHERE id=:id
        """
    with engine.begin() as conn:
        conn.execute(sql_text(sql), {"id": schedule_id})


def list_ready_for_publish(limit: int = 200):
    """
    抓『已到時間』且未標記為已發布/已刪除/已取消的排程。
    只要有 video_id 就納入（不再強制限定 status 必須是 uploaded）。
    """
    col = _pick_video_id_col()
    sql = f"""
        SELECT id, folder_id, sheet_row, schedule_time, status, {col} AS video_id
          FROM public.video_schedules
         WHERE {col} IS NOT NULL
           AND schedule_time <= now()
           AND COALESCE(status,'') NOT IN ('published','deleted','canceled')
         ORDER BY schedule_time ASC
         LIMIT :limit
    """
    with engine.begin() as conn:
        rows = conn.execute(sql_text(sql), {"limit": limit}).mappings().all()
    return [dict(r) for r in rows]

def debug_ready_snapshot(limit: int = 100):
    col = _pick_video_id_col()
    sql = f"""
        SELECT
          id, folder_id, sheet_row, schedule_time, status,
          {col} AS video_id,
          ({col} IS NOT NULL)                        AS has_video_id,
          (schedule_time <= now())                   AS is_due,
          (COALESCE(status,'') NOT IN ('published','deleted','canceled')) AS status_ok
        FROM public.video_schedules
        ORDER BY schedule_time DESC
        LIMIT :limit
    """
    with engine.begin() as conn:
        rows = conn.execute(sql_text(sql), {"limit": limit}).mappings().all()
    return [dict(r) for r in rows]

