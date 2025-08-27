# scripts/resolve_conflicts.py
import os, sys, json, pytz
from datetime import datetime, timedelta, time
from collections import defaultdict
from dotenv import load_dotenv

# 讓 Python 找到頂層套件
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path: sys.path.insert(0, ROOT)

load_dotenv()

from sqlalchemy import text as sql_text
from api.db import engine
from api.services.sheets_service import find_row_by_title_and_folder, update_status_and_views
from api.services.youtube_service import get_youtube_client
from api.services.auto_scheduler import TZ  # 直接用我們共用的 Asia/Taipei


def _yt_reserved_slots_tpe():
    """抓 YouTube 後台已經排定的 publishAt（private+sched），回傳 set[datetime in TPE]"""
    reserved = set()
    yt = get_youtube_client()
    if yt is None:
        return reserved

    resp = yt.search().list(
        part="id", forMine=True, type="video", order="date", maxResults=50
    ).execute()
    ids = [it["id"]["videoId"] for it in resp.get("items", []) if it.get("id", {}).get("videoId")]
    if not ids:
        return reserved

    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        v = yt.videos().list(part="status", id=",".join(chunk)).execute()
        for it in v.get("items", []):
            st = it.get("status", {})
            if st.get("privacyStatus") == "private" and st.get("publishAt"):
                try:
                    dt_utc = datetime.fromisoformat(st["publishAt"].replace("Z","+00:00")).astimezone(pytz.UTC)
                    if dt_utc > datetime.utcnow().replace(tzinfo=pytz.UTC):
                        reserved.add(dt_utc.astimezone(TZ).replace(second=0, microsecond=0))
                except Exception:
                    pass
    return reserved


def _db_reserved_slots_tpe():
    """抓 DB 中 scheduled / uploaded 的時段，轉成 TPE"""
    with engine.begin() as conn:
        rows = conn.execute(sql_text("""
            SELECT id, folder_id, folder_name, video_type, schedule_time, meta_text, status
            FROM video_schedules
            WHERE status IN ('scheduled','uploaded')
        """)).mappings().all()

    slots = defaultdict(list)
    for r in rows:
        t_tpe = r["schedule_time"].astimezone(TZ).replace(second=0, microsecond=0)
        slots[t_tpe].append(r)
    return slots


def _next_free_slot_1830(reserved_set: set, start_after: datetime):
    """找出下一個 18:30 時段，避開 reserved_set"""
    day = start_after.date()
    while True:
        candidate = datetime.combine(day, time(18, 30))
        candidate = TZ.localize(candidate).replace(second=0, microsecond=0)
        if candidate > start_after and candidate not in reserved_set:
            return candidate
        day += timedelta(days=1)


def main():
    # 已被 YouTube 佔用的時段
    yt_reserved = _yt_reserved_slots_tpe()
    # DB 目前排定/已上傳的時段（可能多支卡同一時刻）
    db_slots = _db_reserved_slots_tpe()

    occupied = set(yt_reserved)
    to_move = []
    for tpe_time, items in db_slots.items():
        items_sorted = sorted(items, key=lambda r: r["id"])  # 先進先贏
        keep = items_sorted[0]
        occupied.add(tpe_time)
        for x in items_sorted[1:]:
            to_move.append(x)

    if not to_move and not yt_reserved:
        print("✅ 沒有需要調整的衝突；YouTube 端也沒有外部佔用")
        return

    moved = 0
    for r in to_move:
        cur_tpe = r["schedule_time"].astimezone(TZ).replace(second=0, microsecond=0)
        meta = r.get("meta_text") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except:
                meta = {}
        title = meta.get("title") or r["folder_name"] or r["folder_id"]

        # 找下一個可用時段（固定 18:30）
        nxt = _next_free_slot_1830(occupied, cur_tpe)
        if not nxt:
            continue

        # 寫回 DB（UTC）
        with engine.begin() as conn:
            conn.execute(sql_text("UPDATE video_schedules SET schedule_time=:t WHERE id=:id"),
                         {"t": nxt.astimezone(pytz.UTC), "id": r["id"]})

        # 更新 Sheet 日期（A 欄）
        row_idx = find_row_by_title_and_folder(title, None)
        if row_idx:
            update_status_and_views(row_idx, folder_url=None)
            from api.services.sheets_service import _sheet_id, _sheets, SHEET_TAB
            _sheets().values().update(
                spreadsheetId=_sheet_id(),
                range=f"{SHEET_TAB}!A{row_idx}",
                valueInputOption="USER_ENTERED",
                body={"values": [[nxt.strftime("%Y-%m-%d %H:%M")]]}
            ).execute()

        occupied.add(nxt)
        moved += 1

    print(f"✅ 已處理 YouTube 既有檔期 {len(yt_reserved)} 個；重新排定 {moved} 筆撞檔期到下一個可用時段。")


if __name__ == "__main__":
    main()
