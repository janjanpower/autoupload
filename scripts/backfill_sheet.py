# scripts/backfill_sheet.py
import os, sys, json, pytz
from datetime import datetime
from dotenv import load_dotenv

# 讓 Python 找到頂層套件 api/
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# 載入 .env
load_dotenv()

from sqlalchemy import text as sql_text
from api.db import engine
from api.services.sheets_service import append_published_row, find_row_by_title_and_folder

TZ = pytz.timezone("Asia/Taipei")
PUB_FOLDER_ID = os.getenv("PUBLISHED_FOLDER_ID", "").strip()
PUB_FOLDER_URL = f"https://drive.google.com/drive/folders/{PUB_FOLDER_ID}" if PUB_FOLDER_ID else ""

def main():
    with engine.begin() as conn:
        rows = conn.execute(sql_text("""
            SELECT folder_id, folder_name, schedule_time, meta_text, status
            FROM video_schedules
            WHERE status IN ('scheduled','uploaded')
            ORDER BY schedule_time ASC
        """)).mappings().all()

    added = 0
    for r in rows:
        meta = r["meta_text"] or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except:
                meta = {}
        title = (meta.get("title") or r["folder_name"] or r["folder_id"]).strip()
        # 若表上沒有這個標題的列，就補一列（狀態=已排程；C 先放「已發布資料夾」連結，公開後系統會自動改成子夾連結）
        if find_row_by_title_and_folder(title, None) is None:
            dt_local = r["schedule_time"].astimezone(TZ)
            keywords = ",".join(meta.get("tags", [])) if isinstance(meta.get("tags"), list) else ""
            append_published_row(dt_local, title, PUB_FOLDER_URL, "已排程", keywords, 0)
            added += 1

    print(f"✅ 回填完成：新增 {added} 列到 Google Sheet 的「已發布」分頁")

if __name__ == "__main__":
    main()
