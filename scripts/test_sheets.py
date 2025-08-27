from datetime import datetime
from dotenv import load_dotenv
import os
from api.services.sheets_service import append_published_row

# 讀 .env（REPL/獨立腳本才需要）
load_dotenv()

print("SHEET_ID =", os.getenv("SHEET_ID"))
print("PUBLISHED_FOLDER_ID =", os.getenv("PUBLISHED_FOLDER_ID"))

append_published_row(
    datetime.now(),
    "（測試）排程驗證列，可刪",
    "https://drive.google.com/drive/folders/" + (os.getenv("PUBLISHED_FOLDER_ID") or ""),
    "已排程",
    "測試,關鍵字",
    0
)
print("✅ 已寫入 Google Sheet 的「已發布」分頁一列")
