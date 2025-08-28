import os, json
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# YouTube 預設（與原檔一致）
DEFAULT_YT_OPTS = {
    "categoryId": "22",
    "defaultLanguage": "zh-Hant",
    "defaultAudioLanguage": "zh-Hant",
    "license": "youtube",
    "embeddable": True,
    "publicStatsViewable": True,
    "selfDeclaredMadeForKids": False,
}

class Settings:
    LINE_SECRET     = os.getenv("LINE_CHANNEL_SECRET", "")
    LINE_TOKEN      = os.getenv("LINE_CHANNEL_TOKEN", "")
    DRIVE_PARENT_ID = os.getenv("GOOGLE_DRIVE_PARENT_ID", "")
    SA_JSON_ENV     = os.getenv("GOOGLE_SA_JSON", "")
    LINE_SKIP_SIG   = os.getenv("LINE_SKIP_SIGNATURE", "0") == "1"
    YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
    YT_CLIENT_ID       = os.getenv("YT_CLIENT_ID", "")
    YT_CLIENT_SECRET   = os.getenv("YT_CLIENT_SECRET", "")
    YT_REFRESH_TOKEN   = os.getenv("YT_REFRESH_TOKEN", "")
    YT_DEFAULT_PRIVACY = os.getenv("YT_DEFAULT_PRIVACY", "private")

    # ✅ 用環境變數提供；沒有就給空，避免假資料誤導
    SHEET_ID  = os.getenv("SHEET_ID", "")
    # ✅ 兼容舊命名：優先 SHEET_TAB，退而求其次 TAB_NAME
    SHEET_TAB = os.getenv("SHEET_TAB", os.getenv("TAB_NAME", "已發布"))

    # ✅ DB：統一用 DATABASE_URL，並兼容舊變數
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        os.getenv("RAW_DB_URL", os.getenv("DB_URL", ""))
    )

    def sa_info(self):
        if not self.SA_JSON_ENV:
            return None
        return json.loads(self.SA_JSON_ENV)

settings = Settings()