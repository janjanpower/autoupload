# api/services/google_sa.py
import os, json
from google.oauth2 import service_account

def get_sa_credentials(scopes):
    raw = os.getenv("SA_JSON_ENV") or os.getenv("GOOGLE_SA_JSON")
    path = os.getenv("SA_JSON_PATH") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    info = None
    if raw:
        info = json.loads(raw)
    elif path:
        with open(path, "r", encoding="utf-8") as f:
            info = json.load(f)
    if not info:
        raise RuntimeError("缺少 Service Account 憑證：請設定 SA_JSON_ENV 或 GOOGLE_SA_JSON（單行 JSON），或 SA_JSON_PATH/GOOGLE_APPLICATION_CREDENTIALS（檔案路徑）")
    return service_account.Credentials.from_service_account_info(info, scopes=scopes)
