# api/services/google_sa.py
import os, json
from google.oauth2 import service_account
from googleapiclient.discovery import build

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

def get_google_service(api_name: str, api_version: str, scopes: list[str]):
    """
    建立 Google API service client
    - api_name: e.g. "sheets", "drive"
    - api_version: e.g. "v4"
    - scopes: e.g. ["https://www.googleapis.com/auth/drive"]
    """
    creds = get_sa_credentials(scopes)
    return build(api_name, api_version, credentials=creds, cache_discovery=False)
