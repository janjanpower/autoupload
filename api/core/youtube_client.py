# api/core/youtube_client.py
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import os

def _from_oauth_refresh_token():
    cid  = os.getenv("YT_CLIENT_ID")
    csec = os.getenv("YT_CLIENT_SECRET")
    rtok = os.getenv("YT_REFRESH_TOKEN")
    if not (cid and csec and rtok):
        return None

    # ⚠️ 注意：不要指定 scopes，避免 refresh 時 scope 不相符造成 invalid_scope
    creds = Credentials(
        token=None,
        refresh_token=rtok,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=cid,
        client_secret=csec,
    )
    creds.refresh(Request())
    return build("youtube", "v3", credentials=creds, cache_discovery=False)

def get_youtube_client():
    """取得 YouTube API client，優先使用 OAuth refresh token"""
    return _from_oauth_refresh_token()
