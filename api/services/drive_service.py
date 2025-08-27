# api/services/drive_service.py
import os
import io
import tempfile
from typing import Dict, List, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from ..config import settings

# 所有查詢都支援個人雲端/共用雲端
DRIVE_KW = dict(supportsAllDrives=True, includeItemsFromAllDrives=True)


# ---------------------------
# 建立 Drive v3 Client
# ---------------------------
from .google_sa import get_sa_credentials

_drive = None
def get_drive_service():
    global _drive
    if _drive:
        return _drive
    creds = get_sa_credentials(["https://www.googleapis.com/auth/drive"])
    _drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _drive

# ---------------------------
# 你原本的功能（保留）
# ---------------------------
def list_child_folders(parent_id: str, page_size: int = 200) -> List[Dict]:
    svc = get_drive_service()
    q = f"'{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    res = svc.files().list(
        q=q,
        pageSize=page_size,
        fields="files(id,name),nextPageToken",
        orderBy="name_natural",
        **DRIVE_KW,
    ).execute()
    return res.get("files", [])


def get_single_video_in_folder(folder_id: str) -> Optional[Dict]:
    svc = get_drive_service()
    q = f"'{folder_id}' in parents and mimeType contains 'video/' and trashed = false"
    res = svc.files().list(
        q=q,
        pageSize=1,
        fields="files(id,name,mimeType,videoMediaMetadata(width,height,durationMillis))",
        orderBy="name_natural",
        **DRIVE_KW,
    ).execute()
    files = res.get("files", [])
    return files[0] if files else None


def find_text_file_in_folder(folder_id: str) -> Optional[Dict]:
    svc = get_drive_service()
    q = f"'{folder_id}' in parents and mimeType = 'text/plain' and trashed = false"
    res = svc.files().list(
        q=q,
        pageSize=50,
        fields="files(id,name,size)",
        orderBy="name_natural",
        **DRIVE_KW,
    ).execute()
    files = res.get("files", [])
    return files[0] if files else None


def download_text(file_id: str) -> str:
    svc = get_drive_service()
    request = svc.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read().decode("utf-8", errors="replace")


def upload_text(file_id: str, content: str):
    svc = get_drive_service()
    media_body = MediaIoBaseUpload(
        io.BytesIO(content.encode("utf-8")), mimetype="text/plain", resumable=False
    )
    svc.files().update(fileId=file_id, media_body=media_body, **DRIVE_KW).execute()


def download_to_tempfile(file_id: str, suffix: str = "") -> str:
    svc = get_drive_service()
    req = svc.files().get_media(fileId=file_id)
    fd, path = tempfile.mkstemp(prefix="gdrv_", suffix=suffix)
    with os.fdopen(fd, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return path


# ---------------------------
# 新增：通用清單與下載（YouTube 上傳會用到）
# ---------------------------
def list_files_in_folder(folder_id: str) -> List[Dict]:
    """
    列出資料夾內所有檔案（含 id, name, mimeType, size, modifiedTime）
    供上層自由篩選影片/縮圖/文字檔。
    """
    svc = get_drive_service()
    q = f"'{folder_id}' in parents and trashed = false"
    fields = "nextPageToken, files(id,name,mimeType,size,modifiedTime)"
    page_token: Optional[str] = None
    results: List[Dict] = []
    while True:
        resp = svc.files().list(
            q=q,
            fields=fields,
            pageSize=1000,
            pageToken=page_token,
            orderBy="name_natural",
            **DRIVE_KW,
        ).execute()
        results.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return results


def list_files(folder_id: str) -> List[Dict]:
    """
    別名：相容舊程式。
    """
    return list_files_in_folder(folder_id)


def download_file_to_path(file_id: str, dst_path: str) -> None:
    """
    下載單一檔案到指定路徑。會自動建立目的地資料夾。
    """
    svc = get_drive_service()
    req = svc.files().get_media(fileId=file_id)
    os.makedirs(os.path.dirname(dst_path) or ".", exist_ok=True)
    with open(dst_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def download_file(file_id: str) -> bytes:
    """
    以 bytes 形式下載檔案（給需要記憶體中處理的場景）
    """
    svc = get_drive_service()
    req = svc.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()


def download_binary(file_id: str) -> bytes:
    """
    別名：相容舊程式。
    """
    return download_file(file_id)


# ---------------------------
#（可選）沒有文字檔時自動建立一個模板檔
# ---------------------------
def create_text_in_folder(parent_id: str, content: str, name: str = "meta.txt") -> Dict:
    """
    在指定資料夾內建立一個文字檔，回傳 {id, name}
    """
    svc = get_drive_service()
    file_metadata = {
        "name": name,
        "mimeType": "text/plain",
        "parents": [parent_id],
    }
    media = MediaIoBaseUpload(io.BytesIO(content.encode("utf-8")), mimetype="text/plain")
    f = svc.files().create(body=file_metadata, media_body=media, fields="id,name", **DRIVE_KW).execute()
    return f


