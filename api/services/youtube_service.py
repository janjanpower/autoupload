# api/services/youtube_service.py
from __future__ import annotations

import io
import os
import random
import shutil
import tempfile
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload

# ✅ 統一使用這個取得 YouTube client（由 refresh token 自動換取 access token）
from api.core.youtube_client import get_youtube_client

# ✅ 解析「標題/內文/關鍵字」的人性化格式（或 JSON）→ {title, description, tags}
from api.utils.meta_parser import parse_meta_text


# ---------------------------
# 基礎：取得 YouTube client
# ---------------------------
def _yt():
    return get_youtube_client()


# ---------------------------
# 解析 meta：支援人性化/JSON/純文字
# ---------------------------
def _ensure_meta(meta_text_or_dict) -> Dict:
    """
    將使用者的文字或 dict 轉為 {title, description, tags}
    - 支援 JSON 舊格式
    - 支援「標題：/內文：/關鍵字：」新格式
    - 支援純文字（第一行標題，其餘內文）
    """
    if isinstance(meta_text_or_dict, dict):
        return {
            "title": (meta_text_or_dict.get("title") or "").strip(),
            "description": (meta_text_or_dict.get("description") or "").strip(),
            "tags": meta_text_or_dict.get("tags") or [],
        }
    return parse_meta_text(meta_text_or_dict or "")


# ---------------------------
# YouTube：列出未來要公開的影片清單
# ---------------------------
def list_scheduled_youtube(max_pages: int = 2) -> List[Dict]:
    """
    回傳 YouTube 端目前「已上傳且設定了 *未來* publishAt」的影片。
    格式：[{id, title, publishAt_utc(datetime), url}]
    """
    yt = _yt()

    ch = yt.channels().list(part="contentDetails", mine=True).execute()
    items = ch.get("items", [])
    if not items:
        return []
    uploads_pl = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # 走訪 uploads 播放清單
    video_ids: List[str] = []
    page_token: Optional[str] = None
    pages = 0
    while True:
        resp = yt.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_pl,
            maxResults=50,
            pageToken=page_token,
        ).execute()
        for it in resp.get("items", []):
            video_ids.append(it["contentDetails"]["videoId"])
        page_token = resp.get("nextPageToken")
        pages += 1
        if not page_token or pages >= max_pages:
            break

    if not video_ids:
        return []

    # 查 videos，過濾出「有 publishAt 且在未來」的
    out: List[Dict] = []
    now_utc = datetime.now(timezone.utc)
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i : i + 50]
        vresp = yt.videos().list(part="status,snippet", id=",".join(chunk)).execute()
        for v in vresp.get("items", []):
            st = v.get("status", {})
            publish_at = st.get("publishAt")
            if not publish_at:
                continue
            dt = datetime.fromisoformat(publish_at.replace("Z", "+00:00")).astimezone(
                timezone.utc
            )
            if dt <= now_utc:
                continue
            out.append(
                {
                    "id": v["id"],
                    "title": (v.get("snippet") or {}).get("title", ""),
                    "publishAt_utc": dt,
                    "url": f"https://youtu.be/{v['id']}",
                }
            )
    out.sort(key=lambda x: x["publishAt_utc"])
    return out


# ---------------------------
# YouTube：更新標題/內文/關鍵字
# ---------------------------
# --- 在 youtube_service.py 內，用此版本覆蓋原本的 update_video_metadata() ---
def update_video_metadata(video_id: str, title: str | None = None,
                          description: str | None = None,
                          tags: list[str] | None = None):
    yt = _yt()
    resp = yt.videos().list(part="snippet", id=video_id).execute()
    items = resp.get("items", [])
    if not items:
        raise Exception(f"找不到影片 {video_id}")

    snippet = items[0]["snippet"]
    if title is not None:
        snippet["title"] = title
    if description is not None:
        snippet["description"] = description
    if tags is not None:
        snippet["tags"] = tags

    yt.videos().update(part="snippet", body={"id": video_id, "snippet": snippet}).execute()

    # 成功後嘗試同步 Google Sheet（未設定會靜默略過）
    try:
        from api.services import sheets_service as sheet_service
        sheet_service.safe_update_metadata(video_id, title, description, tags)
    except Exception:
        pass


# ---------------------------
# YouTube：更新 publishAt（定時公開）
# ---------------------------
def update_publish_time(video_id: str, new_dt_utc: datetime):
    """
    更新 publishAt（videos.update part='status'）
    注意：YouTube 端需要 privacyStatus 為 private/unlisted 才能使用定時公開。
    """
    yt = _yt()
    body = {
        "id": video_id,
        "status": {
            "privacyStatus": "private",  # 你的流程若慣用 unlisted 也可改
            "publishAt": new_dt_utc.replace(tzinfo=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        },
    }
    yt.videos().update(part="status", body=body).execute()


# ---------------------------
# 本機：從資料夾挑一張縮圖（可選）
# ---------------------------
def pick_thumbnail_in_folder(folder_path: str) -> Optional[str]:
    """
    從本機資料夾挑選一張縮圖 (jpg/jpeg/png)，回傳檔案路徑；找不到則回傳 None
    """
    if not os.path.isdir(folder_path):
        return None
    valid_ext = {".jpg", ".jpeg", ".png"}
    candidates = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.splitext(f)[1].lower() in valid_ext
    ]
    if not candidates:
        return None
    return random.choice(candidates)

def update_thumbnail(youtube, video_id, thumbnail_path):
    request = youtube.thumbnails().set(
        videoId=video_id,
        media_body=MediaFileUpload(thumbnail_path)
    )
    response = request.execute()
    return response

# 放在 youtube_service.py（和其他函式並列）
import os, tempfile
from googleapiclient.http import MediaFileUpload

def update_thumbnail_file(video_id: str, thumbnail_path: str) -> None:
    """用本機檔案路徑直接更新縮圖"""
    yt = _yt()
    yt.thumbnails().set(videoId=video_id, media_body=thumbnail_path).execute()

def update_thumbnail_from_drive(video_id: str, folder_id: str) -> None:
    """從 Google Drive 資料夾挑一張 jpg/png 下載後，更新為縮圖"""
    files = _list_drive_files(folder_id)
    _, thumb = _pick_drive_files(files)
    if not thumb:
        raise RuntimeError("資料夾內找不到可用的縮圖（jpg/png）")

    tmp = tempfile.mktemp(prefix="ytthumb_", suffix=os.path.splitext(thumb.get("name",""))[1] or ".jpg")
    _download_drive_file(thumb["id"], tmp)
    try:
        update_thumbnail_file(video_id, tmp)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


# ---------------------------
# Google Drive 輔助（盡量相容既有命名）
# ---------------------------
_VIDEO_EXT = {".mp4", ".mov", ".mkv", ".avi", ".m4v", ".webm"}
_IMAGE_EXT = {".jpg", ".jpeg", ".png"}


def _try_import_drive_funcs():
    """
    嘗試從 api.services.drive_service 匯入你可能已有的函式名稱。
    只要抓到其中任一組即可運作；若全抓不到，youtube_upload_from_drive 會拋出清楚的錯誤。
    """
    list_files_fn = None
    download_to_path_fn = None
    download_bytes_fn = None

    try:
        from api.services.drive_service import list_files_in_folder as _list_files_in_folder  # type: ignore
        list_files_fn = _list_files_in_folder
    except Exception:
        try:
            from api.services.drive_service import list_files as _list_files_in_folder  # type: ignore
            list_files_fn = _list_files_in_folder
        except Exception:
            pass

    # 先偏好「直接存到路徑」的函式
    try:
        from api.services.drive_service import download_file_to_path as _download_to_path  # type: ignore
        download_to_path_fn = _download_to_path
    except Exception:
        # 退而求其次：拿 bytes 我們自己寫檔
        try:
            from api.services.drive_service import download_file as _download_file  # type: ignore
            download_bytes_fn = _download_file
        except Exception:
            try:
                from api.services.drive_service import download_binary as _download_file  # type: ignore
                download_bytes_fn = _download_file
            except Exception:
                pass

    return list_files_fn, download_to_path_fn, download_bytes_fn


def _list_drive_files(folder_id: str) -> List[Dict]:
    list_files_fn, _, _ = _try_import_drive_funcs()
    if not list_files_fn:
        raise RuntimeError(
            "找不到 Drive 列檔函式（需要 api.services.drive_service.list_files_in_folder 或 list_files）。"
        )
    # 假設回傳格式為 [{'id','name','mimeType','size',...}, ...]
    return list_files_fn(folder_id)


def _download_drive_file(file_id: str, dst_path: str):
    _, download_to_path_fn, download_bytes_fn = _try_import_drive_funcs()
    if download_to_path_fn:
        download_to_path_fn(file_id, dst_path)
        return
    if download_bytes_fn:
        data = download_bytes_fn(file_id)
        if isinstance(data, (bytes, bytearray)):
            with open(dst_path, "wb") as f:
                f.write(data)
            return
        if isinstance(data, str) and os.path.exists(data):
            shutil.copy(data, dst_path)
            return
    raise RuntimeError(
        "找不到 Drive 下載函式（需要 api.services.drive_service.download_file_to_path 或 download_file / download_binary）。"
    )


def _pick_drive_files(files: List[Dict]) -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    從 Drive 檔案列表挑出影片 & 縮圖（皆可選）
    規則：
    - 影片：mimeType 以 'video/' 開頭，或副檔名在 _VIDEO_EXT
    - 縮圖：mimeType 以 'image/' 開頭，或副檔名在 _IMAGE_EXT
    - 若多個：選擇 size 最大的影片；縮圖任選一個
    """
    def _ext(name: str) -> str:
        return os.path.splitext(name)[1].lower()

    videos: List[Dict] = []
    thumbs: List[Dict] = []
    for f in files:
        name = f.get("name") or ""
        mt = (f.get("mimeType") or "").lower()
        ext = _ext(name)
        if (mt.startswith("video/")) or (ext in _VIDEO_EXT):
            videos.append(f)
        elif (mt.startswith("image/")) or (ext in _IMAGE_EXT):
            thumbs.append(f)

    # 影片挑 size 最大（若無 size 則保持原順序）
    def _size(d: Dict) -> int:
        try:
            return int(d.get("size") or 0)
        except Exception:
            return 0

    chosen_video = sorted(videos, key=_size, reverse=True)[0] if videos else None
    chosen_thumb = thumbs[0] if thumbs else None
    return chosen_video, chosen_thumb


# ---------------------------
# 上傳：從 Google Drive 資料夾到 YouTube
# ---------------------------
def youtube_upload_from_drive(
    folder_id: str,
    meta_text,
    schedule_time_utc: Optional[datetime],
    video_type: str = "long",
) -> str:
    """
    從 Google Drive 的資料夾抓影片（與可選縮圖），上傳到 YouTube。
    :param folder_id: Drive 資料夾 ID
    :param meta_text: 使用者提供的文字（人性化三段式 / JSON 皆可）
    :param schedule_time_utc: 若有，設定 publishAt（UTC）
    :param video_type: "long" / "short"（可依需要調整 categoryId 或標籤）
    :return: 上傳成功的 YouTube videoId
    """
    yt = _yt()
    meta = _ensure_meta(meta_text)

    # 1) 列出 Drive 檔案，挑影片與縮圖
    files = _list_drive_files(folder_id)
    video_file, thumb_file = _pick_drive_files(files)
    if not video_file:
        raise RuntimeError("此資料夾內找不到可上傳的影片檔。")

    # 2) 下載至暫存檔
    os.makedirs("/tmp", exist_ok=True)
    video_tmp = tempfile.mktemp(prefix="ytvid_", suffix=os.path.splitext(video_file.get("name", ""))[1] or ".mp4")
    _download_drive_file(video_file["id"], video_tmp)

    thumb_tmp = None
    if thumb_file:
        thumb_tmp = tempfile.mktemp(prefix="ytthumb_", suffix=os.path.splitext(thumb_file.get("name", ""))[1] or ".jpg")
        try:
            _download_drive_file(thumb_file["id"], thumb_tmp)
        except Exception:
            thumb_tmp = None  # 縮圖抓不到也不影響上傳

        # 3) 準備上傳 body
    body = {
        "snippet": {
            "title": meta["title"],
            "description": meta["description"],
            "tags": meta.get("tags", []),
            "categoryId": os.getenv("YT_DEFAULT_CATEGORY_ID") or "22",  # 人物與網誌
            "defaultLanguage": "zh-Hant",
            "defaultAudioLanguage": "zh-Hant",
        },
        "status": {
            # 走「排程公開」時，privacy 要是 private/unlisted，其餘立即公開就設 public
            "privacyStatus": "private",          # 這支函式有帶 schedule_time_utc，預設 private
            "license": "youtube",                # 標準 YouTube 授權
            "embeddable": True,                  # 允許嵌入
            "publicStatsViewable": True,         # 顯示統計數據
            "madeForKids": False,                # 不是兒童專屬
            "selfDeclaredMadeForKids": False,
        },
    }
    if schedule_time_utc:
        body["status"]["publishAt"] = (
            schedule_time_utc.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        )


    # 4) 上傳影片（resumable）
    media = MediaFileUpload(video_tmp, chunksize=8 * 1024 * 1024, resumable=True, mimetype="video/*")
    request = yt.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
        notifySubscribers=True
    )

    response = None
    while response is None:
        status, response = request.next_chunk()
        # 你可以在此回報進度給內部 log：if status: print(status.progress())

    if not response or "id" not in response:
        raise RuntimeError("YouTube 回傳不含 videoId，請檢查權限或檔案。")

    video_id = response["id"]

    # 5) 設定縮圖（若有）
    try:
        if thumb_tmp and os.path.exists(thumb_tmp):
            yt.thumbnails().set(videoId=video_id, media_body=thumb_tmp).execute()
    except Exception:
        pass  # 縮圖失敗不影響整體流程

    # 6) 清理暫存檔
    try:
        if os.path.exists(video_tmp):
            os.remove(video_tmp)
        if thumb_tmp and os.path.exists(thumb_tmp):
            os.remove(thumb_tmp)
    except Exception:
        pass

    return video_id
