# api/services/auto_scheduler.py

import json
import os
import re
import io
import logging


from googleapiclient.discovery import build
import tempfile
from typing import Dict, List, Optional, Tuple ,Set
from datetime import datetime, timedelta

import pytz
from sqlalchemy import text as sql_text
from sqlalchemy import create_engine, text
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# === 專案內匯入（全部用絕對匯入，避免相對路徑問題） ===
from api.db import engine
from api.core.youtube_client import get_youtube_client
from api.services import scheduler_repo
from api.services.drive_service import get_drive_service
from api.services.youtube_service import update_thumbnail_from_drive, list_scheduled_youtube,list_videos_status_map
from api.services.sheets_service import (
    append_published_row,
    update_status_and_views,
    find_row_by_title_and_folder,
    mark_row_published,  # ← 發布後更新「已發布」＋超連結
    set_published_folder_link,
    clear_sheet_row_status,
    get_sheet_values,
    delete_rows,
    update_title
)

from api.services.google_sa import get_google_service
from api.config import settings

# 固定台北時區
TZ = pytz.timezone("Asia/Taipei")
logger = logging.getLogger(__name__)

PARENT_FOLDER_ID    = os.getenv("PARENT_FOLDER_ID", "")
PUBLISHED_FOLDER_ID = os.getenv("PUBLISHED_FOLDER_ID", "")


# -------------------- Drive helpers --------------------

def _drive():
    return get_drive_service()

def _list_child_folders(parent_id: str) -> List[Dict]:
    q = f"'{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    svc = _drive()
    items: List[Dict] = []
    page_token = None
    while True:
        r = svc.files().list(
            q=q,
            fields="nextPageToken, files(id,name,parents,webViewLink,createdTime)",
            supportsAllDrives=True, includeItemsFromAllDrives=True,
            pageToken=page_token
        ).execute()
        items.extend(r.get("files", []))
        page_token = r.get("nextPageToken")
        if not page_token:
            break
    return items

def _get_text_file_in_folder(folder_id: str, name: str = "meta.txt") -> Optional[str]:
    q = f"'{folder_id}' in parents and name = '{name}' and mimeType = 'text/plain' and trashed = false"
    svc = _drive()
    r = svc.files().list(q=q, fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files = r.get("files", [])
    if not files:
        return None
    fid = files[0]["id"]
    request = svc.files().get_media(fileId=fid, supportsAllDrives=True)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue().decode("utf-8", errors="ignore")

def _pick_one_video_in_folder(folder_id: str) -> Optional[Dict]:
    q = f"'{folder_id}' in parents and mimeType contains 'video/' and trashed = false"
    svc = _drive()
    r = svc.files().list(
        q=q,
        fields="files(id,name,mimeType,videoMediaMetadata(width,height,durationMillis))",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
        pageSize=10,
    ).execute()
    files = r.get("files", [])
    return files[0] if files else None

def _classify_type_by_ratio(folder_id: str) -> Optional[str]:
    v = _pick_one_video_in_folder(folder_id)
    if not v:
        return None
    meta = (v.get("videoMediaMetadata") or {})
    w, h = meta.get("width"), meta.get("height")
    if not w or not h:
        return None
    try:
        return "short" if int(w) < int(h) else "long"
    except Exception:
        return None

def _move_folder_to_published(fid: str) -> str:
    """
    將 fid 移到已發布資料夾，並回傳該資料夾的 webViewLink（沒有就回傳可直接組的 URL）。
    """
    if not PUBLISHED_FOLDER_ID:
        return f"https://drive.google.com/drive/folders/{fid}"
    svc = _drive()
    # 先抓目前 parents
    f = svc.files().get(fileId=fid, fields="id,parents", supportsAllDrives=True).execute()
    parents = ",".join(f.get("parents", [])) if f.get("parents") else None
    svc.files().update(
        fileId=fid,
        addParents=PUBLISHED_FOLDER_ID,
        removeParents=parents,
        fields="id, parents",
        supportsAllDrives=True
    ).execute()
    # 重新抓 webViewLink（或用預設 URL）
    g = svc.files().get(fileId=fid, fields="webViewLink", supportsAllDrives=True).execute()
    return g.get("webViewLink") or f"https://drive.google.com/drive/folders/{fid}"


# -------------------- 檔期/時間規則 --------------------

def _iter_1830_on_weekdays(weekdays: List[int], start_local: Optional[datetime] = None):
    if start_local is None:
        start_local = datetime.now(TZ)
    elif start_local.tzinfo is None:
        start_local = TZ.localize(start_local)
    else:
        start_local = start_local.astimezone(TZ)

    day = start_local.date()
    while True:
        naive_1830 = datetime(day.year, day.month, day.day, 18, 30, 0, 0)
        candidate = TZ.localize(naive_1830)
        if candidate >= start_local and candidate.weekday() in weekdays:
            yield candidate
        day = day + timedelta(days=1)

def _yt_reserved_slots_tpe() -> set:
    """讀 YouTube 後台目前『已排定』時段（private + future publishAt），回傳 Asia/Taipei 的 aware datetime（分、秒清零）"""
    occupied = set()
    yt = get_youtube_client()
    if yt is None:
        return occupied
    try:
        s = yt.search().list(part="id", forMine=True, type="video", order="date", maxResults=50).execute()
        ids = [it["id"]["videoId"] for it in s.get("items", []) if it.get("id", {}).get("videoId")]
        if not ids:
            return occupied
        for i in range(0, len(ids), 50):
            chunk = ids[i:i+50]
            v = yt.videos().list(part="status", id=",".join(chunk)).execute()
            for it in v.get("items", []):
                st = it.get("status", {})
                pa = st.get("publishAt")
                if st.get("privacyStatus") == "private" and pa:
                    try:
                        dt_utc = datetime.fromisoformat(pa.replace("Z", "+00:00")).astimezone(pytz.UTC)
                        if dt_utc > datetime.utcnow().replace(tzinfo=pytz.UTC):
                            occupied.add(dt_utc.astimezone(TZ).replace(second=0, microsecond=0))
                    except Exception:
                        pass
    except Exception:
        return set()
    return occupied

def _alloc_next_free_slots(video_type: str, n: int) -> List[datetime]:
    """
    依規則分配 n 個「不撞檔期」的 18:30 檔位（Asia/Taipei），並回傳為 UTC datetime。
    規則：短片=週一/週五、長片=週三；全部 18:30；避開 DB 舊檔期＋YT 後台既定 publishAt。
    """
    with engine.begin() as conn:
        rows = conn.execute(sql_text("""
            SELECT schedule_time FROM video_schedules
            WHERE status IN ('scheduled','uploaded')
        """)).fetchall()
    occupied = {r[0].astimezone(TZ).replace(second=0, microsecond=0) for r in rows}
    occupied |= _yt_reserved_slots_tpe()

    weekdays = [0, 4] if video_type == "short" else [2]
    gen = _iter_1830_on_weekdays(weekdays, datetime.now(TZ))
    out: List[datetime] = []
    while len(out) < n:
        cand_tpe = next(gen).replace(second=0, microsecond=0)
        if cand_tpe not in occupied:
            out.append(cand_tpe.astimezone(pytz.UTC))
            occupied.add(cand_tpe)
    return out

# -------------------- 自動掃描＋排程上傳 --------------------

def _safe_parse_meta(text: Optional[str]) -> Dict:
    if not text:
        return {}
    try:
        import json
        return json.loads(text) if text.strip().startswith("{") else {}
    except Exception:
        return {}

def _upload_by_folder(folder_id: str, meta: Dict, publish_at_utc: datetime) -> str:
    yt = get_youtube_client()
    if yt is None:
        raise RuntimeError("YouTube client 未就緒")

    svc = _drive()
    r = svc.files().list(
        q=f"'{folder_id}' in parents and mimeType contains 'video/' and trashed=false",
        fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=1
    ).execute()
    vids = r.get("files", [])
    if not vids:
        raise RuntimeError("資料夾內沒有影片檔")
    v = vids[0]

    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    req = svc.files().get_media(fileId=v["id"], supportsAllDrives=True)
    dl = MediaIoBaseDownload(tmp_video, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    tmp_video.flush(); tmp_video.close()

    # optional jpg thumbnail
    thumb_path = None
    rj = svc.files().list(
        q=f"'{folder_id}' in parents and mimeType contains 'image/' and name contains '.jpg' and trashed=false",
        fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=1
    ).execute()
    imgs = rj.get("files", [])
    if imgs:
        ttmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        req2 = svc.files().get_media(fileId=imgs[0]["id"], supportsAllDrives=True)
        dl2 = MediaIoBaseDownload(ttmp, req2)
        done = False
        while not done:
            _, done = dl2.next_chunk()
        ttmp.flush(); ttmp.close()
        thumb_path = ttmp.name

    def _compose_body(meta: Dict, publish_at_utc: datetime) -> Dict:
        privacy = os.getenv("YT_DEFAULT_PRIVACY") or "private"
        body = {
            "snippet": {
                "title": meta.get("title") or "",
                "description": meta.get("description") or "",
                "tags": meta.get("tags") or [],
                "defaultLanguage": "zh-Hant",
                "defaultAudioLanguage": "zh-Hant",
                "categoryId": os.getenv("YT_DEFAULT_CATEGORY_ID") or "22",
            },
            "status": {
                "privacyStatus": privacy,
                "license": "youtube",
                "embeddable": True,
                "publicStatsViewable": True,
                "madeForKids": False,
                "selfDeclaredMadeForKids": False,
            },
        }
        if publish_at_utc:
            body["status"]["publishAt"] = publish_at_utc.astimezone(pytz.UTC).isoformat().replace("+00:00","Z")
        return body

    media = MediaIoBaseUpload(open(tmp_video.name, "rb"), mimetype="video/*", chunksize=8*1024*1024, resumable=True)
    body = _compose_body(meta, publish_at_utc)
    resp = yt.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
        notifySubscribers=True
    ).execute()
    video_id = resp["id"]

    if thumb_path:
        try:
            yt.thumbnails().set(videoId=video_id, media_body=thumb_path).execute()
        except Exception:
            pass

    try:
        os.remove(tmp_video.name)
        if thumb_path:
            os.remove(thumb_path)
    except Exception:
        pass

    return video_id

def scan_and_schedule_from_mother():
    """
    掃描母資料夾 → 配置不撞期的 18:30 檔位（短=一/五；長=三）→ 立刻上傳到 YouTube（publishAt 設為檔位）
    → 上傳成功後寫入 Sheet（狀態=已排程），DB 狀態由 scheduled → uploaded。
    """
    if not PARENT_FOLDER_ID:
        logger.warning("PARENT_FOLDER_ID 未設定，略過掃描")
        return

    folders = _list_child_folders(PARENT_FOLDER_ID)
    if not folders:
        return

    short_candidates: List[Tuple[str, str, Dict]] = []
    long_candidates:  List[Tuple[str, str, Dict]] = []

    for f in folders:
        fid, fname = f["id"], f.get("name", "")
        if scheduler_repo.is_folder_scheduled(fid):
            continue
        vtype = _classify_type_by_ratio(fid) or "long"
        meta_text_raw = _get_text_file_in_folder(fid) or ""
        meta = _safe_parse_meta(meta_text_raw)
        (short_candidates if vtype == "short" else long_candidates).append((fid, fname, meta))

    def _assign_and_upload(cands: List[Tuple[str, str, Dict]], vtype: str):
        if not cands:
            return
        slots = _alloc_next_free_slots(vtype, len(cands))
        for (fid, fname, meta), when_utc in zip(cands, slots):
            # 1) DB: scheduled
            try:
                sid = scheduler_repo.insert_schedule_basic(fid, fname, vtype, when_utc, meta)
                if not sid:
                    continue
            except Exception as e:
                logger.warning("寫入 DB(scheduled) 失敗 %s: %s", fid, e)
                continue

            # 2) 上傳 + 設 publishAt
            try:
                video_id = _upload_by_folder(fid, meta or {}, when_utc)
                scheduler_repo.mark_uploaded(sid, video_id)
            except Exception as e:
                logger.warning("YouTube 上傳失敗 sid=%s, folder=%s: %s", sid, fid, e)
                scheduler_repo.mark_failed(sid, str(e))
                continue

            # 3) Sheet：已排程（C 欄先空白）
            try:
                title = (meta.get("title") if isinstance(meta, dict) else None) or fname
                keywords = ",".join(meta.get("tags", [])) if isinstance(meta, dict) and isinstance(meta.get("tags"), list) else ""
                row_idx = append_published_row(when_utc.astimezone(TZ), title, "", "已排程", keywords, today_views=0)
                if row_idx:
                    scheduler_repo.set_schedule_sheet_row(sid, row_idx)
            except Exception as e:
                logger.warning("Sheet 寫入失敗（已排程） sid=%s, folder=%s: %s", sid, fid, e)

    _assign_and_upload(short_candidates, "short")  # 週一/週五
    _assign_and_upload(long_candidates, "long")    # 週三

# -------------------- 到點補上傳 / 已發布促轉 --------------------

def run_due_uploads():
    now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)
    if not scheduler_repo.acquire_lock(10101):
        return
    try:
        for r in scheduler_repo.get_due_for_upload(now_utc):
            sid = r["id"]; fid = r["folder_id"]
            meta = r.get("meta_text") or {}
            if isinstance(meta, str):
                import json
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}
            try:
                vid = _upload_by_folder(fid, meta, r["schedule_time"])
                scheduler_repo.mark_uploaded(sid, vid)
                try:
                    update_thumbnail_from_drive(vid, fid)
                except Exception:
                    pass
            except Exception as e:
                scheduler_repo.mark_failed(sid, str(e))
                logger.exception("上傳失敗 sid=%s：%s", sid, e)
    finally:
        scheduler_repo.release_lock(10101)

def _promote_batch(yt, ids: List[str], id_to_rec: Dict[str, dict]):
    """處理最多 50 個 video id：若已公開 → DB 標記 published ＋ Sheet: C 欄貼連結、D 欄=已發布。"""
    if not ids:
        return
    resp = yt.videos().list(part="status", id=",".join(ids)).execute()
    items = resp.get("items", [])
    for it in items:
        vid = it["id"]
        status = it.get("status") or {}
        if status.get("privacyStatus") != "public":
            continue
        rec = id_to_rec.get(vid)
        if not rec:
            continue
        # 1) DB
        scheduler_repo.mark_published(rec["id"])
        # 2) Sheet
        row_idx = int(rec.get("sheet_row") or 0)
        if row_idx:
            mark_row_published(row_idx, vid)


def reconcile_sheet_and_drive_for_published(dry_run: bool = False) -> dict:
    """
    補帳：針對 DB 中 status='published' 的排程
      - 先「移動資料夾到已發布資料夾」取得 webViewLink
      - 再把 Sheet『已發布』分頁：C 欄寫入「資料夾連結」、D 欄寫入「已發布」
    說明：
      - 若 DB 沒存 sheet_row，就用 YouTube 標題在表內尋列（find_row_by_title_and_folder）
      - dry_run=True 只回報不落實（連結用可組出的預設 URL）
    回傳：{"checked", "sheet_updated", "moved", "errors": []}
    """
    rows = scheduler_repo.list_published_for_reconcile(limit=300)
    if not rows:
        return {"checked": 0, "sheet_updated": 0, "moved": 0, "errors": []}

    # 先把影片標題抓起來（用於沒有 sheet_row 時的備援尋列）
    yt = get_youtube_client()
    id_to_row = {r["video_id"]: r for r in rows if r.get("video_id")}
    ids = list(id_to_row.keys())
    title_map = {}
    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        resp = yt.videos().list(part="snippet", id=",".join(chunk)).execute()
        for it in resp.get("items", []):
            title_map[it["id"]] = (it.get("snippet") or {}).get("title", "") or ""

    out = {"checked": len(ids), "sheet_updated": 0, "moved": 0, "errors": []}

    for vid in ids:
        rec = id_to_row.get(vid) or {}
        fid = rec.get("folder_id")
        row_idx = int(rec.get("sheet_row") or 0)

        # 1) 先移資料夾，拿到可寫入 Sheet 的連結
        folder_url = ""
        if fid:
            try:
                if dry_run:
                    folder_url = f"https://drive.google.com/drive/folders/{fid}"
                else:
                    folder_url = _move_folder_to_published(fid)  # 需回傳 webViewLink
                if folder_url:
                    out["moved"] += 1
            except Exception as e:
                out["errors"].append(f"move id={rec.get('id')}: {e}")
                # 仍給一個可用的連結避免 C 欄留白
                folder_url = f"https://drive.google.com/drive/folders/{fid}"

        # 2) 找到要更新的列：優先用 DB 的 sheet_row；沒有就用標題搜尋
        if not row_idx:
            try:
                t = title_map.get(vid, "")
                if t:
                    row_idx = find_row_by_title_and_folder(t, None) or 0
            except Exception as e:
                out["errors"].append(f"find-row id={rec.get('id')}: {e}")

        # 3) 寫回 Sheet：C=資料夾連結、D=已發布
        if row_idx and folder_url:
            try:
                if not dry_run:
                    set_published_folder_link(row_idx, folder_url)
                out["sheet_updated"] += 1
            except Exception as e:
                out["errors"].append(f"sheet id={rec.get('id')}: {e}")

    return out

def promote_published_and_move(dry_run: bool = False) -> dict:
    """
    對帳到點的排程是否已公開：
      - 已公開 → DB: published
               → Drive: 移到已發布資料夾，取得 webViewLink
               → Sheet: C 欄寫『資料夾連結』、D 欄寫『已發布』
    會回傳統計值方便 /api/scheduler/promote-now 直接顯示。
    """
    rows = scheduler_repo.list_ready_for_publish(limit=200)
    if not rows:
        return {"status": "ok", "checked": 0, "published": 0, "sheet_updated": 0, "moved": 0, "skipped": 0, "errors": []}

    yt = get_youtube_client()
    id_to_rec = {r["video_id"]: r for r in rows if r.get("video_id")}
    ids = list(id_to_rec.keys())

    # 先查哪些 id 已經 public（同時抓標題，找不到 sheet_row 時可用來搜尋列）
    published_ids, title_map = set(), {}
    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        resp = yt.videos().list(part="status,snippet", id=",".join(chunk)).execute()
        for it in resp.get("items", []):
            vid = it["id"]
            title_map[vid] = (it.get("snippet") or {}).get("title", "") or ""
            if (it.get("status") or {}).get("privacyStatus") == "public":
                published_ids.add(vid)

    out = {"checked": len(ids), "published": 0, "sheet_updated": 0, "moved": 0,
           "skipped": len(ids) - len(published_ids), "errors": []}

    for vid in published_ids:
        rec = id_to_rec.get(vid) or {}
        sid = rec.get("id")
        fid = rec.get("folder_id")
        row_idx = int(rec.get("sheet_row") or 0)

        # 1) DB → published
        try:
            if not dry_run and sid:
                scheduler_repo.mark_published(sid)
            out["published"] += 1
        except Exception as e:
            out["errors"].append(f"DB sid={sid}: {e}")

        # 2) 先移資料夾拿連結（失敗就用預設 URL 當備援）
        folder_url = ""
        if fid:
            try:
                folder_url = _move_folder_to_published(fid) if not dry_run else f"https://drive.google.com/drive/folders/{fid}"
                if folder_url:
                    out["moved"] += 1
            except Exception as e:
                out["errors"].append(f"move sid={sid}: {e}")
                # 仍給一個可用的連結，避免 Sheet 空白
                folder_url = f"https://drive.google.com/drive/folders/{fid}"

        # 3) 找到對應的 Sheet 列：優先用 DB 的 sheet_row；沒有就用標題搜尋
        if not row_idx:
            try:
                t = title_map.get(vid, "")
                if t:
                    row_idx = find_row_by_title_and_folder(t, None) or 0
            except Exception as e:
                out["errors"].append(f"find-row sid={sid}: {e}")

        # 4) Sheet → C 欄=資料夾連結、D 欄=已發布
        if row_idx and folder_url:
            try:
                if not dry_run:
                    set_published_folder_link(row_idx, folder_url)
                out["sheet_updated"] += 1
            except Exception as e:
                out["errors"].append(f"sheet sid={sid}: {e}")

    return out

# -------------------- 其他維運任務 --------------------

def reconcile_youtube_deletions():
    """YT 排程被刪除 → DB 標記 deleted（保守：API 失敗時不動 DB）。"""
    try:
        yt_list = {it["id"] for it in list_scheduled_youtube(max_pages=2)}
    except Exception:
        # 可能是 invalid_grant 或網路錯誤；避免誤判，直接跳過
        return
    rows = scheduler_repo.list_future_uploaded()
    for r in rows:
        vid = r.get("youtube_video_id")
        if vid and (vid not in yt_list):
            scheduler_repo.mark_deleted(r["id"])



def refresh_today_views():
    """回填今日觀看數（F 欄）。"""
    if not scheduler_repo.acquire_lock(10103):
        return
    try:
        rows = scheduler_repo.get_all_published_with_video_id()
        if not rows:
            return
        yt = get_youtube_client()
        if yt is None:
            return
        ids = [r["youtube_video_id"] for r in rows if r.get("youtube_video_id")]
        for i in range(0, len(ids), 50):
            chunk = ids[i:i+50]
            resp = yt.videos().list(part="statistics,snippet", id=",".join(chunk)).execute()
            items = resp.get("items", [])
            for it in items:
                views = int((it.get("statistics") or {}).get("viewCount", "0"))
                title = (it.get("snippet") or {}).get("title", "")
                row = find_row_by_title_and_folder(title, None)
                if row:
                    update_status_and_views(row, today_views=views)
    finally:
        scheduler_repo.release_lock(10103)

# -------------------- APScheduler：單例與註冊 --------------------

_SCHEDULER: BackgroundScheduler | None = None

def get_scheduler() -> BackgroundScheduler:
    """取得全域單例 BackgroundScheduler（不存在就建立一個）。"""
    global _SCHEDULER
    if _SCHEDULER is None:
        _SCHEDULER = BackgroundScheduler(timezone="Asia/Taipei")
    return _SCHEDULER

def _ensure_job(job_id: str, *, func, trigger):
    """若同名 job 不存在才新增，避免重複註冊。"""
    sched = get_scheduler()
    if sched.get_job(job_id):
        return
    sched.add_job(func=func, trigger=trigger, id=job_id)

def start_scheduler():
    """集中註冊所有排程任務並啟動排程器（可重入、具冪等）。"""
    sched = get_scheduler()
    _ensure_job("scan_and_schedule_daily", func=scan_and_schedule_from_mother,
                trigger=CronTrigger(hour=3, minute=0))         # 每日 03:00 掃描排程
    _ensure_job("run_due_uploads", func=run_due_uploads,
                trigger=IntervalTrigger(minutes=30))            # 每 30 分鐘補上傳
    _ensure_job("promote_published", func=promote_published_and_move,
                trigger=IntervalTrigger(minutes=60))            # 每 60 分鐘對帳已發布 → 更新 Sheet
    _ensure_job("reconcile_yt_deletions", func=reconcile_youtube_deletions,
                trigger=IntervalTrigger(minutes=30))           # 每 30 分鐘對帳 YT 刪除
    _ensure_job("refresh_today_views", func=refresh_today_views,
                trigger=IntervalTrigger(hours=24))             # 每日回填今日觀看（可自行調整）
    _ensure_job("reconcile_published_sheet_drive", func=reconcile_sheet_and_drive_for_published,
            trigger=IntervalTrigger(hours=12))
    _ensure_job("reconcile_ytsched_drift", func=reconcile_youtube_schedule_drift,
            trigger=IntervalTrigger(minutes=60))          # NEW：每 60 分鐘同步 YT 後台異動



    if not sched.running:
        sched.start()

# === NEW: 批次抓取影片狀態/時間/標題 ===
def _list_videos_status_map(video_ids: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
    """
    回傳 {videoId: {"privacyStatus": str|None, "publishAt": str|None, "title": str|None}}
    任何錯誤直接丟出例外（上層會保守處理、不動 DB）
    """
    yt = get_youtube_client()
    if yt is None or not video_ids:
        return {}
    out: Dict[str, Dict[str, Optional[str]]] = {}
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        resp = yt.videos().list(part="status,snippet", id=",".join(chunk)).execute()
        for it in resp.get("items", []):
            vid = it.get("id")
            st = (it.get("status") or {})
            sn = (it.get("snippet") or {})
            if not vid:
                continue
            out[vid] = {
                "privacyStatus": (st.get("privacyStatus") or "").lower() or None,
                "publishAt": st.get("publishAt"),
                "title": sn.get("title") or None,
            }
    return out

# === NEW: 同步 YouTube 後台手動異動（時間/狀態）到 DB + Sheet/Drive ===
def reconcile_youtube_schedule_drift() -> dict:
    """
    目的：你在 YouTube 後台手動改時間/提前公開後，DB 能自動跟上。
    策略（保守）：
      - API 失敗（例如 refresh token 掛掉）→ 直接跳過，不動 DB
      - private/unlisted 且有 publishAt → 對齊 DB.schedule_time
      - public → DB.status='published'，同時移資料夾、寫回 Sheet
      - DB 是 deleted 但影片實際存在 → 拉回 uploaded
      - 不在本批結果清單的 id → 本函式不標 deleted（交由原本刪除對帳任務處理）
    回傳：統計資訊
    """
    print("=== DEBUG YouTube meta ===")
    print(json.dumps(meta, ensure_ascii=False, indent=2))
    # 只掃「未來 60 天會發」與「近 7 天內新建/可能剛公開」的候選（避免全表掃描）
    with engine.begin() as conn:
        rows = conn.execute(sql_text("""
            SELECT id, folder_id, sheet_row, youtube_video_id, status, schedule_time
            FROM video_schedules
            WHERE youtube_video_id IS NOT NULL
              AND status IN ('uploaded','scheduled','deleted')
              AND (
                    schedule_time IS NULL
                 OR schedule_time < (NOW() + INTERVAL '60 days')
                 OR created_at > (NOW() - INTERVAL '7 days')
              )
            ORDER BY COALESCE(schedule_time, created_at) DESC
            LIMIT 500
        """)).mappings().all()

    id_map = {r["youtube_video_id"]: r for r in rows if r["youtube_video_id"]}
    video_ids = list(id_map.keys())
    if not video_ids:
        return {"checked": 0, "sched_aligned": 0, "published_fixed": 0, "undeleted": 0, "sheet_updated": 0, "moved": 0, "errors": []}

    # 呼叫 YouTube（失敗直接跳過，避免亂動 DB）
    try:
        meta = list_videos_status_map(video_ids)
    except Exception as e:
        return {
            "checked": len(video_ids),
            "sched_aligned": 0,
            "published_fixed": 0,
            "undeleted": 0,
            "sheet_updated": 0,
            "moved": 0,
            "errors": [f"yt:{e}"]
        }

    out = {"checked": len(video_ids), "sched_aligned": 0, "published_fixed": 0, "undeleted": 0, "sheet_updated": 0, "moved": 0, "errors": []}

    # 逐一比對 & 修正
    for vid, r in id_map.items():
        m = meta.get(vid)
        if not m:
            # 這批沒看到該影片；在此函式不做刪除判定
            continue

        privacy = (m.get("privacyStatus") or "").lower()
        pa = m.get("publishAt")
        row_idx = int(r.get("sheet_row") or 0)
        fid = r.get("folder_id")
        rec_id = r.get("id")
        db_sched: Optional[datetime] = r.get("schedule_time")

        # A) private/unlisted + 有 publishAt → 對齊 DB.schedule_time
        if privacy in ("private", "unlisted") and pa:
            try:
                api_dt = datetime.fromisoformat(pa.replace("Z", "+00:00"))
                if (db_sched is None) or (abs((db_sched - api_dt).total_seconds()) > 60):
                    try:
                        with engine.begin() as conn:
                            conn.execute(sql_text("""
                                UPDATE video_schedules
                                SET schedule_time = :t, status = 'scheduled'
                                WHERE id = :id
                            """), {"t": api_dt, "id": rec_id})
                        out["sched_aligned"] += 1
                    except Exception as e:
                        out["errors"].append(f"db-sched id={rec_id}: {e}")
            except Exception:
                pass

        if privacy == "public" and r.get("status") != "published":
            # 1) DB 標記 published
            try:
                with engine.begin() as conn:
                    conn.execute(sql_text("""
                        UPDATE video_schedules
                        SET status = 'published'
                        WHERE id = :id
                    """), {"id": rec_id})
                out["published_fixed"] += 1
            except Exception as e:
                out["errors"].append(f"db-published id={rec_id}: {e}")

            # 2) 取 YouTube 最新標題
            title = None
            try:
                yt_meta = meta.get(vid, {})
                snippet = yt_meta.get("snippet") or {}
                title = snippet.get("title")
                if title:
                    with engine.begin() as conn:
                        conn.execute(sql_text("""
                            UPDATE video_schedules
                            SET meta_text = :title
                            WHERE id = :id
                        """), {"title": title, "id": rec_id})
            except Exception as e:
                out["errors"].append(f"yt-title id={rec_id}: {e}")

            # 3) 移資料夾，取得連結（失敗給預設 URL）
            folder_url = ""
            if fid:
                try:
                    folder_url = _move_folder_to_published(fid)
                    if folder_url:
                        out["moved"] += 1
                except Exception as e:
                    out["errors"].append(f"move id={rec_id}: {e}")
                    folder_url = f"https://drive.google.com/drive/folders/{fid}"

            # 4) 寫回 Sheet（影片連結、狀態、資料夾連結、標題）
            if row_idx and folder_url:
                try:
                    # 4a) 寫入「影片連結 + 狀態=已發布」
                    mark_row_published(row_idx, vid)
                    # 4b) 寫入「資料夾連結」
                    set_published_folder_link(row_idx, folder_url)
                    # 4c) 寫入「標題」
                    if title:
                        update_title(row_idx, title)
                    out["sheet_updated"] += 1
                except Exception as e:
                    out["errors"].append(f"sheet id={rec_id}: {e}")

        # C) DB 誤標 deleted，但影片還在 → 拉回 uploaded
        if privacy in ("private", "unlisted", "public") and r.get("status") == "deleted":
            try:
                with engine.begin() as conn:
                    conn.execute(sql_text("""
                        UPDATE video_schedules
                        SET status = 'uploaded'
                        WHERE id = :id
                    """), {"id": rec_id})
                out["undeleted"] += 1
            except Exception as e:
                out["errors"].append(f"db-undelete id={rec_id}: {e}")

    return out


def _fetch_existing_youtube_ids_from_db() -> Set[str]:
    """
    從 DB 撈出目前存在的 youtube_video_id 當白名單。
    """
    engine = create_engine(settings.DATABASE_URL)
    sql = text("""
        SELECT youtube_video_id
        FROM public.video_schedules
        WHERE youtube_video_id IS NOT NULL
          AND youtube_video_id <> ''
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return {r[0] for r in rows}


def _youtube_video_exists(y, vid: str) -> bool:
    """
    回傳影片是否還存在於 YouTube（含私密/不公開仍會回傳 items>0）。
    被刪除、移除或 ID 不存在會回傳 False。
    """
    try:
        resp = y.videos().list(part="id", id=vid).execute()
        return bool(resp.get("items"))
    except Exception as e:
        logging.warning("YT exists check failed for %s: %s", vid, e)
        return True



_YT_ID_RE = re.compile(r"(?:v=|/shorts/|youtu\.be/|/embed/)([A-Za-z0-9_-]{11})|^([A-Za-z0-9_-]{11})$")
def _extract_id(cell: str) -> str:
    if not cell:
        return ""
    m = _YT_ID_RE.search(str(cell).strip())
    return (m.group(1) or m.group(2)) if m else ""

def reconcile_youtube_deletions_and_sheet(dry_run: bool = True) -> dict:
    """
    對照 DB（真相）與 YouTube，刪除 Google Sheet「已發布」分頁中不該存在的列。
    規則：
      - 該列（C 欄）解析出的 YouTube ID 不在 DB -> 列入刪除
      - 或是該 ID 在 YouTube 已不存在 -> 列入刪除
    注意：若該列抓不到任何 YouTube ID/URL，為安全起見「跳過不刪」。
    """
    assert settings.SHEET_ID,  "SHEET_ID is required"
    tab = getattr(settings, "SHEET_TAB", None) or getattr(settings, "TAB_NAME", None)
    assert tab, "SHEET_TAB (or TAB_NAME) is required"

    # Google Sheets 服務
    sheets_srv = get_google_service(
        "sheets", "v4", ["https://www.googleapis.com/auth/spreadsheets"]
    )
    sheet = sheets_srv.spreadsheets()

    yt = get_youtube_client()
    # 讀 Sheet：A:日期  B:標題  C:YOUTUBE ID/URL  D:資料夾位置 ...（讀寬一點避免越界）
    rows: List[List[str]] = get_sheet_values(sheet, settings.SHEET_ID, tab, "A2:Z")

    # DB 現存影片 ID（白名單）
    db_ids = set(x for x in (_fetch_existing_youtube_ids_from_db() or []) if x)

    to_delete_rows: List[int] = []
    reasons: List[Tuple[int, str, str]] = []  # (row_index, title, reason)
    examined = 0

    # 由環境變數決定 ID 欄位（預設 C 欄），但我們仍會做網址→ID 轉換
    col_letter = (getattr(settings, "SHEET_YT_COL", os.getenv("SHEET_YT_COL", "C")) or "C").upper()
    col_idx = max(0, ord(col_letter) - ord('A'))

    for idx, row in enumerate(rows, start=2):  # 從第2列（跳過表頭）
        examined += 1
        title = row[1].strip() if len(row) > 1 and row[1] else ""

        yt_id = _extract_id(row[col_idx]) if len(row) > col_idx else ""
        if not yt_id:
            # 這列沒有任何可辨識的 YouTube 連結/ID → 不刪，留給人工
            reasons.append((idx, title, "no_id_in_col"))
            continue

        reason = None
        if yt_id not in db_ids:
            reason = "missing_in_db"
        else:
            # DB 有，仍再確認 YT 是否還存在
            if not _youtube_video_exists(yt, yt_id):
                reason = "missing_on_youtube"

        if reason:
            to_delete_rows.append(idx)
            reasons.append((idx, title, reason))

    # ---- 安全閥：避免大量誤刪 ----
    MAX_RATIO = float(os.getenv("RECONCILE_MAX_RATIO", "0.3"))  # >30% 中止
    MAX_COUNT = int(os.getenv("RECONCILE_MAX_COUNT", "10"))     # 或一次最多 10 列
    ratio = (len(to_delete_rows) / examined) if examined else 0.0

    if not db_ids:
        return {
            "examined": examined, "deleted_rows": [],
            "dry_run": True, "reasons_preview": reasons[:20],
            "error": "DB empty; abort."
        }

    if ratio > MAX_RATIO or len(to_delete_rows) > MAX_COUNT:
        return {
            "examined": examined, "deleted_rows": [],
            "dry_run": True, "reasons_preview": reasons[:20],
            "error": f"Safety stop: would delete {len(to_delete_rows)}/{examined} rows"
        }

    # 真正刪除
    if not dry_run and to_delete_rows:
        delete_rows(sheet, settings.SHEET_ID, tab, to_delete_rows)

    return {
        "examined": examined,
        "deleted_rows": to_delete_rows,
        "dry_run": dry_run,
        "reasons_preview": reasons[:20],
        "total_marked": len(to_delete_rows),
    }