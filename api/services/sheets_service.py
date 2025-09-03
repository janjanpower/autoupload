"""
Google Sheets service helpers (patched)
- Adds robust row resolution to avoid writing to the wrong row when the sheet is sorted/filtered or rows are inserted.
- Backward compatible: existing callers that pass `row` still work; functions now verify and re-locate the correct row using YouTube ID (preferred), folder URL, or (title + datetime string) before writing.
- Supports TAB_NAME fallback if SHEET_TAB is not set.
- Optional columns for SID / YOUTUBE_ID can be configured via env vars to embed immutable keys per row.

ENV (commonly used)
- SHEET_ID: Spreadsheet ID
- SHEET_TAB or TAB_NAME: target sheet name (tab)
- SHEET_TITLE_COL (default "B")
- SHEET_YT_COL    (default "C")  # hyperlink to YouTube
- SHEET_FOLDER_COL(default "D")
- SHEET_STATUS_COL(default "E")
- (optional) SHEET_SID_COL (e.g. "H")
- (optional) SHEET_YTID_COL (e.g. "I")  # pure YouTube ID column to strengthen identity

This module was patched from your original file to implement safe resolution.
"""
from __future__ import annotations

import os, re, json, logging
from typing import List, Optional
from datetime import datetime

from googleapiclient.discovery import build as gbuild
from google.oauth2 import service_account

# =============================
# Config
# =============================
SHEET_ID = os.getenv("SHEET_ID") or ""
SHEET_TAB = os.getenv("SHEET_TAB") or os.getenv("TAB_NAME") or "已發布"

COL_TITLE  = os.getenv("SHEET_TITLE_COL",  "B")
COL_YT     = os.getenv("SHEET_YT_COL",     "C")  # stores hyperlink to YT (or URL)
COL_FOLDER = os.getenv("SHEET_FOLDER_COL", "D")
COL_STATUS = os.getenv("SHEET_STATUS_COL", "E")

# Optional identity columns (recommended for "never write wrong row")
COL_SID    = os.getenv("SHEET_SID_COL")      # DB side id, e.g. primary key
COL_YTID   = os.getenv("SHEET_YTID_COL")     # pure YouTube ID column

# =============================
# SA / Clients
# =============================

def _sheet_id() -> str:
    sid = (SHEET_ID or "").strip()
    if not sid:
        raise RuntimeError("SHEET_ID 未設定")
    return sid

def _get_sa_credentials(scopes):
    raw = os.getenv("SA_JSON_ENV") or os.getenv("GOOGLE_SA_JSON")
    path = os.getenv("SA_JSON_PATH") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    info = None
    if raw:
        try:
            info = json.loads(raw)
        except Exception as e:
            raise RuntimeError("Service Account JSON 不是有效的 JSON（請確認 SA_JSON_ENV / GOOGLE_SA_JSON）") from e
    elif path:
        if not os.path.exists(path):
            raise RuntimeError(f"找不到 Service Account 檔案：{path}")
        with open(path, "r", encoding="utf-8") as f:
            info = json.load(f)
    else:
        raise RuntimeError("缺少 Service Account 憑證：請設定 SA_JSON_ENV / GOOGLE_SA_JSON，或 SA_JSON_PATH/GOOGLE_APPLICATION_CREDENTIALS")

    return service_account.Credentials.from_service_account_info(info, scopes=scopes)

_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def _sheets():
    creds = _get_sa_credentials(_SHEETS_SCOPES)
    return gbuild("sheets", "v4", credentials=creds, cache_discovery=False).spreadsheets()

# =============================
# Utilities
# =============================

def _a1(col: str, row: int) -> str:
    return f"{SHEET_TAB}!{col}{row}"

def _get(range_a1: str):
    return _sheets().values().get(spreadsheetId=_sheet_id(), range=f"{SHEET_TAB}!{range_a1}").execute().get("values", [])

def _get_col(col: str):
    return _get(f"{col}:{col}")  # entire column

def _batch_update(data_ranges):
    return _sheets().values().batchUpdate(
        spreadsheetId=_sheet_id(),
        body={"valueInputOption": "USER_ENTERED", "data": data_ranges},
    ).execute()

# =============================
# Row resolution (to prevent wrong-row writes)
# =============================

def _find_row_by_youtube_id(yid: str) -> Optional[int]:
    if not yid:
        return None
    col_vals = _get_col(COL_YT)
    # Scan from row 2 (skip header); i is 1-based spreadsheet row index
    for i, row in enumerate(col_vals, start=1):
        if i == 1:
            continue
        cell = (row[0] if row else "") or ""
        if not cell:
            continue
        if yid in cell:
            return i
    # optional pure ID column
    if COL_YTID:
        id_vals = _get_col(COL_YTID)
        for i, row in enumerate(id_vals, start=1):
            if i == 1:
                continue
            cell = (row[0] if row else "") or ""
            if cell.strip() == yid:
                return i
    return None

def _find_row_by_folder_url(folder_url: str) -> Optional[int]:
    if not folder_url:
        return None
    col_vals = _get_col(COL_FOLDER)
    for i, row in enumerate(col_vals, start=1):
        if i == 1:
            continue
        cell = (row[0] if row else "") or ""
        if folder_url in cell:
            return i
    return None

def _find_row_by_title_and_date(title: str, date_str: str) -> Optional[int]:
    titles = _get_col(COL_TITLE)
    dates  = _get_col("A")  # assume A holds date string written by append_published_row
    candidates = []
    max_len = max(len(titles), len(dates))
    for i in range(2, max_len + 1):
        t = (titles[i-1][0] if i-1 < len(titles) and titles[i-1] else "") or ""
        d = (dates[i-1][0]  if i-1 < len(dates)  and dates[i-1]  else "") or ""
        if t == (title or ""):
            candidates.append((i, d))
    for i, d in candidates:
        if d == (date_str or ""):
            return i
    return candidates[0][0] if candidates else None

def resolve_sheet_row(
    hint_row: Optional[int],
    *,
    expect_title: Optional[str] = None,
    expect_date_str: Optional[str] = None,
    youtube_id: Optional[str] = None,
    folder_url: Optional[str] = None,
) -> Optional[int]:
    """Safely resolve the correct row to write.
    Strategy:
    1) If hint_row given, verify B{hint_row} equals expected title (if provided). If ok, use it.
       If no title provided, still allow using hint_row as last resort (but only after YT/Folder checks).
    2) If youtube_id provided, locate using YT column (and optional YTID column). If found, use it.
    3) If folder_url provided, locate in folder column.
    4) If title(+date) provided, locate by equality.
    5) Fallback to hint_row if exists (>1).
    """
    # 2) Prefer immutable key: YouTube ID
    row = _find_row_by_youtube_id(youtube_id or "")
    if row:
        return row

    # 3) Next: folder URL
    row = _find_row_by_folder_url(folder_url or "")
    if row:
        return row

    # 1) Verify hint by title
    if hint_row and hint_row > 1 and expect_title:
        t = _get(f"{COL_TITLE}{hint_row}:{COL_TITLE}{hint_row}")
        title_at_row = (t[0][0] if t and t[0] else "") or ""
        if title_at_row == expect_title:
            return hint_row

    # 4) Title + date
    if expect_title:
        row = _find_row_by_title_and_date(expect_title, expect_date_str or "")
        if row:
            return row

    # 5) Last resort: hint_row (if any)
    if hint_row and hint_row > 1:
        return hint_row

    return None

# =============================
# Row appends & updates (now guarded by resolver)
# =============================

def append_published_row(
    dt_local: datetime,
    title: str,
    folder_url: str,
    status: str,
    keywords: str,
    today_views: int = 0,
    *,
    sid: Optional[str] = None,
    youtube_id: Optional[str] = None,
) -> int:
    """Append a new row and return its index.
    Also writes optional identity columns (SID / YTID) if configured.
    Date format aligns with resolver (YYYY-MM-DD HH:MM).
    """
    values = [[
        dt_local.strftime("%Y-%m-%d %H:%M"),  # A
        title,                                  # B
        "",                                     # C (YT link) initially blank
        folder_url,                              # D
        status,                                  # E
        keywords,                                # F
        int(today_views or 0),                   # G
    ]]
    rng = f"{SHEET_TAB}!A:G"
    resp = _sheets().values().append(
        spreadsheetId=_sheet_id(),
        range=rng,
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()
    updated_range = (resp.get("updates") or {}).get("updatedRange", "")
    m = re.search(r"![A-Z]+(\d+):", updated_range)
    row_idx = int(m.group(1)) if m else 0

    # Write identity columns if configured
    data = []
    if row_idx:
        if COL_SID and sid is not None:
            data.append({"range": _a1(COL_SID, row_idx), "values": [[str(sid)]]})
        if COL_YTID and youtube_id:
            data.append({"range": _a1(COL_YTID, row_idx), "values": [[youtube_id]]})
        if data:
            _batch_update(data)
    return row_idx


def update_title_by_row(row_index: int, new_title: str) -> None:
    if not row_index:
        return
    _sheets().values().update(
        spreadsheetId=_sheet_id(),
        range=_a1(COL_TITLE, row_index),
        valueInputOption="USER_ENTERED",
        body={"values": [[new_title]]},
    ).execute()


def mark_row_published(row: int, video_id: str, *, expect_title: Optional[str]=None, expect_date_str: Optional[str]=None) -> None:
    """Mark as published and write YT link. Safely resolve row using video_id first."""
    real_row = resolve_sheet_row(row, youtube_id=video_id, expect_title=expect_title, expect_date_str=expect_date_str)
    if not real_row:
        logging.warning("mark_row_published: 無法定位列 (row=%s, yid=%s)", row, video_id)
        return
    set_youtube_link(real_row, video_id)
    set_status(real_row, "已發布", youtube_id=video_id)


# Legacy helpers kept (with minor fixes)

def _fetch_all_rows() -> List[List[str]]:
    r = _sheets().values().get(spreadsheetId=_sheet_id(), range=f"{SHEET_TAB}!A:G").execute()
    return r.get("values", [])


def _first_data_row_index() -> int:
    return 2


def find_row_by_title_and_folder(title: Optional[str], folder_url: Optional[str]) -> Optional[int]:
    rows = _fetch_all_rows()
    for idx, row in enumerate(rows, start=1):
        if idx < _first_data_row_index():
            continue
        t = (row[1] if len(row) > 1 else "").strip()  # B
        d = (row[3] if len(row) > 3 else "").strip()  # D
        if folder_url:
            if t == (title or "") and (folder_url in d):
                return idx
        else:
            if t == (title or ""):
                return idx
    return None


def update_status_and_views(
    row_index: int,
    status: Optional[str] = None,
    today_views: Optional[int] = None,
    folder_url: Optional[str] = None,
    *,
    youtube_id: Optional[str] = None,
    expect_title: Optional[str] = None,
    expect_date_str: Optional[str] = None,
):
    real_row = resolve_sheet_row(row_index, youtube_id=youtube_id, folder_url=folder_url, expect_title=expect_title, expect_date_str=expect_date_str)
    if not real_row:
        logging.warning("update_status_and_views: 無法定位列 (row=%s, yid=%s, folder=%s)", row_index, youtube_id, folder_url)
        return
    data = []
    if status is not None:
        data.append({"range": _a1(COL_STATUS, real_row), "values": [[status]]})
    if today_views is not None:
        data.append({"range": _a1("G", real_row), "values": [[int(today_views)]]})
    if folder_url:
        data.append({"range": _a1(COL_FOLDER, real_row), "values": [[f'=HYPERLINK("{folder_url}","{folder_url}")']]})
    if not data:
        return
    _batch_update(data)


# Additional API kept / fixed

def set_published_folder_link(row: int, folder_url: str, *, youtube_id: Optional[str]=None, expect_title: Optional[str]=None, expect_date_str: Optional[str]=None) -> None:
    """Safely set folder link and mark status. Tries to re-locate row by YT ID or folder URL first."""
    real_row = resolve_sheet_row(row, youtube_id=youtube_id, folder_url=folder_url, expect_title=expect_title, expect_date_str=expect_date_str)
    if not real_row:
        logging.warning("set_published_folder_link: 無法定位列 (row=%s, yid=%s, folder=%s)", row, youtube_id, folder_url)
        return
    _sheets().values().update(
        spreadsheetId=_sheet_id(),
        range=_a1(COL_FOLDER, real_row),
        body={"values": [[folder_url]]},
        valueInputOption="RAW",
    ).execute()
    set_status(real_row, "已發布")


def clear_sheet_row_status(row_idx: int, status: str = "已刪除", *, youtube_id: Optional[str]=None, expect_title: Optional[str]=None, expect_date_str: Optional[str]=None):
    real_row = resolve_sheet_row(row_idx, youtube_id=youtube_id, expect_title=expect_title, expect_date_str=expect_date_str)
    if not real_row:
        logging.warning("clear_sheet_row_status: 無法定位列 (row=%s, yid=%s)", row_idx, youtube_id)
        return
    _sheets().values().update(
        spreadsheetId=_sheet_id(),
        range=f"{SHEET_TAB}!{COL_FOLDER}{real_row}:{COL_STATUS}{real_row}",
        valueInputOption="RAW",
        body={"values": [["", status]]},
    ).execute()


# Meta-sheet helpers preserved (minimal cleanups)
_SCOPES_META = ["https://www.googleapis.com/auth/spreadsheets"]

def _svc_meta():
    json_str = os.getenv("GOOGLE_SA_JSON") or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    ssid     = os.getenv("SHEET_ID")       or os.getenv("SHEETS_SPREADSHEET_ID", "")
    sheet_nm = os.getenv("SHEET_NAME")     or os.getenv("SHEETS_SHEET_NAME", "Sheet1")
    if not json_str or not ssid:
        raise RuntimeError("Sheets 未設定（缺 GOOGLE_SA_JSON / SHEET_ID）")
    info = json.loads(json_str)
    creds = service_account.Credentials.from_service_account_info(info, scopes=_SCOPES_META)
    cli = gbuild("sheets", "v4", credentials=creds, cache_discovery=False)
    return cli, ssid, sheet_nm


def _A1(col_idx: int, row_idx: int) -> str:
    s, i = "", col_idx
    while i:
        i, r = divmod(i - 1, 26)
        s = chr(65 + r) + s
    return f"{s}{row_idx}"


def _headers(svc, ssid: str, sheet: str) -> list[str]:
    r = svc.spreadsheets().values().get(spreadsheetId=ssid, range=f"{sheet}!1:1").execute()
    return (r.get("values") or [[]])[0]


def _ensure_headers(svc, ssid: str, sheet: str) -> dict[str,int]:
    hdr = _headers(svc, ssid, sheet)
    if not hdr:
        hdr = ["video_id","title","description","tags"]
        svc.spreadsheets().values().update(
            spreadsheetId=ssid, range=f"{sheet}!1:1",
            valueInputOption="RAW", body={"values":[hdr]}
        ).execute()
    return {h:i+1 for i,h in enumerate(hdr)}


def _find_row(svc, ssid: str, sheet: str, vid: str, col_idx: int) -> Optional[int]:
    col_letter = _A1(col_idx,1)[:-1]
    r = svc.spreadsheets().values().get(
        spreadsheetId=ssid, range=f"{sheet}!{col_letter}2:{col_letter}"
    ).execute()
    for i, row in enumerate(r.get("values", []), start=2):
        if (row[0] if row else "").strip() == vid:
            return i
    return None


def safe_update_metadata(video_id: str, title: str, description: str, tags: list[str]):
    # 未設定就安靜跳過（不影響主流程）
    try:
        svc, ssid, sheet = _svc_meta()
    except Exception:
        return

    cols = _ensure_headers(svc, ssid, sheet)
    for need in ["video_id","title","description","tags"]:
        if need not in cols:
            hdr = _headers(svc, ssid, sheet) or []
            hdr.append(need)
            svc.spreadsheets().values().update(
                spreadsheetId=ssid, range=f"{sheet}!1:1",
                valueInputOption="RAW", body={"values":[hdr]}
            ).execute()
            cols = _ensure_headers(svc, ssid, sheet)

    row = _find_row(svc, ssid, sheet, video_id, cols["video_id"])
    if row is None:
        r = svc.spreadsheets().values().get(spreadsheetId=ssid, range=f"{sheet}!A:A").execute()
        cnt = len(r.get("values", []))
        row = max(cnt + 1, 2)

    data = [
        {"range": f"{sheet}!{_A1(cols['video_id'], row)}", "values": [[video_id]]},
        {"range": f"{sheet}!{_A1(cols['title'], row)}",    "values": [[title or ""]]},
        {"range": f"{sheet}!{_A1(cols['description'], row)}", "values": [[description or ""]]},
        {"range": f"{sheet}!{_A1(cols['tags'], row)}",       "values": [[", ".join(tags or [])]]},
    ]
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=ssid, body={"valueInputOption":"RAW","data":data}
    ).execute()


# Simple cell writers (now guarded)

def update_title(row: int, title: str, *, youtube_id: Optional[str]=None, folder_url: Optional[str]=None, expect_date_str: Optional[str]=None) -> None:
    real_row = resolve_sheet_row(row, youtube_id=youtube_id, folder_url=folder_url, expect_title=title, expect_date_str=expect_date_str)
    if not real_row:
        logging.warning("update_title: 無法定位列 (row=%s, yid=%s, folder=%s)", row, youtube_id, folder_url)
        return
    _sheets().values().update(
        spreadsheetId=_sheet_id(),
        range=_a1(COL_TITLE, real_row),
        body={"values": [[title]]},
        valueInputOption="USER_ENTERED",
    ).execute()


def set_youtube_link(row: int, video_id: str) -> None:
    """Write hyperlink to YT; row will be safely re-located by video_id before writing."""
    real_row = resolve_sheet_row(row, youtube_id=video_id)
    if not real_row:
        logging.warning("set_youtube_link: 無法定位列 (row=%s, yid=%s)", row, video_id)
        return
    url = f"https://youtu.be/{video_id}"
    formula = f'=HYPERLINK("{url}", "{video_id}")'
    _sheets().values().update(
        spreadsheetId=_sheet_id(),
        range=_a1(COL_YT, real_row),
        body={"values": [[formula]]},
        valueInputOption="USER_ENTERED",
    ).execute()
    # also write pure ID if we have dedicated column
    if COL_YTID:
        _sheets().values().update(
            spreadsheetId=_sheet_id(),
            range=_a1(COL_YTID, real_row),
            body={"values": [[video_id]]},
            valueInputOption="RAW",
        ).execute()


def set_status(row: int, text: str, *, youtube_id: Optional[str]=None, folder_url: Optional[str]=None, expect_title: Optional[str]=None, expect_date_str: Optional[str]=None) -> None:
    real_row = resolve_sheet_row(row, youtube_id=youtube_id, folder_url=folder_url, expect_title=expect_title, expect_date_str=expect_date_str)
    if not real_row:
        logging.warning("set_status: 無法定位列 (row=%s, yid=%s, folder=%s)", row, youtube_id, folder_url)
        return
    _sheets().values().update(
        spreadsheetId=_sheet_id(),
        range=_a1(COL_STATUS, real_row),
        body={"values": [[text]]},
        valueInputOption="RAW",
    ).execute()


# Convenience wrappers kept from original file (unchanged APIs used by other modules)

def get_sheet_values(sheet, spreadsheet_id: str, tab_name: str, range_: str):
    try:
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=f"{tab_name}!{range_}").execute()
        return result.get("values", [])
    except Exception:
        logging.exception("❌ get_sheet_values failed")
        return []


def _get_sheet_gid(sheet, spreadsheet_id: str, tab_name: str) -> int:
    meta = sheet.get(spreadsheetId=spreadsheet_id, includeGridData=False).execute()
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == tab_name:
            return int(props.get("sheetId"))
    raise RuntimeError(f"Tab '{tab_name}' not found in spreadsheet")


def delete_rows(sheet, spreadsheet_id: str, tab_name: str, row_indexes: list[int]):
    try:
        if not row_indexes:
            return
        gid = _get_sheet_gid(sheet, spreadsheet_id, tab_name)
        requests = []
        for idx in sorted(row_indexes, reverse=True):
            requests.append({
                "deleteDimension": {
                    "range": {
                        "sheetId": gid,
                        "dimension": "ROWS",
                        "startIndex": idx - 1,
                        "endIndex": idx,
                    }
                }
            })
        body = {"requests": requests}
        sheet.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    except Exception:
        logging.exception("❌ delete_rows failed")
        raise


def move_folder_to_published(folder_id: str, published_parent_id: str) -> None:
    if not folder_id or not published_parent_id:
        return
    creds = _get_sa_credentials([
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ])
    drv = gbuild("drive", "v3", credentials=creds, cache_discovery=False)
    meta = drv.files().get(fileId=folder_id, fields="parents").execute()
    old_parents = ",".join(meta.get("parents", []))
    drv.files().update(
        fileId=folder_id,
        addParents=published_parent_id,
        removeParents=old_parents,
        fields="id, parents",
    ).execute()
