"""
Google Sheets helpers — FINAL (safe row resolution)

這版重點：
- 任何『寫入既有列』動作前，會先用 **YouTube ID ➜ 資料夾連結 ➜ 標題+日期 ➜ 列號** 依序重新定位正確列，避免表格排序/插入列導致『跑錯列』。
- 支援 `SHEET_TAB`，若未設定則 fallback `TAB_NAME`。
- C 欄預設寫入 **純 YouTube ID**；若要改成寫超連結公式，設定 `SHEET_YT_AS_LINK=true`。
- 可選：設定 `SHEET_SID_COL`、`SHEET_YTID_COL`，會在新增列時一併寫入 SID / YouTube ID，供未來更穩定定位。

必要環境變數：
- SHEET_ID
- SHEET_TAB（或 TAB_NAME）

可選環境變數（都有預設）：
- SHEET_TITLE_COL=B, SHEET_YT_COL=C, SHEET_FOLDER_COL=D, SHEET_STATUS_COL=E
- SHEET_YT_AS_LINK=false
- SHEET_SID_COL（例如 H）、SHEET_YTID_COL（例如 I）
"""
from __future__ import annotations

import os
import re
import json
import logging
from typing import List, Optional
from datetime import datetime

from googleapiclient.discovery import build as gbuild
from google.oauth2 import service_account

# -----------------------------------------------------
# Env / Config
# -----------------------------------------------------
SHEET_ID = (os.getenv("SHEET_ID") or "").strip()
SHEET_TAB = (os.getenv("SHEET_TAB") or os.getenv("TAB_NAME") or "已發布").strip()

COL_TITLE  = os.getenv("SHEET_TITLE_COL",  "B")
COL_YT     = os.getenv("SHEET_YT_COL",     "C")   # C 欄：預設存純 YouTube ID
COL_FOLDER = os.getenv("SHEET_FOLDER_COL", "D")
COL_STATUS = os.getenv("SHEET_STATUS_COL", "E")

# 以純 ID 定位更穩定（可選，放在表尾）
COL_SID    = os.getenv("SHEET_SID_COL")      # 例如 H
COL_YTID   = os.getenv("SHEET_YTID_COL")     # 例如 I

YT_AS_LINK = (os.getenv("SHEET_YT_AS_LINK", "false").lower() == "true")

# -----------------------------------------------------
# Clients
# -----------------------------------------------------
_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _need(v: str, name: str) -> str:
    if not v:
        raise RuntimeError(f"缺少環境變數：{name}")
    return v


def _creds():
    raw = os.getenv("SA_JSON_ENV") or os.getenv("GOOGLE_SA_JSON")
    path = os.getenv("SA_JSON_PATH") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    info = None
    if raw:
        info = json.loads(raw)
    elif path:
        with open(path, "r", encoding="utf-8") as f:
            info = json.load(f)
    else:
        raise RuntimeError("找不到 Service Account JSON，請設定 GOOGLE_SA_JSON 或 SA_JSON_PATH")

    return service_account.Credentials.from_service_account_info(info, scopes=_SHEETS_SCOPES)


def _svc():
    return gbuild("sheets", "v4", credentials=_creds(), cache_discovery=False).spreadsheets()


# -----------------------------------------------------
# Utilities
# -----------------------------------------------------

def _a1(col: str, row: int) -> str:
    return f"{SHEET_TAB}!{col}{row}"


def _get(range_a1: str):
    return _svc().values().get(spreadsheetId=_need(SHEET_ID, "SHEET_ID"), range=f"{SHEET_TAB}!{range_a1}").execute().get("values", [])


def _get_col(col: str):
    return _get(f"{col}:{col}")


def _batch_update(data_ranges):
    if not data_ranges:
        return
    _svc().values().batchUpdate(
        spreadsheetId=_need(SHEET_ID, "SHEET_ID"),
        body={"valueInputOption": "USER_ENTERED", "data": data_ranges},
    ).execute()


# -----------------------------------------------------
# Row resolution（避免跑錯列）
# -----------------------------------------------------

def _find_row_by_youtube_id(yid: str) -> Optional[int]:
    if not yid:
        return None
    # 先掃 C 欄（常態）
    col_vals = _get_col(COL_YT)
    for i, row in enumerate(col_vals, start=1):
        if i == 1:
            continue  # skip header
        cell = (row[0] if row else "") or ""
        if cell.strip() == yid or yid in cell:
            return i
    # 再掃純 ID 欄（若有）
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
    dates  = _get_col("A")
    max_len = max(len(titles), len(dates))
    cand = []
    for i in range(2, max_len + 1):
        t = (titles[i-1][0] if i-1 < len(titles) and titles[i-1] else "") or ""
        d = (dates[i-1][0]  if i-1 < len(dates)  and dates[i-1]  else "") or ""
        if t == (title or ""):
            cand.append((i, d))
    for i, d in cand:
        if d == (date_str or ""):
            return i
    return cand[0][0] if cand else None


def resolve_sheet_row(
    hint_row: Optional[int],
    *,
    expect_title: Optional[str] = None,
    expect_date_str: Optional[str] = None,
    youtube_id: Optional[str] = None,
    folder_url: Optional[str] = None,
) -> Optional[int]:
    # 1) 最優先：YouTube ID（C 欄 / YTID 欄）
    r = _find_row_by_youtube_id(youtube_id or "")
    if r:
        return r
    # 2) 其次：資料夾連結（D 欄）
    r = _find_row_by_folder_url(folder_url or "")
    if r:
        return r
    # 3) 驗證 hint_row 對不對（B 欄）
    if hint_row and hint_row > 1 and expect_title:
        t = _get(f"{COL_TITLE}{hint_row}:{COL_TITLE}{hint_row}")
        title_at_row = (t[0][0] if t and t[0] else "") or ""
        if title_at_row == expect_title:
            return hint_row
    # 4) 標題 + 日期
    if expect_title:
        r = _find_row_by_title_and_date(expect_title, expect_date_str or "")
        if r:
            return r
    # 5) 最後才直接用列號
    if hint_row and hint_row > 1:
        return hint_row
    return None


# -----------------------------------------------------
# Appends & Updates（皆走 resolver）
# -----------------------------------------------------

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
    """新增一列，回傳列號。會依設定把 SID / YTID 寫到指定欄位。C 欄預設為空，等對帳時再寫入 YT ID。"""
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID 未設定")

    values = [[
        dt_local.strftime("%Y-%m-%d %H:%M"),  # A 日期（字串）
        title,                                  # B 標題
        "",                                     # C YouTube ID（先留空）
        folder_url,                              # D 資料夾位置
        status,                                  # E 狀態
        keywords,                                # F 關鍵字
        int(today_views or 0),                   # G 今日觀看
    ]]
    rng = f"{SHEET_TAB}!A:G"
    resp = _svc().values().append(
        spreadsheetId=_need(SHEET_ID, "SHEET_ID"),
        range=rng,
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()

    updated_range = (resp.get("updates") or {}).get("updatedRange", "")
    m = re.search(r"![A-Z]+(\d+):", updated_range)
    row_idx = int(m.group(1)) if m else 0

    # 寫入身份欄位（若有設定）
    data = []
    if row_idx:
        if COL_SID and sid is not None:
            data.append({"range": _a1(COL_SID, row_idx), "values": [[str(sid)]]})
        if COL_YTID and youtube_id:
            data.append({"range": _a1(COL_YTID, row_idx), "values": [[youtube_id]]})
        if data:
            _batch_update(data)
    return row_idx


def set_youtube_link(row: int, video_id: str) -> None:
    """把 YouTube ID 寫到 C 欄；若 `SHEET_YT_AS_LINK=true`，則寫入 HYPERLINK 公式。亦會（若設定）把純 ID 寫到 `SHEET_YTID_COL`。"""
    real_row = resolve_sheet_row(row, youtube_id=video_id)
    if not real_row:
        logging.warning("set_youtube_link: 無法定位列 (row=%s, yid=%s)", row, video_id)
        return
    if YT_AS_LINK:
        url = f"https://youtu.be/{video_id}"
        value = f'=HYPERLINK("{url}", "{video_id}")'
    else:
        value = video_id
    _svc().values().update(
        spreadsheetId=_need(SHEET_ID, "SHEET_ID"),
        range=_a1(COL_YT, real_row),
        body={"values": [[value]]},
        valueInputOption="USER_ENTERED",
    ).execute()
    # 同步純 ID 欄（若有設定）
    if COL_YTID:
        _svc().values().update(
            spreadsheetId=_need(SHEET_ID, "SHEET_ID"),
            range=_a1(COL_YTID, real_row),
            body={"values": [[video_id]]},
            valueInputOption="RAW",
        ).execute()


def set_status(row: int, text: str, *, youtube_id: Optional[str]=None, folder_url: Optional[str]=None, expect_title: Optional[str]=None, expect_date_str: Optional[str]=None) -> None:
    real_row = resolve_sheet_row(row, youtube_id=youtube_id, folder_url=folder_url, expect_title=expect_title, expect_date_str=expect_date_str)
    if not real_row:
        logging.warning("set_status: 無法定位列 (row=%s, yid=%s, folder=%s)", row, youtube_id, folder_url)
        return
    _svc().values().update(
        spreadsheetId=_need(SHEET_ID, "SHEET_ID"),
        range=_a1(COL_STATUS, real_row),
        body={"values": [[text]]},
        valueInputOption="RAW",
    ).execute()


def set_published_folder_link(row: int, folder_url: str, *, youtube_id: Optional[str]=None, expect_title: Optional[str]=None, expect_date_str: Optional[str]=None) -> None:
    real_row = resolve_sheet_row(row, youtube_id=youtube_id, folder_url=folder_url, expect_title=expect_title, expect_date_str=expect_date_str)
    if not real_row:
        logging.warning("set_published_folder_link: 無法定位列 (row=%s, yid=%s, folder=%s)", row, youtube_id, folder_url)
        return
    _svc().values().update(
        spreadsheetId=_need(SHEET_ID, "SHEET_ID"),
        range=_a1(COL_FOLDER, real_row),
        body={"values": [[folder_url]]},
        valueInputOption="USER_ENTERED",
    ).execute()


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
        data.append({"range": _a1(COL_FOLDER, real_row), "values": [[folder_url]]})
    _batch_update(data)


# -----------------------------------------------------
# 兼容舊有 API（若其他模組直接呼叫）
# -----------------------------------------------------

def get_sheet_values(sheet, spreadsheet_id: str, tab_name: str, range_: str):
    try:
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=f"{tab_name}!{range_}").execute()
        return result.get("values", [])
    except Exception:
        logging.exception("get_sheet_values failed")
        return []


def _get_sheet_gid(sheet, spreadsheet_id: str, tab_name: str) -> int:
    meta = sheet.get(spreadsheetId=spreadsheet_id, includeGridData=False).execute()
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == tab_name:
            return int(props.get("sheetId"))
    raise RuntimeError(f"Tab '{tab_name}' not found in spreadsheet")


def delete_rows(sheet, spreadsheet_id: str, tab_name: str, row_indexes: List[int]):
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
    sheet.batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()
