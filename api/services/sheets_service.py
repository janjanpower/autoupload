# api/services/sheets_service.py
import os, re
SHEET_ID = os.getenv("SHEET_ID") or ""
SHEET_TAB = os.getenv("SHEET_TAB") or "已發布"

import os, json, re
from typing import List, Optional
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
from api.services.google_sa import get_google_service
from api.config import settings

def _sheet_id() -> str:
    sid = os.getenv("SHEET_ID", "").strip()
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

def _sheets():
    creds = _get_sa_credentials([
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return svc.spreadsheets()

def append_published_row(dt_local, title, folder_url, status, keywords, today_views=0) -> int:
    """新增一列，回傳該列的列號（int）。"""
    values = [[
        dt_local.strftime("%Y-%m-%d %H:%M"),
        title,
        folder_url,
        status,
        keywords,
        int(today_views or 0),
    ]]
    rng = f"{SHEET_TAB}!A:F"
    resp = _sheets().values().append(                 # ← 改用 _sheets()
        spreadsheetId=_sheet_id(),                    # ← 改用 _sheet_id()
        range=rng,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values}
    ).execute()
    updated_range = (resp.get("updates") or {}).get("updatedRange", "")
    m = re.search(r"![A-Z]+(\d+):", updated_range)
    return int(m.group(1)) if m else 0

def update_title_by_row(row_index: int, new_title: str) -> None:
    """把排程表（已發布分頁）第 row_index 列的 B 欄更新為新標題。"""
    if not row_index:
        return
    rng = f"{SHEET_TAB}!B{row_index}"
    _sheets().values().update(                        # ← 改用 _sheets()
        spreadsheetId=_sheet_id(),                   # ← 改用 _sheet_id()
        range=rng,
        valueInputOption="USER_ENTERED",
        body={"values": [[new_title]]}
    ).execute()

def mark_row_published(row_index: int, video_id: str) -> None:
    if not row_index:
        return
    url = f"https://youtu.be/{video_id}"
    _sheets().values().batchUpdate(
        spreadsheetId=_sheet_id(),
        body={"valueInputOption": "USER_ENTERED",
              "data": [
                  {"range": f"{SHEET_TAB}!C{row_index}", "values": [[f'=HYPERLINK("{url}","{url}")']]},
                  {"range": f"{SHEET_TAB}!D{row_index}", "values": [["已發布"]]},
              ]}
    ).execute()


def _fetch_all_rows() -> List[List[str]]:
    r = _sheets().values().get(
        spreadsheetId=_sheet_id(),
        range=f"{SHEET_TAB}!A:F"
    ).execute()
    return r.get("values", [])

def _first_data_row_index() -> int:
    return 2

def find_row_by_title_and_folder(title: Optional[str], folder_url: Optional[str]) -> Optional[int]:
    rows = _fetch_all_rows()
    for idx, row in enumerate(rows, start=1):
        if idx < _first_data_row_index():
            continue
        t = (row[1] if len(row) > 1 else "").strip()
        c = (row[2] if len(row) > 2 else "").strip()
        if folder_url:
            if t == (title or "") and (folder_url in c):
                return idx
        else:
            if t == (title or ""):
                return idx
    return None

def update_status_and_views(row_index: int,
                            status: Optional[str] = None,
                            today_views: Optional[int] = None,
                            folder_url: Optional[str] = None):
    data = []
    if status is not None:
        data.append({"range": f"{SHEET_TAB}!D{row_index}", "values": [[status]]})
    if today_views is not None:
        data.append({"range": f"{SHEET_TAB}!F{row_index}", "values": [[int(today_views)]]})
    if folder_url:
        data.append({"range": f"{SHEET_TAB}!C{row_index}",
                     "values": [[f'=HYPERLINK("{folder_url}","{folder_url}")']]})
    if not data:
        return
    _sheets().values().batchUpdate(
        spreadsheetId=_sheet_id(),
        body={"valueInputOption": "USER_ENTERED", "data": data}
    ).execute()


# 需要的環境變數：
# GOOGLE_SERVICE_ACCOUNT_JSON  = Service Account JSON（整段放進去）
# SHEETS_SPREADSHEET_ID       = 該試算表 ID
# SHEETS_SHEET_NAME           = 工作表名稱（預設 'Sheet1'）
# 欄位需求：第一列含欄位：video_id, title, description, tags

_SCOPES_META = ["https://www.googleapis.com/auth/spreadsheets"]

def _svc_meta():
    json_str = os.getenv("GOOGLE_SA_JSON") or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    ssid     = os.getenv("SHEET_ID")       or os.getenv("SHEETS_SPREADSHEET_ID", "")
    sheet_nm = os.getenv("SHEET_NAME")     or os.getenv("SHEETS_SHEET_NAME", "Sheet1")
    if not json_str or not ssid:
        raise RuntimeError("Sheets 未設定（缺 GOOGLE_SA_JSON / SHEET_ID）")
    info = json.loads(json_str)
    creds = Credentials.from_service_account_info(info, scopes=_SCOPES_META)
    cli = build("sheets", "v4", credentials=creds, cache_discovery=False)
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
        {"range": f"{sheet}!{_A1(cols['title'], row)}", "values": [[title or ""]]},
        {"range": f"{sheet}!{_A1(cols['description'], row)}", "values": [[description or ""]]},
        {"range": f"{sheet}!{_A1(cols['tags'], row)}", "values": [[", ".join(tags or [])]]},
    ]
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=ssid, body={"valueInputOption":"RAW","data":data}
    ).execute()

def set_published_folder_link(row_index: int, folder_url: str) -> None:
    """
    已發布後：把 C 欄寫入『資料夾連結』、D 欄寫入『已發布』。
    """
    if not row_index:
        return
    data = [
        {"range": f"{SHEET_TAB}!C{row_index}", "values": [[f'=HYPERLINK("{folder_url}","{folder_url}")']]},
        {"range": f"{SHEET_TAB}!D{row_index}", "values": [["已發布"]]},
    ]
    _sheets().values().batchUpdate(
        spreadsheetId=_sheet_id(),
        body={"valueInputOption": "USER_ENTERED", "data": data}
    ).execute()

def clear_sheet_row_status(row_idx: int, status: str = "已刪除"):
    """清空 C 欄並在 D 欄標記 '已刪除'"""
    if not row_idx:
        return
    _sheets().values().update(
        spreadsheetId=_sheet_id(),
        range=f"{SHEET_TAB}!C{row_idx}:D{row_idx}",
        valueInputOption="RAW",
        body={"values": [["", status]]}
    ).execute()

