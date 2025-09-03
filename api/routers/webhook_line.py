"""
LINE Webhook Router — 完整版（與新版 sheets_service 相容）

重點改動：
- 直接自 sheets_service 匯入 `append_published_row`、`update_title_by_row`、`find_row_by_title_and_folder`（已在該服務內提供相容 shim）。
- 移除本檔案內部對上述兩個函式的重複定義，避免未定義變數（_svc/_need/SHEET_ID）造成 NameError。
- 保留你原有的上架／編輯流程與狀態機邏輯。
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
import json, os

from fastapi import APIRouter, Request, BackgroundTasks, Header, HTTPException
from sqlalchemy.engine import Row, RowMapping

from api.config import settings
from api.services import scheduler_repo
from api.services.scheduler_repo import (
    get_state, set_state, reset_state, insert_schedule, list_scheduled, list_all,
    update_uploaded, update_error, cancel_schedule, update_schedule_time, set_schedule_sheet_row
)
from api.services.drive_service import (
    list_child_folders, get_single_video_in_folder, find_text_file_in_folder, download_text, upload_text
)
from api.services.youtube_service import (
    youtube_upload_from_drive, update_thumbnail_from_drive, pick_thumbnail_in_folder, update_video_metadata
)
from api.services.sheets_service import append_published_row

from api.utils.meta_parser import parse_meta_text
from api.utils.timefmt import parse_time_ymdhm, format_tw_with_weekday
from api.utils.line_api import verify_signature, reply_text, push_text
from api.schemas.state_constants import (
    S_IDLE, S_UPLOAD_TYPE, S_PICK_FOLDER_FOR_UPLOAD, S_PREVIEW_META, S_WAIT_EDIT_META, S_WAIT_SCHEDULE_TIME,
    S_PICK_FOLDER_MODIFY, S_WAIT_EDIT_META_ONLY, S_MODIFY_SCHEDULE_PICK, S_MODIFY_SCHEDULE_ACTION, S_PICK_PLATFORM,
    S_SCHEDULE_PICK, S_SCHEDULE_EDIT_MENU, S_SCHEDULE_EDIT_TITLE, S_SCHEDULE_EDIT_DESC, S_SCHEDULE_EDIT_TAGS, S_SCHEDULE_EDIT_TIME,
    MENU_TEXT, SUBMENU_PLATFORM, SUBMENU_UPLOAD, TZ
)

router = APIRouter()

# 文字檔模板（JSON 格式，和系統使用的欄位對齊）
TEMPLATE_META = (
    "標題：\n"
    "內文：\n"
    "關鍵字：\n"
    "（關鍵字可用空格或逗號分隔，例如：旅遊 美食 台中）"
)

def _collapse_ws(s: Any) -> str:
    """壓縮各種空白（含全形空白/換行/Tab/不斷行空白），並去頭尾空白。"""
    if s is None:
        return ""
    t = str(s)
    for ch in ["\u3000", "\u00A0", "\r", "\n", "\t"]:
        t = t.replace(ch, " ")
    return " ".join(t.strip().split())


def _parse_tags_input(text: str) -> List[str]:
    """支援逗號或換行分隔，去掉重複/空白。"""
    raw = (text or "").replace("\r", "\n").replace("，", ",")
    tags: List[str] = []
    for line in raw.split("\n"):
        parts = [p.strip() for p in line.split(",") if p.strip()]
        for p in parts:
            if p not in tags:
                tags.append(p)
    return tags


def _ensure_drive_parent_or_reply(reply_token: str) -> bool:
    if not settings.DRIVE_PARENT_ID:
        reply_text(reply_token, "系統未設定 GOOGLE_DRIVE_PARENT_ID，請先設定母資料夾 ID。")
        return False
    return True


def detect_main_text_intent(text_in: str):
    t = (text_in or "").strip().replace(" ", "").lower()
    if t in {"取消", "退出", "返回", "回主選單", "cancel"}:
        return "5"  # 新選單：5=取消
    if t in {"上架", "發佈", "發布"}:
        return "1"
    if t in {"影片清單", "清單", "列表", "資料夾清單"}:
        return "2"
    if t in {"修改檔案", "改檔案", "編輯檔案", "編輯文字", "改文字檔"}:
        return "3"
    # 舊說法一律導向「目前排程」
    if t in {"目前排程", "查詢排程", "排程清單", "排程列表", "修改排程", "改排程", "調整排程", "變更時間", "排程時間"}:
        return "4"  # 新選單：4=目前排程
    return None


# ===== Drive/YouTube 輔助 =====

def classify_folder_type(folder_id: str) -> Optional[str]:
    v = get_single_video_in_folder(folder_id)
    if not v:
        return None
    meta = v.get("videoMediaMetadata", {}) or {}
    w, h = meta.get("width"), meta.get("height")
    if w == 1920 and h == 1080:
        return "long"
    if w == 1080 and h == 1920:
        return "short"
    if isinstance(w, int) and isinstance(h, int):
        return "long" if w > h else "short"
    return None


def list_folders_by_type(video_type: str) -> List[Dict]:
    folders = list_child_folders(settings.DRIVE_PARENT_ID)
    return [f for f in folders if classify_folder_type(f["id"]) == video_type]


def format_folder_list(folders: List[Dict], add_cancel: bool = False) -> str:
    if not folders:
        return "找不到符合的資料夾。"
    lines = [f"{i+1}. {f['name']}" for i, f in enumerate(folders)]
    if add_cancel:
        lines.append(f"{len(folders)+1}. 取消")
    return "\n".join(lines)


def _do_main_choice(choice: str, reply_token: str, line_user_id: str):
    reset_state(line_user_id)
    if choice == "1":
        reply_text(reply_token, SUBMENU_PLATFORM)
        set_state(line_user_id, S_PICK_PLATFORM, {})
    elif choice == "2":
        if not _ensure_drive_parent_or_reply(reply_token):
            return
        folders = list_child_folders(settings.DRIVE_PARENT_ID)
        reply_text(reply_token, "影片資料夾：\n" + format_folder_list(folders) + "\n\n（輸入「?」可回主選單）")
    elif choice == "3":
        if not _ensure_drive_parent_or_reply(reply_token):
            return
        folders = list_child_folders(settings.DRIVE_PARENT_ID)
        set_state(line_user_id, S_PICK_FOLDER_MODIFY, {"folders": folders})
        reply_text(reply_token, "請輸入要修改檔案的資料夾編號：\n" + format_folder_list(folders, add_cancel=True))

    elif choice == "4":
        # 目前排程：直接查 YouTube 端
        from api.services.youtube_service import list_scheduled_youtube
        items = list_scheduled_youtube()
        if not items:
            reply_text(reply_token, "YouTube 端目前沒有任何『定時公開』的上架排程。")
            return

        lines = []
        for i, it in enumerate(items, start=1):
            when_local = it["publishAt_utc"].astimezone(TZ).strftime("%Y-%m-%d %H:%M")
            lines.append(f"{i}. {it['title']} - {when_local}\n{it['url']}")

        # 只存「ID 清單」，避免把 datetime 放進 state
        yt_ids = [it["id"] for it in items]
        set_state(line_user_id, S_SCHEDULE_PICK, {"yt_ids": yt_ids})
        reply_text(
            reply_token,
            "YouTube 端目前排程：\n\n" + "\n\n".join(lines) +
            "\n\n請輸入要「修改」的編號（或輸入「取消」返回）。"
        )
        return


# ===== 通用回覆工具 =====

def _col(row: Any, key: str, default=None):
    m = getattr(row, "_mapping", None)
    if m is not None:
        return m.get(key, default)
    return getattr(row, key, default)


def _fmt_when(val) -> str:
    """把 UTC datetime / ISO 字串 / None 安全轉成台北時間 'YYYY-MM-DD HH:MM'。"""
    if val is None or val in ("", "null"):
        return "-"
    if isinstance(val, datetime):
        if val.tzinfo is None:
            val = val.replace(tzinfo=timezone.utc)
        tpe = val.astimezone(timezone(timedelta(hours=8)))
        return tpe.strftime("%Y-%m-%d %H:%M")
    try:
        s = str(val)
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        tpe = dt.astimezone(timezone(timedelta(hours=8)))
        return tpe.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(val)


def _parse_tpe(text: str) -> datetime:
    """把使用者輸入的台北時間 'YYYY-MM-DD HH:MM' 轉成 tz-aware UTC datetime。"""
    dt_naive = datetime.strptime(text.strip(), "%Y-%m-%d %H:%M")
    tpe = dt_naive.replace(tzinfo=timezone(timedelta(hours=8)))
    return tpe.astimezone(timezone.utc)


def _send(reply_token: str, text: str):
    try:
        return reply_text(reply_token, text)  # 若已有 reply_text
    except NameError:
        return push_text(reply_token, text)   # 若只有 push_text


# ===== 導覽選單：列出全部或可修改的排程 =====

def handle_menu_show_all_schedules(line_user_id: str, reply_token: str):
    rows = scheduler_repo.list_all(line_user_id)
    if not rows:
        _send(reply_token, "目前沒有任何排程。")
        return

    lines = []
    for r in rows:
        _id = _col(r, "id")
        _folder = _col(r, "folder_name", "")
        _type = _col(r, "video_type", "")
        _dt = _col(r, "t", None) or _col(r, "schedule_time", None)
        _status = _col(r, "status", "")
        when = _fmt_when(_dt)
        lines.append(f"#{_id} {_folder} ({_type}) - {when} [{_status}]")

    _send(reply_token, "目前排程：\n" + "\n".join(lines))


def handle_menu_modify_schedules(line_user_id: str, reply_token: str):
    rows = scheduler_repo.list_scheduled(line_user_id)
    if not rows:
        _send(reply_token, "目前沒有可修改的排程（尚未上傳的 scheduled）。")
        return

    lines = []
    for r in rows:
        _id = _col(r, "id")
        _folder = _col(r, "folder_name", "")
        _type = _col(r, "video_type", "")
        _dt = _col(r, "t", None) or _col(r, "schedule_time", None)
        when = _fmt_when(_dt)
        lines.append(f"#{_id} {_folder} ({_type}) - {when}")

    set_state(line_user_id, "S_MODIFY_SCHEDULE_PICK", {"opts": [int(_col(r, "id")) for r in rows]})
    _send(
        reply_token,
        "請回覆要修改的排程編號：\n" + "\n".join(lines) + "\n\n（輸入「取消」可返回主選單）"
    )


# ===== 正式 Webhook 端點 =====

@router.post("/webhook/line")
async def line_webhook(request: Request, background_tasks: BackgroundTasks, x_line_signature: str | None = Header(default=None, alias="X-Line-Signature")):
    body = await request.body()
    LINE_SKIP_SIGNATURE = os.getenv("LINE_SKIP_SIGNATURE", "0") in {"1", "true", "True"}
    if not LINE_SKIP_SIGNATURE:
        if not x_line_signature:
            raise HTTPException(status_code=403, detail="Missing X-Line-Signature header")
        verify_signature(body, x_line_signature)

    payload = await request.json()
    events = payload.get("events", [])
    for ev in events:
        if ev.get("type") != "message":
            continue
        msg = ev.get("message", {})
        if msg.get("type") != "text":
            continue

        message_text = (msg.get("text") or "").strip()
        reply_token = ev.get("replyToken", "")
        source = ev.get("source", {})
        line_user_id = source.get("userId") or "unknown"

        stage, data = get_state(line_user_id)

        # 主選單意圖快速入口
        main_choice = detect_main_text_intent(message_text)
        if main_choice:
            _do_main_choice(main_choice, reply_token, line_user_id)
            continue

        if message_text in {"?", "menu", "Menu"}:
            reset_state(line_user_id)
            reply_text(reply_token, MENU_TEXT)
            continue

        if stage == S_IDLE:
            if message_text in {"1", "2", "3", "4", "5"}:
                _do_main_choice(message_text, reply_token, line_user_id)
                continue
            reply_text(reply_token, MENU_TEXT)
            continue

        # ====== 上架流程 ======
        if stage == S_PICK_PLATFORM:
            t = message_text.strip().replace(" ", "").lower()
            if t in {"2", "取消", "返回", "回主選單", "cancel"}:
                _do_main_choice("5", reply_token, line_user_id)   # 取消 → 5
                continue
            if t in {"1", "youtube", "yt", "y"}:
                data = {"platform": "youtube"}
                set_state(line_user_id, S_UPLOAD_TYPE, data)
                reply_text(reply_token, SUBMENU_UPLOAD)
                continue
            reply_text(reply_token, "請輸入：\n1. YouTube\n2. 取消")
            continue

        if stage == S_UPLOAD_TYPE:
            if message_text == "3":
                _do_main_choice("5", reply_token, line_user_id)   # 取消 → 5
                continue
            if message_text not in {"1", "2"}:
                reply_text(reply_token, "請輸入 1（長片）/ 2（短影音）或 3（取消）")
                continue
            if not _ensure_drive_parent_or_reply(reply_token):
                continue
            vtype = "long" if message_text == "1" else "short"
            folders = list_folders_by_type(vtype)
            if not folders:
                reply_text(reply_token, f"找不到符合「{ '長片' if vtype=='long' else '短影音' }」的資料夾。")
                reset_state(line_user_id)
                continue
            set_state(
                line_user_id,
                S_PICK_FOLDER_FOR_UPLOAD,
                {"platform": data.get("platform", "youtube"), "vtype": vtype, "folders": folders},
            )
            reply_text(
                reply_token,
                f"請選擇要上架的資料夾（{ '長片' if vtype=='long' else '短影音' }）：\n" + format_folder_list(folders, add_cancel=True),
            )
            continue

        if stage == S_PICK_FOLDER_FOR_UPLOAD:
            if not message_text.isdigit():
                reply_text(reply_token, "請輸入清單中的編號（或輸入「取消」。）")
                continue
            folders = data.get("folders", [])
            idx = int(message_text) - 1
            if idx == len(folders):
                _do_main_choice("5", reply_token, line_user_id)   # 取消 → 5
                continue
            if not (0 <= idx < len(folders)):
                reply_text(reply_token, "編號超出範圍，請重新輸入。")
                continue
            chosen = folders[idx]
            meta = find_text_file_in_folder(chosen["id"])

            if meta:
                meta_text = download_text(meta["id"])
                hint = "目前文字檔內容："
            else:
                # 沒有文字檔：提供模板，讓使用者直接複製貼上
                meta_text = TEMPLATE_META
                hint = "（找不到文字檔，以下提供『模板』，請複製後直接貼上）"

            set_state(
                line_user_id,
                S_PREVIEW_META,
                {
                    "platform": data.get("platform", "youtube"),
                    "vtype": data["vtype"],
                    "folder": chosen,
                    "meta": meta or {},
                    "meta_text": meta_text,
                },
            )
            reply_text(
                reply_token,
                f"{hint}\n\n{meta_text}\n\n若需要修改，請直接貼上「完整的新內容」；"
                f"若正確請回覆「確認」。\n輸入「取消」返回"
            )
            continue

        if stage == S_PREVIEW_META:
            if message_text == "確認":
                set_state(line_user_id, S_WAIT_SCHEDULE_TIME, data)
                reply_text(reply_token, "請輸入上架時間\nYYYY-MM-DD HH:mm\n或輸入「取消」返回")
                continue
            else:
                data["pending_meta_text"] = message_text
                set_state(line_user_id, S_WAIT_EDIT_META, data)
                reply_text(reply_token, "已收到新內容\n回覆「確認」即可覆寫\n或重新貼上以更新\n輸入「取消」返回")
                continue

        if message_text == "確認":
            meta = data.get("meta") or {}
            # 若有既有文字檔 → 覆寫；若沒有 → 直接用內容繼續流程（不強迫建立檔案）
            if meta.get("id"):
                try:
                    upload_text(meta["id"], data["pending_meta_text"])
                    data["meta_text"] = data["pending_meta_text"]
                except Exception as e:
                    reply_text(reply_token, f"覆寫失敗：{e}")
                    reset_state(line_user_id)
                    continue
                set_state(line_user_id, S_WAIT_SCHEDULE_TIME, data)
                reply_text(reply_token, "已覆寫文字檔。\n請輸入上架時間\nYYYY-MM-DD HH:mm\n或輸入「取消」返回")
            else:
                # 沒有文字檔：直接把使用者貼的內容當作本次上架用的 meta_text
                data["meta_text"] = data["pending_meta_text"]
                set_state(line_user_id, S_WAIT_SCHEDULE_TIME, data)
                reply_text(
                    reply_token,
                    "已接收新內容（目前資料夾中沒有文字檔，將不建立檔案）。\n"
                    "請輸入上架時間\nYYYY-MM-DD HH:mm\n或輸入「取消」返回"
                )
            continue

        if stage == S_WAIT_SCHEDULE_TIME:
            if message_text == "取消":
                reset_state(line_user_id)
                reply_text(reply_token, "已取消，回到主選單。\n\n" + MENU_TEXT)
                return {"ok": True}

            dt_utc = parse_time_ymdhm(message_text)
            if not dt_utc:
                reply_text(reply_token, "時間格式不正確，請用：YYYY-MM-DD HH:mm\n（或輸入「取消」返回）")
                return {"ok": True}

            folder = data["folder"]
            vtype = data["vtype"]
            meta = data.get("meta") or {}
            meta_text = data.get("meta_text", "")

            # ★ 取得 sid（很重要，後面要用來回寫 sheet_row）
            sid = insert_schedule(
                line_user_id, folder["id"], folder["name"], vtype,
                meta.get("id"), meta_text, dt_utc
            )

            local_time = dt_utc.astimezone(TZ).strftime("%Y-%m-%d %H:%M")
            prefix = "長片" if vtype == "long" else "短影音"
            reply_text(
                reply_token,
                f"已加入排程並開始上傳：\n{prefix}：{folder['name']}\n預定公開時間：{local_time}\n狀態：uploading…",
            )
            reset_state(line_user_id)

            def _do_upload():
                try:
                    vid = youtube_upload_from_drive(folder["id"], meta_text, dt_utc, vtype)
                    update_uploaded(line_user_id, folder["id"], dt_utc, vid)

                    # ★ 寫入排程表，接回列號並寫回 DB（很重要）
                    try:
                        meta = parse_meta_text(meta_text or "")
                        row_idx = append_published_row(
                            dt_local=dt_utc.astimezone(TZ),
                            title=(meta.get("title") or folder["name"]),
                            folder_url="",                 # 先空白，等公開後再補
                            status="已排程",
                            keywords=",".join(meta.get("tags", [])),
                            today_views=0
                        )
                        if row_idx:
                            set_schedule_sheet_row(sid, row_idx)
                    except Exception as e:
                        import logging
                        logging.getLogger(__name__).exception("寫入 Sheet 失敗：%s", e)

                    # 4) 設定縮圖（可失敗，不影響主流程）
                    try:
                        update_thumbnail_from_drive(vid, folder["id"])
                    except Exception:
                        pass

                    # 5) 通知
                    url = f"https://youtu.be/{vid}"
                    when = format_tw_with_weekday(dt_utc)
                    push_text(line_user_id, f"✅ 上傳完成：{folder['name']}\nYouTube URL :\n{url}\n⚠️將於 {when} 公開")

                except Exception as e:
                    update_error(line_user_id, folder["id"], dt_utc, e)
                    push_text(line_user_id, f"❌ 上傳失敗：{folder['name']}\n錯誤：{e}")

            if settings.YT_REFRESH_TOKEN:
                background_tasks.add_task(_do_upload)
            else:
                push_text(line_user_id, "⚠️ 尚未設定 YouTube OAuth（YT_* 環境變數），目前只記錄了排程，未上傳。")

            return {"ok": True}

        # ====== 檔案文字修改（純覆寫） ======
        if stage == S_PICK_FOLDER_MODIFY:
            if not message_text.isdigit():
                reply_text(reply_token, "請輸入清單中的編號（或輸入「取消」。）")
                continue
            folders = data.get("folders", [])
            idx = int(message_text) - 1
            if idx == len(folders):
                _do_main_choice("5", reply_token, line_user_id)  # 取消 → 5
                continue
            if not (0 <= idx < len(folders)):
                reply_text(reply_token, "編號超出範圍，請重新輸入。")
                continue
            chosen = folders[idx]
            meta = find_text_file_in_folder(chosen["id"])
            if meta:
                meta_text = download_text(meta["id"])
                hint = "目前文字檔內容："
            else:
                meta_text = TEMPLATE_META
                hint = "（找不到文字檔，以下提供『模板』，請複製後直接貼上；此流程需已有文字檔才能覆寫）"

            set_state(line_user_id, S_WAIT_EDIT_META_ONLY, {"folder": chosen, "meta": meta or {}, "meta_text": meta_text})
            reply_text(
                reply_token,
                f"{hint}\n\n{meta_text}\n\n欲修改請直接貼上「完整新內容」，回覆「確認」後將覆寫文字檔。"
                f"\n或輸入「取消」返回"
            )
            continue

        if stage == S_WAIT_EDIT_META_ONLY:
            if message_text == "確認":
                meta = data.get("meta") or {}
                if not meta.get("id"):
                    reset_state(line_user_id)
                    reply_text(
                        reply_token,
                        "此功能需資料夾已有文字檔才能覆寫。\n"
                        "請先在該資料夾新增一個文字檔（例如：meta.txt），內容可使用上方模板。\n\n" + MENU_TEXT
                    )
                    continue
                try:
                    new_text = data.get("pending_meta_text", data.get("meta_text", ""))
                    upload_text(meta["id"], new_text)
                except Exception as e:
                    reply_text(reply_token, f"覆寫失敗：{e}")
                    reset_state(line_user_id)
                    continue
                reset_state(line_user_id)
                reply_text(reply_token, "已覆寫完成。\n\n" + MENU_TEXT)
                continue

        # ====== 新版「目前排程 → 選影片 → 編輯 (YouTube)」 ======
        if stage == S_SCHEDULE_PICK:
            if message_text.strip() in {"取消", "?"}:
                reset_state(line_user_id)
                reply_text(reply_token, "已取消，回到主選單。\n\n" + MENU_TEXT)
                return
            if not message_text.strip().isdigit():
                reply_text(reply_token, "請輸入清單前的數字編號，或「取消」返回。")
                return

            idx = int(message_text.strip())
            yt_ids = (data or {}).get("yt_ids", [])
            if not yt_ids or not (1 <= idx <= len(yt_ids)):
                reply_text(reply_token, "編號超出範圍，請重新輸入。")
                return

            video_id = yt_ids[idx - 1]
            set_state(line_user_id, S_SCHEDULE_EDIT_MENU, {"video_id": video_id})
            reply_text(
                reply_token,
                "要修改哪一個欄位？\n"
                "1. 標題\n"
                "2. 內文\n"
                "3. 關鍵字（以逗號分隔）\n"
                "4. 上架時間（YYYY-MM-DD HH:MM，台北時間）\n"
                "5. 取消"
            )
            return

        if stage == S_SCHEDULE_EDIT_MENU:
            vid = (data or {}).get("video_id")
            t = message_text.strip()
            if t in {"5", "取消", "?"}:
                reset_state(line_user_id)
                reply_text(reply_token, "已取消，回到主選單。\n\n" + MENU_TEXT)
                continue
            if t == "1":
                set_state(line_user_id, S_SCHEDULE_EDIT_TITLE, {"video_id": vid})
                reply_text(reply_token, "請輸入新的『標題』：")
                continue
            if t == "2":
                set_state(line_user_id, S_SCHEDULE_EDIT_DESC, {"video_id": vid})
                reply_text(reply_token, "請輸入新的『內文』：")
                continue
            if t == "3":
                set_state(line_user_id, S_SCHEDULE_EDIT_TAGS, {"video_id": vid})
                reply_text(reply_token, "請輸入新的『關鍵字』（以逗號分隔）：")
                continue
            if t == "4":
                set_state(line_user_id, S_SCHEDULE_EDIT_TIME, {"video_id": vid})
                reply_text(reply_token, "請輸入新的『上架時間』（YYYY-MM-DD HH:MM，台北時間）：")
                continue
            reply_text(reply_token, "請輸入 1-5 其中之一。")
            continue

        if stage in (S_SCHEDULE_EDIT_TITLE, S_SCHEDULE_EDIT_DESC, S_SCHEDULE_EDIT_TAGS, S_SCHEDULE_EDIT_TIME):
            vid = (data or {}).get("video_id")
            if not vid:
                reset_state(line_user_id)
                reply_text(reply_token, "找不到該影片，已返回主選單。\n\n" + MENU_TEXT)
                continue

            # 1) 標題/內文/關鍵字 → 直接更新 YouTube
            if stage in (S_SCHEDULE_EDIT_TITLE, S_SCHEDULE_EDIT_DESC, S_SCHEDULE_EDIT_TAGS):
                from api.services.youtube_service import update_video_metadata
                try:
                    if stage == S_SCHEDULE_EDIT_TITLE:
                        new_title = _collapse_ws(message_text)
                        if not new_title:
                            reply_text(reply_token, "❗ 標題不能為空白，請重新輸入新的『標題』：")
                            continue
                        if len(new_title) > 100:
                            reply_text(reply_token, f"❗ 標題過長（{len(new_title)} 字），請控制在 100 字以內，重新輸入：")
                            continue

                        # 1) 更新 YouTube
                        update_video_metadata(video_id=vid, title=new_title)

                        # 2) 嘗試同步更新 Sheet
                        try:
                            rec = scheduler_repo.get_by_video_id(vid)
                            row_idx = int(rec.get("sheet_row") or 0) if rec else 0
                            if row_idx:
                                from api.services.sheets_service import resolve_sheet_row, _svc, _a1, COL_TITLE, SHEET_ID, SHEET_TAB
                                real_row = resolve_sheet_row(row_idx, youtube_id=vid)
                                if real_row:
                                    _svc().values().update(
                                        spreadsheetId=SHEET_ID,
                                        range=_a1(COL_TITLE, real_row),
                                        body={"values": [[new_title]]},
                                        valueInputOption="USER_ENTERED",
                                    ).execute()
                        except Exception as e:
                            import logging
                            logging.getLogger(__name__).warning("同步更新 Sheet 失敗：%s", e)

                        reset_state(line_user_id)
                        reply_text(reply_token, "✅ 已更新 YouTube 標題。")
                        continue

                    elif stage == S_SCHEDULE_EDIT_DESC:
                        new_desc = message_text.replace("\r", "")
                        update_video_metadata(video_id=vid, description=new_desc)
                        reset_state(line_user_id)
                        reply_text(reply_token, "✅ 已更新 YouTube 內文。")
                        continue

                    else:  # S_SCHEDULE_EDIT_TAGS
                        tags = _parse_tags_input(message_text)
                        if not tags:
                            reply_text(reply_token, "❗ 請至少提供一個關鍵字（用逗號或換行分隔），再輸入一次：")
                            continue
                        joined_len = sum(len(t) for t in tags) + max(0, len(tags) - 1)
                        if joined_len > 450:
                            kept = []
                            total = 0
                            for t in tags:
                                add = len(t) + (1 if kept else 0)
                                if total + add > 450:
                                    break
                                kept.append(t)
                                total += add
                            tags = kept
                            reply_text(reply_token, "ℹ️ 關鍵字總長度過長，已自動截斷至可接受範圍。")
                        update_video_metadata(video_id=vid, tags=tags)
                        reset_state(line_user_id)
                        reply_text(reply_token, "✅ 已更新 YouTube 關鍵字。")
                        continue

                except Exception as e:
                    reply_text(reply_token, f"❌ 更新失敗：{e}\n請再輸入一次。")
                    continue

            # 2) 上架時間 → 直接更新 YouTube publishAt
            if stage == S_SCHEDULE_EDIT_TIME:
                from api.services.youtube_service import update_publish_time
                dt_utc = parse_time_ymdhm(message_text)
                if not dt_utc:
                    reply_text(reply_token, "時間格式錯誤，範例：2025-08-23 14:00（台北時間）。請再試一次：")
                    continue
                try:
                    update_publish_time(video_id=vid, new_dt_utc=dt_utc)
                    reset_state(line_user_id)
                    reply_text(reply_token, "✅ 已更新 YouTube 上架時間。")
                except Exception as e:
                    reply_text(reply_token, f"❌ 更新失敗：{e}\n請再輸入一次。")
                continue

        # 預設：看不懂就回主選單
        reply_text(reply_token, "看不懂這個指令。\n\n" + MENU_TEXT)

    return {"ok": True}


@router.get("/webhook/line")
async def line_webhook_get():
    return {"ok": True}
