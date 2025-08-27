# api/utils/meta_parser.py
from __future__ import annotations
import json
import re
from typing import Dict, List

_TITLE = ("標題", "title")
_DESC  = ("內文", "說明", "內容", "description", "desc")
_TAGS  = ("關鍵字", "標籤", "tags", "tag")

_LABEL_RE = re.compile(r"^\s*([^\s：:]+)\s*[：:]\s*(.*)$")  # e.g. 標題：xxx / title: xxx

def _split_tags(s: str) -> List[str]:
    # 支援：逗號（中/英）、空白、換行
    parts = re.split(r"[,\uFF0C\s]+", s.strip())
    out, seen = [], set()
    for p in parts:
        if not p:
            continue
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out

def parse_meta_text(text: str) -> Dict:
    """
    解析「友善三段式」或 JSON 文字，回傳 {title, description, tags}
    規則：
    - 先嘗試 JSON（相容舊資料）
    - 再嘗試「標題/內文/關鍵字」標籤格式
    - 如果完全沒標籤：第一行當標題，其餘當內文，tags=[]
    """
    if not isinstance(text, str):
        return {"title": "", "description": "", "tags": []}

    s = text.strip()
    if not s:
        return {"title": "", "description": "", "tags": []}

    # 1) JSON 相容
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            title = str(obj.get("title", "") or "")
            desc  = str(obj.get("description", "") or "")
            tags  = obj.get("tags", [])
            if not isinstance(tags, list):
                tags = _split_tags(str(tags))
            return {"title": title, "description": desc, "tags": tags}
    except Exception:
        pass

    # 2) 標籤格式
    lines = s.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    # 掃描每一行，抓出標籤與內容
    blocks = []  # [(label, first_line_content, start_idx)]
    for i, line in enumerate(lines):
        m = _LABEL_RE.match(line)
        if not m:
            continue
        label = m.group(1).strip().lower()
        content = m.group(2)  # 同行內容
        blocks.append((label, content, i))

    def _find_block(names: tuple[str, ...]):
        for b in blocks:
            if b[0] in [n.lower() for n in names]:
                return b
        return None

    title_b = _find_block(_TITLE)
    desc_b  = _find_block(_DESC)
    tags_b  = _find_block(_TAGS)

    title, desc, tags = "", "", []

    # 標題：單行
    if title_b:
        title = title_b[1].strip()

    # 內文：多行，直到下一個標籤或結尾
    if desc_b:
        start = desc_b[2]
        # 找下一個標籤的起點（大於 start 的最小 index）
        next_starts = sorted([b[2] for b in blocks if b[2] > start])
        end = next_starts[0] if next_starts else len(lines)
        # 同行內容 + 之後的行
        desc_lines = []
        first_line = lines[start]
        m0 = _LABEL_RE.match(first_line)
        desc_lines.append(m0.group(2) if m0 else "")
        if end - start > 1:
            desc_lines.extend(lines[start + 1:end])
        desc = "\n".join(desc_lines).strip()

    # 關鍵字：單/多行都可以，最後合併切詞
    if tags_b:
        start = tags_b[2]
        next_starts = sorted([b[2] for b in blocks if b[2] > start])
        end = next_starts[0] if next_starts else len(lines)
        tag_lines = []
        first_line = lines[start]
        m0 = _LABEL_RE.match(first_line)
        tag_lines.append(m0.group(2) if m0 else "")
        if end - start > 1:
            tag_lines.extend(lines[start + 1:end])
        tags = _split_tags("\n".join(tag_lines))

    # 3) 若仍然沒有標籤，fallback：第一行為標題，其餘為內文
    if not (title or desc or tags):
        first, *rest = lines
        title = first.strip()
        desc = "\n".join(rest).strip()
        tags = []

    return {"title": title, "description": desc, "tags": tags}
