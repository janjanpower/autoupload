import hmac, hashlib, base64, requests
from fastapi import HTTPException
from ..config import settings

def verify_signature(body: bytes, signature: str):
    if settings.LINE_SKIP_SIG:
        return
    if not settings.LINE_SECRET:
        raise HTTPException(status_code=500, detail="LINE_CHANNEL_SECRET 未設定")
    mac = hmac.new(settings.LINE_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expect = base64.b64encode(mac).decode("utf-8")
    if not hmac.compare_digest(signature or "", expect):
        raise HTTPException(status_code=400, detail="Invalid X-Line-Signature")

def reply_text(reply_token: str, text: str):
    if not settings.LINE_TOKEN:
        raise HTTPException(status_code=500, detail="LINE_CHANNEL_TOKEN 未設定")
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {"Authorization": f"Bearer {settings.LINE_TOKEN}", "Content-Type": "application/json"}
    payload = {"replyToken": reply_token, "messages": [{"type": "text", "text": text[:5000]}]}
    r = requests.post(url, headers=headers, json=payload, timeout=15)
    if r.status_code >= 300:
        raise HTTPException(status_code=400, detail=f"LINE reply error: {r.text}")

def push_text(user_id: str, text: str):
    if not settings.LINE_TOKEN:
        return
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {settings.LINE_TOKEN}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": text[:5000]}]}
    requests.post(url, headers=headers, json=payload, timeout=15)
