from fastapi import APIRouter

router = APIRouter()

@router.post("/n8n/compose")
async def n8n_compose(payload: dict):
    return {"ok": True, "echo": payload}
