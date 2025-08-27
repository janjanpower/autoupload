from datetime import datetime, timezone
from ..schemas.state_constants import TZ

def parse_time_ymdhm(s: str):
    try:
        dt_naive = datetime.strptime(s.strip(), "%Y-%m-%d %H:%M")
        dt_local = TZ.localize(dt_naive)
        return dt_local.astimezone(timezone.utc)
    except Exception:
        return None

def format_tw_with_weekday(dt_utc: datetime) -> str:
    local_dt = dt_utc.astimezone(TZ)
    w = "一二三四五六日"[local_dt.weekday()]
    return f"{local_dt:%Y-%m-%d} ({w}) {local_dt:%H:%M}"
