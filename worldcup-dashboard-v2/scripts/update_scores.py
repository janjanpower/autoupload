#!/usr/bin/env python3
"""Update worldcup-dashboard-v2/data/matches.json.

Default source: football-data.org, using GitHub Secret FOOTBALL_DATA_TOKEN.
If no token is configured, the script keeps the existing seed file and only refreshes metadata,
so the GitHub Action will not fail before the API is connected.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys
import urllib.request
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data" / "matches.json"
TOKEN = os.getenv("FOOTBALL_DATA_TOKEN", "").strip()
API_URL = os.getenv("FOOTBALL_DATA_URL", "https://api.football-data.org/v4/competitions/WC/matches")


def load_current() -> dict:
    if DATA_PATH.exists():
        return json.loads(DATA_PATH.read_text(encoding="utf-8"))
    return {"matches": []}


def save(payload: dict) -> None:
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def normalize_football_data(raw: dict, current: dict) -> dict:
    known_by_pair = {(m.get("home"), m.get("away")): m for m in current.get("matches", [])}
    known_by_id = {m.get("id"): m for m in current.get("matches", [])}
    out = []
    for idx, match in enumerate(raw.get("matches", []), start=1):
        home = (match.get("homeTeam") or {}).get("name") or "TBD"
        away = (match.get("awayTeam") or {}).get("name") or "TBD"
        score = (match.get("score") or {}).get("fullTime") or {}
        status = match.get("status") or "SCHEDULED"
        utc_date = match.get("utcDate") or ""
        date = utc_date[:10] if utc_date else ""
        seed = known_by_pair.get((home, away), known_by_id.get(idx, {}))
        out.append(
            {
                "id": seed.get("id", idx),
                "date": seed.get("date", date),
                "group": seed.get("group", ""),
                "home": seed.get("home", home),
                "away": seed.get("away", away),
                "home_score": score.get("home") if status in {"FINISHED", "IN_PLAY", "PAUSED"} else None,
                "away_score": score.get("away") if status in {"FINISHED", "IN_PLAY", "PAUSED"} else None,
                "status": status,
            }
        )
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": "football-data.org",
        "matches": out or current.get("matches", []),
    }


def fetch_from_football_data() -> dict:
    req = urllib.request.Request(API_URL, headers={"X-Auth-Token": TOKEN})
    with urllib.request.urlopen(req, timeout=30) as res:
        return json.loads(res.read().decode("utf-8"))


def main() -> int:
    current = load_current()
    if not TOKEN:
        current["updated_at"] = datetime.now(timezone.utc).isoformat()
        current["source"] = "manual seed; set FOOTBALL_DATA_TOKEN to enable scheduled API updates"
        save(current)
        print("FOOTBALL_DATA_TOKEN not set; kept existing v2 data.")
        return 0

    try:
        raw = fetch_from_football_data()
        payload = normalize_football_data(raw, current)
        save(payload)
        print(f"Updated {len(payload.get('matches', []))} v2 matches.")
        return 0
    except Exception as exc:
        print(f"Failed to update v2 scores: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
