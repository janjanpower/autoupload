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
import unicodedata
import urllib.request
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data" / "matches.json"
VERSION_PATH = ROOT / "data" / "version.json"
TOKEN = os.getenv("FOOTBALL_DATA_TOKEN", "").strip()
API_URL = os.getenv("FOOTBALL_DATA_URL", "https://api.football-data.org/v4/competitions/WC/matches")
TEAMS_API_URL = os.getenv("FOOTBALL_DATA_TEAMS_URL", "https://api.football-data.org/v4/competitions/WC/teams")
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "").strip()
API_FOOTBALL_LIVE_URL = os.getenv("API_FOOTBALL_LIVE_URL", "https://v3.football.api-sports.io/fixtures?live=all")
LIVE_STATUSES = {"1H", "HT", "2H", "ET", "BT", "P", "LIVE"}
API_FOOTBALL_STATUS_MAP = {
    "1H": "IN_PLAY",
    "HT": "PAUSED",
    "2H": "IN_PLAY",
    "ET": "IN_PLAY",
    "BT": "PAUSED",
    "P": "PAUSED",
    "LIVE": "IN_PLAY",
    "FT": "FINISHED",
    "AET": "FINISHED",
    "PEN": "FINISHED",
    "NS": "SCHEDULED",
    "TBD": "SCHEDULED",
    "PST": "POSTPONED",
    "CANC": "CANCELLED",
    "ABD": "SUSPENDED",
}


def load_current() -> dict:
    if DATA_PATH.exists():
        return json.loads(DATA_PATH.read_text(encoding="utf-8"))
    return {"matches": []}


def save(payload: dict) -> None:
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    version = {
        "version": payload.get("updated_at") or datetime.now(timezone.utc).isoformat(),
        "source": payload.get("source", "unknown"),
    }
    VERSION_PATH.write_text(json.dumps(version, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def norm_team(name: str | None) -> str:
    text = unicodedata.normalize("NFKD", name or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return "".join(ch.lower() for ch in text if ch.isalnum())


def score_value(value: object) -> int | None:
    return value if isinstance(value, int) else None


def normalize_football_data(raw: dict, current: dict) -> dict:
    known_by_pair = {
        (m.get("home"), m.get("away")): m
        for m in current.get("matches", [])
        if m.get("home") != "TBD" and m.get("away") != "TBD"
    }
    known_by_id = {m.get("id"): m for m in current.get("matches", [])}
    out = []
    for idx, match in enumerate(raw.get("matches", []), start=1):
        home = (match.get("homeTeam") or {}).get("name") or "TBD"
        away = (match.get("awayTeam") or {}).get("name") or "TBD"
        score_obj = match.get("score") or {}
        score = score_obj.get("fullTime") or {}
        half_score = score_obj.get("halfTime") or {}
        status = match.get("status") or "SCHEDULED"
        minute = match.get("minute") or match.get("elapsed") or match.get("matchMinute")
        added_time = match.get("addedTime") or match.get("extra") or match.get("injuryTime")
        utc_date = match.get("utcDate") or ""
        date = utc_date[:10] if utc_date else ""
        seed = known_by_id.get(idx, {}) if home == "TBD" or away == "TBD" else known_by_pair.get((home, away), known_by_id.get(idx, {}))
        out.append(
            {
                "id": seed.get("id", idx),
                "date": seed.get("date", date),
                "utc_date": utc_date or seed.get("utc_date", ""),
                "group": seed.get("group", ""),
                "home": seed.get("home", home),
                "away": seed.get("away", away),
                "home_score": score.get("home") if status in {"FINISHED", "IN_PLAY", "PAUSED"} else None,
                "away_score": score.get("away") if status in {"FINISHED", "IN_PLAY", "PAUSED"} else None,
                "home_half_score": half_score.get("home") if status in {"FINISHED", "IN_PLAY", "PAUSED"} else None,
                "away_half_score": half_score.get("away") if status in {"FINISHED", "IN_PLAY", "PAUSED"} else None,
                "minute": minute,
                "added_time": added_time,
                "status": status,
            }
        )
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": "football-data.org",
        "matches": out or current.get("matches", []),
        "teams": current.get("teams", []),
    }


def fetch_from_api_football(url: str) -> dict:
    req = urllib.request.Request(url, headers={"x-apisports-key": API_FOOTBALL_KEY})
    with urllib.request.urlopen(req, timeout=30) as res:
        return json.loads(res.read().decode("utf-8"))


def api_football_match_patch(fixture: dict) -> dict | None:
    teams = fixture.get("teams") or {}
    home = (teams.get("home") or {}).get("name")
    away = (teams.get("away") or {}).get("name")
    if not home or not away:
        return None
    status = ((fixture.get("fixture") or {}).get("status") or {})
    short = status.get("short") or ""
    goals = fixture.get("goals") or {}
    score = fixture.get("score") or {}
    halftime = score.get("halftime") or {}
    return {
        "home": home,
        "away": away,
        "status": API_FOOTBALL_STATUS_MAP.get(short, short or "IN_PLAY"),
        "home_score": score_value(goals.get("home")),
        "away_score": score_value(goals.get("away")),
        "home_half_score": score_value(halftime.get("home")),
        "away_half_score": score_value(halftime.get("away")),
        "minute": status.get("elapsed"),
        "added_time": status.get("extra"),
        "api_football_status": short,
    }


def merge_api_football_live(payload: dict) -> dict:
    if not API_FOOTBALL_KEY:
        return payload
    live_raw = fetch_from_api_football(API_FOOTBALL_LIVE_URL)
    patches = [
        patch
        for patch in (api_football_match_patch(row) for row in live_raw.get("response", []))
        if patch and patch.get("api_football_status") in LIVE_STATUSES
    ]
    if not patches:
        payload["source"] = f"{payload.get('source', 'seed')} + API-Football live checked"
        return payload
    by_pair = {(norm_team(m.get("home")), norm_team(m.get("away"))): m for m in payload.get("matches", [])}
    applied = 0
    for patch in patches:
        match = by_pair.get((norm_team(patch["home"]), norm_team(patch["away"])))
        if not match:
            match = by_pair.get((norm_team(patch["away"]), norm_team(patch["home"])))
            if not match:
                continue
            patch = {
                **patch,
                "home_score": patch.get("away_score"),
                "away_score": patch.get("home_score"),
                "home_half_score": patch.get("away_half_score"),
                "away_half_score": patch.get("home_half_score"),
            }
        for key in ("home_score", "away_score", "home_half_score", "away_half_score", "minute", "added_time"):
            if patch.get(key) is not None:
                match[key] = patch[key]
        match["status"] = patch.get("status") or match.get("status")
        match["api_football_status"] = patch.get("api_football_status")
        applied += 1
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    payload["source"] = f"{payload.get('source', 'seed')} + API-Football live"
    payload["api_football_live_matches"] = applied
    return payload


def normalize_football_data_teams(raw: dict, current: dict) -> list[dict]:
    teams = []
    for team in raw.get("teams", []):
        area = team.get("area") or {}
        teams.append(
            {
                "id": team.get("id"),
                "name": team.get("name") or "",
                "short_name": team.get("shortName") or "",
                "tla": team.get("tla") or "",
                "crest": team.get("crest") or "",
                "area": area.get("name") or "",
                "area_code": area.get("code") or "",
                "flag": area.get("flag") or "",
            }
        )
    return teams or current.get("teams", [])


def fetch_from_football_data(url: str) -> dict:
    req = urllib.request.Request(url, headers={"X-Auth-Token": TOKEN})
    with urllib.request.urlopen(req, timeout=30) as res:
        return json.loads(res.read().decode("utf-8"))


def main() -> int:
    current = load_current()
    if not TOKEN:
        current["updated_at"] = datetime.now(timezone.utc).isoformat()
        current["source"] = "manual seed; set FOOTBALL_DATA_TOKEN to enable scheduled API updates"
        try:
            current = merge_api_football_live(current)
        except Exception as exc:
            print(f"Failed to update API-Football live data: {exc}", file=sys.stderr)
        save(current)
        print("FOOTBALL_DATA_TOKEN not set; kept existing v2 data.")
        return 0

    try:
        raw = fetch_from_football_data(API_URL)
        payload = normalize_football_data(raw, current)
        try:
            payload = merge_api_football_live(payload)
        except Exception as exc:
            print(f"Failed to update API-Football live data: {exc}", file=sys.stderr)
        try:
            teams_raw = fetch_from_football_data(TEAMS_API_URL)
            payload["teams"] = normalize_football_data_teams(teams_raw, current)
        except Exception as exc:
            payload["teams"] = current.get("teams", [])
            print(f"Failed to update v2 teams: {exc}", file=sys.stderr)
        save(payload)
        print(f"Updated {len(payload.get('matches', []))} v2 matches.")
        print(f"Updated {len(payload.get('teams', []))} v2 teams.")
        return 0
    except Exception as exc:
        print(f"Failed to update v2 scores: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
