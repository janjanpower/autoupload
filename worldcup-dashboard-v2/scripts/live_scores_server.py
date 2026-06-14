#!/usr/bin/env python3
"""Local live score proxy for worldcup-dashboard-v2.

The browser must not receive FOOTBALL_DATA_TOKEN. Run this server locally with
FOOTBALL_DATA_TOKEN set, then the dashboard can poll /api/live-scores.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8788
ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data" / "matches.json"
API_URL = os.getenv("FOOTBALL_DATA_URL", "https://api.football-data.org/v4/competitions/WC/matches")
TEAMS_API_URL = os.getenv("FOOTBALL_DATA_TEAMS_URL", "https://api.football-data.org/v4/competitions/WC/teams")


def load_current() -> dict:
    if DATA_PATH.exists():
        return json.loads(DATA_PATH.read_text(encoding="utf-8"))
    return {"matches": []}


def fetch_json(url: str, token: str) -> dict:
    req = urllib.request.Request(url, headers={"X-Auth-Token": token})
    with urllib.request.urlopen(req, timeout=20) as res:
        return json.loads(res.read().decode("utf-8"))


def normalize_matches(raw: dict, current: dict) -> dict:
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
        out.append({
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
        })
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": "football-data.org live proxy",
        "matches": out or current.get("matches", []),
        "teams": current.get("teams", []),
    }


def normalize_teams(raw: dict, current: dict) -> list[dict]:
    teams = []
    for team in raw.get("teams", []):
        area = team.get("area") or {}
        teams.append({
            "id": team.get("id"),
            "name": team.get("name") or "",
            "short_name": team.get("shortName") or "",
            "tla": team.get("tla") or "",
            "crest": team.get("crest") or "",
            "area": area.get("name") or "",
            "area_code": area.get("code") or "",
            "flag": area.get("flag") or "",
        })
    return teams or current.get("teams", [])


def live_payload() -> dict:
    token = os.getenv("FOOTBALL_DATA_TOKEN", "").strip()
    if not token:
        raise RuntimeError("FOOTBALL_DATA_TOKEN is not set")
    current = load_current()
    payload = normalize_matches(fetch_json(API_URL, token), current)
    try:
        payload["teams"] = normalize_teams(fetch_json(TEAMS_API_URL, token), current)
    except Exception as exc:
        payload["teams"] = current.get("teams", [])
        payload["teams_error"] = str(exc)
    return payload


class Handler(BaseHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.end_headers()

    def do_GET(self) -> None:
        if self.path.split("?", 1)[0] != "/api/live-scores":
            self.send_json({"ok": False, "error": "not found"}, status=404)
            return
        try:
            payload = live_payload()
            payload["ok"] = True
            self.send_json(payload)
        except Exception as exc:
            self.send_json({"ok": False, "error": str(exc)}, status=500)

    def send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"{self.address_string()} - {fmt % args}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Live score proxy: http://{args.host}:{args.port}/api/live-scores")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
