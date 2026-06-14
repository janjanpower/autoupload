#!/usr/bin/env python3
"""Build team-insights.json for reverse score simulation.

Sources are optional and cached into one static JSON file:
- API-Football, when API_FOOTBALL_KEY is configured.
- Kaggle international results CSV, when KAGGLE_RESULTS_CSV points to a local CSV.
- OpenFootball World Cup datasets, fetched from public GitHub raw URLs by default.
"""
from __future__ import annotations

import csv
import json
import os
import pathlib
import statistics
import sys
import urllib.parse
import urllib.request
from collections import defaultdict, deque
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data" / "team-insights.json"
MATCHES_PATH = ROOT / "data" / "matches.json"

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "").strip()
API_FOOTBALL_HOST = os.getenv("API_FOOTBALL_HOST", "v3.football.api-sports.io")
API_FOOTBALL_BASE = os.getenv("API_FOOTBALL_BASE", f"https://{API_FOOTBALL_HOST}")
API_FOOTBALL_COMPETITION = os.getenv("API_FOOTBALL_COMPETITION", "")
KAGGLE_RESULTS_CSV = os.getenv("KAGGLE_RESULTS_CSV", str(ROOT / "data" / "external" / "international_results.csv"))
OPENFOOTBALL_URLS = [
    u.strip()
    for u in os.getenv(
        "OPENFOOTBALL_URLS",
        "https://raw.githubusercontent.com/openfootball/worldcup/master/2022--qatar/cup.txt",
    ).split(",")
    if u.strip()
]

ALIASES = {
    "Mexico": "墨西哥",
    "South Africa": "南非",
    "South Korea": "大韓民國",
    "Korea Republic": "大韓民國",
    "Czechia": "捷克",
    "Canada": "加拿大",
    "Bosnia-Herzegovina": "波士尼亞與赫塞哥維納",
    "Bosnia and Herzegovina": "波士尼亞與赫塞哥維納",
    "Qatar": "卡達",
    "Switzerland": "瑞士",
    "Brazil": "巴西",
    "Morocco": "摩洛哥",
    "Haiti": "海地",
    "Scotland": "蘇格蘭",
    "United States": "美國",
    "USA": "美國",
    "Paraguay": "巴拉圭",
    "Australia": "澳洲",
    "Turkey": "土耳其",
    "Türkiye": "土耳其",
    "Germany": "德國",
    "Curaçao": "庫拉索",
    "Curacao": "庫拉索",
    "Ivory Coast": "象牙海岸",
    "Côte d’Ivoire": "象牙海岸",
    "Côte d'Ivoire": "象牙海岸",
    "Ecuador": "厄瓜多",
    "Netherlands": "荷蘭",
    "Japan": "日本",
    "Sweden": "瑞典",
    "Tunisia": "突尼西亞",
    "Belgium": "比利時",
    "Egypt": "埃及",
    "Iran": "伊朗",
    "New Zealand": "紐西蘭",
    "Spain": "西班牙",
    "Cape Verde Islands": "佛得角",
    "Cape Verde": "佛得角",
    "Saudi Arabia": "沙烏地阿拉伯",
    "Uruguay": "烏拉圭",
    "France": "法國",
    "Senegal": "塞內加爾",
    "Iraq": "伊拉克",
    "Norway": "挪威",
    "Argentina": "阿根廷",
    "Algeria": "阿爾及利亞",
    "Austria": "奧地利",
    "Jordan": "約旦",
    "Portugal": "葡萄牙",
    "Congo DR": "剛果民主共和國",
    "DR Congo": "剛果民主共和國",
    "Uzbekistan": "烏茲別克",
    "Colombia": "哥倫比亞",
    "England": "英格蘭",
    "Croatia": "克羅埃西亞",
    "Ghana": "迦納",
    "Panama": "巴拿馬",
}


def team_name(name: str) -> str:
    return ALIASES.get((name or "").strip(), (name or "").strip())


def load_matches() -> list[dict]:
    if not MATCHES_PATH.exists():
        return []
    return json.loads(MATCHES_PATH.read_text(encoding="utf-8")).get("matches", [])


def empty_team() -> dict:
    return {
        "matches": 0,
        "goals_for": 0,
        "goals_against": 0,
        "recent_goals_for": [],
        "recent_goals_against": [],
        "sources": [],
    }


def add_match(teams: dict, matchups: dict, home: str, away: str, hg: int, ag: int, source: str) -> None:
    home, away = team_name(home), team_name(away)
    if not home or not away:
        return
    for team, gf, ga in ((home, hg, ag), (away, ag, hg)):
        row = teams[team]
        row["matches"] += 1
        row["goals_for"] += gf
        row["goals_against"] += ga
        row["recent_goals_for"].append(gf)
        row["recent_goals_against"].append(ga)
        if source not in row["sources"]:
            row["sources"].append(source)
    key = "|".join(sorted([home, away]))
    matchup = matchups[key]
    matchup["matches"] += 1
    matchup["total_goals"] += hg + ag
    matchup["scores"].append({"home": home, "away": away, "hg": hg, "ag": ag, "source": source})


def add_seed_matches(teams: dict, matchups: dict) -> None:
    for match in load_matches():
        hg, ag = match.get("home_score"), match.get("away_score")
        if hg is None or ag is None:
            continue
        add_match(teams, matchups, match.get("home", ""), match.get("away", ""), int(hg), int(ag), "matches.json")


def add_kaggle_csv(teams: dict, matchups: dict, path: str) -> bool:
    csv_path = pathlib.Path(path)
    if not csv_path.exists():
        return False
    rows = list(csv.DictReader(csv_path.open("r", encoding="utf-8-sig", newline="")))
    rows = rows[-3000:]
    for row in rows:
        try:
            add_match(teams, matchups, row.get("home_team", ""), row.get("away_team", ""), int(row.get("home_score", 0)), int(row.get("away_score", 0)), "kaggle")
        except ValueError:
            continue
    return True


def fetch_json(url: str, headers: dict[str, str] | None = None) -> dict:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=25) as res:
        return json.loads(res.read().decode("utf-8"))


def add_api_football(teams: dict, matchups: dict) -> bool:
    if not API_FOOTBALL_KEY:
        return False
    headers = {"x-apisports-key": API_FOOTBALL_KEY}
    params = {}
    if API_FOOTBALL_COMPETITION:
        params["league"] = API_FOOTBALL_COMPETITION
    params["season"] = os.getenv("API_FOOTBALL_SEASON", "2026")
    url = f"{API_FOOTBALL_BASE}/fixtures?{urllib.parse.urlencode(params)}"
    raw = fetch_json(url, headers)
    for item in raw.get("response", []):
        goals = item.get("goals") or {}
        if goals.get("home") is None or goals.get("away") is None:
            continue
        home = ((item.get("teams") or {}).get("home") or {}).get("name") or ""
        away = ((item.get("teams") or {}).get("away") or {}).get("name") or ""
        add_match(teams, matchups, home, away, int(goals["home"]), int(goals["away"]), "api-football")
    return True


def add_openfootball(teams: dict, matchups: dict) -> bool:
    # OpenFootball text formats vary. Keep this lightweight and tolerant:
    # lines like "Brazil 2-0 Serbia" are parsed when present.
    used = False
    for url in OPENFOOTBALL_URLS:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "worldcup-dashboard"})
            with urllib.request.urlopen(req, timeout=25) as res:
                text = res.read().decode("utf-8", errors="ignore")
        except Exception as exc:
            print(f"OpenFootball fetch failed: {url}: {exc}", file=sys.stderr)
            continue
        for line in text.splitlines():
            clean = " ".join(line.strip().split())
            if not clean or "-" not in clean:
                continue
            parts = clean.rsplit(" ", 2)
            if len(parts) < 3 or "-" not in parts[1]:
                continue
            left, score, right = parts
            try:
                hg, ag = [int(x) for x in score.split("-", 1)]
            except ValueError:
                continue
            add_match(teams, matchups, left, right, hg, ag, "openfootball")
            used = True
    return used


def finalize(teams: dict, matchups: dict, sources: list[str]) -> dict:
    out_teams = {}
    for team, row in teams.items():
        recent_for = row["recent_goals_for"][-12:]
        recent_against = row["recent_goals_against"][-12:]
        matches = row["matches"] or 1
        gf_avg = row["goals_for"] / matches
        ga_avg = row["goals_against"] / matches
        recent_gf = statistics.mean(recent_for) if recent_for else gf_avg
        recent_ga = statistics.mean(recent_against) if recent_against else ga_avg
        out_teams[team] = {
            "matches": row["matches"],
            "goals_for_per_match": round(gf_avg, 3),
            "goals_against_per_match": round(ga_avg, 3),
            "recent_goals_for_per_match": round(recent_gf, 3),
            "recent_goals_against_per_match": round(recent_ga, 3),
            "tempo": round((recent_gf + recent_ga) / 2, 3),
            "sources": sorted(row["sources"]),
        }
    out_matchups = {}
    for key, row in matchups.items():
        recent = list(row["scores"])[-6:]
        out_matchups[key] = {
            "matches": row["matches"],
            "average_total_goals": round(row["total_goals"] / max(1, row["matches"]), 3),
            "recent": recent,
        }
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": ", ".join(sources) if sources else "seed only",
        "teams": out_teams,
        "matchups": out_matchups,
    }


def main() -> int:
    teams: dict = defaultdict(empty_team)
    matchups: dict = defaultdict(lambda: {"matches": 0, "total_goals": 0, "scores": deque(maxlen=12)})
    sources = []
    add_seed_matches(teams, matchups)
    sources.append("matches.json")
    if add_kaggle_csv(teams, matchups, KAGGLE_RESULTS_CSV):
        sources.append("kaggle")
    try:
        if add_openfootball(teams, matchups):
            sources.append("openfootball")
    except Exception as exc:
        print(f"OpenFootball update failed: {exc}", file=sys.stderr)
    try:
        if add_api_football(teams, matchups):
            sources.append("api-football")
    except Exception as exc:
        print(f"API-Football update failed: {exc}", file=sys.stderr)
    payload = finalize(teams, matchups, sources)
    DATA_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Updated team insights: {len(payload['teams'])} teams, {len(payload['matchups'])} matchups.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
