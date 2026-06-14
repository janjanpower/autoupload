#!/usr/bin/env python3
"""Local matchup research server for worldcup-dashboard-v2.

Run this on the creator's machine, then open the dashboard from localhost.
The static page calls this server before sending the strategy prompt to Ollama.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import pathlib
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8777
DEFAULT_CACHE = pathlib.Path(os.getenv("LOCALAPPDATA", pathlib.Path.home())) / "worldcup-dashboard-v2" / "research-cache.json"
SEARCH_URL = "https://duckduckgo.com/html/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) WorldCupDashboardV2/1.0"


@dataclass
class SearchResult:
    title: str
    url: str
    snippet: str


class DuckDuckGoParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.results: list[SearchResult] = []
        self._capture_title = False
        self._capture_snippet = False
        self._title_parts: list[str] = []
        self._snippet_parts: list[str] = []
        self._current_url = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = {k: v or "" for k, v in attrs}
        classes = set(attr.get("class", "").split())
        if tag == "a" and "result__a" in classes:
            self._capture_title = True
            self._title_parts = []
            self._snippet_parts = []
            self._current_url = self._clean_url(attr.get("href", ""))
        elif "result__snippet" in classes:
            self._capture_snippet = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._capture_title:
            self._capture_title = False
        if self._capture_snippet:
            title = self._clean_text(" ".join(self._title_parts))
            snippet = self._clean_text(" ".join(self._snippet_parts))
            if title and self._current_url:
                self.results.append(SearchResult(title=title, url=self._current_url, snippet=snippet))
            self._capture_snippet = False

    def handle_data(self, data: str) -> None:
        if self._capture_title:
            self._title_parts.append(data)
        if self._capture_snippet:
            self._snippet_parts.append(data)

    @staticmethod
    def _clean_text(value: str) -> str:
        return html.unescape(" ".join(value.split()))

    @staticmethod
    def _clean_url(value: str) -> str:
        if value.startswith("//duckduckgo.com/l/?"):
            parsed = urllib.parse.urlparse("https:" + value)
            target = urllib.parse.parse_qs(parsed.query).get("uddg", [""])[0]
            return urllib.parse.unquote(target)
        return value


def load_cache(path: pathlib.Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(path: pathlib.Path, cache: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def cache_key(query: str) -> str:
    return hashlib.sha256(query.encode("utf-8")).hexdigest()


def search(query: str, cache: dict, cache_path: pathlib.Path, ttl_seconds: int, limit: int) -> list[dict]:
    now = int(time.time())
    key = cache_key(query)
    item = cache.get(key)
    if item and now - int(item.get("created_at", 0)) < ttl_seconds:
        return item.get("results", [])[:limit]

    params = urllib.parse.urlencode({"q": query, "kl": "wt-wt"})
    req = urllib.request.Request(f"{SEARCH_URL}?{params}", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=12) as res:
        body = res.read().decode("utf-8", errors="replace")
    parser = DuckDuckGoParser()
    parser.feed(body)
    results = [
        {"title": r.title, "url": r.url, "snippet": r.snippet}
        for r in parser.results
        if r.title and r.url
    ][:limit]
    cache[key] = {"created_at": now, "query": query, "results": results}
    save_cache(cache_path, cache)
    return results


def build_queries(home: str, away: str, selected: str, date: str) -> list[tuple[str, str]]:
    base = f"{home} vs {away}"
    return [
        ("matchup", f"{base} World Cup 2026 preview tactics"),
        ("availability", f"{base} injuries suspensions yellow cards red cards football"),
        ("selected_team", f"{selected} football injuries suspension squad news"),
        ("home_team", f"{home} football lineup injuries suspension cards"),
        ("away_team", f"{away} football lineup injuries suspension cards"),
        ("chinese", f"{home} {away} 世界盃 傷兵 黃牌 紅牌 戰術"),
        ("date", f"{base} {date} match news"),
    ]


class ResearchHandler(BaseHTTPRequestHandler):
    cache_path: pathlib.Path = DEFAULT_CACHE
    ttl_seconds: int = 1800
    per_query_limit: int = 3

    def do_OPTIONS(self) -> None:
        self._send_empty(204)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/health":
            self._send_json({"ok": True})
            return
        if parsed.path != "/api/research":
            self._send_json({"error": "not found"}, 404)
            return

        qs = urllib.parse.parse_qs(parsed.query)
        home = qs.get("home", [""])[0].strip()
        away = qs.get("away", [""])[0].strip()
        selected = qs.get("selected", [home])[0].strip() or home
        date = qs.get("date", [""])[0].strip()
        if not home or not away:
            self._send_json({"error": "home and away are required"}, 400)
            return

        cache = load_cache(self.cache_path)
        sections = []
        warnings = []
        for category, query in build_queries(home, away, selected, date):
            try:
                sections.append({"category": category, "query": query, "results": search(query, cache, self.cache_path, self.ttl_seconds, self.per_query_limit)})
            except Exception as exc:
                warnings.append(f"{category}: {exc}")

        self._send_json(
            {
                "ok": True,
                "source": "local_research_server",
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "match": {"home": home, "away": away, "selected": selected, "date": date},
                "sections": sections,
                "warnings": warnings,
                "note": "Search results are raw public web snippets. Verify important injuries, suspensions, and cards against official sources before relying on them.",
            }
        )

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[research] {self.address_string()} - {fmt % args}")

    def _send_empty(self, status: int) -> None:
        self.send_response(status)
        self._cors_headers()
        self.end_headers()

    def _send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self._cors_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _cors_headers(self) -> None:
        origin = self.headers.get("Origin", "")
        if origin.startswith(("http://localhost:", "http://127.0.0.1:", "http://[::1]:")):
            self.send_header("Access-Control-Allow-Origin", origin)
        elif not origin:
            self.send_header("Access-Control-Allow-Origin", "http://localhost:8765")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local research server for the World Cup dashboard.")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--cache", default=str(DEFAULT_CACHE))
    parser.add_argument("--ttl", type=int, default=1800, help="Cache TTL in seconds.")
    parser.add_argument("--limit", type=int, default=3, help="Results per query.")
    args = parser.parse_args()

    ResearchHandler.cache_path = pathlib.Path(args.cache)
    ResearchHandler.ttl_seconds = args.ttl
    ResearchHandler.per_query_limit = args.limit
    server = ThreadingHTTPServer((args.host, args.port), ResearchHandler)
    print(f"Local research server: http://{args.host}:{args.port}/api/research")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
