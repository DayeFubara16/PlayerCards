
"""
Player_Action_Scraper.py
────────────────────────

On-demand, cache-backed per-player/per-event action scraper for Sofascore.

This is intentionally NOT a full-dataset crawl. It is designed for:
- one player card
- one player similarity investigation
- selected high-value scouting cases

It tries a small set of player-event endpoints and saves whatever Sofascore returns.
Coverage can vary by league/match, so missing endpoints are recorded, not fatal.

Experimental endpoints this script can probe:
  /event/{event_id}/player/{player_id}/statistics
  /event/{event_id}/player/{player_id}/rating-breakdown
  /event/{event_id}/player/{player_id}/incidents
  /event/{event_id}/player/{player_id}/touches

Usage:
  python Player_Action_Scraper.py --player-id 994546 --season 2025-26 --match-log data/raw/player_match_logs.csv --out-dir cards/actions --cache-dir cache/action_cache --delay 1.2 --format both
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import time
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from curl_cffi import requests as cf_requests


API_BASE = "https://api.sofascore.com/api/v1"
DEFAULT_MATCH_LOG = "data/raw/player_match_logs.csv"
DEFAULT_OUT_DIR = "cards/actions"
DEFAULT_CACHE_DIR = "cache/action_cache"
REQUEST_DELAY = 1.2

DEFAULT_ENDPOINTS = [
    "statistics",
    "rating-breakdown",
    "incidents",
    "touches",
]

_session = cf_requests.Session(impersonate="safari")
_session.headers.update({
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.sofascore.com",
    "Referer": "https://www.sofascore.com/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
})


def clean_filename(text: str | None, fallback: str = "Player") -> str:
    text = str(text or fallback).strip()
    text = re.sub(r"[^\w\s.-]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "_", text)
    return text or fallback


def to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        v = float(value)
        return None if math.isnan(v) else v
    except Exception:
        return None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def first_present(row: dict[str, Any], names: list[str]) -> Any:
    lower = {k.lower().strip(): k for k in row.keys()}
    for name in names:
        key = lower.get(name.lower())
        if key is not None and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def clean_json(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: clean_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_json(v) for v in obj]
    if isinstance(obj, tuple):
        return [clean_json(v) for v in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj


def safe_json_dump(obj: Any, path: str | Path) -> None:
    Path(path).write_text(
        json.dumps(clean_json(obj), indent=2, ensure_ascii=False, allow_nan=False, default=str),
        encoding="utf-8",
    )


def read_rows(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Match log not found: {p.resolve()}")
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def filter_rows(rows: list[dict[str, Any]], player_id: int, season: str | None, league: str | None) -> list[dict[str, Any]]:
    player_cols = ["player_id", "playerId", "sofascore_player_id", "id"]
    season_cols = ["season", "season_name", "seasonName", "year"]
    league_cols = ["league", "competition", "tournament", "uniqueTournament", "unique_tournament"]

    matched = []
    for row in rows:
        row_pid = to_int(first_present(row, player_cols))
        if row_pid != int(player_id):
            continue

        if season:
            row_season = str(first_present(row, season_cols) or "").strip()
            if row_season and row_season != season:
                continue

        if league:
            row_league = str(first_present(row, league_cols) or "").strip()
            if row_league and row_league.lower() != league.lower():
                continue

        matched.append(row)

    return matched


def extract_events(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    event_cols = ["event_id", "eventId", "match_id", "matchId", "id"]
    minutes_cols = ["minutes_played", "minutes", "mins"]
    date_cols = ["date", "match_date", "start_date", "startDate", "startTimestamp", "start_timestamp"]
    home_cols = ["home_team", "homeTeam", "home"]
    away_cols = ["away_team", "awayTeam", "away"]
    player_name_cols = ["player_name", "profile_name", "player", "name", "short_name"]
    team_cols = ["team", "team_name", "player_team"]

    events: OrderedDict[int, dict[str, Any]] = OrderedDict()

    for row in rows:
        event_id = to_int(first_present(row, event_cols))
        if event_id is None:
            continue

        minutes = to_float(first_present(row, minutes_cols)) or 0.0

        if event_id not in events:
            events[event_id] = {
                "event_id": event_id,
                "date": first_present(row, date_cols),
                "home_team": first_present(row, home_cols),
                "away_team": first_present(row, away_cols),
                "team": first_present(row, team_cols),
                "minutes": minutes,
                "player_name": first_present(row, player_name_cols),
            }
        else:
            events[event_id]["minutes"] = max(events[event_id].get("minutes") or 0.0, minutes)

    return list(events.values())


def endpoint_path(event_id: int, player_id: int, endpoint: str) -> str:
    endpoint = endpoint.strip().strip("/")
    return f"event/{event_id}/player/{player_id}/{endpoint}"


def cache_path(cache_dir: str | Path, player_id: int, event_id: int, endpoint: str) -> Path:
    safe_endpoint = endpoint.replace("/", "__")
    d = Path(cache_dir) / str(player_id) / str(event_id)
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{safe_endpoint}.json"


def _get(path_or_url: str, delay: float, retries: int = 3, quiet_404: bool = True) -> tuple[dict | list | None, dict[str, Any]]:
    url = path_or_url if str(path_or_url).startswith("http") else f"{API_BASE}/{str(path_or_url).lstrip('/')}"

    meta = {
        "url": url,
        "status_code": None,
        "ok": False,
        "error": None,
        "fetched_at_utc": utc_now(),
    }

    for attempt in range(retries):
        try:
            r = _session.get(url, timeout=20)
            meta["status_code"] = int(r.status_code)

            if r.status_code == 404 and quiet_404:
                meta["error"] = "404"
                return None, meta

            if r.status_code == 429:
                wait = max(1.0, delay * 4) * (2 ** attempt)
                print(f"    [rate limited] sleeping {wait:.1f}s ...")
                time.sleep(wait)
                continue

            if not r.ok:
                short = url.split("/api/v1/")[-1]
                snippet = r.text[:220].replace("\n", " ")
                meta["error"] = f"HTTP {r.status_code}: {snippet}"
                print(f"    [HTTP {r.status_code}] {short} | {snippet}")
                return None, meta

            data = r.json()
            meta["ok"] = True

            if delay > 0:
                time.sleep(delay)

            return data, meta

        except Exception as exc:
            meta["error"] = str(exc)
            if attempt < retries - 1:
                print(f"    [retry {attempt + 1}/{retries}] {exc}")
                time.sleep(1.0 * (attempt + 1))
            else:
                print(f"    [error] {url}: {exc}")

    return None, meta


def fetch_endpoint(
    event_id: int,
    player_id: int,
    endpoint: str,
    cache_dir: str | Path,
    delay: float,
    no_fetch: bool = False,
    refresh_cache: bool = False,
) -> dict[str, Any]:
    cp = cache_path(cache_dir, player_id, event_id, endpoint)

    if cp.exists() and not refresh_cache:
        try:
            cached = json.loads(cp.read_text(encoding="utf-8"))
            cached["_cache_source"] = "cache"
            return cached
        except Exception:
            pass

    if no_fetch:
        return {
            "player_id": player_id,
            "event_id": event_id,
            "endpoint": endpoint,
            "available": False,
            "source": "missing-cache",
            "data": None,
            "meta": {"error": "missing-cache"},
        }

    path = endpoint_path(event_id, player_id, endpoint)
    data, meta = _get(path, delay=delay)

    payload = {
        "player_id": player_id,
        "event_id": event_id,
        "endpoint": endpoint,
        "available": bool(data),
        "source": "fetch",
        "data": data,
        "meta": meta,
    }

    safe_json_dump(payload, cp)
    return payload


ACTION_KEY_HINTS = {
    "actions", "events", "incidents", "touches", "passes", "carries",
    "ballCarries", "ball_carries", "dribbles", "duels", "shots",
    "tackles", "interceptions", "recoveries", "items", "rows",
}


def looks_like_action_dict(d: dict[str, Any]) -> bool:
    if not isinstance(d, dict):
        return False

    keys = {str(k).lower() for k in d.keys()}

    has_coord = (
        {"x", "y"}.issubset(keys)
        or {"startx", "starty"}.issubset(keys)
        or {"start_x", "start_y"}.issubset(keys)
        or {"fromx", "fromy"}.issubset(keys)
    )

    has_action_type = any(k in keys for k in [
        "type", "actiontype", "action_type", "eventtype", "event_type",
        "name", "stat", "category",
    ])

    has_minute = any(k in keys for k in ["minute", "time", "period", "second"])

    return has_coord or (has_action_type and has_minute)


def flatten_value(value: Any, prefix: str = "", max_depth: int = 2) -> dict[str, Any]:
    out: dict[str, Any] = {}

    if max_depth < 0:
        return out

    if isinstance(value, dict):
        for k, v in value.items():
            key = f"{prefix}_{k}" if prefix else str(k)
            if isinstance(v, (str, int, float, bool)) or v is None:
                out[key] = v
            elif isinstance(v, dict):
                out.update(flatten_value(v, key, max_depth=max_depth - 1))
            else:
                out[key] = json.dumps(clean_json(v), ensure_ascii=False, default=str)[:500] if v is not None else None

    return out


def walk_action_like(obj: Any, endpoint: str, path: str = "") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            actionish = [x for x in obj if looks_like_action_dict(x)]
            if actionish:
                for item in actionish:
                    flat = flatten_value(item)
                    flat["_endpoint"] = endpoint
                    flat["_source_path"] = path or "$"
                    rows.append(flat)
                return rows

        for i, item in enumerate(obj):
            rows.extend(walk_action_like(item, endpoint=endpoint, path=f"{path}[{i}]"))
        return rows

    if isinstance(obj, dict):
        for k, v in obj.items():
            child_path = f"{path}.{k}" if path else str(k)
            if isinstance(v, list):
                rows.extend(walk_action_like(v, endpoint=endpoint, path=child_path))
            elif isinstance(v, dict):
                rows.extend(walk_action_like(v, endpoint=endpoint, path=child_path))

    return rows


def flatten_payloads(raw_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flat_rows: list[dict[str, Any]] = []

    for ev in raw_events:
        event_info = ev.get("event") or {}
        event_id = ev.get("event_id")

        for endpoint_record in ev.get("endpoints", []):
            endpoint = endpoint_record.get("endpoint")
            data = endpoint_record.get("data")
            available = endpoint_record.get("available")

            action_rows = walk_action_like(data, endpoint=endpoint)

            for row in action_rows:
                row["player_id"] = ev.get("player_id")
                row["event_id"] = event_id
                row["endpoint"] = endpoint
                row["endpoint_available"] = available
                row["match_date"] = event_info.get("date")
                row["home_team"] = event_info.get("home_team")
                row["away_team"] = event_info.get("away_team")
                row["team"] = event_info.get("team")
                row["minutes"] = event_info.get("minutes")
                flat_rows.append(row)

    return flat_rows


def write_flat_csv(path: str | Path, rows: list[dict[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        with p.open("w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    preferred = [
        "player_id", "event_id", "endpoint", "_endpoint", "_source_path",
        "match_date", "home_team", "away_team", "team", "minutes",
        "type", "name", "category", "minute", "second",
        "x", "y", "endX", "endY", "end_x", "end_y",
        "startX", "startY", "start_x", "start_y",
        "outcome", "result", "successful", "value",
    ]

    all_keys = []
    seen = set()
    for k in preferred:
        if any(k in r for r in rows):
            all_keys.append(k)
            seen.add(k)

    for r in rows:
        for k in r.keys():
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    with p.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def summarize_raw(raw_events: list[dict[str, Any]], flat_rows: list[dict[str, Any]]) -> dict[str, Any]:
    endpoint_counts = defaultdict(lambda: {"available_events": 0, "missing_events": 0, "flat_rows": 0})

    for ev in raw_events:
        for endpoint_record in ev.get("endpoints", []):
            endpoint = endpoint_record.get("endpoint")
            if endpoint_record.get("available"):
                endpoint_counts[endpoint]["available_events"] += 1
            else:
                endpoint_counts[endpoint]["missing_events"] += 1

    for r in flat_rows:
        endpoint_counts[r.get("endpoint") or r.get("_endpoint")]["flat_rows"] += 1

    return {
        "events": len(raw_events),
        "events_with_any_available_endpoint": sum(
            1 for ev in raw_events if any(e.get("available") for e in ev.get("endpoints", []))
        ),
        "flat_action_rows": len(flat_rows),
        "endpoint_summary": dict(endpoint_counts),
        "note": (
            "Flat rows are best-effort action-like extractions. Raw JSON remains "
            "the source of truth because Sofascore endpoint schemas may vary."
        ),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Cache-backed per-player/per-event Sofascore action scraper.")
    ap.add_argument("--player-id", "-p", type=int, required=True)
    ap.add_argument("--season", "-s", default="2025-26")
    ap.add_argument("--league", "-l", default=None)
    ap.add_argument("--match-log", "-i", default=DEFAULT_MATCH_LOG)
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--cache-dir", default=DEFAULT_CACHE_DIR)
    ap.add_argument("--delay", type=float, default=REQUEST_DELAY)
    ap.add_argument("--endpoints", default=",".join(DEFAULT_ENDPOINTS), help="Comma-separated endpoint suffixes after /event/{event_id}/player/{player_id}/")
    ap.add_argument("--format", choices=["json", "csv", "both"], default="both")
    ap.add_argument("--no-fetch", action="store_true", help="Use cache only; do not call Sofascore.")
    ap.add_argument("--refresh-cache", action="store_true", help="Refetch even if cached.")
    ap.add_argument("--max-events", type=int, default=None, help="Debug/testing cap on event count.")
    args = ap.parse_args()

    endpoints = [e.strip().strip("/") for e in str(args.endpoints).split(",") if e.strip()]
    if not endpoints:
        raise ValueError("No endpoints requested.")

    rows = read_rows(args.match_log)
    player_rows = filter_rows(rows, args.player_id, args.season, args.league)

    if not player_rows:
        raise ValueError("No match-log rows found for that player/season/league.")

    events = extract_events(player_rows)
    if args.max_events:
        events = events[: int(args.max_events)]

    if not events:
        raise ValueError("No event IDs found for that player/season/league.")

    player_name = first_present(player_rows[0], ["player_name", "profile_name", "player", "name", "short_name"])
    player_name = str(player_name or args.player_id)

    print(f"\nBuilding action cache for {player_name} ({args.player_id})")
    print(f"Season: {args.season} | League: {args.league or 'Any'}")
    print(f"Events found: {len(events)}")
    print(f"Endpoints: {', '.join(endpoints)}")
    print(f"Cache dir: {args.cache_dir}\n")

    raw_events: list[dict[str, Any]] = []

    for i, event in enumerate(events, start=1):
        event_id = int(event["event_id"])
        print(f"  [{i}/{len(events)}] event {event_id}")

        endpoint_records = []
        for endpoint in endpoints:
            rec = fetch_endpoint(
                event_id=event_id,
                player_id=args.player_id,
                endpoint=endpoint,
                cache_dir=args.cache_dir,
                delay=args.delay,
                no_fetch=args.no_fetch,
                refresh_cache=args.refresh_cache,
            )
            available = bool(rec.get("available"))
            source = rec.get("_cache_source") or rec.get("source")
            status = rec.get("meta", {}).get("status_code")
            print(f"      {endpoint}: {'ok' if available else 'missing'} ({source}, status={status})")
            endpoint_records.append(rec)

        raw_events.append({
            "player_id": args.player_id,
            "player_name": player_name,
            "season": args.season,
            "league": args.league,
            "event_id": event_id,
            "event": event,
            "endpoints": endpoint_records,
        })

    flat_rows = flatten_payloads(raw_events)
    summary = summarize_raw(raw_events, flat_rows)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    base = f"{clean_filename(player_name)}_{args.player_id}_{str(args.season).replace('/', '-')}_actions"
    raw_path = out_dir / f"{base}_raw.json"
    flat_path = out_dir / f"{base}_flat.csv"
    summary_path = out_dir / f"{base}_summary.json"

    written = []

    if args.format in {"json", "both"}:
        raw_payload = {
            "meta": {
                "player_id": args.player_id,
                "player_name": player_name,
                "season": args.season,
                "league": args.league,
                "match_log": str(args.match_log),
                "events_found": len(events),
                "endpoints": endpoints,
                "generated_at_utc": utc_now(),
                "cache_dir": str(args.cache_dir),
                "no_fetch": bool(args.no_fetch),
                "refresh_cache": bool(args.refresh_cache),
            },
            "summary": summary,
            "events": raw_events,
        }
        safe_json_dump(raw_payload, raw_path)
        safe_json_dump({"meta": raw_payload["meta"], **summary}, summary_path)
        written.extend([raw_path, summary_path])

    if args.format in {"csv", "both"}:
        write_flat_csv(flat_path, flat_rows)
        written.append(flat_path)

    print("\n" + "─" * 60)
    print(f"Player:           {player_name}")
    print(f"Events:           {len(events)}")
    print(f"Flat action rows: {len(flat_rows)}")
    print("Endpoint summary:")
    for endpoint, info in summary["endpoint_summary"].items():
        print(
            f"  {endpoint}: available_events={info['available_events']} "
            f"missing_events={info['missing_events']} flat_rows={info['flat_rows']}"
        )
    print("Outputs:")
    for p in written:
        print(f"  {p}")
    print("─" * 60)


if __name__ == "__main__":
    main()
