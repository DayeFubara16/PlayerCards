"""
Player_Action_Scraper.py
────────────────────────

Cache-backed per-player/per-event Sofascore player-action scraper.

Primary goal:
- Extract raw player action-map data such as passes, ball carries, dribbles,
  and defensive actions from Sofascore's per-player rating-breakdown endpoint.

This is intentionally NOT a full-dataset crawler. It is designed for:
- one player card
- one player similarity investigation
- selected high-value scouting cases

Known useful endpoint:
  /event/{event_id}/player/{player_id}/rating-breakdown

Other optional probe endpoints:
  /event/{event_id}/player/{player_id}/statistics
  /event/{event_id}/player/{player_id}/incidents
  /event/{event_id}/player/{player_id}/touches

Notes:
- Sofascore endpoint coverage varies by competition, match, and app version.
- Missing endpoints are recorded but are not fatal.
- Raw JSON is always saved because endpoint schemas can drift.
- The flat CSV is a best-effort normalization layer.

Example:
  python Player_Action_Scraper.py \
    --player-id 978838 \
    --season 2025-26 \
    --match-log data/raw/player_match_logs.csv \
    --out-dir cards/actions \
    --cache-dir cache/action_cache \
    --delay 1.2 \
    --format both
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import time
from collections import Counter, OrderedDict, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from curl_cffi import requests as cf_requests


API_BASE = "https://api.sofascore.com/api/v1"
DEFAULT_MATCH_LOG = "data/raw/player_match_logs.csv"
DEFAULT_OUT_DIR = "cards/actions"
DEFAULT_CACHE_DIR = "cache/action_cache"
REQUEST_DELAY = 1.2

# Keep rating-breakdown first because it is the important action-map endpoint.
DEFAULT_ENDPOINTS = [
    "rating-breakdown",
    "statistics",
]

OPTIONAL_PROBE_ENDPOINTS = [
    "incidents",
    "touches",
]

# These are the most useful action groups normally found in rating-breakdown.
# The parser also falls back to all list-valued keys, so this list is not a hard limit.
PREFERRED_RATING_BREAKDOWN_CATEGORIES = [
    "passes",
    "ball-carries",
    "ballCarries",
    "dribbles",
    "defensive",
]

SESSION = cf_requests.Session(impersonate="safari")
SESSION.headers.update(
    {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.sofascore.com",
        "Referer": "https://www.sofascore.com/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
    }
)


# ─────────────────────────────────────────────────────────────────────────────
# Basic utilities
# ─────────────────────────────────────────────────────────────────────────────


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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


def first_present(row: dict[str, Any], names: Iterable[str]) -> Any:
    lower = {k.lower().strip(): k for k in row.keys()}
    for name in names:
        key = lower.get(str(name).lower())
        if key is not None and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def clean_json(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): clean_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_json(v) for v in obj]
    if isinstance(obj, tuple):
        return [clean_json(v) for v in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    return obj


def safe_json_dump(obj: Any, path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps(clean_json(obj), indent=2, ensure_ascii=False, allow_nan=False, default=str),
        encoding="utf-8",
    )


def read_rows(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Match log not found: {p.resolve()}")
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


# ─────────────────────────────────────────────────────────────────────────────
# Match-log parsing
# ─────────────────────────────────────────────────────────────────────────────


def filter_rows(
    rows: list[dict[str, Any]],
    player_id: int,
    season: str | None,
    league: str | None,
) -> list[dict[str, Any]]:
    player_cols = ["player_id", "playerId", "sofascore_player_id", "id"]
    season_cols = ["season", "season_name", "seasonName", "year"]
    league_cols = ["league", "competition", "tournament", "uniqueTournament", "unique_tournament"]

    matched: list[dict[str, Any]] = []

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


# ─────────────────────────────────────────────────────────────────────────────
# Fetching and cache
# ─────────────────────────────────────────────────────────────────────────────


def endpoint_path(event_id: int, player_id: int, endpoint: str) -> str:
    endpoint = endpoint.strip().strip("/")
    return f"event/{event_id}/player/{player_id}/{endpoint}"


def cache_path(cache_dir: str | Path, player_id: int, event_id: int, endpoint: str) -> Path:
    safe_endpoint = endpoint.replace("/", "__")
    d = Path(cache_dir) / str(player_id) / str(event_id)
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{safe_endpoint}.json"


def get_json(
    path_or_url: str,
    delay: float,
    retries: int = 3,
    quiet_404: bool = True,
) -> tuple[dict[str, Any] | list[Any] | None, dict[str, Any]]:
    url = path_or_url if str(path_or_url).startswith("http") else f"{API_BASE}/{str(path_or_url).lstrip('/')}"

    meta: dict[str, Any] = {
        "url": url,
        "status_code": None,
        "ok": False,
        "error": None,
        "fetched_at_utc": utc_now(),
    }

    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=20)
            meta["status_code"] = int(r.status_code)

            if r.status_code == 404 and quiet_404:
                meta["error"] = "404"
                return None, meta

            if r.status_code == 429:
                wait = max(1.0, delay * 4) * (2**attempt)
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
    data, meta = get_json(path, delay=delay)

    payload = {
        "player_id": player_id,
        "event_id": event_id,
        "endpoint": endpoint,
        "available": data is not None,
        "source": "fetch",
        "data": data,
        "meta": meta,
    }

    safe_json_dump(payload, cp)
    return payload


# ─────────────────────────────────────────────────────────────────────────────
# Schema inspection
# ─────────────────────────────────────────────────────────────────────────────


def schema_summary(obj: Any, max_depth: int = 3, path: str = "$") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    if max_depth < 0:
        return rows

    if isinstance(obj, dict):
        rows.append(
            {
                "path": path,
                "type": "dict",
                "keys": list(obj.keys())[:50],
                "len": len(obj),
            }
        )
        for key, value in obj.items():
            rows.extend(schema_summary(value, max_depth=max_depth - 1, path=f"{path}.{key}"))

    elif isinstance(obj, list):
        sample_type = type(obj[0]).__name__ if obj else None
        rows.append(
            {
                "path": path,
                "type": "list",
                "len": len(obj),
                "sample_type": sample_type,
            }
        )
        if obj:
            rows.extend(schema_summary(obj[0], max_depth=max_depth - 1, path=f"{path}[0]"))

    else:
        rows.append(
            {
                "path": path,
                "type": type(obj).__name__,
                "sample": obj,
            }
        )

    return rows


def top_level_debug(data: Any) -> dict[str, Any]:
    if isinstance(data, dict):
        out: dict[str, Any] = {"type": "dict", "keys": list(data.keys())}
        list_lengths = {}
        for key, value in data.items():
            if isinstance(value, list):
                list_lengths[key] = len(value)
        out["list_lengths"] = list_lengths
        return out

    if isinstance(data, list):
        return {"type": "list", "len": len(data), "sample_type": type(data[0]).__name__ if data else None}

    return {"type": type(data).__name__}


# ─────────────────────────────────────────────────────────────────────────────
# Flattening helpers
# ─────────────────────────────────────────────────────────────────────────────


def flatten_value(value: Any, prefix: str = "", max_depth: int = 3) -> dict[str, Any]:
    out: dict[str, Any] = {}

    if max_depth < 0:
        return out

    if not isinstance(value, dict):
        return out

    for key, child in value.items():
        out_key = f"{prefix}_{key}" if prefix else str(key)

        if isinstance(child, (str, int, float, bool)) or child is None:
            out[out_key] = child
        elif isinstance(child, dict):
            out.update(flatten_value(child, out_key, max_depth=max_depth - 1))
        else:
            out[out_key] = json.dumps(clean_json(child), ensure_ascii=False, default=str)[:1000]

    return out


def get_nested(d: dict[str, Any], *paths: str) -> Any:
    for path in paths:
        cur: Any = d
        ok = True
        for part in path.split("."):
            if not isinstance(cur, dict) or part not in cur:
                ok = False
                break
            cur = cur[part]
        if ok and cur not in (None, ""):
            return cur
    return None


def normalize_action_item(item: dict[str, Any], category: str, endpoint: str) -> dict[str, Any]:
    row = flatten_value(item, max_depth=4)

    row["category"] = category
    row["endpoint"] = endpoint
    row["_endpoint"] = endpoint

    # Sofascore rating-breakdown commonly uses nested coordinate dicts.
    row["x"] = get_nested(item, "playerCoordinates.x", "coordinates.x", "startCoordinates.x", "from.x")
    row["y"] = get_nested(item, "playerCoordinates.y", "coordinates.y", "startCoordinates.y", "from.y")
    row["end_x"] = get_nested(item, "passEndCoordinates.x", "endCoordinates.x", "to.x")
    row["end_y"] = get_nested(item, "passEndCoordinates.y", "endCoordinates.y", "to.y")

    # Normalize common labels without deleting original columns.
    row["minute"] = get_nested(item, "time", "minute") or row.get("minute")
    row["action_type"] = item.get("type") or item.get("name") or item.get("eventType") or category
    row["outcome"] = item.get("outcome") or item.get("result")

    return row


def rating_breakdown_categories(data: dict[str, Any]) -> list[str]:
    if not isinstance(data, dict):
        return []

    categories: list[str] = []
    seen = set()

    for key in PREFERRED_RATING_BREAKDOWN_CATEGORIES:
        if key in data and isinstance(data[key], list):
            categories.append(key)
            seen.add(key)

    # Fallback: include any list-valued top-level keys. This protects you if
    # Sofascore renames or adds categories.
    for key, value in data.items():
        if key not in seen and isinstance(value, list):
            categories.append(key)
            seen.add(key)

    return categories


def flatten_rating_breakdown(data: Any, endpoint: str) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []

    rows: list[dict[str, Any]] = []
    for category in rating_breakdown_categories(data):
        items = data.get(category)
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue
            rows.append(normalize_action_item(item, category=category, endpoint=endpoint))

    return rows


def flatten_statistics(data: Any, endpoint: str) -> list[dict[str, Any]]:
    """
    Statistics are usually aggregates, not action events.
    This returns one stat row per aggregate item only if a list structure exists.
    It is useful for auditing but not for action maps.
    """
    rows: list[dict[str, Any]] = []

    if isinstance(data, dict):
        # Common structure: groups -> statisticsItems. Keep this broad.
        for key, value in data.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        row = flatten_value(item, max_depth=3)
                        row["category"] = key
                        row["endpoint"] = endpoint
                        row["_endpoint"] = endpoint
                        row["row_kind"] = "aggregate_stat"
                        rows.append(row)

    return rows


def looks_like_action_dict(d: dict[str, Any]) -> bool:
    keys = {str(k).lower() for k in d.keys()}

    direct_coord = (
        {"x", "y"}.issubset(keys)
        or {"startx", "starty"}.issubset(keys)
        or {"start_x", "start_y"}.issubset(keys)
        or {"fromx", "fromy"}.issubset(keys)
    )

    nested_coord = any(
        key.lower() in keys
        for key in [
            "playercoordinates",
            "passendcoordinates",
            "coordinates",
            "startcoordinates",
            "endcoordinates",
        ]
    )

    has_action_type = any(
        k in keys
        for k in [
            "type",
            "actiontype",
            "action_type",
            "eventtype",
            "event_type",
            "name",
            "stat",
            "category",
        ]
    )

    has_minute = any(k in keys for k in ["minute", "time", "period", "second"])

    return direct_coord or nested_coord or (has_action_type and has_minute)


def walk_action_like(obj: Any, endpoint: str, path: str = "") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            actionish = [x for x in obj if looks_like_action_dict(x)]
            if actionish:
                category = path.split(".")[-1].replace("[0]", "") if path else "unknown"
                for item in actionish:
                    row = normalize_action_item(item, category=category, endpoint=endpoint)
                    row["_source_path"] = path or "$"
                    rows.append(row)
                return rows

        for i, item in enumerate(obj):
            rows.extend(walk_action_like(item, endpoint=endpoint, path=f"{path}[{i}]"))
        return rows

    if isinstance(obj, dict):
        for key, value in obj.items():
            child_path = f"{path}.{key}" if path else str(key)
            if isinstance(value, (list, dict)):
                rows.extend(walk_action_like(value, endpoint=endpoint, path=child_path))

    return rows


def flatten_endpoint_record(endpoint_record: dict[str, Any]) -> list[dict[str, Any]]:
    endpoint = endpoint_record.get("endpoint")
    data = endpoint_record.get("data")

    if endpoint == "rating-breakdown":
        return flatten_rating_breakdown(data, endpoint=endpoint)

    if endpoint == "statistics":
        return flatten_statistics(data, endpoint=endpoint)

    return walk_action_like(data, endpoint=endpoint)


def flatten_payloads(raw_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flat_rows: list[dict[str, Any]] = []

    for ev in raw_events:
        event_info = ev.get("event") or {}
        event_id = ev.get("event_id")

        for endpoint_record in ev.get("endpoints", []):
            endpoint = endpoint_record.get("endpoint")
            available = endpoint_record.get("available")
            action_rows = flatten_endpoint_record(endpoint_record)

            for row in action_rows:
                row["player_id"] = ev.get("player_id")
                row["player_name"] = ev.get("player_name")
                row["event_id"] = event_id
                row["endpoint"] = endpoint
                row["endpoint_available"] = available
                row["match_date"] = event_info.get("date")
                row["home_team"] = event_info.get("home_team")
                row["away_team"] = event_info.get("away_team")
                row["team"] = event_info.get("team")
                row["minutes_played_matchlog"] = event_info.get("minutes")
                flat_rows.append(row)

    return flat_rows


# ─────────────────────────────────────────────────────────────────────────────
# Writing outputs
# ─────────────────────────────────────────────────────────────────────────────


def write_flat_csv(path: str | Path, rows: list[dict[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    preferred = [
        "player_id",
        "player_name",
        "event_id",
        "endpoint",
        "category",
        "action_type",
        "row_kind",
        "match_date",
        "home_team",
        "away_team",
        "team",
        "minutes_played_matchlog",
        "minute",
        "second",
        "period",
        "x",
        "y",
        "end_x",
        "end_y",
        "outcome",
        "result",
        "successful",
        "value",
        "rating",
        "_endpoint",
        "_source_path",
        "endpoint_available",
    ]

    all_keys: list[str] = []
    seen = set()

    for key in preferred:
        if any(key in r for r in rows):
            all_keys.append(key)
            seen.add(key)

    for row in rows:
        for key in row.keys():
            if key not in seen:
                all_keys.append(key)
                seen.add(key)

    with p.open("w", encoding="utf-8", newline="") as f:
        if not all_keys:
            f.write("")
            return
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def summarize_raw(raw_events: list[dict[str, Any]], flat_rows: list[dict[str, Any]]) -> dict[str, Any]:
    endpoint_counts = defaultdict(lambda: {"available_events": 0, "missing_events": 0, "flat_rows": 0})
    category_counts = Counter()
    endpoint_debug: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for ev in raw_events:
        for endpoint_record in ev.get("endpoints", []):
            endpoint = endpoint_record.get("endpoint")
            if endpoint_record.get("available"):
                endpoint_counts[endpoint]["available_events"] += 1
            else:
                endpoint_counts[endpoint]["missing_events"] += 1

            endpoint_debug[endpoint].append(
                {
                    "event_id": ev.get("event_id"),
                    "available": endpoint_record.get("available"),
                    "source": endpoint_record.get("_cache_source") or endpoint_record.get("source"),
                    "status_code": (endpoint_record.get("meta") or {}).get("status_code"),
                    "debug": top_level_debug(endpoint_record.get("data")),
                }
            )

    for row in flat_rows:
        endpoint = row.get("endpoint") or row.get("_endpoint") or "unknown"
        category = row.get("category") or "unknown"
        endpoint_counts[endpoint]["flat_rows"] += 1
        category_counts[f"{endpoint}:{category}"] += 1

    return {
        "events": len(raw_events),
        "events_with_any_available_endpoint": sum(
            1 for ev in raw_events if any(e.get("available") for e in ev.get("endpoints", []))
        ),
        "flat_rows": len(flat_rows),
        "endpoint_summary": dict(endpoint_counts),
        "category_counts": dict(category_counts),
        "endpoint_debug_sample": {k: v[:5] for k, v in endpoint_debug.items()},
        "note": (
            "For player action maps, rating-breakdown is the key endpoint. "
            "Statistics rows are usually aggregate stats, not raw actions. "
            "Raw JSON remains the source of truth because Sofascore schemas can vary."
        ),
    }


def write_schema_debug(out_dir: Path, base: str, raw_events: list[dict[str, Any]]) -> Path:
    schema_rows: list[dict[str, Any]] = []

    for ev in raw_events:
        event_id = ev.get("event_id")
        for endpoint_record in ev.get("endpoints", []):
            endpoint = endpoint_record.get("endpoint")
            data = endpoint_record.get("data")
            for row in schema_summary(data, max_depth=3):
                row["event_id"] = event_id
                row["endpoint"] = endpoint
                schema_rows.append(row)

    schema_path = out_dir / f"{base}_schema_debug.json"
    safe_json_dump(schema_rows, schema_path)
    return schema_path


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Cache-backed per-player/per-event Sofascore action scraper.")
    ap.add_argument("--player-id", "-p", type=int, required=True)
    ap.add_argument("--season", "-s", default="2025-26")
    ap.add_argument("--league", "-l", default=None)
    ap.add_argument("--match-log", "-i", default=DEFAULT_MATCH_LOG)
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--cache-dir", default=DEFAULT_CACHE_DIR)
    ap.add_argument("--delay", type=float, default=REQUEST_DELAY)
    ap.add_argument(
        "--endpoints",
        default=",".join(DEFAULT_ENDPOINTS),
        help="Comma-separated endpoint suffixes after /event/{event_id}/player/{player_id}/",
    )
    ap.add_argument(
        "--probe-optional",
        action="store_true",
        help="Also probe optional/experimental per-player endpoints like incidents,touches.",
    )
    ap.add_argument("--format", choices=["json", "csv", "both"], default="both")
    ap.add_argument("--no-fetch", action="store_true", help="Use cache only; do not call Sofascore.")
    ap.add_argument("--refresh-cache", action="store_true", help="Refetch even if cached.")
    ap.add_argument("--max-events", type=int, default=None, help="Debug/testing cap on event count.")
    ap.add_argument("--schema-debug", action="store_true", help="Write a schema-debug JSON file for returned payloads.")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    endpoints = [e.strip().strip("/") for e in str(args.endpoints).split(",") if e.strip()]
    if args.probe_optional:
        for endpoint in OPTIONAL_PROBE_ENDPOINTS:
            if endpoint not in endpoints:
                endpoints.append(endpoint)

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

        endpoint_records: list[dict[str, Any]] = []
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
            debug = top_level_debug(rec.get("data")) if available else {}

            if isinstance(debug.get("list_lengths"), dict) and debug["list_lengths"]:
                list_info = ", ".join(f"{k}={v}" for k, v in debug["list_lengths"].items())
                print(f"      {endpoint}: ok ({source}, status={status}) | {list_info}")
            else:
                print(f"      {endpoint}: {'ok' if available else 'missing'} ({source}, status={status})")

            endpoint_records.append(rec)

        raw_events.append(
            {
                "player_id": args.player_id,
                "player_name": player_name,
                "season": args.season,
                "league": args.league,
                "event_id": event_id,
                "event": event,
                "endpoints": endpoint_records,
            }
        )

    flat_rows = flatten_payloads(raw_events)
    summary = summarize_raw(raw_events, flat_rows)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    base = f"{clean_filename(player_name)}_{args.player_id}_{str(args.season).replace('/', '-')}_actions"
    raw_path = out_dir / f"{base}_raw.json"
    flat_path = out_dir / f"{base}_flat.csv"
    summary_path = out_dir / f"{base}_summary.json"

    written: list[Path] = []

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

    if args.schema_debug:
        written.append(write_schema_debug(out_dir, base, raw_events))

    print("\n" + "─" * 60)
    print(f"Player:           {player_name}")
    print(f"Events:           {len(events)}")
    print(f"Flat rows:        {len(flat_rows)}")
    print("Endpoint summary:")
    for endpoint, info in summary["endpoint_summary"].items():
        print(
            f"  {endpoint}: available_events={info['available_events']} "
            f"missing_events={info['missing_events']} flat_rows={info['flat_rows']}"
        )

    if summary.get("category_counts"):
        print("Category counts:")
        for category, count in sorted(summary["category_counts"].items()):
            print(f"  {category}: {count}")

    print("Outputs:")
    for p in written:
        print(f"  {p}")
    print("─" * 60)


if __name__ == "__main__":
    main()
