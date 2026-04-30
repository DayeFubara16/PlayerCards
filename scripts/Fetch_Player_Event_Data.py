"""
Fetch_Player_Event_Data.py
──────────────────────────
Match-log-driven Sofascore spatial-data fetcher.

Sofascore's old season aggregate player endpoints such as:
  /player/{player_id}/shotmap/{season_id}
  /player/{player_id}/heatmap/{season_id}
  /player/{player_id}/statistics/season/{season_id}
  /player/{player_id}/last-matches/{n}
now commonly return 404.

This version pivots to the public per-event endpoints that Player_Scraper.py
already uses/depends on:
  • /event/{event_id}/shotmap
  • /event/{event_id}/player/{player_id}/heatmap

Workflow:
  1. Read player_match_logs.csv.
  2. Filter rows for player_id, season, and optionally league.
  3. Collect event_id values from those rows.
  4. Fetch each event shotmap, keep only this player's shots.
  5. Optionally fetch each event player heatmap and merge cells.
  6. Write one season-style JSON output for player cards.

Usage:
  python Fetch_Player_Event_Data.py --player-id 839956 --season 2025-26 --league "Premier League"
  python Fetch_Player_Event_Data.py -p 839956 -s 2025-26 --match-log player_match_logs.csv --delay 1.2
  python Fetch_Player_Event_Data.py -p 839956 -s 2025-26 --no-heatmap

Output file: {player_id}_{season}.json unless --out is provided.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
import re
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from curl_cffi import requests as cf_requests

API_BASE = "https://api.sofascore.com/api/v1"
DEFAULT_SEASON = "2025-26"
DEFAULT_MATCH_LOG = "data\\raw\\player_match_logs.csv"
REQUEST_DELAY = 0.5

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


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get(path_or_url: str, retries: int = 3, quiet_404: bool = True) -> dict | list | None:
    """GET a Sofascore API path or full URL with basic retry/rate-limit handling."""
    url = path_or_url if path_or_url.startswith("http") else f"{API_BASE}/{path_or_url.lstrip('/')}"

    for attempt in range(retries):
        try:
            r = _session.get(url, timeout=20)

            if r.status_code == 404 and quiet_404:
                return None

            if r.status_code == 429:
                wait = max(1.0, REQUEST_DELAY * 4) * (2 ** attempt)
                print(f"    [rate limited] sleeping {wait:.1f}s ...")
                time.sleep(wait)
                continue

            if not r.ok:
                snippet = r.text[:220].replace("\n", " ")
                short = url.split("/api/v1/")[-1]
                print(f"    [HTTP {r.status_code}] {short} | {snippet}")
                return None

            data = r.json()
            if REQUEST_DELAY > 0:
                time.sleep(REQUEST_DELAY)
            return data

        except Exception as e:
            if attempt < retries - 1:
                print(f"    [retry {attempt + 1}/{retries}] {e}")
                time.sleep(1.0 * (attempt + 1))
            else:
                print(f"    [error] {url}: {e}")

    return None


def _ts_to_date(ts: int | str | None) -> str | None:
    if ts in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(float(ts)), tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return None


def _as_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _first_present(row: dict[str, Any], names: list[str]) -> Any:
    """Return the first non-empty value for any case-insensitive column name."""
    lower_map = {k.lower().strip(): k for k in row.keys()}
    for name in names:
        key = lower_map.get(name.lower())
        if key is not None and row.get(key) not in (None, ""):
            return row.get(key)
    return None


# ── Match log reading ────────────────────────────────────────────────────────

def read_match_log_rows(match_log_path: str | Path) -> list[dict[str, Any]]:
    path = Path(match_log_path)
    if not path.exists():
        raise FileNotFoundError(f"Match log not found: {path.resolve()}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        raise ValueError(f"Match log is empty: {path.resolve()}")

    return rows


def filter_player_match_rows(
    rows: list[dict[str, Any]],
    player_id: int,
    season: str | None,
    league: str | None,
) -> list[dict[str, Any]]:
    """Filter CSV rows for this player/season/league using flexible column names."""
    matched: list[dict[str, Any]] = []
    player_cols = ["player_id", "playerId", "sofascore_player_id", "id"]
    season_cols = ["season", "season_name", "seasonName", "year"]
    league_cols = ["league", "competition", "tournament", "uniqueTournament", "unique_tournament"]

    for row in rows:
        row_player_id = _as_int(_first_present(row, player_cols))
        if row_player_id != player_id:
            continue

        if season:
            row_season = str(_first_present(row, season_cols) or "").strip()
            # Do not discard if the CSV has no season column at all.
            if row_season and row_season != season:
                continue

        if league:
            row_league = str(_first_present(row, league_cols) or "").strip()
            # Do not discard if the CSV has no league column at all.
            if row_league and row_league.lower() != league.lower():
                continue

        matched.append(row)

    return matched


def extract_events_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return unique event records, preserving CSV order."""
    event_cols = ["event_id", "eventId", "match_id", "matchId", "id"]
    date_cols = ["date", "match_date", "start_date", "startDate", "startTimestamp", "start_timestamp"]
    home_cols = ["home_team", "homeTeam", "home"]
    away_cols = ["away_team", "awayTeam", "away"]
    round_cols = ["round", "roundInfo", "gameweek", "matchweek"]
    player_name_cols = ["player_name", "player", "name", "short_name", "shortName"]

    events: OrderedDict[int, dict[str, Any]] = OrderedDict()
    for row in rows:
        event_id = _as_int(_first_present(row, event_cols))
        if event_id is None:
            continue

        if event_id not in events:
            raw_date = _first_present(row, date_cols)
            date = _ts_to_date(raw_date) if str(raw_date or "").isdigit() else raw_date
            events[event_id] = {
                "event_id": event_id,
                "date": date,
                "home_team": _first_present(row, home_cols),
                "away_team": _first_present(row, away_cols),
                "round": _first_present(row, round_cols),
                "player_name_from_csv": _first_present(row, player_name_cols),
            }

    return list(events.values())


# ── Profile ──────────────────────────────────────────────────────────────────

def fetch_profile(player_id: int, fallback_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    print("  Fetching profile ...", end=" ", flush=True)
    data = _get(f"player/{player_id}")
    if data:
        player = data.get("player") if isinstance(data, dict) else None
        player = player or data
        country = player.get("country") or player.get("nationality") or {}
        nat = country.get("name") if isinstance(country, dict) else country
        team_block = player.get("team") or {}
        result = {
            "player_id": player_id,
            "name": player.get("name") or player.get("shortName"),
            "short_name": player.get("shortName"),
            "date_of_birth": _ts_to_date(player.get("dateOfBirthTimestamp")),
            "nationality": nat,
            "height_cm": player.get("height"),
            "preferred_foot": player.get("preferredFoot"),
            "position": player.get("position"),
            "jersey_number": player.get("jerseyNumber") or player.get("shirtNumber"),
            "team": team_block.get("name") if isinstance(team_block, dict) else None,
            "market_value_eur": player.get("proposedMarketValue") or player.get("marketValue"),
            "contract_until": _ts_to_date(player.get("contractUntilTimestamp")),
            "injury_status": player.get("injury"),
        }
        print(f"OK  ({result.get('name') or player_id})")
        return result

    # Fallback from CSV, useful if profile endpoint is unavailable.
    rows = fallback_rows or []
    name = None
    team = None
    position = None
    if rows:
        first = rows[0]
        name = _first_present(first, ["player_name", "player", "name", "short_name", "shortName"])
        team = _first_present(first, ["team", "team_name", "club"])
        position = _first_present(first, ["position", "player_position"])

    print("not found; using CSV fallback")
    return {"player_id": player_id, "name": name, "team": team, "position": position}


# ── Per-event fetching/parsing ────────────────────────────────────────────────

def _extract_shot_list(data: dict | list | None) -> list[dict[str, Any]]:
    if not data:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    for key in ("shotmap", "shots", "data"):
        value = data.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    return []


def _shot_player_id(shot: dict[str, Any]) -> int | None:
    """Sofascore shotmap shapes vary; try all known player-id locations."""
    direct = _as_int(shot.get("playerId") or shot.get("player_id"))
    if direct is not None:
        return direct

    player = shot.get("player") or shot.get("scoredBy") or shot.get("shotBy")
    if isinstance(player, dict):
        return _as_int(player.get("id") or player.get("playerId"))

    return None


def _parse_shot(shot: dict[str, Any], event_meta: dict[str, Any]) -> dict[str, Any]:
    coords = shot.get("playerCoordinates") or shot.get("coordinates") or {}
    if not isinstance(coords, dict):
        coords = {}

    player = shot.get("player") if isinstance(shot.get("player"), dict) else {}
    block_coords = shot.get("blockCoordinates") if isinstance(shot.get("blockCoordinates"), dict) else {}
    goal_coords = shot.get("goalMouthCoordinates") if isinstance(shot.get("goalMouthCoordinates"), dict) else {}

    return {
        "event_id": event_meta.get("event_id"),
        "date": event_meta.get("date"),
        "home_team": event_meta.get("home_team"),
        "away_team": event_meta.get("away_team"),
        "round": event_meta.get("round"),
        "player_id": _shot_player_id(shot),
        "player_name": player.get("name") or player.get("shortName"),
        "x": coords.get("x") if coords else shot.get("x"),
        "y": coords.get("y") if coords else shot.get("y"),
        "xg": _as_float(shot.get("xg")),
        "xgot": _as_float(shot.get("xgot") or shot.get("xgOT")),
        "is_on_target": shot.get("isOnTarget"),
        "is_goal": bool(shot.get("goalType") is not None or shot.get("shotType") == "goal"),
        "goal_type": shot.get("goalType"),
        "shot_type": shot.get("shotType"),
        "body_part": shot.get("bodyPart"),
        "situation": shot.get("situation"),
        "minute": shot.get("time"),
        "added_time": shot.get("addedTime"),
        "period": shot.get("period"),
        "id": shot.get("id"),
        "block_x": block_coords.get("x"),
        "block_y": block_coords.get("y"),
        "goal_mouth_x": goal_coords.get("x"),
        "goal_mouth_y": goal_coords.get("y"),
        "goal_mouth_z": goal_coords.get("z"),
    }


def fetch_event_shotmap_for_player(event_meta: dict[str, Any], player_id: int) -> list[dict[str, Any]]:
    event_id = event_meta["event_id"]
    data = _get(f"event/{event_id}/shotmap")
    shots = _extract_shot_list(data)
    parsed: list[dict[str, Any]] = []

    for shot in shots:
        shot_pid = _shot_player_id(shot)
        if shot_pid == player_id:
            parsed.append(_parse_shot(shot, event_meta))

    return parsed


def _extract_heatmap_points(data: dict | list | None) -> list[Any]:
    if not data:
        return []
    if isinstance(data, list):
        return data
    for key in ("heatmap", "points", "data"):
        value = data.get(key)
        if isinstance(value, list):
            return value
    return []


def _parse_heatmap_point(point: Any, event_meta: dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(point, dict):
        x = point.get("x")
        y = point.get("y")
        value = point.get("value", 1)
    elif isinstance(point, (list, tuple)) and len(point) >= 2:
        x = point[0]
        y = point[1]
        value = point[2] if len(point) > 2 else 1
    else:
        return None

    return {
        "event_id": event_meta.get("event_id"),
        "date": event_meta.get("date"),
        "x": _as_float(x),
        "y": _as_float(y),
        "value": _as_float(value) or 1.0,
    }


def fetch_event_player_heatmap(event_meta: dict[str, Any], player_id: int) -> list[dict[str, Any]]:
    event_id = event_meta["event_id"]
    data = _get(f"event/{event_id}/player/{player_id}/heatmap")
    raw_points = _extract_heatmap_points(data)

    parsed: list[dict[str, Any]] = []
    for point in raw_points:
        parsed_point = _parse_heatmap_point(point, event_meta)
        if parsed_point and parsed_point["x"] is not None and parsed_point["y"] is not None:
            parsed.append(parsed_point)

    return parsed


# ── Aggregation ───────────────────────────────────────────────────────────────

def aggregate_heatmap(points: list[dict[str, Any]], precision: int = 2) -> dict[str, Any]:
    """Merge per-match heatmap points into an aggregated season heatmap."""
    cells: dict[tuple[float, float], float] = defaultdict(float)
    for p in points:
        x = _as_float(p.get("x"))
        y = _as_float(p.get("y"))
        if x is None or y is None:
            continue
        key = (round(x, precision), round(y, precision))
        cells[key] += _as_float(p.get("value")) or 1.0

    merged = [
        {"x": x, "y": y, "value": round(value, 4)}
        for (x, y), value in sorted(cells.items(), key=lambda item: (item[0][0], item[0][1]))
    ]
    total = round(sum(p["value"] for p in merged), 4)

    return {
        "points": merged,
        "raw_points_count": len(points),
        "cell_count": len(merged),
        "total_weight": total,
        "note": "Aggregated from /event/{event_id}/player/{player_id}/heatmap; x/y on Sofascore 0-100 pitch scale.",
    }


def _count_by(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        val = item.get(key) or "unknown"
        counts[str(val)] = counts.get(str(val), 0) + 1
    return counts


def summarize_shots(shots: list[dict[str, Any]]) -> dict[str, Any]:
    if not shots:
        return {}
    return {
        "total_shots": len(shots),
        "goals": sum(1 for s in shots if s.get("is_goal")),
        "on_target": sum(1 for s in shots if s.get("is_on_target")),
        "total_xg": round(sum(_as_float(s.get("xg")) or 0 for s in shots), 4),
        "total_xgot": round(sum(_as_float(s.get("xgot")) or 0 for s in shots), 4),
        "by_body_part": _count_by(shots, "body_part"),
        "by_situation": _count_by(shots, "situation"),
        "by_shot_type": _count_by(shots, "shot_type"),
    }

def clean_filename(text: str | None, fallback: str = "Player") -> str:
    text = str(text or fallback).strip()
    text = re.sub(r"[^\w\s.-]", "", text)
    text = re.sub(r"\s+", "_", text)
    return text or fallback


def build_output(
    player_id: int,
    season: str,
    league: str | None,
    match_log: str | Path,
    include_heatmap: bool = True,
) -> dict[str, Any]:
    print(f"\nFetching data for player_id={player_id}  season={season}  league={league or 'Any'}")
    print(f"Match log: {match_log}\n")

    rows = read_match_log_rows(match_log)
    player_rows = filter_player_match_rows(rows, player_id, season, league)
    events = extract_events_from_rows(player_rows)

    print(f"  Match-log rows matched: {len(player_rows)}")
    print(f"  Unique events found:    {len(events)}")

    if not events:
        raise ValueError(
            "No event_ids found for that player/season/league in the match log. "
            "Check --match-log, --player-id, --season, and --league."
        )

    profile = fetch_profile(player_id, player_rows)

    all_shots: list[dict[str, Any]] = []
    all_heatmap_points: list[dict[str, Any]] = []
    event_results: list[dict[str, Any]] = []

    print("  Fetching per-event data ...")
    for i, event_meta in enumerate(events, start=1):
        event_id = event_meta["event_id"]
        label = f"    [{i}/{len(events)}] event {event_id}"

        shots = fetch_event_shotmap_for_player(event_meta, player_id)
        all_shots.extend(shots)

        heat_points: list[dict[str, Any]] = []
        if include_heatmap:
            heat_points = fetch_event_player_heatmap(event_meta, player_id)
            all_heatmap_points.extend(heat_points)

        print(f"{label}: {len(shots)} shots" + (f", {len(heat_points)} heatmap pts" if include_heatmap else ""))
        event_results.append({
            **event_meta,
            "shots": len(shots),
            "heatmap_points": len(heat_points),
        })

    heatmap = aggregate_heatmap(all_heatmap_points) if include_heatmap else {}

    return {
        "meta": {
            "player_id": player_id,
            "season": season,
            "league": league,
            "source": "player_match_logs.csv + per-event Sofascore endpoints",
            "match_log": str(match_log),
            "events_found": len(events),
            "events_with_shots": sum(1 for e in event_results if e["shots"] > 0),
            "events_with_heatmap": sum(1 for e in event_results if e["heatmap_points"] > 0),
            "fetched_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        "profile": profile,
        "season_stats": {},
        "shotmap": {
            "count": len(all_shots),
            "shots": all_shots,
            "summary": summarize_shots(all_shots),
            "note": "Aggregated by fetching /event/{event_id}/shotmap for each match and filtering shots by player_id.",
        },
        "heatmap": heatmap,
        "touch_positions": {
            "count": 0,
            "points": [],
            "note": "Sofascore public per-event raw touch positions were not used; heatmap is available per event instead.",
        },
        "recent_form": {
            "matches_fetched": len(event_results),
            "events": event_results,
            "note": "Derived from player_match_logs.csv, not /player/{id}/last-matches/{n}.",
        },
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    global REQUEST_DELAY

    ap = argparse.ArgumentParser(
        description="Aggregate player spatial/event data from Sofascore per-event endpoints using player_match_logs.csv"
    )
    ap.add_argument("--player-id", "-p", type=int, default=None, help="Sofascore player ID")
    ap.add_argument("--season", "-s", type=str, default=None, help="Season string, e.g. 2025-26")
    ap.add_argument("--league", "-l", type=str, default=None, help="Optional league filter, e.g. Premier League")
    ap.add_argument("--match-log", type=str, default=DEFAULT_MATCH_LOG, help="Path to player_match_logs.csv")
    ap.add_argument("--out", "-o", type=str, default=None, help="Output JSON path")
    ap.add_argument("--delay", type=float, default=REQUEST_DELAY, help="Seconds between requests")
    ap.add_argument("--no-heatmap", action="store_true", help="Skip /event/{event_id}/player/{player_id}/heatmap calls")
    args = ap.parse_args()

    REQUEST_DELAY = max(0.0, args.delay)

    player_id = args.player_id
    if player_id is None:
        try:
            player_id = int(input("Enter player_id: ").strip())
        except ValueError:
            print("Invalid player_id — must be an integer.")
            return

    season = args.season or input(f"Enter season (default {DEFAULT_SEASON}): ").strip() or DEFAULT_SEASON

    output = build_output(
        player_id=player_id,
        season=season,
        league=args.league,
        match_log=args.match_log,
        include_heatmap=not args.no_heatmap,
    )

    player_name = output["profile"].get("name") or f"player_{player_id}"
    base = f"{clean_filename(player_name)}_{player_id}_{season.replace('/', '-')}"

    if args.out:
        out_path = Path(args.out)

        # If --out is a directory or has no file suffix, write inside it.
        if out_path.exists() and out_path.is_dir():
            out_path = out_path / f"{base}.json"
        elif out_path.suffix.lower() != ".json":
            out_path.mkdir(parents=True, exist_ok=True)
            out_path = out_path / f"{base}.json"
    else:
        out_path = Path(f"{base}.json")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    out_path.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    print(f"\n{'─' * 50}")
    print(f"  Player:      {output['profile'].get('name') or '?'}")
    print(f"  Season:      {season}")
    print(f"  Events:      {output['meta']['events_found']}")
    print(f"  Shots:       {output['shotmap']['count']}")
    print(f"  Heatmap:     {output['heatmap'].get('cell_count', 0)} cells")
    print(f"  Output:      {out_path}")
    print(f"{'─' * 50}")


if __name__ == "__main__":
    main()
