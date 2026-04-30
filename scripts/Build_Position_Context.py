from __future__ import annotations

"""
Build_Position_Context.py
=========================

Cached spatial-position enrichment for the player pipeline.

Purpose
-------
This script builds a reusable JSON file that maps:

    event_id -> player_id -> avg_x / avg_y

It is designed to sit between the scraper and Position_Arbitrator:

    player_match_logs.csv
        ↓
    Build_Position_Context.py
        ↓
    position_context_by_event.json
        ↓
    Position_Arbitrator_v8/v9+ --position-context position_context_by_event.json

Why separate?
-------------
Average-position data is event-level and should be fetched once per match,
not once per player. This avoids expensive per-player heatmaps while giving
the arbitrator real spatial evidence.

Default endpoint tried:
    /event/{event_id}/average-positions

Optional fallback:
    /event/{event_id}/player/{player_id}/heatmap

Heatmap fallback is OFF by default because it is one request per player and
can 404 frequently.

Typical usage
-------------
python Build_Position_Context.py \
  --input player_match_logs.csv \
  --output position_context_by_event.json \
  --season 2025-26 \
  --league "Premier League" \
  --delay 1.2

Use with arbitrator:
python Position_Arbitrator_v9.py \
  --input player_season_totals.csv \
  --output player_season_totals_arbitrated.csv \
  --season 2025-26 \
  --league "Premier League" \
  --position-context position_context_by_event.json
"""

import argparse
import json
import math
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
from curl_cffi import requests as cf_requests


API_BASE = "https://api.sofascore.com/api/v1"
DEFAULT_DELAY = 0.35


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

REQUEST_DELAY = DEFAULT_DELAY


def sleep_if_needed() -> None:
    if REQUEST_DELAY > 0:
        time.sleep(REQUEST_DELAY)


def get_json(url: str, retries: int = 3, quiet_404: bool = True) -> dict[str, Any] | list[Any] | None:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            r = _session.get(url, timeout=20)
            if r.status_code == 404 and quiet_404:
                return None
            if r.status_code == 429:
                wait = max(1.0, REQUEST_DELAY * 4) * (2 ** attempt)
                print(f"  [rate limited] sleeping {wait:.1f}s ...")
                time.sleep(wait)
                continue
            if not r.ok:
                snippet = r.text[:250].replace("\n", " ")
                raise RuntimeError(f"HTTP {r.status_code} for {url} | {snippet}")
            data = r.json()
            sleep_if_needed()
            return data
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                print(f"  [retry {attempt + 1}/{retries}] {e}")
                time.sleep(1.0 * (attempt + 1))
            else:
                print(f"  [failed] {url}: {e}")
                return None
    print(f"  [failed] {url}: {last_err}")
    return None


def parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    text = str(value).strip().replace("%", "").replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def int_or_none(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


def load_existing_context(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "meta": {
                "schema": "position_context_by_event.v1",
                "sources": ["event_average_positions"],
            },
            "events": {},
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        print(f"[WARN] Could not parse existing context at {path}; starting fresh.")
        return {"meta": {"schema": "position_context_by_event.v1"}, "events": {}}

    if "events" not in payload or not isinstance(payload["events"], dict):
        # Backward compatibility with bare event map.
        payload = {"meta": {"schema": "position_context_by_event.v1"}, "events": payload}
    payload.setdefault("meta", {})
    payload.setdefault("events", {})
    return payload


def write_context(payload: dict[str, Any], path: Path) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def candidate_player_id(obj: Any) -> int | None:
    if not isinstance(obj, dict):
        return None

    direct = int_or_none(
        obj.get("playerId")
        or obj.get("player_id")
        or obj.get("id")
        or obj.get("participantId")
    )
    if direct is not None:
        return direct

    for key in ["player", "participant", "person", "athlete"]:
        nested = obj.get(key)
        if isinstance(nested, dict):
            nested_id = int_or_none(nested.get("id") or nested.get("playerId"))
            if nested_id is not None:
                return nested_id

    return None


def extract_xy(obj: dict[str, Any]) -> tuple[float | None, float | None]:
    """
    Handles common shapes:
    - {"averageX": 52, "averageY": 43}
    - {"avgX": 52, "avgY": 43}
    - {"x": 52, "y": 43}
    - {"averagePosition": {"x": 52, "y": 43}}
    - {"position": {"x": 52, "y": 43}}
    """
    x_keys = ["averageX", "avgX", "average_x", "avg_x", "x", "positionX"]
    y_keys = ["averageY", "avgY", "average_y", "avg_y", "y", "positionY"]

    x = next((parse_float(obj.get(k)) for k in x_keys if parse_float(obj.get(k)) is not None), None)
    y = next((parse_float(obj.get(k)) for k in y_keys if parse_float(obj.get(k)) is not None), None)

    if x is not None and y is not None:
        return x, y

    for nested_key in ["averagePosition", "position", "avgPosition", "coordinates", "point"]:
        nested = obj.get(nested_key)
        if isinstance(nested, dict):
            nx = next((parse_float(nested.get(k)) for k in x_keys if parse_float(nested.get(k)) is not None), None)
            ny = next((parse_float(nested.get(k)) for k in y_keys if parse_float(nested.get(k)) is not None), None)
            if nx is not None and ny is not None:
                return nx, ny

    return x, y


def iter_dicts(node: Any):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from iter_dicts(value)
    elif isinstance(node, list):
        for item in node:
            yield from iter_dicts(item)


def fetch_average_positions(event_id: int) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """
    Returns player_id -> {avg_x, avg_y, source, raw_keys}
    """
    url = f"{API_BASE}/event/{event_id}/average-positions"
    payload = get_json(url, quiet_404=True)

    diagnostics = {
        "endpoint": "average-positions",
        "available": payload is not None,
        "records_seen": 0,
        "players_with_xy": 0,
    }

    if payload is None:
        return {}, diagnostics

    players: dict[str, dict[str, Any]] = {}

    for obj in iter_dicts(payload):
        pid = candidate_player_id(obj)
        if pid is None:
            continue

        x, y = extract_xy(obj)
        diagnostics["records_seen"] += 1

        if x is None or y is None:
            continue

        # Keep plausible football coordinates only.
        # Sofascore generally uses 0..100-style coordinates for pitch locations.
        if not (0 <= x <= 100 and 0 <= y <= 100):
            continue

        players[str(pid)] = {
            "avg_x": round(float(x), 4),
            "avg_y": round(float(y), 4),
            "source": "average-positions",
        }

    diagnostics["players_with_xy"] = len(players)
    return players, diagnostics


def fetch_player_heatmap_average(event_id: int, player_id: int) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """
    Expensive fallback. OFF by default.

    Attempts to average points from:
        /event/{event_id}/player/{player_id}/heatmap
    """
    url = f"{API_BASE}/event/{event_id}/player/{player_id}/heatmap"
    payload = get_json(url, quiet_404=True)
    diagnostics = {
        "endpoint": "player-heatmap",
        "event_id": event_id,
        "player_id": player_id,
        "available": payload is not None,
        "points_seen": 0,
    }
    if payload is None:
        return None, diagnostics

    points = []
    for obj in iter_dicts(payload):
        x, y = extract_xy(obj)
        if x is None or y is None:
            continue
        if 0 <= x <= 100 and 0 <= y <= 100:
            weight = parse_float(obj.get("value") or obj.get("count") or obj.get("weight")) or 1.0
            points.append((x, y, max(weight, 0.0)))

    diagnostics["points_seen"] = len(points)
    if not points:
        return None, diagnostics

    total_w = sum(w for _, _, w in points) or 1.0
    avg_x = sum(x * w for x, _, w in points) / total_w
    avg_y = sum(y * w for _, y, w in points) / total_w

    return {
        "avg_x": round(float(avg_x), 4),
        "avg_y": round(float(avg_y), 4),
        "source": "player-heatmap-average",
    }, diagnostics


def unique_event_player_pairs(df: pd.DataFrame) -> dict[int, set[int]]:
    if "event_id" not in df.columns:
        raise ValueError("Input CSV must include event_id.")

    player_col = "player_id" if "player_id" in df.columns else None
    if player_col is None:
        raise ValueError("Input CSV must include player_id.")

    event_players: dict[int, set[int]] = defaultdict(set)

    for _, row in df.iterrows():
        eid = int_or_none(row.get("event_id"))
        pid = int_or_none(row.get(player_col))
        if eid is None or pid is None:
            continue
        event_players[eid].add(pid)

    return event_players


def filter_input(df: pd.DataFrame, season: str | None, league: str | None) -> pd.DataFrame:
    out = df.copy()
    if season is not None and "season" in out.columns:
        out = out.loc[out["season"].astype(str) == str(season)].copy()
    if league is not None and "league" in out.columns:
        out = out.loc[out["league"].astype(str).str.lower() == str(league).lower()].copy()
    return out


def build_position_context(
    input_csv: str,
    output_json: str,
    season: str | None,
    league: str | None,
    refresh: bool,
    use_heatmaps_for_missing: bool,
    player_limit_per_event: int | None,
    write_every: int,
) -> dict[str, Any]:
    df = pd.read_csv(input_csv)
    df = filter_input(df, season=season, league=league)

    if df.empty:
        raise ValueError("No rows left after season/league filters.")

    event_players = unique_event_player_pairs(df)

    out_path = Path(output_json)
    context = load_existing_context(out_path)
    events_ctx = context.setdefault("events", {})

    context["meta"].update({
        "schema": "position_context_by_event.v1",
        "input_csv": input_csv,
        "season_filter": season,
        "league_filter": league,
        "use_heatmaps_for_missing": use_heatmaps_for_missing,
    })

    event_ids = sorted(event_players)
    print(f"Found {len(event_ids)} event(s) in input after filters.")

    fetched = 0
    skipped = 0
    avg_success = 0
    heatmap_success = 0

    for i, event_id in enumerate(event_ids, start=1):
        key = str(event_id)
        existing = events_ctx.get(key)

        if existing and not refresh:
            skipped += 1
            continue

        print(f"[{i}/{len(event_ids)}] event {event_id}")

        players, avg_diag = fetch_average_positions(event_id)
        if players:
            avg_success += 1

        event_block = {
            "event_id": event_id,
            "source": "average-positions" if players else None,
            "average_positions_available": bool(players),
            "players": players,
            "diagnostics": {
                "average_positions": avg_diag,
                "heatmaps": {},
            },
        }

        if use_heatmaps_for_missing:
            missing = sorted(event_players[event_id] - {int(p) for p in players.keys() if str(p).isdigit()})
            if player_limit_per_event is not None:
                missing = missing[:player_limit_per_event]

            for pid in missing:
                hm_record, hm_diag = fetch_player_heatmap_average(event_id, pid)
                event_block["diagnostics"]["heatmaps"][str(pid)] = hm_diag
                if hm_record:
                    event_block["players"][str(pid)] = hm_record
                    heatmap_success += 1

            if event_block["players"] and event_block["source"] is None:
                event_block["source"] = "player-heatmap-average"

        events_ctx[key] = event_block
        fetched += 1

        if write_every > 0 and fetched % write_every == 0:
            write_context(context, out_path)
            print(f"  [checkpoint] wrote {out_path}")

    write_context(context, out_path)

    print("\nDone.")
    print(f"Output: {out_path}")
    print(f"Fetched events: {fetched}")
    print(f"Skipped cached events: {skipped}")
    print(f"Events with average-position players: {avg_success}")
    print(f"Heatmap player fallbacks succeeded: {heatmap_success}")

    return context


def main() -> None:
    global REQUEST_DELAY

    ap = argparse.ArgumentParser(description="Build cached event/player average-position context for arbitration.")
    ap.add_argument("--input", "-i", default="player_match_logs.csv", help="Input match-level CSV with event_id/player_id.")
    ap.add_argument("--output", "-o", default="position_context_by_event.json", help="Output JSON cache path.")
    ap.add_argument("--season", "-s", default=None, help="Optional season filter.")
    ap.add_argument("--league", "-l", default=None, help="Optional league filter.")
    ap.add_argument("--delay", type=float, default=DEFAULT_DELAY, help=f"Delay after successful requests. Default: {DEFAULT_DELAY}.")
    ap.add_argument("--refresh", action="store_true", help="Refetch events even if already present in output JSON.")
    ap.add_argument("--use-heatmaps-for-missing", action="store_true", help="Expensive fallback: fetch player heatmaps for players missing average-position data.")
    ap.add_argument("--player-limit-per-event", type=int, default=None, help="Optional cap for heatmap fallback players per event.")
    ap.add_argument("--write-every", type=int, default=5, help="Checkpoint output after this many fetched events. Default: 5.")
    args = ap.parse_args()

    REQUEST_DELAY = max(0.0, args.delay)

    build_position_context(
        input_csv=args.input,
        output_json=args.output,
        season=args.season,
        league=args.league,
        refresh=args.refresh,
        use_heatmaps_for_missing=args.use_heatmaps_for_missing,
        player_limit_per_event=args.player_limit_per_event,
        write_every=args.write_every,
    )


if __name__ == "__main__":
    main()
