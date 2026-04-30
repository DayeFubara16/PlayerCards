"""
Build_Player_Season_Heatmap.py
──────────────────────────────
On-demand aggregated season heatmap builder.

Rebuilds a season-wide player visual/action-density heatmap from match-level Sofascore endpoints:
  /event/{event_id}/player/{player_id}/heatmap

Designed to be player-by-player and cache-backed, not a full-dataset crawl.

V3: visual-only. This script no longer estimates tactical position. Use
Build_Position_Context.py + Position_Arbitrator.py for positional labels.

Usage:
  python Build_Player_Season_Heatmap.py --player-id 2427970 --season 2025-26 --league "Premier League" --match-log player_match_logs.csv --delay 1.2
  python Build_Player_Season_Heatmap.py --player-id 934386 --season 2025-26 --league "Premier League" --format both
  python Build_Player_Season_Heatmap.py --player-id 839956 --season 2025-26 --no-fetch
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
DEFAULT_MATCH_LOG = "player_match_logs.csv"
DEFAULT_CACHE_DIR = ".heatmap_cache"
REQUEST_DELAY = 0.7

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


def first_present(row: dict[str, Any], names: list[str]) -> Any:
    lower = {k.lower().strip(): k for k in row.keys()}
    for name in names:
        key = lower.get(name.lower())
        if key is not None and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def weighted_mode(items: list[tuple[Any, float]]) -> Any:
    weights: dict[Any, float] = defaultdict(float)
    for value, weight in items:
        if value is None or str(value).strip() == "":
            continue
        weights[value] += float(weight or 0.0)
    if not weights:
        return None
    return max(weights.items(), key=lambda kv: kv[1])[0]


def cache_path(cache_dir: str | Path, player_id: int, event_id: int) -> Path:
    d = Path(cache_dir) / str(player_id)
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{event_id}.json"


def _get(path_or_url: str, delay: float, retries: int = 3, quiet_404: bool = True) -> dict | list | None:
    url = path_or_url if str(path_or_url).startswith("http") else f"{API_BASE}/{str(path_or_url).lstrip('/')}"
    for attempt in range(retries):
        try:
            r = _session.get(url, timeout=20)
            if r.status_code == 404 and quiet_404:
                return None
            if r.status_code == 429:
                wait = max(1.0, delay * 4) * (2 ** attempt)
                print(f"    [rate limited] sleeping {wait:.1f}s ...")
                time.sleep(wait)
                continue
            if not r.ok:
                short = url.split("/api/v1/")[-1]
                snippet = r.text[:220].replace("\n", " ")
                print(f"    [HTTP {r.status_code}] {short} | {snippet}")
                return None
            data = r.json()
            if delay > 0:
                time.sleep(delay)
            return data
        except Exception as e:
            if attempt < retries - 1:
                print(f"    [retry {attempt + 1}/{retries}] {e}")
                time.sleep(1.0 * (attempt + 1))
            else:
                print(f"    [error] {url}: {e}")
    return None


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
    role_cols = ["role_position", "primary_role_position", "player_position", "position", "base_position"]
    player_name_cols = ["player_name", "profile_name", "player", "name", "short_name"]
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
                "minutes": minutes,
                "role_position": first_present(row, role_cols),
                "player_name": first_present(row, player_name_cols),
            }
        else:
            events[event_id]["minutes"] = max(events[event_id].get("minutes") or 0.0, minutes)
    return list(events.values())


def summarize_source_labels(rows: list[dict[str, Any]]) -> dict[str, Any]:
    minutes_cols = ["minutes_played", "minutes", "mins"]
    label_cols = {
        "role_position": ["role_position", "primary_role_position"],
        "player_position": ["player_position", "position"],
        "base_position": ["base_position"],
        "role_family": ["role_family"],
        "profile_position": ["profile_position", "profile_position_raw", "sofascore_position", "canonical_position"],
    }
    out = {}
    for label_name, cols in label_cols.items():
        items = []
        for row in rows:
            value = first_present(row, cols)
            minutes = to_float(first_present(row, minutes_cols)) or 0.0
            if value not in (None, ""):
                items.append((str(value).strip(), minutes))
        out[f"{label_name}_mode"] = weighted_mode(items)
        weights = defaultdict(float)
        for v, w in items:
            weights[v] += float(w or 0.0)
        out[f"{label_name}_minutes"] = {k: round(v, 2) for k, v in sorted(weights.items(), key=lambda kv: kv[1], reverse=True)}
    return out


def extract_heatmap_points(data: dict | list | None) -> list[Any]:
    if not data:
        return []
    if isinstance(data, list):
        return data
    for key in ("heatmap", "points", "data"):
        value = data.get(key)
        if isinstance(value, list):
            return value
    return []


def parse_point(point: Any, event_id: int, weight_multiplier: float = 1.0, flip_axes: bool = False) -> dict[str, Any] | None:
    if isinstance(point, dict):
        x = to_float(point.get("x"))
        y = to_float(point.get("y"))
        value = to_float(point.get("value")) or 1.0
    elif isinstance(point, (list, tuple)) and len(point) >= 2:
        x = to_float(point[0])
        y = to_float(point[1])
        value = to_float(point[2]) if len(point) > 2 else 1.0
        value = value or 1.0
    else:
        return None
    if x is None or y is None:
        return None
    if flip_axes:
        x, y = y, x
    if not (-5 <= x <= 105 and -5 <= y <= 105):
        return None
    return {"event_id": event_id, "x": float(x), "y": float(y), "value": float(value) * float(weight_multiplier)}


def fetch_event_heatmap(event_id: int, player_id: int, cache_dir: str | Path, delay: float, no_fetch: bool = False, refresh_cache: bool = False) -> tuple[list[Any], str]:
    cp = cache_path(cache_dir, player_id, event_id)
    if cp.exists() and not refresh_cache:
        try:
            cached = json.loads(cp.read_text(encoding="utf-8"))
            if isinstance(cached, dict) and "raw_points" in cached:
                return cached.get("raw_points") or [], "cache"
        except Exception:
            pass
    if no_fetch:
        return [], "missing-cache"
    data = _get(f"event/{event_id}/player/{player_id}/heatmap", delay=delay)
    raw_points = extract_heatmap_points(data)
    payload = {
        "player_id": player_id,
        "event_id": event_id,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "raw_points": raw_points,
        "available": bool(raw_points),
    }
    cp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return raw_points, "fetch" if raw_points else "fetch-empty"


def build_aggregated_heatmap(player_id: int, events: list[dict[str, Any]], cache_dir: str | Path, delay: float, no_fetch: bool = False, refresh_cache: bool = False, flip_axes: bool = False) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    all_points: list[dict[str, Any]] = []
    event_results = []
    print("  Fetching/caching event heatmaps ...")
    for i, event in enumerate(events, start=1):
        event_id = int(event["event_id"])
        raw_points, source = fetch_event_heatmap(event_id, player_id, cache_dir, delay, no_fetch=no_fetch, refresh_cache=refresh_cache)
        parsed = []
        minutes = to_float(event.get("minutes")) or 90.0
        minutes_weight = max(0.05, min(1.0, minutes / 90.0))
        for p in raw_points:
            parsed_point = parse_point(p, event_id, weight_multiplier=minutes_weight, flip_axes=flip_axes)
            if parsed_point:
                parsed.append(parsed_point)
        all_points.extend(parsed)
        event_results.append({**event, "heatmap_source": source, "raw_points": len(raw_points), "parsed_points": len(parsed), "minutes_weight": round(minutes_weight, 4)})
        print(f"    [{i}/{len(events)}] event {event_id}: {len(parsed)} pts ({source})")
    return all_points, event_results


def merge_cells(points: list[dict[str, Any]], precision: int = 2) -> list[dict[str, Any]]:
    cells: dict[tuple[float, float], float] = defaultdict(float)
    for p in points:
        x = to_float(p.get("x"))
        y = to_float(p.get("y"))
        value = to_float(p.get("value")) or 1.0
        if x is None or y is None:
            continue
        cells[(round(x, precision), round(y, precision))] += value
    return [{"x": x, "y": y, "value": round(v, 6)} for (x, y), v in sorted(cells.items(), key=lambda kv: (kv[0][0], kv[0][1]))]


def weighted_share(points: list[dict[str, Any]], predicate) -> float:
    total = sum(to_float(p.get("value")) or 0.0 for p in points)
    if total <= 0:
        return 0.0
    part = sum((to_float(p.get("value")) or 0.0) for p in points if predicate(p))
    return float(part / total)


def weighted_mean(points: list[dict[str, Any]], coord: str) -> float | None:
    total = sum(to_float(p.get("value")) or 0.0 for p in points)
    if total <= 0:
        return None
    return float(sum((to_float(p.get(coord)) or 0.0) * (to_float(p.get("value")) or 0.0) for p in points) / total)


def weighted_std(points: list[dict[str, Any]], coord: str, mean: float | None) -> float | None:
    if mean is None:
        return None
    total = sum(to_float(p.get("value")) or 0.0 for p in points)
    if total <= 0:
        return None
    var = sum(((to_float(p.get(coord)) or 0.0) - mean) ** 2 * (to_float(p.get("value")) or 0.0) for p in points) / total
    return float(math.sqrt(var))



def summarize_heatmap_visual(points: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Visual/action-density summary only.

    This is not a tactical position estimate. Heatmaps can be biased by touch
    locations, defensive actions, set pieces, pressing, and team structure.
    """
    if not points:
        return {
            "centroid_x": None,
            "centroid_y": None,
            "spread_x": None,
            "spread_y": None,
            "point_count": 0,
            "total_weight": 0.0,
            "zone_shares": {},
            "note": "No heatmap points available.",
        }

    x = weighted_mean(points, "x")
    y = weighted_mean(points, "y")
    sx = weighted_std(points, "x", x)
    sy = weighted_std(points, "y", y)

    defensive_third = weighted_share(points, lambda p: (to_float(p["x"]) or 0) < 33.3)
    middle_third = weighted_share(points, lambda p: 33.3 <= (to_float(p["x"]) or 0) < 66.7)
    attacking_third = weighted_share(points, lambda p: (to_float(p["x"]) or 0) >= 66.7)

    # Sofascore heatmap y-axis is visual/action-density only here.
    left_lane = weighted_share(points, lambda p: (to_float(p["y"]) or 0) < 33.3)
    central_lane = weighted_share(points, lambda p: 33.3 <= (to_float(p["y"]) or 0) <= 66.7)
    right_lane = weighted_share(points, lambda p: (to_float(p["y"]) or 0) > 66.7)

    wide_lane = left_lane + right_lane
    high_wide_share = weighted_share(
        points,
        lambda p: (to_float(p["x"]) or 0) >= 60 and ((to_float(p["y"]) or 0) < 30 or (to_float(p["y"]) or 0) > 70),
    )
    deep_wide_share = weighted_share(
        points,
        lambda p: (to_float(p["x"]) or 0) < 60 and ((to_float(p["y"]) or 0) < 30 or (to_float(p["y"]) or 0) > 70),
    )
    central_high_share = weighted_share(points, lambda p: (to_float(p["x"]) or 0) >= 60 and 33 <= (to_float(p["y"]) or 0) <= 67)
    deep_central_share = weighted_share(points, lambda p: (to_float(p["x"]) or 0) < 60 and 33 <= (to_float(p["y"]) or 0) <= 67)

    total_weight = sum(to_float(p.get("value")) or 0.0 for p in points)

    return {
        "centroid_x": None if x is None else round(x, 4),
        "centroid_y": None if y is None else round(y, 4),
        "spread_x": None if sx is None else round(sx, 4),
        "spread_y": None if sy is None else round(sy, 4),
        "point_count": len(points),
        "total_weight": round(total_weight, 4),
        "zone_shares": {
            "defensive_third": round(defensive_third, 4),
            "middle_third": round(middle_third, 4),
            "attacking_third": round(attacking_third, 4),
            "left_lane": round(left_lane, 4),
            "central_lane": round(central_lane, 4),
            "right_lane": round(right_lane, 4),
            "wide_lane": round(wide_lane, 4),
            "high_wide_share": round(high_wide_share, 4),
            "deep_wide_share": round(deep_wide_share, 4),
            "central_high_share": round(central_high_share, 4),
            "deep_central_share": round(deep_central_share, 4),
        },
        "note": (
            "Visual/action-density summary only. Do not use this as a tactical "
            "position estimate; use Position_Arbitrator output for position."
        ),
    }


def estimate_position(points: list[dict[str, Any]], source_labels: dict[str, Any] | None = None) -> dict[str, Any]:
    if not points:
        return {"estimated_position": None, "estimated_role_group": None, "estimated_lane": "Unknown", "confidence": 0.0, "reason": "No heatmap points available."}
    x = weighted_mean(points, "x")
    y = weighted_mean(points, "y")
    sx = weighted_std(points, "x", x)
    sy = weighted_std(points, "y", y)
    defensive_third = weighted_share(points, lambda p: (to_float(p["x"]) or 0) < 33.3)
    middle_third = weighted_share(points, lambda p: 33.3 <= (to_float(p["x"]) or 0) < 66.7)
    attacking_third = weighted_share(points, lambda p: (to_float(p["x"]) or 0) >= 66.7)
    left_lane = weighted_share(points, lambda p: (to_float(p["y"]) or 0) < 33.3)
    central_lane = weighted_share(points, lambda p: 33.3 <= (to_float(p["y"]) or 0) <= 66.7)
    right_lane = weighted_share(points, lambda p: (to_float(p["y"]) or 0) > 66.7)
    wide_lane = left_lane + right_lane
    dominant_side = "Left" if left_lane > right_lane + 0.08 else "Right" if right_lane > left_lane + 0.08 else "Mixed"
    box_presence = weighted_share(points, lambda p: (to_float(p["x"]) or 0) >= 78 and 28 <= (to_float(p["y"]) or 0) <= 72)
    high_wide_share = weighted_share(points, lambda p: (to_float(p["x"]) or 0) >= 60 and ((to_float(p["y"]) or 0) < 30 or (to_float(p["y"]) or 0) > 70))
    deep_wide_share = weighted_share(points, lambda p: (to_float(p["x"]) or 0) < 60 and ((to_float(p["y"]) or 0) < 30 or (to_float(p["y"]) or 0) > 70))
    central_high_share = weighted_share(points, lambda p: (to_float(p["x"]) or 0) >= 60 and 33 <= (to_float(p["y"]) or 0) <= 67)
    deep_central_share = weighted_share(points, lambda p: (to_float(p["x"]) or 0) < 60 and 33 <= (to_float(p["y"]) or 0) <= 67)
    width_score = min(100.0, 100.0 * (wide_lane * 0.75 + high_wide_share * 0.75))
    forward_score = min(100.0, 100.0 * (attacking_third * 0.8 + box_presence * 0.6))
    centrality_score = min(100.0, 100.0 * (central_lane * 0.75 + central_high_share * 0.65))
    defensive_score = min(100.0, 100.0 * (defensive_third * 0.75 + deep_central_share * 0.30))
    wingback_score = min(100.0, 100.0 * (wide_lane * 0.55 + middle_third * 0.35 + attacking_third * 0.25 + defensive_third * 0.25))
    striker_score = min(100.0, 100.0 * (box_presence * 0.85 + central_high_share * 0.75 + attacking_third * 0.50))
    estimated_position = "Hybrid"
    estimated_group = "HYB"
    lane = "Mixed"
    reason_parts = []
    if x is not None and y is not None:
        if x < 22 and central_lane >= 0.45:
            estimated_position, estimated_group, lane = "GK/CB", "GK/CB", "Deep Central"
            reason_parts.append("very deep central heatmap")
        elif x < 43 and central_lane >= 0.45:
            estimated_position = "CB" if defensive_third >= 0.45 else "DM/CB"
            estimated_group = "CB" if defensive_third >= 0.45 else "DM"
            lane = "Central Defensive"
            reason_parts.append("deep central defensive heatmap")
        elif x < 58 and central_lane >= 0.45:
            estimated_position = "DM/CM"
            estimated_group = "DM" if defensive_third > attacking_third else "CM"
            lane = "Central Midfield"
            reason_parts.append("central midfield heatmap")
        elif wide_lane >= 0.52 and attacking_third >= 0.45:
            side_code = "LW" if dominant_side == "Left" else "RW" if dominant_side == "Right" else "W"
            estimated_position, estimated_group = side_code, "AM-W"
            lane = f"{dominant_side} Wide Attack" if dominant_side != "Mixed" else "Wide Attack"
            reason_parts.append("wide and advanced heatmap")
        elif wide_lane >= 0.52 and middle_third + defensive_third >= 0.50:
            side_code = "LWB/LB" if dominant_side == "Left" else "RWB/RB" if dominant_side == "Right" else "WB/FB"
            estimated_position, estimated_group = side_code, "WB/FB"
            lane = f"{dominant_side} Wide Defensive" if dominant_side != "Mixed" else "Wide Defensive"
            reason_parts.append("wide heatmap with deeper progression")
        elif striker_score >= 55 and central_high_share >= 0.35:
            estimated_position, estimated_group, lane = "ST", "ST", "Central Forward"
            reason_parts.append("central advanced/box heatmap")
        elif attacking_third >= 0.45 and central_lane >= 0.42:
            estimated_position, estimated_group, lane = "AM-C/ST-SS", "AM-C", "Central Attack"
            reason_parts.append("central advanced heatmap")
        elif central_lane >= 0.42:
            estimated_position, estimated_group, lane = "CM", "CM", "Central"
            reason_parts.append("central heatmap")
        else:
            reason_parts.append("mixed spatial profile")
    total_weight = sum(to_float(p.get("value")) or 0.0 for p in points)
    n_points = len(points)
    sample_conf = min(1.0, math.log1p(n_points) / math.log1p(250))
    dominance = max(left_lane, central_lane, right_lane)
    phase_dominance = max(defensive_third, middle_third, attacking_third)
    clarity = dominance * 0.55 + phase_dominance * 0.45
    confidence = max(0.0, min(0.95, 0.25 + 0.45 * sample_conf + 0.30 * clarity))
    label_note = ""
    if source_labels:
        role_mode = str(source_labels.get("role_position_mode") or source_labels.get("player_position_mode") or "").upper()
        profile_mode = str(source_labels.get("profile_position_mode") or "").upper()
        if role_mode or profile_mode:
            label_note = f"source labels profile={profile_mode or '?'} role={role_mode or '?'}"
    return {
        "estimated_position": estimated_position,
        "estimated_role_group": estimated_group,
        "estimated_lane": lane,
        "confidence": round(confidence, 4),
        "reason": "; ".join(reason_parts + ([label_note] if label_note else [])),
        "spatial_summary": {"centroid_x": None if x is None else round(x, 4), "centroid_y": None if y is None else round(y, 4), "spread_x": None if sx is None else round(sx, 4), "spread_y": None if sy is None else round(sy, 4), "point_count": n_points, "total_weight": round(total_weight, 4)},
        "zone_shares": {"defensive_third": round(defensive_third, 4), "middle_third": round(middle_third, 4), "attacking_third": round(attacking_third, 4), "left_lane": round(left_lane, 4), "central_lane": round(central_lane, 4), "right_lane": round(right_lane, 4), "wide_lane": round(wide_lane, 4), "dominant_side": dominant_side, "box_presence_proxy": round(box_presence, 4), "high_wide_share": round(high_wide_share, 4), "deep_wide_share": round(deep_wide_share, 4), "central_high_share": round(central_high_share, 4), "deep_central_share": round(deep_central_share, 4)},
        "scores": {"width_score": round(width_score, 2), "forward_score": round(forward_score, 2), "centrality_score": round(centrality_score, 2), "defensive_score": round(defensive_score, 2), "wingback_score": round(wingback_score, 2), "striker_score": round(striker_score, 2)},
    }


def fetch_profile_name(player_id: int, delay: float) -> str | None:
    data = _get(f"player/{player_id}", delay=delay)
    if isinstance(data, dict):
        player = data.get("player") if isinstance(data.get("player"), dict) else data
        return player.get("name") or player.get("shortName")
    return None


def write_cells_csv(path: str | Path, cells: list[dict[str, Any]]) -> None:
    with Path(path).open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["x", "y", "value"])
        writer.writeheader()
        writer.writerows(cells)


def main() -> None:
    global REQUEST_DELAY
    ap = argparse.ArgumentParser(description="Build player season visual/action-density heatmap from per-event Sofascore heatmaps.")
    ap.add_argument("--player-id", "-p", type=int, required=True)
    ap.add_argument("--season", "-s", default="2025-26")
    ap.add_argument("--league", "-l", default=None)
    ap.add_argument("--match-log", "-i", default=DEFAULT_MATCH_LOG)
    ap.add_argument("--out-dir", default=".")
    ap.add_argument("--cache-dir", default=DEFAULT_CACHE_DIR)
    ap.add_argument("--delay", type=float, default=REQUEST_DELAY)
    ap.add_argument("--format", choices=["json", "csv", "both"], default="both")
    ap.add_argument("--precision", type=int, default=2)
    ap.add_argument("--no-fetch", action="store_true", help="Use cache only; do not call Sofascore.")
    ap.add_argument("--refresh-cache", action="store_true", help="Refetch even if cached.")
    ap.add_argument("--flip-axes", action="store_true", help="Swap x/y if endpoint axes are visually determined to be flipped.")
    args = ap.parse_args()
    REQUEST_DELAY = max(0.0, args.delay)
    rows = read_rows(args.match_log)
    player_rows = filter_rows(rows, args.player_id, args.season, args.league)
    events = extract_events(player_rows)
    source_labels = summarize_source_labels(player_rows)
    if not player_rows:
        raise ValueError("No match-log rows found for that player/season/league.")
    if not events:
        raise ValueError("No event IDs found for that player/season/league.")
    player_name = first_present(player_rows[0], ["player_name", "profile_name", "player", "name", "short_name"])
    if not player_name and not args.no_fetch:
        player_name = fetch_profile_name(args.player_id, args.delay)
    player_name = str(player_name or args.player_id)
    print(f"\nBuilding heatmap for {player_name} ({args.player_id})")
    print(f"Season: {args.season} | League: {args.league or 'Any'}")
    print(f"Events found: {len(events)}")
    print(f"Cache dir: {args.cache_dir}\n")
    points, event_results = build_aggregated_heatmap(player_id=args.player_id, events=events, cache_dir=args.cache_dir, delay=args.delay, no_fetch=args.no_fetch, refresh_cache=args.refresh_cache, flip_axes=args.flip_axes)
    cells = merge_cells(points, precision=args.precision)
    visual_summary = summarize_heatmap_visual(cells)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    base = f"{clean_filename(player_name)}_{args.player_id}_{str(args.season).replace('/', '-')}_heatmap_position"
    payload = {
        "meta": {"player_id": args.player_id, "player_name": player_name, "season": args.season, "league": args.league, "match_log": str(args.match_log), "events_found": len(events), "events_with_heatmap": sum(1 for e in event_results if e["parsed_points"] > 0), "cache_dir": str(args.cache_dir), "no_fetch": bool(args.no_fetch), "refresh_cache": bool(args.refresh_cache), "flip_axes": bool(args.flip_axes), "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")},
        "source_position_labels": source_labels,
        "position_estimate": None,
        "position_note": "Visual heatmap only. Tactical position is intentionally not estimated here; use Position_Arbitrator output.",
        "visual_summary": visual_summary,
        "event_results": event_results,
        "heatmap": {"cell_count": len(cells), "raw_point_count": len(points), "precision": args.precision, "points": cells, "note": "Aggregated from /event/{event_id}/player/{player_id}/heatmap, weighted by match minutes when available."},
    }
    written = []
    if args.format in {"json", "both"}:
        json_path = out_dir / f"{base}.json"
        json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        written.append(json_path)
    if args.format in {"csv", "both"}:
        csv_path = out_dir / f"{base}_cells.csv"
        write_cells_csv(csv_path, cells)
        written.append(csv_path)
    print("\n" + "─" * 60)
    print(f"Player:       {player_name}")
    print(f"Events:       {len(events)}")
    print(f"Heatmaps:     {payload['meta']['events_with_heatmap']}/{len(events)}")
    print(f"Cells:        {len(cells)}")
    print("Position:     not estimated by this script")
    print("Note:         visual/action-density heatmap only; use Position_Arbitrator for position")
    print(f"Centroid:     x={visual_summary.get('centroid_x')} y={visual_summary.get('centroid_y')}")
    print("Outputs:")
    for p in written:
        print(f"  {p}")
    print("─" * 60)


if __name__ == "__main__":
    main()
