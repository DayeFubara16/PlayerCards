from __future__ import annotations

"""
Build_Position_Context.py
=========================
v2 — adds per-event possession context for downstream P-Adj calculation.

Purpose
-------
Produces TWO outputs every run:

  1. position_context_by_event.json  — the JSON cache (spatial + possession)
  2. player_match_logs.csv (enriched) — the input CSV with two new columns
     joined in so the file stays connected to the pipeline:

       oppo_poss_pct   float  opponent possession % for that match row
                              (= away_pct when venue=="home", home_pct otherwise)
       poss_available  bool   True if possession was resolved for this event

     Player_Collapse reads this enriched CSV directly and applies P-Adj using
     oppo_poss_pct without needing to open the JSON.

Pipeline
--------
    player_match_logs.csv  ←─────────────────────────────────┐
        │                                                     │ enriched
        ▼                                                     │ CSV out
    Build_Position_Context.py          ← you are here        │
        │                                                     │
        ├── position_context_by_event.json  (→ Arbitrator)   │
        │                                                     │
        └── player_match_logs.csv (enriched) ────────────────┘
                │
                ▼
            Add_Player_Ages.py
                │
                ▼
            Player_Collapse.py   ← reads oppo_poss_pct, applies P-Adj before per-90
                │
                ▼
            player_season_totals.csv   (contains *_padj columns)
                │
                ▼
            Position_Arbitrator
                --position-context position_context_by_event.json

Enriched CSV columns added
--------------------------
    oppo_poss_pct   float | NaN
        Opponent possession percentage for this specific match row.
        Derived from the match statistics endpoint as:
            venue == "home"  →  oppo_poss_pct = away_pct
            venue == "away"  →  oppo_poss_pct = home_pct
        NaN when the statistics endpoint was unavailable for this event.

    poss_available  bool
        True  → oppo_poss_pct is populated and reliable
        False → statistics endpoint returned nothing; oppo_poss_pct is NaN

P-Adj formula (applied in Player_Collapse per match row, before summing)
------------------------------------------------------------------------
    padj_scalar = oppo_poss_pct / 50.0
    stat_padj   = stat_raw * padj_scalar

    This normalises every player to a hypothetical 50/50 possession baseline.
    A player on a 35% possession team gets their defensive stats scaled down
    (they faced more ball but that was expected); a player on a 65% possession
    team gets scaled up.

Stats targeted for P-Adj (applied in Player_Collapse):
    tackles_total, tackles_won, last_man_tackles,
    interceptions, clearances, clearance_off_line,
    blocked_shots, recoveries,
    duels_total, duels_won, duels_lost,
    aerial_duels_total, aerial_duels_won, aerial_duels_lost,
    challenges_lost, errors_leading_to_shot, errors_leading_to_goal

JSON possession block shape (per event)
---------------------------------------
    "possession": {
        "home_pct": 58.3,
        "away_pct": 41.7,
        "source": "match-statistics"
    }

Schema
------
v1: event_id → { players, diagnostics }
v2: event_id → { players, possession, diagnostics }
    meta.schema = "position_context_by_event.v2"

Backward compatibility
----------------------
v1 files are read and upgraded in-place: events without a possession block
are fetched on the next run unless --skip-possession is passed.

Endpoints used
--------------
    /event/{event_id}/average-positions   (spatial, unchanged from v1)
    /event/{event_id}/statistics          (possession, NEW in v2)

Heatmap fallback is OFF by default (expensive, 404-prone).

Typical usage
-------------
python Build_Position_Context.py \\
  --input data/raw/player_match_logs.csv \\
  --output data/context/position_context_by_event.json \\
  --enrich-csv data/raw/player_match_logs.csv \\
  --season 2025-26 \\
  --delay 1.2

Omit --enrich-csv to skip writing the enriched CSV (JSON-only run).
Pass --skip-possession for a position-only run (v1 behaviour).
Pass --refresh-possession to backfill possession on a v1 cache without
re-hitting the average-positions endpoint.
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

SCHEMA_VERSION = "position_context_by_event.v2"

# Defensive counting stats that receive P-Adj treatment in Player_Collapse.
# Stored here as the canonical reference so both files stay in sync.
PADJ_DEFENSIVE_STATS = [
    "tackles_total", "tackles_won", "last_man_tackles",
    "interceptions",
    "clearances", "clearance_off_line", "blocked_shots",
    "recoveries",
    "duels_total", "duels_won", "duels_lost",
    "aerial_duels_total", "aerial_duels_won", "aerial_duels_lost",
    "challenges_lost",
    "errors_leading_to_shot", "errors_leading_to_goal",
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
                "schema": SCHEMA_VERSION,
                "sources": ["event_average_positions", "match_statistics"],
            },
            "events": {},
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        print(f"[WARN] Could not parse existing context at {path}; starting fresh.")
        return {"meta": {"schema": SCHEMA_VERSION}, "events": {}}

    if "events" not in payload or not isinstance(payload["events"], dict):
        # Backward compatibility with bare event map (pre-meta).
        payload = {"meta": {"schema": SCHEMA_VERSION}, "events": payload}
    payload.setdefault("meta", {})
    payload.setdefault("events", {})

    # Upgrade v1 → v2: possession blocks will be back-filled on next run for
    # any event that lacks them (i.e. the event block has no "possession" key).
    if payload["meta"].get("schema") == "position_context_by_event.v1":
        print("[INFO] Upgrading cache schema v1 → v2; possession will be fetched for uncached events.")
        payload["meta"]["schema"] = SCHEMA_VERSION

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


def fetch_match_possession(event_id: int) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """
    Fetch home/away possession percentages from /event/{event_id}/statistics.

    Sofascore's statistics payload is a list of period groups, each containing
    a "groups" list of stat categories. Possession lives in the category whose
    "groupName" matches "possession" (case-insensitive), under a stat item
    whose "name" matches "Ball possession" (or similar). Values are returned
    as strings like "58%" for the home side and "42%" for the away side.

    Returns
    -------
    possession : dict or None
        {
            "home_pct": float,   # e.g. 58.3
            "away_pct": float,   # e.g. 41.7
            "source": "match-statistics",
        }
        None if the endpoint is unavailable or no possession stat is found.

    diagnostics : dict
        Fetch metadata for audit/debugging stored in the event block.

    Resolving opponent possession in Player_Collapse
    ------------------------------------------------
    venue == "home"  →  oppo_poss_pct = away_pct
    venue == "away"  →  oppo_poss_pct = home_pct
    padj_scalar = oppo_poss_pct / 50.0
    """
    url = f"{API_BASE}/event/{event_id}/statistics"
    payload = get_json(url, quiet_404=True)

    diagnostics: dict[str, Any] = {
        "endpoint": "match-statistics",
        "event_id": event_id,
        "available": payload is not None,
        "possession_found": False,
    }

    if payload is None:
        return None, diagnostics

    # Sofascore wraps statistics in {"statistics": [...]} at the top level.
    stat_groups: list[Any] = []
    if isinstance(payload, dict):
        stat_groups = payload.get("statistics", payload.get("groups", []))
    elif isinstance(payload, list):
        stat_groups = payload

    home_pct: float | None = None
    away_pct: float | None = None

    # Sofascore returns possession inside "Match overview" (not a dedicated
    # possession group).  Matching on groupName is therefore fragile — instead
    # we scan every statisticsItem across all groups and periods and match on
    # the stable `key` field ("ballPossession") with item `name` as fallback.
    for period_block in stat_groups:
        if not isinstance(period_block, dict):
            continue
        for category in period_block.get("groups", []):
            if not isinstance(category, dict):
                continue
            for item in category.get("statisticsItems", []):
                if not isinstance(item, dict):
                    continue
                item_key  = str(item.get("key",  "")).lower()
                item_name = str(item.get("name", "")).lower()
                if item_key != "ballpossession" and "possession" not in item_name:
                    continue
                # Prefer the numeric homeValue/awayValue fields; fall back to
                # the display strings ("58%") which parse_float strips fine.
                home_val = parse_float(item.get("homeValue") if item.get("homeValue") is not None
                                       else item.get("home"))
                away_val = parse_float(item.get("awayValue") if item.get("awayValue") is not None
                                       else item.get("away"))
                if home_val is not None and away_val is not None:
                    home_pct = round(home_val, 2)
                    away_pct = round(away_val, 2)
                    break
            if home_pct is not None:
                break
        if home_pct is not None:
            break

    if home_pct is None or away_pct is None:
        diagnostics["possession_found"] = False
        return None, diagnostics

    # Sanity check: possession should sum to ~100 and each side be in 0-100.
    total = home_pct + away_pct
    if not (85.0 <= total <= 115.0):
        diagnostics["possession_found"] = False
        diagnostics["sanity_fail"] = f"home={home_pct} + away={away_pct} = {total} (expected ~100)"
        return None, diagnostics

    diagnostics["possession_found"] = True
    diagnostics["home_pct"] = home_pct
    diagnostics["away_pct"] = away_pct

    return {
        "home_pct": home_pct,
        "away_pct": away_pct,
        "source": "match-statistics",
    }, diagnostics


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


def enrich_match_logs(
    df_full: pd.DataFrame,
    events_ctx: dict[str, Any],
) -> pd.DataFrame:
    """
    Join oppo_poss_pct and poss_available onto every row of the full match
    log DataFrame using the possession blocks already in events_ctx.

    Resolution rule (per row):
        venue == "home"  →  oppo_poss_pct = away_pct
        venue == "away"  →  oppo_poss_pct = home_pct
        venue missing / unrecognised  →  NaN (poss_available = False)

    Rows whose event_id has no possession block (endpoint unavailable, or
    --skip-possession was used) get NaN / False.

    The join is done on event_id only — no player-level lookup needed because
    possession is a match-level value shared by all players in that match.
    """
    out = df_full.copy()

    # Build a lookup: event_id (int) → (home_pct, away_pct) | (None, None)
    poss_lookup: dict[int, tuple[float | None, float | None]] = {}
    for key, block in events_ctx.items():
        eid = int_or_none(key)
        if eid is None:
            continue
        poss = block.get("possession") if isinstance(block, dict) else None
        if isinstance(poss, dict):
            poss_lookup[eid] = (poss.get("home_pct"), poss.get("away_pct"))
        else:
            poss_lookup[eid] = (None, None)

    oppo_poss_vals: list[float | None] = []
    poss_avail_vals: list[bool] = []

    venue_col = "venue" if "venue" in out.columns else None

    for _, row in out.iterrows():
        eid = int_or_none(row.get("event_id"))
        if eid is None or eid not in poss_lookup:
            oppo_poss_vals.append(None)
            poss_avail_vals.append(False)
            continue

        home_pct, away_pct = poss_lookup[eid]
        if home_pct is None or away_pct is None:
            oppo_poss_vals.append(None)
            poss_avail_vals.append(False)
            continue

        venue = str(row.get(venue_col, "")).strip().lower() if venue_col else ""
        if venue == "home":
            oppo_poss_vals.append(away_pct)
            poss_avail_vals.append(True)
        elif venue == "away":
            oppo_poss_vals.append(home_pct)
            poss_avail_vals.append(True)
        else:
            # venue column absent or unrecognised value — cannot resolve side
            oppo_poss_vals.append(None)
            poss_avail_vals.append(False)

    out["oppo_poss_pct"] = oppo_poss_vals
    out["poss_available"] = poss_avail_vals

    return out


def build_position_context(
    input_csv: str,
    output_json: str,
    enrich_csv: str | None,
    season: str | None,
    league: str | None,
    refresh: bool,
    refresh_possession: bool,
    skip_possession: bool,
    use_heatmaps_for_missing: bool,
    player_limit_per_event: int | None,
    write_every: int,
) -> dict[str, Any]:
    # Read the full CSV first — we need it unfiltered for the enriched output.
    df_full = pd.read_csv(input_csv)
    df = filter_input(df_full, season=season, league=league)

    if df.empty:
        raise ValueError("No rows left after season/league filters.")

    event_players = unique_event_player_pairs(df)

    out_path = Path(output_json)
    context = load_existing_context(out_path)
    events_ctx = context.setdefault("events", {})

    context["meta"].update({
        "schema": SCHEMA_VERSION,
        "input_csv": input_csv,
        "enrich_csv": enrich_csv,
        "season_filter": season,
        "league_filter": league,
        "use_heatmaps_for_missing": use_heatmaps_for_missing,
        "skip_possession": skip_possession,
        "padj_defensive_stats": PADJ_DEFENSIVE_STATS,
    })

    event_ids = sorted(event_players)
    print(f"Found {len(event_ids)} event(s) in input after filters.")
    if skip_possession:
        print("[INFO] --skip-possession set: possession will not be fetched.")

    fetched = 0
    skipped = 0
    avg_success = 0
    heatmap_success = 0
    possession_success = 0
    possession_skipped = 0

    for i, event_id in enumerate(event_ids, start=1):
        key = str(event_id)
        existing = events_ctx.get(key)

        need_positions = refresh or not existing
        need_possession = (
            not skip_possession
            and (refresh or refresh_possession or not existing or "possession" not in (existing or {}))
        )

        if not need_positions and not need_possession:
            skipped += 1
            continue

        print(f"[{i}/{len(event_ids)}] event {event_id}", end="")
        if not need_positions:
            print(" [positions cached]", end="")
        if not need_possession:
            print(" [possession cached]", end="")
        print()

        # ── Spatial average positions ──────────────────────────────────────
        if need_positions:
            players, avg_diag = fetch_average_positions(event_id)
            if players:
                avg_success += 1

            event_block: dict[str, Any] = {
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

            # Carry forward any cached possession block.
            if existing and "possession" in existing:
                event_block["possession"] = existing["possession"]

        else:
            event_block = dict(existing)  # positions cached; preserve full block

        # ── Possession ────────────────────────────────────────────────────
        if need_possession:
            poss_record, poss_diag = fetch_match_possession(event_id)
            event_block["possession"] = poss_record  # None if unavailable
            event_block.setdefault("diagnostics", {})["match_statistics"] = poss_diag
            if poss_record is not None:
                possession_success += 1
                print(f"  possession: home={poss_record['home_pct']}%  away={poss_record['away_pct']}%")
            else:
                print("  possession: unavailable")
        else:
            possession_skipped += 1

        events_ctx[key] = event_block
        fetched += 1

        if write_every > 0 and fetched % write_every == 0:
            write_context(context, out_path)
            print(f"  [checkpoint] wrote {out_path}")

    # ── Final JSON write ───────────────────────────────────────────────────
    write_context(context, out_path)

    # ── Enriched CSV output ────────────────────────────────────────────────
    # Always join from the *full* unfiltered DataFrame so the enriched file
    # contains all leagues/seasons, not just the filtered subset processed
    # in this run.  Events outside the filter still get possession resolved
    # from whatever is already cached in events_ctx.
    if enrich_csv:
        enrich_path = Path(enrich_csv)
        df_enriched = enrich_match_logs(df_full, events_ctx)
        df_enriched.to_csv(enrich_path, index=False)
        resolved = df_enriched["poss_available"].sum()
        total_rows = len(df_enriched)
        print(f"\nEnriched CSV:                   {enrich_path}")
        print(f"Rows with oppo_poss_pct:        {resolved}/{total_rows} "
              f"({100 * resolved / total_rows:.1f}%)")

    print("\nDone.")
    print(f"JSON context:                   {out_path}")
    print(f"Events processed (any fetch):   {fetched}")
    print(f"Events fully skipped (cached):  {skipped}")
    print(f"Events with avg-position data:  {avg_success}")
    print(f"Heatmap player fallbacks:       {heatmap_success}")
    if not skip_possession:
        print(f"Events with possession data:    {possession_success}")
        print(f"Events possession skipped:      {possession_skipped}")

    return context


def main() -> None:
    global REQUEST_DELAY

    ap = argparse.ArgumentParser(
        description=(
            "Build cached event/player context for the analytics pipeline. "
            "v2: fetches per-event possession and writes an enriched match log "
            "CSV (with oppo_poss_pct) to keep the file in-stream for Player_Collapse."
        )
    )
    ap.add_argument("--input", "-i", default="player_match_logs.csv",
                    help="Input match-level CSV with event_id/player_id/venue.")
    ap.add_argument("--output", "-o", default="position_context_by_event.json",
                    help="Output JSON context cache path.")
    ap.add_argument("--enrich-csv", default=None,
                    help=(
                        "Path to write the enriched match-log CSV with oppo_poss_pct "
                        "and poss_available joined in. Typically the same path as --input "
                        "so it overwrites in-place and stays connected to the pipeline. "
                        "Omit to skip the CSV output (JSON-only run)."
                    ))
    ap.add_argument("--season", "-s", default=None,
                    help="Optional season filter (controls which events are fetched; "
                         "enriched CSV always covers all rows in --input).")
    ap.add_argument("--league", "-l", default=None,
                    help="Optional league filter (same scoping as --season).")
    ap.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                    help=f"Delay after successful requests (seconds). Default: {DEFAULT_DELAY}.")
    ap.add_argument("--refresh", action="store_true",
                    help="Refetch ALL data (positions + possession) even if already cached.")
    ap.add_argument("--refresh-possession", action="store_true",
                    help="Refetch only possession for events that already have positions cached. "
                         "Useful for backfilling v1 → v2 caches.")
    ap.add_argument("--skip-possession", action="store_true",
                    help="Skip possession fetch entirely (position-only run, v1 behaviour). "
                         "Enriched CSV will still be written but oppo_poss_pct will reflect "
                         "only what is already cached.")
    ap.add_argument("--use-heatmaps-for-missing", action="store_true",
                    help="Expensive fallback: fetch per-player heatmaps for players missing "
                         "average-position data.")
    ap.add_argument("--player-limit-per-event", type=int, default=None,
                    help="Optional cap for heatmap fallback players per event.")
    ap.add_argument("--write-every", type=int, default=5,
                    help="Checkpoint JSON after this many processed events. Default: 5.")
    args = ap.parse_args()

    REQUEST_DELAY = max(0.0, args.delay)

    build_position_context(
        input_csv=args.input,
        output_json=args.output,
        enrich_csv=args.enrich_csv,
        season=args.season,
        league=args.league,
        refresh=args.refresh,
        refresh_possession=args.refresh_possession,
        skip_possession=args.skip_possession,
        use_heatmaps_for_missing=args.use_heatmaps_for_missing,
        player_limit_per_event=args.player_limit_per_event,
        write_every=args.write_every,
    )


if __name__ == "__main__":
    main()