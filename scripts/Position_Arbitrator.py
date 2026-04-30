"""
Position_Arbitrator.py
──────────────────────
Resolve conflicting player position sources into a final comparison cohort.

Problem this solves
-------------------
Some players are stored as ST/AM/etc. from match lineup data even though their
Sofascore profile or heatmap clearly indicates a wide role. Example: a player
with profile position RW may have match rows logged as ST.

This script preserves all source labels and adds arbitration fields:

  • profile_position_raw
  • profile_position_group
  • match_position_mode
  • match_positions_played
  • match_role_mode
  • match_roles_played
  • arbitrated_position
  • arbitrated_role_group
  • arbitrated_lane
  • arbitrated_confidence
  • position_conflict_flag
  • arbitration_reason

It is intentionally conservative:
  - it does not overwrite your raw position columns
  - it flags conflicts for review
  - it upgrades obvious wide attackers/fullbacks into wide cohorts

V2/V3 additions:
  - confidence is now an evidence-strength score, not a fixed label
  - optional spatial average-position columns are used when available
  - optional --position-context JSON can merge cached event/player average positions
  - spatial evidence is preferred over expensive per-player heatmaps
  - V3 downweights upstream heuristic position labels and prevents winger-like forwards from being locked to ST
  - V4 also prevents elite winger-creators from being mislabeled AM-HYB just because central evidence is high
  - V5 slightly lowers the winger-style threshold and labels AM-W lanes as Wide Forward
  - V6 adds family-first classification: defender, midfielder, attacker, then role split
  - V7 adds modern defender refinement: CB-FB hybrids and FB/WB separation
  - V8 adds a wide-defender gate before midfield classification to catch WB/RWB-style players misread as CM
  - V9 adds creative wide-defender rescue (Grimaldo/Dimarco archetype) and better ST vs SS separation
  - V10 prioritizes cached average-position context from Build_Position_Context.py when available
  - V11 collapses event-level position_context_by_event.json into season-level player average positions when input has no event_id
  - V11 guarded: season spatial evidence is merged, but does not blindly override strong style/role evidence

Recommended downstream use
--------------------------
Use `arbitrated_position` or `arbitrated_role_group` as the cohort column in
similarity/percentiles/role classification.

Typical usage
-------------
  python Position_Arbitrator.py --input player_match_logs_with_ages.csv --output player_match_logs_arbitrated.csv
  python Position_Arbitrator.py --input player_season_totals.csv --output player_season_totals_arbitrated.csv
  python Position_Arbitrator.py --input player_season_totals.csv --player-id 12345 --format both
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PROFILE_POSITION_COLUMNS = [
    "profile_position",
    "profile_position_raw",
    "canonical_position",
    "sofascore_position",
    "position",
    "player_position",
]

MATCH_ROLE_COLUMNS = [
    "role_position",
    "primary_role_position",
    "secondary_role_position",
]

MATCH_POSITION_COLUMNS = [
    "player_position",
    "position",
    "lineup_position",
    "match_position",
    "base_position",
]

MINUTES_COL = "minutes_played"

SPATIAL_X_COLUMNS = [
    "avg_x", "average_x", "averageX", "position_x", "positionX",
    "average_position_x", "heatmap_avg_x",
]
SPATIAL_Y_COLUMNS = [
    "avg_y", "average_y", "averageY", "position_y", "positionY",
    "average_position_y", "heatmap_avg_y",
]

POSITION_CONTEXT_SOURCE_COL = "position_context_source"


POSITION_ALIASES = {
    # Goalkeeper
    "G": "GK", "GK": "GK", "GOALKEEPER": "GK",

    # Centre backs
    "CB": "CB", "DC": "CB", "D C": "CB", "DEFENDER CENTRE": "CB", "CENTRE BACK": "CB",
    "CENTER BACK": "CB",

    # Fullbacks
    "RB": "RB", "DR": "RB", "D R": "RB", "RIGHT BACK": "RB",
    "LB": "LB", "DL": "LB", "D L": "LB", "LEFT BACK": "LB",
    "FB": "FB", "FULL BACK": "FB",

    # Wing backs
    "RWB": "RWB", "WBR": "RWB", "WB R": "RWB", "D/WB R": "RWB",
    "LWB": "LWB", "WBL": "LWB", "WB L": "LWB", "D/WB L": "LWB",
    "WB": "WB", "WING BACK": "WB",

    # Defensive mids
    "DM": "DM", "DMC": "DM", "DM C": "DM", "DEFENSIVE MIDFIELDER": "DM",

    # Central mids
    "CM": "CM", "MC": "CM", "M C": "CM", "MIDFIELDER CENTRE": "CM",
    "LCM": "CM", "RCM": "CM",

    # Wide midfielders
    "RM": "RM", "MR": "RM", "M R": "RM", "RIGHT MIDFIELDER": "RM",
    "LM": "LM", "ML": "LM", "M L": "LM", "LEFT MIDFIELDER": "LM",
    "WM": "WM",

    # Attacking mids
    "AM": "AM", "AMC": "AMC", "AM C": "AMC", "ATTACKING MIDFIELDER": "AM",
    "CAM": "AMC",
    "RAM": "AMR", "AMR": "AMR", "AM R": "AMR",
    "LAM": "AML", "AML": "AML", "AM L": "AML",

    # Wingers / wide forwards
    "RW": "RW", "RIGHT WINGER": "RW", "RIGHT FORWARD": "RW", "RF": "RW",
    "LW": "LW", "LEFT WINGER": "LW", "LEFT FORWARD": "LW", "LF": "LW",
    "W": "W", "WINGER": "W",

    # Strikers
    "ST": "ST", "CF": "ST", "F": "ST", "FW": "ST", "FORWARD": "ST",
    "STRIKER": "ST", "CENTRE FORWARD": "ST", "CENTER FORWARD": "ST",
    "SS": "SS", "SECOND STRIKER": "SS",
}


SIDE_MAP = {
    "RW": "Right", "AMR": "Right", "RM": "Right", "RWB": "Right", "RB": "Right",
    "LW": "Left", "AML": "Left", "LM": "Left", "LWB": "Left", "LB": "Left",
}


GROUP_MAP = {
    "GK": "GK",
    "CB": "CB",
    "RB": "FB", "LB": "FB", "FB": "FB",
    "RWB": "WB", "LWB": "WB", "WB": "WB",
    "DM": "DM",
    "CM": "CM",
    "RM": "WM", "LM": "WM", "WM": "WM",
    "AMC": "AM-C", "AM": "AM-C",
    "AMR": "AM-W", "AML": "AM-W", "RW": "AM-W", "LW": "AM-W", "W": "AM-W",
    "ST": "ST", "SS": "ST-SS",
}


WIDE_ATTACK_CODES = {"RW", "LW", "AMR", "AML", "RM", "LM", "W"}
WIDE_DEF_CODES = {"RB", "LB", "RWB", "LWB"}
CENTRAL_ATTACK_CODES = {"AMC", "AM", "SS"}
STRIKER_CODES = {"ST", "CF"}


def normalize_position(value: Any) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    clean = text.upper()
    clean = clean.replace("-", " ").replace("_", " ").replace("/", " / ")
    clean = re.sub(r"\s+", " ", clean).strip()

    # Compact forms like "D/WB R" can be handled manually.
    compact = clean.replace(" ", "")
    if compact in {"D/WBR", "WBR"}:
        return "RWB"
    if compact in {"D/WBL", "WBL"}:
        return "LWB"

    return POSITION_ALIASES.get(clean) or POSITION_ALIASES.get(compact) or clean


def position_group(code: str | None) -> str | None:
    if not code:
        return None
    return GROUP_MAP.get(code, code)


def position_side(code: str | None) -> str | None:
    return SIDE_MAP.get(code or "")


def find_first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def row_number(row: pd.Series, candidates: list[str]) -> float | None:
    lower_map = {str(c).lower(): c for c in row.index}
    for cand in candidates:
        actual = lower_map.get(cand.lower())
        if actual is None:
            continue
        try:
            v = float(row.get(actual))
            return None if math.isnan(v) else v
        except Exception:
            continue
    return None


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def score_gap_strength(a: float, b: float, scale: float = 35.0) -> float:
    """Return 0..1 strength based on the absolute gap between two evidence scores."""
    return clamp(abs(float(a) - float(b)) / scale)


def source_agreement_strength(candidates: list[str | None]) -> float:
    groups = [position_group(c) for c in candidates if c]
    groups = [g for g in groups if g]
    if not groups:
        return 0.0
    unique = set(groups)
    if len(unique) == 1:
        return 1.0
    if len(unique) == 2:
        return 0.45
    return 0.2


GENERIC_POSITION_CODES = {"D", "M", "F", "FW", "DEFENDER", "MIDFIELDER", "FORWARD", None}
HEURISTIC_SOURCE_TOKENS = {"heuristic_stats", "heuristic_base"}


def is_heuristic_position_source(row: pd.Series) -> bool:
    src = str(row.get("position_source") or "").strip().lower()
    return any(tok in src for tok in HEURISTIC_SOURCE_TOKENS)


def is_generic_position(code: str | None) -> bool:
    return code in GENERIC_POSITION_CODES


def winger_like_from_style(
    profile_code: str | None,
    match_pos: str | None,
    match_role: str | None,
    wide_score: float,
    central_score: float,
    row: pd.Series,
) -> tuple[bool, list[str]]:
    """
    Conservative style-based winger flag for cases where the source position is generic
    or was created by our own heuristic rather than by true lineup/spatial data.

    Key idea:
    - Do not let a high central-creation score automatically make someone central.
      Wingers can also have high xA, key passes, and final-third passing.
    - Require a high absolute wide score plus at least one concrete wide/take-on signal.
    """
    reasons = []
    # Prefer per90 columns explicitly; season-total inputs often contain both raw totals and per90s.
    crosses = row_number(row, ["crosses_total_per90", "crosses_total_p90"]) or 0.0
    takeons = row_number(row, ["dribbles_attempted_per90", "dribbles_attempted_p90", "contests_total_per90", "contests_total_p90"]) or 0.0
    takeons_won = row_number(row, ["dribbles_completed_per90", "dribbles_completed_p90", "contests_won_per90", "contests_won_p90"]) or 0.0
    prog_carries = row_number(row, ["progressive_carries_per90", "progressive_carries_p90"]) or 0.0
    prog_dist = row_number(row, ["progressive_carry_distance_per90", "progressive_carry_distance_p90", "carry_distance_per90", "carry_distance_p90"]) or 0.0
    dribble_value = row_number(row, ["dribble_value"]) or 0.0

    if wide_score >= 68:
        reasons.append(f"wide_score={wide_score:.1f}>=68")
    if crosses >= 1.2:
        reasons.append(f"crosses_p90={crosses:.2f}")
    if takeons >= 4.5:
        reasons.append(f"takeons_p90={takeons:.2f}")
    if takeons_won >= 2.0:
        reasons.append(f"takeons_won_p90={takeons_won:.2f}")
    if prog_carries >= 2.5:
        reasons.append(f"prog_carries_p90={prog_carries:.2f}")
    if prog_dist >= 35:
        reasons.append(f"prog_carry_dist_p90={prog_dist:.1f}")
    if dribble_value >= 0.15:
        reasons.append(f"dribble_value={dribble_value:.2f}")

    generic_or_wide = (
        is_generic_position(profile_code)
        or profile_code in WIDE_ATTACK_CODES
        or match_pos in {"ST", "SS", "AMC", "AM", "F"}
        or match_role in {"ST", "SS", "AMC", "AM", "F"}
    )

    # Central score can exceed wide score for creative wingers; only block if the central edge is huge.
    central_edge = central_score - wide_score
    ok = (
        generic_or_wide
        and wide_score >= 68
        and len(reasons) >= 3
        and central_edge < 25
    )
    if central_edge >= 25:
        reasons.append(f"blocked_central_edge={central_edge:.1f}")
    return ok, reasons


def confidence_from_evidence(
    *,
    base: float,
    agreement: float = 0.0,
    evidence_gap: float = 0.0,
    spatial_strength: float = 0.0,
    profile_bonus: float = 0.0,
    conflict_penalty: float = 0.0,
) -> float:
    """Evidence-strength score. Not a probability."""
    score = (
        base
        + 0.16 * clamp(agreement)
        + 0.14 * clamp(evidence_gap)
        + 0.18 * clamp(spatial_strength)
        + profile_bonus
        - conflict_penalty
    )
    return round(clamp(score, 0.35, 0.95), 2)


def spatial_role_from_xy(base_code: str | None, x: float | None, y: float | None) -> tuple[str | None, str | None, str | None, float, str]:
    """
    Convert average position into a role guess.

    Assumption:
    - x is vertical pitch progression, usually 0..100 from own goal to opponent goal.
    - y is lateral pitch position, usually 0..100 left to right.
    If a provider flips axes, this will be noisy, so confidence is deliberately capped.
    """
    if x is None or y is None:
        return None, None, None, 0.0, "no_spatial_data"
    if not (0 <= x <= 100 and 0 <= y <= 100):
        return None, None, None, 0.0, "spatial_out_of_range"

    base = normalize_position(base_code)
    if base in {"GK"}:
        return "GK", "GK", "Central", 0.85, "spatial_base_gk"

    side = "Right" if y <= 33 else "Left" if y >= 67 else "Central"
    wide = side in {"Left", "Right"}
    side_code = "L" if side == "Left" else "R" if side == "Right" else ""

    # Defenders
    if position_group(base) in {"CB", "FB", "WB"} or base in {"D", "DEFENDER"}:
        if wide:
            if x >= 54:
                role = f"{side_code}WB"
                group = "WB"
                lane = f"{side} Wing Back"
                strength = 0.78
            else:
                role = f"{side_code}B"
                group = "FB"
                lane = f"{side} Fullback"
                strength = 0.82
        else:
            role, group, lane, strength = "CB", "CB", "Central Defence", 0.82
        return role, group, lane, strength, "spatial_average_position"

    # Midfielders
    if position_group(base) in {"DM", "CM", "AM-C", "AM-W", "WM"} or base in {"M", "MIDFIELDER"}:
        if wide:
            if x >= 66:
                role = f"{side_code}W"
                group = "AM-W"
                lane = f"{side} Wide Forward"
                strength = 0.72
            elif x >= 50:
                role = f"{side_code}M"
                group = "WM"
                lane = f"{side} Wide Midfield"
                strength = 0.72
            else:
                role = f"{side_code}M"
                group = "WM"
                lane = f"{side} Wide Midfield"
                strength = 0.62
        else:
            if x <= 43:
                role, group, lane, strength = "DM", "DM", "Defensive Midfield", 0.75
            elif x >= 62:
                role, group, lane, strength = "AMC", "AM-C", "Central Attacking Midfield", 0.72
            else:
                role, group, lane, strength = "CM", "CM", "Central Midfield", 0.72
        return role, group, lane, strength, "spatial_average_position"

    # Forwards
    if position_group(base) in {"ST", "ST-SS", "AM-W", "AM-C"} or base in {"F", "FW", "FORWARD"}:
        if wide:
            role = f"{side_code}W"
            group = "AM-W"
            lane = f"{side} Wide Forward"
            strength = 0.78
        else:
            if x <= 62:
                role, group, lane, strength = "SS", "ST-SS", "Central Support Forward", 0.65
            else:
                role, group, lane, strength = "ST", "ST", "Central Forward", 0.76
        return role, group, lane, strength, "spatial_average_position"

    return None, None, None, 0.0, "spatial_base_unknown"


def _position_zone_from_xy(x: float | None, y: float | None) -> str | None:
    if x is None or y is None:
        return None
    try:
        x = float(x)
        y = float(y)
    except Exception:
        return None
    if not (0 <= x <= 100 and 0 <= y <= 100):
        return None

    vertical = "deep" if x < 35 else "mid" if x < 62 else "high"
    lateral = "right" if y < 33 else "left" if y > 67 else "central"
    return f"{vertical}_{lateral}"


def _extract_position_context_records(context_path: str | None) -> list[dict[str, Any]]:
    """
    Flatten position_context_by_event.json into event/player records.

    Output columns:
    - event_id
    - player_id
    - avg_x
    - avg_y
    - position_context_source
    """
    if not context_path:
        return []

    path = Path(context_path)
    if not path.exists():
        raise FileNotFoundError(f"Position context JSON not found: {context_path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    records: list[dict[str, Any]] = []

    if isinstance(payload, list):
        for r in payload:
            if not isinstance(r, dict):
                continue
            records.append({
                "event_id": r.get("event_id"),
                "player_id": r.get("player_id") or r.get("id"),
                "avg_x": r.get("avg_x") or r.get("average_x") or r.get("averageX") or r.get("x"),
                "avg_y": r.get("avg_y") or r.get("average_y") or r.get("averageY") or r.get("y"),
                POSITION_CONTEXT_SOURCE_COL: r.get(POSITION_CONTEXT_SOURCE_COL) or r.get("source") or "position_context_json",
            })
        return records

    if not isinstance(payload, dict):
        return records

    event_map = payload.get("events") if isinstance(payload.get("events"), dict) else payload
    for event_id, event_block in event_map.items():
        if not isinstance(event_block, dict):
            continue
        players = event_block.get("players") or event_block.get("player_positions") or {}
        if isinstance(players, dict):
            for pid, pb in players.items():
                if not isinstance(pb, dict):
                    continue
                records.append({
                    "event_id": event_id,
                    "player_id": pid,
                    "avg_x": pb.get("avg_x") or pb.get("average_x") or pb.get("averageX") or pb.get("x"),
                    "avg_y": pb.get("avg_y") or pb.get("average_y") or pb.get("averageY") or pb.get("y"),
                    POSITION_CONTEXT_SOURCE_COL: pb.get("source") or event_block.get("source") or "position_context_json",
                })
        elif isinstance(players, list):
            for pb in players:
                if not isinstance(pb, dict):
                    continue
                records.append({
                    "event_id": event_id,
                    "player_id": pb.get("player_id") or pb.get("id"),
                    "avg_x": pb.get("avg_x") or pb.get("average_x") or pb.get("averageX") or pb.get("x"),
                    "avg_y": pb.get("avg_y") or pb.get("average_y") or pb.get("averageY") or pb.get("y"),
                    POSITION_CONTEXT_SOURCE_COL: pb.get("source") or event_block.get("source") or "position_context_json",
                })

    return records


def _season_position_summary_from_context(records: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Collapse event-level average positions to one row per player.

    V13 adds distribution features because one season centroid can hide role truth:
    - inverted wingbacks can average central
    - wingers can roam inside
    - CBs can lean left/right in buildup

    Coordinates note:
    In this Sofascore feed, lower y values map to the right side and higher y
    values map to the left side. This is why y < 33 is right and y > 67 is left.
    """
    if not records:
        return pd.DataFrame()

    ctx = pd.DataFrame(records)
    if ctx.empty or "player_id" not in ctx.columns:
        return pd.DataFrame()

    ctx["player_id"] = pd.to_numeric(ctx["player_id"], errors="coerce")
    ctx["avg_x"] = pd.to_numeric(ctx["avg_x"], errors="coerce")
    ctx["avg_y"] = pd.to_numeric(ctx["avg_y"], errors="coerce")
    if "event_id" in ctx.columns:
        ctx["event_id"] = ctx["event_id"].astype(str)

    ctx = ctx.dropna(subset=["player_id", "avg_x", "avg_y"])
    ctx = ctx.loc[
        ctx["avg_x"].between(0, 100, inclusive="both")
        & ctx["avg_y"].between(0, 100, inclusive="both")
    ].copy()
    if ctx.empty:
        return pd.DataFrame()

    ctx["player_id"] = ctx["player_id"].astype(int)

    ctx["is_wide"] = (ctx["avg_y"] < 33) | (ctx["avg_y"] > 67)
    ctx["is_right"] = ctx["avg_y"] < 33
    ctx["is_left"] = ctx["avg_y"] > 67
    ctx["is_central"] = ~ctx["is_wide"]
    ctx["is_high"] = ctx["avg_x"] >= 62
    ctx["is_deep"] = ctx["avg_x"] < 35
    ctx["is_mid"] = (ctx["avg_x"] >= 35) & (ctx["avg_x"] < 62)
    ctx["is_high_wide"] = ctx["is_high"] & ctx["is_wide"]
    ctx["is_mid_wide"] = ctx["is_mid"] & ctx["is_wide"]
    ctx["is_deep_central"] = ctx["is_deep"] & ctx["is_central"]

    grouped = ctx.groupby("player_id", dropna=False)

    summary = grouped.agg(
        season_avg_x=("avg_x", "mean"),
        season_avg_y=("avg_y", "mean"),
        season_median_x=("avg_x", "median"),
        season_median_y=("avg_y", "median"),
        season_std_x=("avg_x", "std"),
        season_std_y=("avg_y", "std"),
        spatial_matches_used=("event_id", "nunique") if "event_id" in ctx.columns else ("avg_x", "count"),
        spatial_wide_matches=("is_wide", "sum"),
        spatial_right_matches=("is_right", "sum"),
        spatial_left_matches=("is_left", "sum"),
        spatial_central_matches=("is_central", "sum"),
        spatial_high_matches=("is_high", "sum"),
        spatial_mid_matches=("is_mid", "sum"),
        spatial_deep_matches=("is_deep", "sum"),
        spatial_high_wide_matches=("is_high_wide", "sum"),
        spatial_mid_wide_matches=("is_mid_wide", "sum"),
        spatial_deep_central_matches=("is_deep_central", "sum"),
    ).reset_index()

    summary["season_std_x"] = summary["season_std_x"].fillna(0.0)
    summary["season_std_y"] = summary["season_std_y"].fillna(0.0)

    denom = summary["spatial_matches_used"].replace(0, np.nan)
    for col in [
        "spatial_wide", "spatial_right", "spatial_left", "spatial_central",
        "spatial_high", "spatial_mid", "spatial_deep",
        "spatial_high_wide", "spatial_mid_wide", "spatial_deep_central",
    ]:
        match_col = f"{col}_matches"
        pct_col = f"{col}_pct"
        summary[pct_col] = (summary[match_col] / denom).fillna(0.0).round(4)

    for col in ["season_avg_x", "season_avg_y", "season_median_x", "season_median_y", "season_std_x", "season_std_y"]:
        summary[col] = summary[col].round(4)

    summary["season_position_zone"] = [
        _position_zone_from_xy(x, y)
        for x, y in zip(summary["season_avg_x"], summary["season_avg_y"])
    ]

    def dominant_side(row: pd.Series) -> str:
        if row.get("spatial_right_pct", 0) >= 0.45 and row.get("spatial_right_pct", 0) >= row.get("spatial_left_pct", 0):
            return "Right"
        if row.get("spatial_left_pct", 0) >= 0.45 and row.get("spatial_left_pct", 0) > row.get("spatial_right_pct", 0):
            return "Left"
        return "Central/Mixed"

    summary["spatial_dominant_side"] = summary.apply(dominant_side, axis=1)
    summary[POSITION_CONTEXT_SOURCE_COL] = "season_collapsed_average_positions_v13"
    return summary


def apply_position_context(df: pd.DataFrame, context_path: str | None) -> pd.DataFrame:
    """
    Merge cached event/player average-position context into the dataframe.

    V11 behavior:
    - If input is match-level and has event_id + player_id:
      merge exact event/player avg_x and avg_y.
    - If input is season-level and has player_id but no event_id:
      collapse the entire position_context_by_event.json into season-level
      player averages and merge:
        season_avg_x, season_avg_y, season_std_x, season_std_y,
        spatial_matches_used, season_position_zone.
      Then fill avg_x/avg_y from season_avg_x/season_avg_y so the existing
      spatial arbitration path works unchanged.
    """
    records = _extract_position_context_records(context_path)
    if not records:
        return df

    out = df.copy()

    # Match-level merge when event_id exists.
    if "event_id" in out.columns and "player_id" in out.columns:
        ctx = pd.DataFrame(records)
        if ctx.empty:
            return out

        for col in ["event_id", "player_id"]:
            if col in ctx.columns:
                ctx[col] = ctx[col].astype(str)

        out["_event_id_str"] = out["event_id"].astype(str)
        out["_player_id_str"] = out["player_id"].astype(str)
        ctx = ctx.rename(columns={"event_id": "_event_id_str", "player_id": "_player_id_str"})

        merge_cols = ["_event_id_str", "_player_id_str", "avg_x", "avg_y", POSITION_CONTEXT_SOURCE_COL]
        ctx = ctx[[c for c in merge_cols if c in ctx.columns]].drop_duplicates(["_event_id_str", "_player_id_str"])
        out = out.merge(ctx, on=["_event_id_str", "_player_id_str"], how="left", suffixes=("", "_ctx"))

        for base_col in ["avg_x", "avg_y", POSITION_CONTEXT_SOURCE_COL]:
            ctx_col = f"{base_col}_ctx"
            if ctx_col in out.columns:
                if base_col in out.columns:
                    out[base_col] = out[base_col].where(out[base_col].notna(), out[ctx_col])
                    out = out.drop(columns=[ctx_col])
                else:
                    out = out.rename(columns={ctx_col: base_col})

        out = out.drop(columns=["_event_id_str", "_player_id_str"], errors="ignore")
        return out

    # Season-level merge when only player_id exists.
    if "player_id" not in out.columns:
        return out

    summary = _season_position_summary_from_context(records)
    if summary.empty:
        return out

    out["_player_id_int"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    summary["_player_id_int"] = pd.to_numeric(summary["player_id"], errors="coerce").astype("Int64")
    summary = summary.drop(columns=["player_id"], errors="ignore")

    out = out.merge(summary, on="_player_id_int", how="left", suffixes=("", "_ctx"))

    # Preserve any existing avg_x/avg_y, but fill from season averages.
    if "avg_x" not in out.columns:
        out["avg_x"] = np.nan
    if "avg_y" not in out.columns:
        out["avg_y"] = np.nan

    out["avg_x"] = out["avg_x"].where(out["avg_x"].notna(), out.get("season_avg_x"))
    out["avg_y"] = out["avg_y"].where(out["avg_y"].notna(), out.get("season_avg_y"))

    # If a context-source column already existed, fill missing values.
    ctx_source_col = f"{POSITION_CONTEXT_SOURCE_COL}_ctx"
    if ctx_source_col in out.columns:
        if POSITION_CONTEXT_SOURCE_COL in out.columns:
            out[POSITION_CONTEXT_SOURCE_COL] = out[POSITION_CONTEXT_SOURCE_COL].where(
                out[POSITION_CONTEXT_SOURCE_COL].notna(),
                out[ctx_source_col],
            )
            out = out.drop(columns=[ctx_source_col])
        else:
            out = out.rename(columns={ctx_source_col: POSITION_CONTEXT_SOURCE_COL})

    out = out.drop(columns=["_player_id_int"], errors="ignore")
    return out


def weighted_mode(values: list[tuple[Any, float]]) -> Any:
    weights = defaultdict(float)
    for value, weight in values:
        if value is None or pd.isna(value) or str(value).strip() == "":
            continue
        weights[value] += float(weight or 0.0)
    if not weights:
        return None
    return max(weights.items(), key=lambda kv: kv[1])[0]


def weighted_position_summary(group: pd.DataFrame, cols: list[str], minutes_col: str = MINUTES_COL) -> tuple[str | None, str]:
    """Return weighted mode and minutes summary string like ST:500, RW:250."""
    minutes = numeric(group[minutes_col]) if minutes_col in group.columns else pd.Series([1.0] * len(group), index=group.index)
    pos_minutes = defaultdict(float)

    for col in cols:
        if col not in group.columns:
            continue
        for idx, raw in group[col].items():
            code = normalize_position(raw)
            if not code:
                continue
            pos_minutes[code] += float(minutes.loc[idx] if pd.notna(minutes.loc[idx]) else 0.0)

    if not pos_minutes:
        return None, ""

    mode = max(pos_minutes.items(), key=lambda kv: kv[1])[0]
    summary = ", ".join(f"{k}:{round(v, 1)}" for k, v in sorted(pos_minutes.items(), key=lambda kv: kv[1], reverse=True))
    return mode, summary


def stat(row: pd.Series, candidates: list[str]) -> float | None:
    lower_map = {c.lower(): c for c in row.index}
    for c in candidates:
        for cand in [c, f"{c}_per90", f"{c}_p90"]:
            actual = lower_map.get(cand.lower())
            if actual is None:
                continue
            try:
                v = float(row.get(actual))
                return None if math.isnan(v) else v
            except Exception:
                pass
    return None


def percentile(value: float | None, series: pd.Series) -> float | None:
    if value is None:
        return None
    vals = numeric(series).dropna()
    if vals.empty:
        return None
    return float((vals <= value).mean() * 100.0)


def ensure_per90(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if MINUTES_COL not in out.columns:
        return out
    minutes = numeric(out[MINUTES_COL])
    if minutes.notna().sum() == 0:
        return out

    exclude = {
        "player_id", "event_id", "match_id", "season_id", "shirt_number", "age", "height_cm",
        "matches", "minutes_played", "teams_played_count", "positions_played_count",
        "date_of_birth", "age_as_of",
    }
    non_numeric_context = {
        "player_name", "profile_name", "season", "league", "team", "nationality",
        "preferred_foot", "player_position", "position", "profile_position",
        "base_position", "role_family", "role_position", "primary_role_position",
        "secondary_role_position", "positions_played_list",
    }
    for col in list(out.columns):
        if col in exclude or col in non_numeric_context:
            continue
        if col.endswith("_per90") or col.endswith("_p90"):
            continue
        if col.endswith("_pct") or "accuracy" in col.lower():
            continue
        vals = numeric(out[col])
        if vals.notna().sum() == 0:
            continue
        new_col = f"{col}_per90"
        if new_col not in out.columns:
            out[new_col] = np.where(minutes > 0, vals * 90.0 / minutes, np.nan)
    return out


def wide_attack_evidence(row: pd.Series, cohort: pd.DataFrame) -> tuple[float, list[str]]:
    metrics = {
        "crosses_total": ["crosses_total_per90", "crosses_total_p90"],
        "crosses_accurate": ["crosses_accurate_per90", "crosses_accurate_p90"],
        "dribbles_attempted": ["dribbles_attempted_per90", "dribbles_attempted_p90", "contests_total_per90"],
        "contests_won": ["contests_won_per90", "contests_won_p90"],
        "progressive_carries": ["progressive_carries_per90", "progressive_carries_p90"],
        "carry_distance": ["progressive_carry_distance_per90", "carry_distance_per90"],
        "touches_opp_box": ["touches_opp_box_per90", "touches_opp_box_p90"],
    }
    weights = {
        "crosses_total": 1.0,
        "crosses_accurate": 1.0,
        "dribbles_attempted": 1.2,
        "contests_won": 1.1,
        "progressive_carries": 1.2,
        "carry_distance": 0.8,
        "touches_opp_box": 0.6,
    }

    vals = []
    used = []
    lower_cols = {c.lower(): c for c in cohort.columns}
    for label, candidates in metrics.items():
        actual = next((lower_cols.get(c.lower()) for c in candidates if lower_cols.get(c.lower())), None)
        if actual is None:
            continue
        try:
            value = float(row.get(actual))
        except Exception:
            continue
        if math.isnan(value):
            continue
        pct = percentile(value, cohort[actual])
        if pct is None:
            continue
        vals.append((pct, weights[label]))
        used.append(actual)
    if not vals:
        return 0.0, used
    return float(sum(v * w for v, w in vals) / sum(w for _, w in vals)), used


def central_attack_evidence(row: pd.Series, cohort: pd.DataFrame) -> tuple[float, list[str]]:
    metrics = {
        "key_passes": ["key_passes_per90", "key_passes_p90"],
        "xa": ["xa_per90", "xa_p90"],
        "passes_total": ["passes_total_per90", "passes_total_p90"],
        "passes_opp": ["passes_opposition_half_total_per90", "passes_opposition_half_total_p90"],
        "pass_value": ["pass_value"],
        "assists": ["assists_per90", "assists_p90"],
    }
    weights = {
        "key_passes": 1.2,
        "xa": 1.2,
        "passes_total": 0.8,
        "passes_opp": 0.9,
        "pass_value": 1.1,
        "assists": 0.5,
    }

    vals = []
    used = []
    lower_cols = {c.lower(): c for c in cohort.columns}
    for label, candidates in metrics.items():
        actual = next((lower_cols.get(c.lower()) for c in candidates if lower_cols.get(c.lower())), None)
        if actual is None:
            continue
        try:
            value = float(row.get(actual))
        except Exception:
            continue
        if math.isnan(value):
            continue
        pct = percentile(value, cohort[actual])
        if pct is None:
            continue
        vals.append((pct, weights[label]))
        used.append(actual)
    if not vals:
        return 0.0, used
    return float(sum(v * w for v, w in vals) / sum(w for _, w in vals)), used



def metric_value(row: pd.Series, names: list[str]) -> float:
    """Prefer per90 variants, then raw names."""
    candidates = []
    for n in names:
        candidates.extend([f"{n}_per90", f"{n}_p90"])
    candidates.extend(names)
    return row_number(row, candidates) or 0.0


def role_scores_v6(row: pd.Series) -> dict[str, float]:
    """Transparent, deliberately simple role fingerprints."""
    # Attacking / wide
    crosses = metric_value(row, ["crosses_total"])
    takeons = metric_value(row, ["dribbles_attempted", "contests_total"])
    takeons_won = metric_value(row, ["dribbles_completed", "contests_won"])
    prog_carries = metric_value(row, ["progressive_carries"])
    prog_dist = metric_value(row, ["progressive_carry_distance", "carry_distance"])
    touches_box = metric_value(row, ["touches_opp_box"])
    shots = metric_value(row, ["shots_total"])
    xg = metric_value(row, ["xg"])
    xa = metric_value(row, ["xa"])
    key_passes = metric_value(row, ["key_passes"])
    passes_opp = metric_value(row, ["passes_opposition_half_total"])
    dribble_value = row_number(row, ["dribble_value"]) or 0.0
    shot_value = row_number(row, ["shot_value"]) or 0.0
    pass_value = row_number(row, ["pass_value"]) or 0.0

    # Midfield / defence
    tackles = metric_value(row, ["tackles_total"])
    tackles_won = metric_value(row, ["tackles_won"])
    interceptions = metric_value(row, ["interceptions"])
    recoveries = metric_value(row, ["recoveries"])
    passes = metric_value(row, ["passes_total"])
    own_half = metric_value(row, ["passes_own_half_total"])
    long_balls = metric_value(row, ["long_balls_total"])

    clearances = metric_value(row, ["clearances"])
    blocks = metric_value(row, ["blocked_shots"])
    aerial_total = metric_value(row, ["aerial_duels_total"])
    aerial_won = metric_value(row, ["aerial_duels_won"])
    duels = metric_value(row, ["duels_total"])
    duels_won = metric_value(row, ["duels_won"])

    # Normalize-ish by using intuitive football thresholds.
    wide = (
        min(crosses / 3.0, 1) * 1.3
        + min(takeons / 5.0, 1) * 1.4
        + min(takeons_won / 2.5, 1) * 0.9
        + min(prog_carries / 3.0, 1) * 1.0
        + min(prog_dist / 60.0, 1) * 0.7
        + min(dribble_value / 0.35, 1) * 0.8
    )

    st = (
        min(shots / 3.5, 1) * 1.4
        + min(xg / 0.45, 1) * 1.5
        + min(touches_box / 5.0, 1) * 1.1
        + min(shot_value / 0.25, 1) * 0.6
        - min(crosses / 4.0, 1) * 0.4
    )

    ss_am = (
        min(key_passes / 2.2, 1) * 1.1
        + min(xa / 0.25, 1) * 1.2
        + min(passes_opp / 25.0, 1) * 0.8
        + min(pass_value / 0.35, 1) * 0.7
        + min(shots / 2.2, 1) * 0.4
    )

    dm = (
        min(tackles / 3.0, 1) * 1.0
        + min(interceptions / 1.7, 1) * 1.1
        + min(recoveries / 7.0, 1) * 1.0
        + min(own_half / 25.0, 1) * 0.7
        + min(long_balls / 4.0, 1) * 0.4
        - min(touches_box / 3.0, 1) * 0.5
        - min(shots / 2.0, 1) * 0.3
    )

    cm = (
        min(passes / 55.0, 1) * 1.1
        + min(recoveries / 6.0, 1) * 0.7
        + min(prog_carries / 2.2, 1) * 0.7
        + min(passes_opp / 25.0, 1) * 0.7
        + min(tackles / 2.2, 1) * 0.5
        + min(pass_value / 0.30, 1) * 0.5
    )

    am = (
        min(key_passes / 2.0, 1) * 1.1
        + min(xa / 0.25, 1) * 1.0
        + min(touches_box / 3.5, 1) * 0.8
        + min(shots / 2.0, 1) * 0.7
        + min(passes_opp / 25.0, 1) * 0.5
        - min(own_half / 30.0, 1) * 0.3
    )

    cb = (
        min(clearances / 4.5, 1) * 1.5
        + min(aerial_won / 2.5, 1) * 1.1
        + min(aerial_total / 4.0, 1) * 0.6
        + min(blocks / 1.0, 1) * 0.8
        + min(interceptions / 1.5, 1) * 0.5
        - min(crosses / 1.5, 1) * 0.7
        - min(takeons / 2.0, 1) * 0.4
    )

    fb = (
        min(crosses / 2.2, 1) * 1.0
        + min(tackles / 2.5, 1) * 0.8
        + min(recoveries / 6.0, 1) * 0.7
        + min(prog_carries / 2.0, 1) * 0.8
        + min(passes_opp / 20.0, 1) * 0.4
        - min(shots / 1.5, 1) * 0.4
    )

    wb = (
        min(crosses / 3.0, 1) * 1.0
        + min(prog_carries / 3.0, 1) * 1.0
        + min(prog_dist / 60.0, 1) * 0.7
        + min(takeons / 3.0, 1) * 0.5
        + min(tackles / 2.0, 1) * 0.4
    )

    return {
        "W": round(wide, 3),
        "ST": round(st, 3),
        "SS": round(ss_am, 3),
        "DM": round(dm, 3),
        "CM": round(cm, 3),
        "AM": round(am, 3),
        "CB": round(cb, 3),
        "FB": round(fb, 3),
        "WB": round(wb, 3),
    }


def best_role(scores: dict[str, float], roles: list[str]) -> tuple[str, float, float]:
    ordered = sorted(((r, scores.get(r, 0.0)) for r in roles), key=lambda x: x[1], reverse=True)
    best, best_score = ordered[0]
    second = ordered[1][1] if len(ordered) > 1 else 0.0
    return best, best_score, second


def defender_refinement_v7(row: pd.Series, scores: dict[str, float]) -> tuple[str, str, str, str]:
    """
    Refine defender labels into:
    - CB
    - FB
    - WB
    - CB-FB hybrid

    This is deliberately style-based, because without reliable average-position
    data we cannot always know left/right side. It solves cases like Ben White,
    Piero Hincapié, Nathan Aké, Jules Koundé, etc.
    """
    crosses = metric_value(row, ["crosses_total"])
    prog_carries = metric_value(row, ["progressive_carries"])
    prog_dist = metric_value(row, ["progressive_carry_distance", "carry_distance"])
    takeons = metric_value(row, ["dribbles_attempted", "contests_total"])
    passes_opp = metric_value(row, ["passes_opposition_half_total"])
    key_passes = metric_value(row, ["key_passes"])
    tackles = metric_value(row, ["tackles_total"])
    recoveries = metric_value(row, ["recoveries"])

    clearances = metric_value(row, ["clearances"])
    aerial_won = metric_value(row, ["aerial_duels_won"])
    aerial_total = metric_value(row, ["aerial_duels_total"])
    blocks = metric_value(row, ["blocked_shots"])
    interceptions = metric_value(row, ["interceptions"])

    cb_signal = (
        min(clearances / 4.0, 1) * 1.4
        + min(aerial_won / 2.3, 1) * 1.0
        + min(aerial_total / 3.8, 1) * 0.6
        + min(blocks / 0.9, 1) * 0.7
        + min(interceptions / 1.4, 1) * 0.5
    )
    wide_def_signal = (
        min(crosses / 2.0, 1) * 1.0
        + min(prog_carries / 2.2, 1) * 0.9
        + min(prog_dist / 50.0, 1) * 0.7
        + min(passes_opp / 20.0, 1) * 0.5
        + min(tackles / 2.4, 1) * 0.6
        + min(recoveries / 5.5, 1) * 0.5
    )
    wingback_signal = (
        min(crosses / 3.0, 1) * 1.1
        + min(prog_carries / 3.0, 1) * 1.0
        + min(prog_dist / 65.0, 1) * 0.8
        + min(takeons / 2.5, 1) * 0.5
        + min(key_passes / 1.0, 1) * 0.5
    )

    # Modern hybrid defender: enough CB defending plus enough wide/fullback work.
    if cb_signal >= 2.45 and wide_def_signal >= 2.15:
        return (
            "CB-FB",
            "CB-FB",
            "Hybrid Defensive",
            f"cb_signal={cb_signal:.2f}, wide_def_signal={wide_def_signal:.2f}, wingback_signal={wingback_signal:.2f}",
        )

    # Wingback: more aggressive wide profile than ordinary FB.
    if wingback_signal >= 2.75 and wide_def_signal >= 2.30 and scores.get("WB", 0) >= scores.get("FB", 0) - 0.25:
        return (
            "WB",
            "WB",
            "Wide Wing Back",
            f"cb_signal={cb_signal:.2f}, wide_def_signal={wide_def_signal:.2f}, wingback_signal={wingback_signal:.2f}",
        )

    role, _, _ = best_role(scores, ["CB", "FB", "WB"])
    if role == "CB":
        return "CB", "CB", "Central Defence", f"cb_signal={cb_signal:.2f}, wide_def_signal={wide_def_signal:.2f}, wingback_signal={wingback_signal:.2f}"
    if role == "WB":
        return "WB", "WB", "Wide Wing Back", f"cb_signal={cb_signal:.2f}, wide_def_signal={wide_def_signal:.2f}, wingback_signal={wingback_signal:.2f}"
    return "FB", "FB", "Wide Defensive", f"cb_signal={cb_signal:.2f}, wide_def_signal={wide_def_signal:.2f}, wingback_signal={wingback_signal:.2f}"


def wide_defender_gate_v8(row: pd.Series, scores: dict[str, float]) -> tuple[bool, str, str, str, float, str]:
    """
    Catch wingbacks/fullbacks who look like mobile midfielders in the stat profile.

    Designed for cases like:
    - Wesley at Roma as RWB
    - Hakimi / Davies / Porro-type wide defenders

    We require a combination of:
    - wide progression/carrying/crossing
    - defensive activity
    - low-ish pure striker signal
    """
    crosses = metric_value(row, ["crosses_total"])
    prog_carries = metric_value(row, ["progressive_carries"])
    prog_dist = metric_value(row, ["progressive_carry_distance", "carry_distance"])
    takeons = metric_value(row, ["dribbles_attempted", "contests_total"])
    passes_opp = metric_value(row, ["passes_opposition_half_total"])
    key_passes = metric_value(row, ["key_passes"])

    tackles = metric_value(row, ["tackles_total"])
    interceptions = metric_value(row, ["interceptions"])
    recoveries = metric_value(row, ["recoveries"])
    duels_won = metric_value(row, ["duels_won"])

    shots = metric_value(row, ["shots_total"])
    xg = metric_value(row, ["xg"])
    touches_box = metric_value(row, ["touches_opp_box"])

    wide_lane_score = (
        min(crosses / 2.2, 1) * 1.2
        + min(prog_carries / 2.4, 1) * 1.0
        + min(prog_dist / 55.0, 1) * 0.8
        + min(takeons / 2.4, 1) * 0.6
        + min(passes_opp / 22.0, 1) * 0.5
        + min(key_passes / 1.0, 1) * 0.4
    )
    defensive_work_score = (
        min(tackles / 2.2, 1) * 1.0
        + min(interceptions / 1.2, 1) * 0.8
        + min(recoveries / 5.5, 1) * 0.8
        + min(duels_won / 4.0, 1) * 0.5
    )
    striker_penalty = (
        min(shots / 2.5, 1) * 0.6
        + min(xg / 0.30, 1) * 0.7
        + min(touches_box / 4.0, 1) * 0.4
    )

    wb_score = wide_lane_score + defensive_work_score - striker_penalty
    evidence = (
        f"wide_lane_score={wide_lane_score:.2f}, defensive_work_score={defensive_work_score:.2f}, "
        f"striker_penalty={striker_penalty:.2f}, wb_score={wb_score:.2f}"
    )

    # Aggressive enough to rescue RWB/LWB profiles from CM, but not so aggressive that it
    # steals true wide attackers who do very little defending.
    def_score = max(scores.get("FB", 0), scores.get("WB", 0))
    mid_score = max(scores.get("DM", 0), scores.get("CM", 0), scores.get("AM", 0))
    winger_score = scores.get("W", 0)

    # Safety gates:
    # - do not steal true wide forwards/wingers whose W score clearly dominates
    # - do not steal true CMs/DMs when midfield score clearly dominates fullback/wingback score
    # - require the defender-side model to be at least competitive with the midfield model
    defender_competitive = def_score >= mid_score - 0.35
    not_obvious_winger = winger_score <= def_score + 0.90

    evidence = (
        evidence
        + f", def_score={def_score:.2f}, mid_score={mid_score:.2f}, winger_score={winger_score:.2f}, "
        + f"defender_competitive={defender_competitive}, not_obvious_winger={not_obvious_winger}"
    )

    if (
        wide_lane_score >= 2.55
        and defensive_work_score >= 1.85
        and wb_score >= 2.75
        and defender_competitive
        and not_obvious_winger
    ):
        # If very attacking, call WB rather than FB.
        if wide_lane_score >= 3.35 or scores.get("WB", 0) >= scores.get("FB", 0) - 0.15:
            return True, "WB", "WB", "Wide Wing Back", round(clamp(0.58 + min(wb_score / 8.0, 0.16), 0.56, 0.78), 2), evidence
        return True, "FB", "FB", "Wide Defensive", round(clamp(0.56 + min(wb_score / 8.0, 0.15), 0.54, 0.76), 2), evidence

    return False, "", "", "", 0.0, evidence


def family_hint_from_sources(profile_code: str | None, match_pos: str | None, match_role: str | None) -> str | None:
    groups = [position_group(c) for c in [profile_code, match_pos, match_role] if c]
    if any(g in {"CB", "FB", "WB"} for g in groups):
        return "DEF"
    if any(g in {"DM", "CM", "AM-C", "AM-W", "WM"} for g in groups):
        return "MID"
    if any(g in {"ST", "ST-SS"} for g in groups):
        return "ATT"
    return None



def creative_wide_defender_v9(row: pd.Series, scores: dict[str, float]) -> tuple[bool, str, str, str, float, str]:
    """
    Catch elite creative wide defenders who statistically resemble midfielders:
    Grimaldo / Dimarco / TAA / Cancelo-type seasons.
    """
    crosses = metric_value(row, ["crosses_total"])
    key_passes = metric_value(row, ["key_passes"])
    xa = metric_value(row, ["xa"])
    passes_opp = metric_value(row, ["passes_opposition_half_total"])
    prog_carries = metric_value(row, ["progressive_carries"])
    prog_dist = metric_value(row, ["progressive_carry_distance", "carry_distance"])
    tackles = metric_value(row, ["tackles_total"])
    recoveries = metric_value(row, ["recoveries"])
    interceptions = metric_value(row, ["interceptions"])
    shots = metric_value(row, ["shots_total"])
    xg = metric_value(row, ["xg"])

    creativity = (
        min(crosses / 3.0, 1) * 1.1
        + min(key_passes / 1.8, 1) * 1.0
        + min(xa / 0.22, 1) * 1.0
        + min(passes_opp / 24.0, 1) * 0.8
    )
    wide_progression = (
        min(prog_carries / 2.6, 1) * 0.9
        + min(prog_dist / 55.0, 1) * 0.8
        + min(crosses / 2.6, 1) * 0.6
    )
    defensive_base = (
        min(tackles / 1.8, 1) * 0.8
        + min(recoveries / 5.0, 1) * 0.8
        + min(interceptions / 1.0, 1) * 0.5
    )
    striker_penalty = (
        min(shots / 2.6, 1) * 0.6
        + min(xg / 0.28, 1) * 0.7
    )

    total = creativity + wide_progression + defensive_base - striker_penalty
    evidence = f"creativity={creativity:.2f}, wide_progression={wide_progression:.2f}, defensive_base={defensive_base:.2f}, striker_penalty={striker_penalty:.2f}, total={total:.2f}"

    # Needs real wide/fullback compatibility in base model
    def_score = max(scores.get("FB", 0), scores.get("WB", 0))
    mid_score = max(scores.get("CM", 0), scores.get("AM", 0))

    winger_score = scores.get("W", 0)
    # Safety: do not steal obvious wingers.
    if total >= 3.6 and def_score >= mid_score - 0.25 and def_score >= winger_score - 0.85 and winger_score <= def_score + 0.85:
        # Highly attacking wide defender => WB
        if creativity >= 2.4 or scores.get("WB", 0) >= scores.get("FB", 0) - 0.1:
            return True, "WB", "WB", "Creative Wing Back", 0.74, evidence
        return True, "FB", "FB", "Creative Fullback", 0.72, evidence

    return False, "", "", "", 0.0, evidence

def family_first_arbitration_v6(
    row: pd.Series,
    profile_code: str | None,
    match_pos: str | None,
    match_role: str | None,
    heuristic_source: bool,
    agreement: float,
    spatial_pos: str | None,
    spatial_group: str | None,
    spatial_lane: str | None,
    spatial_strength: float,
) -> dict[str, Any] | None:
    """
    V6 rule block for the classes v5 struggled with:
    - DM/CM/AM
    - CB/FB/WB
    - W/ST/SS
    Returns an arbitration dict when it has enough evidence; otherwise None.
    """
    scores = role_scores_v6(row)
    source_family = family_hint_from_sources(profile_code, match_pos, match_role)

    # V13 zone-distribution rescue: centroid may be central, but repeated wide/high-wide
    # appearances identify wingers and wingbacks. Run before single-centroid spatial override.
    wide_pct = row_number(row, ["spatial_wide_pct"]) or 0.0
    high_wide_pct = row_number(row, ["spatial_high_wide_pct"]) or 0.0
    mid_wide_pct = row_number(row, ["spatial_mid_wide_pct"]) or 0.0
    right_pct = row_number(row, ["spatial_right_pct"]) or 0.0
    left_pct = row_number(row, ["spatial_left_pct"]) or 0.0
    spatial_n = row_number(row, ["spatial_matches_used"]) or 0.0
    side_code = "R" if right_pct >= left_pct else "L"
    side_lane = "Right" if right_pct >= left_pct else "Left"

    # Wide defender first, so RWB/LWB profiles are not stolen by winger rescue.
    if spatial_n >= 6 and wide_pct >= 0.55 and source_family in {"MID", "DEF"}:
        wide_def_score = max(scores.get("FB", 0), scores.get("WB", 0))
        mid_score = max(scores.get("CM", 0), scores.get("DM", 0), scores.get("AM", 0))
        winger_score = scores.get("W", 0)
        # The +0.55 margin blocks obvious wingers.
        # Exception: extreme repeated wide-side usage with some high-wide presence
        # catches creative wingbacks like Grimaldo even when their attacking style
        # gives them a high W score.
        creative_wb_exception = wide_pct >= 0.85 and 0.12 <= high_wide_pct <= 0.45 and wide_def_score >= 1.90
        if wide_def_score >= mid_score - 1.15 and (winger_score <= wide_def_score + 0.65 or creative_wb_exception):
            # Fullback vs wingback:
            # - True defender-source players default to FB unless WB evidence is clearly stronger.
            # - Midfield/wide-defender rescues can become WB when they are repeatedly wide/inverted.
            if source_family == "DEF":
                wb_or_fb = "WB" if (scores.get("WB", 0) >= scores.get("FB", 0) + 0.20 or high_wide_pct >= 0.30) else "FB"
            else:
                wb_or_fb = "WB" if (scores.get("WB", 0) >= scores.get("FB", 0) - 0.35 or high_wide_pct >= 0.14 or wide_pct >= 0.85) else "FB"

            return {
                "arbitrated_position": f"{side_code}{wb_or_fb if wb_or_fb == 'WB' else 'B'}",
                "arbitrated_role_group": wb_or_fb,
                "arbitrated_lane": f"{side_lane} {'Wing Back' if wb_or_fb == 'WB' else 'Fullback'}",
                "arbitrated_confidence": 0.81,
                "position_conflict_flag": True,
                "arbitration_reason": "V13 zone-distribution rescue selected wide defender from repeated wide average positions.",
                "position_evidence": f"v6_scores={scores}; wide_pct={wide_pct:.2f}; mid_wide_pct={mid_wide_pct:.2f}; high_wide_pct={high_wide_pct:.2f}; right_pct={right_pct:.2f}; left_pct={left_pct:.2f}; spatial_n={spatial_n}",
            }

    # Winger rescue second.
    if spatial_n >= 6 and wide_pct >= 0.38 and source_family in {"MID", "ATT"}:
        attacking_best = max(scores.get("CM", 0), scores.get("DM", 0), scores.get("AM", 0), scores.get("ST", 0), scores.get("SS", 0))
        if scores.get("W", 0) >= attacking_best - 0.45:
            return {
                "arbitrated_position": f"{side_code}W",
                "arbitrated_role_group": "AM-W",
                "arbitrated_lane": f"{side_lane} Wide Forward",
                "arbitrated_confidence": 0.82,
                "position_conflict_flag": bool(heuristic_source),
                "arbitration_reason": "V13 zone-distribution rescue selected winger from repeated wide/high-wide average positions.",
                "position_evidence": f"v6_scores={scores}; wide_pct={wide_pct:.2f}; high_wide_pct={high_wide_pct:.2f}; right_pct={right_pct:.2f}; left_pct={left_pct:.2f}; spatial_n={spatial_n}",
            }

    # Spatial data is powerful, but season averages can be misleading:
    # - CBs often average left/right in build-up
    # - inverted WBs can average central
    # - wide forwards can drift central
    # So use it as an override only when it agrees with role-score evidence.
    if spatial_pos and spatial_strength >= 0.62:
        def_score = max(scores.get("CB", 0), scores.get("FB", 0), scores.get("WB", 0))
        mid_score = max(scores.get("DM", 0), scores.get("CM", 0), scores.get("AM", 0))
        att_score = max(scores.get("W", 0), scores.get("ST", 0), scores.get("SS", 0))

        spatial_is_coherent = True

        # Do not let left/right CB build-up positions become fullbacks
        # if CB statistical evidence is clearly stronger.
        if source_family == "DEF" and spatial_group in {"FB", "WB"}:
            spatial_is_coherent = max(scores.get("FB", 0), scores.get("WB", 0)) >= scores.get("CB", 0) - 0.35

        # Do not let a midfielder become a wide midfielder purely from y coordinate
        # unless wide/defensive-wide evidence is competitive.
        if source_family == "MID" and spatial_group in {"WM", "AM-W"}:
            spatial_is_coherent = (
                scores.get("W", 0) >= mid_score + 0.10
                or max(scores.get("FB", 0), scores.get("WB", 0)) >= mid_score - 0.25
            )

        # Do not let attackers become central/wide purely by coordinate if attacking
        # score says the opposite very strongly.
        if source_family == "ATT" and spatial_group in {"AM-W"}:
            spatial_is_coherent = scores.get("W", 0) >= max(scores.get("ST", 0), scores.get("SS", 0)) + 0.05

        if spatial_is_coherent:
            conf = confidence_from_evidence(
                base=0.58,
                agreement=agreement,
                evidence_gap=0.45,
                spatial_strength=spatial_strength,
                profile_bonus=0.03 if profile_code else 0.0,
                conflict_penalty=0.02 if heuristic_source else 0.0,
            )
            return {
                "arbitrated_position": spatial_pos,
                "arbitrated_role_group": spatial_group,
                "arbitrated_lane": spatial_lane,
                "arbitrated_confidence": conf,
                "position_conflict_flag": bool(heuristic_source),
                "arbitration_reason": "V12 used season-collapsed spatial evidence because it was coherent with role-score evidence.",
                "position_evidence": (
                    f"v6_scores={scores}; spatial={spatial_pos}; spatial_strength={spatial_strength:.2f}; "
                    f"source_family={source_family}; spatial_is_coherent={spatial_is_coherent}"
                ),
            }

    # Defender splitter: rescue generic D into CB/FB/WB/CB-FB.
    if source_family == "DEF" or profile_code in {"D"} or match_pos in {"D"} or match_role in {"D"}:
        role, top, second = best_role(scores, ["CB", "FB", "WB"])
        gap = top - second
        pos, group, lane, def_detail = defender_refinement_v7(row, scores)

        # Hybrids should carry a slightly lower confidence than clean CB/FB calls.
        hybrid_penalty = 0.04 if group == "CB-FB" else 0.0
        conf = round(clamp(
            0.52
            + min(max(gap, 0) / 2.5, 0.18)
            + min(top / 5.0, 0.12)
            + (0.05 if not heuristic_source else 0)
            - hybrid_penalty,
            0.45,
            0.86,
        ), 2)

        if top >= 1.4:
            return {
                "arbitrated_position": pos,
                "arbitrated_role_group": group,
                "arbitrated_lane": lane,
                "arbitrated_confidence": conf,
                "position_conflict_flag": bool(heuristic_source or gap < 0.6 or group == "CB-FB"),
                "arbitration_reason": f"V7 defender refinement selected {pos}; top role={role}, top score={top:.2f}, next={second:.2f}.",
                "position_evidence": f"v6_scores={scores}; {def_detail}; source_family={source_family}",
            }

    # V8 wide-defender gate: run before CM fallback, because wingbacks often look like mobile midfielders.
    wide_def_hit, wd_pos, wd_group, wd_lane, wd_conf, wd_evidence = wide_defender_gate_v8(row, scores)
    if wide_def_hit and (source_family in {"MID", "DEF"} or profile_code in {"M", "D"} or match_pos in {"M", "D"} or match_role in {"M", "D"}):
        return {
            "arbitrated_position": wd_pos,
            "arbitrated_role_group": wd_group,
            "arbitrated_lane": wd_lane,
            "arbitrated_confidence": wd_conf,
            "position_conflict_flag": True,
            "arbitration_reason": f"V8 wide-defender gate selected {wd_pos}; player looked like a wingback/fullback rather than CM.",
            "position_evidence": f"v6_scores={scores}; {wd_evidence}; source_family={source_family}",
        }

    # V9 creative wide-defender rescue (Grimaldo archetype)
    cw_hit, cw_pos, cw_group, cw_lane, cw_conf, cw_evidence = creative_wide_defender_v9(row, scores)
    if cw_hit and (source_family in {"MID", "DEF"} or profile_code in {"M", "D"} or match_pos in {"M", "D"} or match_role in {"M", "D"}):
        return {
            "arbitrated_position": cw_pos,
            "arbitrated_role_group": cw_group,
            "arbitrated_lane": cw_lane,
            "arbitrated_confidence": cw_conf,
            "position_conflict_flag": True,
            "arbitration_reason": f"V9 creative wide-defender rescue selected {cw_pos}.",
            "position_evidence": f"v6_scores={scores}; {cw_evidence}; source_family={source_family}",
        }

    # Midfield splitter: do not call a midfielder wide unless wide evidence is genuinely dominant.
    if source_family == "MID" or profile_code == "M" or match_pos == "M" or match_role == "M":
        mid_role, mid_top, mid_second = best_role(scores, ["DM", "CM", "AM"])
        wide_is_dominant = scores["W"] >= 3.7 and scores["W"] >= mid_top + 0.45

        if wide_is_dominant:
            role, top, second = "W", scores["W"], mid_top
        else:
            role, top, second = mid_role, mid_top, mid_second

        gap = top - second
        if role == "W":
            pos, group, lane = "W", "AM-W", "Wide Forward"
        elif role == "DM":
            pos, group, lane = "DM", "DM", "Defensive Midfield"
        elif role == "CM":
            pos, group, lane = "CM", "CM", "Central Midfield"
        else:
            pos, group, lane = "AMC", "AM-C", "Central Attacking Midfield"

        if heuristic_source or profile_code in {"M", "F", "D"} or match_pos in {"M", "F", "D"}:
            conf = round(clamp(0.50 + min(max(gap, 0) / 2.2, 0.17) + min(top / 5.5, 0.13), 0.46, 0.84), 2)
            return {
                "arbitrated_position": pos,
                "arbitrated_role_group": group,
                "arbitrated_lane": lane,
                "arbitrated_confidence": conf,
                "position_conflict_flag": bool(heuristic_source or gap < 0.45),
                "arbitration_reason": f"V6 midfield splitter selected {pos}; top score={top:.2f}, next={second:.2f}.",
                "position_evidence": f"v6_scores={scores}; source_family={source_family}; wide_is_dominant={wide_is_dominant}",
            }

    # Attacker / winger splitter.
    if source_family == "ATT" or profile_code in {"F", "FW"} or match_pos in {"F", "FW"} or match_role in {"F", "FW"}:
        role, top, second = best_role(scores, ["W", "ST", "SS"])

        # For creative centre-forwards, keep SS only when clearly close AND not a strong scorer.
        if (
            role == "ST"
            and scores["SS"] >= scores["ST"] - 0.25
            and scores["SS"] >= 2.55
            and scores["ST"] <= 3.05
        ):
            role, top, second = "SS", scores["SS"], scores["ST"]

        # Family gate: high W score should beat ST/SS for true wide forwards.
        if scores["W"] >= 3.2 and scores["W"] >= scores["ST"] + 0.6:
            role, top, second = "W", scores["W"], max(scores["ST"], scores["SS"])

        gap = top - second
        if role == "W":
            pos, group, lane = "W", "AM-W", "Wide Forward"
        elif role == "ST":
            pos, group, lane = "ST", "ST", "Central Forward"
        else:
            pos, group, lane = "SS", "ST-SS", "Central Support Forward"

        if heuristic_source or profile_code in {"M", "F", "D"} or match_pos in {"M", "F", "D"}:
            conf = round(clamp(0.50 + min(max(gap, 0) / 2.2, 0.17) + min(top / 5.5, 0.13), 0.46, 0.84), 2)
            return {
                "arbitrated_position": pos,
                "arbitrated_role_group": group,
                "arbitrated_lane": lane,
                "arbitrated_confidence": conf,
                "position_conflict_flag": bool(heuristic_source or gap < 0.45),
                "arbitration_reason": f"V6 attacker splitter selected {pos}; top score={top:.2f}, next={second:.2f}.",
                "position_evidence": f"v6_scores={scores}; source_family={source_family}",
            }

    return None


def arbitrate_row(row: pd.Series, df: pd.DataFrame) -> dict[str, Any]:
    profile_code = normalize_position(row.get("profile_position_raw"))
    match_pos = normalize_position(row.get("match_position_mode"))
    match_role = normalize_position(row.get("match_role_mode"))

    candidates = [profile_code, match_pos, match_role]
    candidates = [c for c in candidates if c]
    agreement = source_agreement_strength(candidates)
    heuristic_source = is_heuristic_position_source(row)

    # If the upstream scraper created ST/SS/AM from heuristic stats, do not treat
    # those labels as strong source agreement. They are evidence, not ground truth.
    if heuristic_source:
        agreement = min(agreement, 0.45)

    result = {
        "arbitrated_position": None,
        "arbitrated_role_group": None,
        "arbitrated_lane": None,
        "arbitrated_confidence": 0.0,
        "position_conflict_flag": False,
        "arbitration_reason": "",
        "position_evidence": "",
    }

    if not candidates:
        result.update({
            "arbitrated_position": None,
            "arbitrated_role_group": None,
            "arbitrated_lane": "Unknown",
            "arbitrated_confidence": 0.35,
            "position_conflict_flag": True,
            "arbitration_reason": "No usable profile or match position found.",
            "position_evidence": "none",
        })
        return result

    # Evidence cohort for attacking players.
    attacking_groups = {"AM-C", "AM-W", "ST", "ST-SS", "WM"}
    attack_cohort = df.loc[df["_base_group"].isin(attacking_groups)].copy()
    if attack_cohort.empty:
        attack_cohort = df

    wide_score, wide_used = wide_attack_evidence(row, attack_cohort)
    central_score, central_used = central_attack_evidence(row, attack_cohort)
    gap_strength = score_gap_strength(wide_score, central_score)

    x = row_number(row, SPATIAL_X_COLUMNS)
    y = row_number(row, SPATIAL_Y_COLUMNS)
    spatial_base = profile_code or match_role or match_pos
    spatial_pos, spatial_group, spatial_lane, spatial_strength, spatial_source = spatial_role_from_xy(spatial_base, x, y)

    raw_groups = {position_group(c) for c in candidates if c}
    conflict = len({g for g in raw_groups if g}) > 1

    evidence_bits = [
        f"sources={','.join(candidates)}",
        f"agreement={agreement:.2f}",
        f"wide_score={wide_score:.1f}",
        f"central_score={central_score:.1f}",
    ]
    if spatial_pos:
        evidence_bits.append(f"spatial={spatial_pos}@x={x:.1f},y={y:.1f},strength={spatial_strength:.2f}")
    if heuristic_source:
        evidence_bits.append("upstream_position_source=heuristic")

    v6_result = family_first_arbitration_v6(
        row=row,
        profile_code=profile_code,
        match_pos=match_pos,
        match_role=match_role,
        heuristic_source=heuristic_source,
        agreement=agreement,
        spatial_pos=spatial_pos,
        spatial_group=spatial_group,
        spatial_lane=spatial_lane,
        spatial_strength=spatial_strength,
    )
    if v6_result is not None:
        return v6_result

    winger_style, winger_reasons = winger_like_from_style(profile_code, match_pos, match_role, wide_score, central_score, row)
    if heuristic_source and winger_style:
        # This catches Doku/Diomande-type cases:
        # upstream labels say ST/SS/AM because they are high-touch creators,
        # but the actual style profile is clearly winger-like.
        side_hint = None
        if spatial_pos in {"RW", "LW"}:
            side_hint = spatial_pos
        pos = side_hint or "W"
        lane = "Wide Forward" if side_hint is None else ("Right Wide Forward" if side_hint == "RW" else "Left Wide Forward")
        conf = confidence_from_evidence(
            base=0.56,
            agreement=agreement,
            evidence_gap=max(gap_strength, 0.35),
            spatial_strength=spatial_strength if spatial_group == "AM-W" else 0.0,
            profile_bonus=0.0,
            conflict_penalty=0.02,
        )
        result.update({
            "arbitrated_position": pos,
            "arbitrated_role_group": "AM-W",
            "arbitrated_lane": lane,
            "arbitrated_confidence": conf,
            "position_conflict_flag": True,
            "arbitration_reason": (
                "Upstream position was generated by heuristic stats and produced a central attacking label, "
                "but the player's style profile is strongly winger-like. "
                f"Signals: {', '.join(winger_reasons)}."
            ),
            "position_evidence": "; ".join(evidence_bits),
        })
        return result

    # Spatial override: if we have average-position data and it cleanly resolves a generic source label.
    generic_codes = GENERIC_POSITION_CODES
    if spatial_pos and (
        profile_code in generic_codes
        or match_pos in generic_codes
        or match_role in generic_codes
        or agreement < 0.6
        or heuristic_source
    ):
        conf = confidence_from_evidence(
            base=0.52,
            agreement=agreement,
            evidence_gap=gap_strength,
            spatial_strength=spatial_strength,
            profile_bonus=0.03 if profile_code else 0.0,
            conflict_penalty=0.06 if conflict else 0.0,
        )
        result.update({
            "arbitrated_position": spatial_pos,
            "arbitrated_role_group": spatial_group,
            "arbitrated_lane": spatial_lane,
            "arbitrated_confidence": conf,
            "position_conflict_flag": bool(conflict),
            "arbitration_reason": (
                f"Used average-position spatial evidence ({spatial_source}) to resolve generic/conflicting labels. "
                f"Wide score={wide_score:.1f}, central score={central_score:.1f}."
            ),
            "position_evidence": "; ".join(evidence_bits),
        })
        return result

    # Strong override: explicit profile wide attacker.
    if profile_code in {"RW", "LW", "AMR", "AML"}:
        side = position_side(profile_code)
        conflict = any(c in {"ST", "AMC", "AM"} for c in [match_pos, match_role] if c)
        if match_role == "ST" or match_pos == "ST":
            pos = f"{profile_code}/ST" if wide_score >= 45 else profile_code
            group = "AM-W"
            lane = f"{side} Wide Forward" if side else "Wide Forward"
            reason = (
                f"Profile position is {profile_code}; match data includes ST. "
                "Resolved as wide attacker with striker hybrid flag."
            )
            base = 0.58
        else:
            pos = profile_code
            group = "AM-W"
            lane = f"{side} Wide" if side else "Wide"
            reason = f"Profile position is explicitly wide ({profile_code})."
            base = 0.62
        conf = confidence_from_evidence(
            base=base,
            agreement=agreement,
            evidence_gap=gap_strength,
            spatial_strength=spatial_strength if spatial_group == "AM-W" else 0.0,
            profile_bonus=0.07,
            conflict_penalty=0.05 if conflict else 0.0,
        )
        result.update({
            "arbitrated_position": pos,
            "arbitrated_role_group": group,
            "arbitrated_lane": lane,
            "arbitrated_confidence": conf,
            "position_conflict_flag": bool(conflict),
            "arbitration_reason": reason + f" Wide evidence={wide_score:.1f}, central evidence={central_score:.1f}.",
            "position_evidence": "; ".join(evidence_bits),
        })
        return result

    # Profile says wide fullback/wingback.
    if profile_code in {"RB", "LB", "RWB", "LWB"}:
        side = position_side(profile_code)
        group = position_group(profile_code)
        conflict = any(position_group(c) not in {group, None} for c in [match_pos, match_role] if c)
        conf = confidence_from_evidence(
            base=0.60,
            agreement=agreement,
            spatial_strength=spatial_strength if spatial_group in {"FB", "WB"} else 0.0,
            profile_bonus=0.07,
            conflict_penalty=0.05 if conflict else 0.0,
        )
        result.update({
            "arbitrated_position": profile_code,
            "arbitrated_role_group": group,
            "arbitrated_lane": f"{side} Wide Defensive" if side else "Wide Defensive",
            "arbitrated_confidence": conf,
            "position_conflict_flag": bool(conflict),
            "arbitration_reason": f"Profile position is explicitly wide defensive ({profile_code}).",
            "position_evidence": "; ".join(evidence_bits),
        })
        return result

    # Midfield split: DM / CM / AM from spatial data if generic M was the source.
    if spatial_pos and raw_groups & {"DM", "CM", "AM-C", "AM-W", "WM"} and (profile_code in {"M", None} or match_pos in {"M", None}):
        conf = confidence_from_evidence(
            base=0.50,
            agreement=agreement,
            evidence_gap=gap_strength,
            spatial_strength=spatial_strength,
            profile_bonus=0.02 if profile_code else 0.0,
            conflict_penalty=0.04 if conflict else 0.0,
        )
        result.update({
            "arbitrated_position": spatial_pos,
            "arbitrated_role_group": spatial_group,
            "arbitrated_lane": spatial_lane,
            "arbitrated_confidence": conf,
            "position_conflict_flag": bool(conflict),
            "arbitration_reason": "Generic midfield label split using average-position spatial evidence.",
            "position_evidence": "; ".join(evidence_bits),
        })
        return result

    # Match says AM, evidence says wide or central.
    if raw_groups & {"AM-C", "AM-W", "WM"}:
        if spatial_pos and spatial_group in {"AM-W", "WM", "AM-C"} and spatial_strength >= 0.70:
            group, pos, lane = spatial_group, spatial_pos, spatial_lane
            reason = f"Attacking-midfielder source label refined by spatial evidence ({spatial_source})."
            spatial_component = spatial_strength
        elif wide_score >= central_score + 12:
            group = "AM-W"
            pos = "W"
            lane = "Wide Forward"
            reason = f"Attacking-midfielder source label; wide evidence leads central by {wide_score - central_score:.1f}."
            spatial_component = 0.0
        elif central_score >= wide_score + 12:
            group = "AM-C"
            pos = "AMC"
            lane = "Central"
            reason = f"Attacking-midfielder source label; central evidence leads wide by {central_score - wide_score:.1f}."
            spatial_component = 0.0
        else:
            group = "AM-HYB"
            pos = "AM-HYB"
            lane = "Hybrid"
            reason = f"Attacking-midfielder source label; wide and central evidence are close ({wide_score:.1f} vs {central_score:.1f})."
            spatial_component = 0.0
        conf = confidence_from_evidence(
            base=0.48,
            agreement=agreement,
            evidence_gap=gap_strength,
            spatial_strength=spatial_component,
            profile_bonus=0.03 if profile_code else 0.0,
            conflict_penalty=0.05 if conflict else 0.0,
        )
        result.update({
            "arbitrated_position": pos,
            "arbitrated_role_group": group,
            "arbitrated_lane": lane,
            "arbitrated_confidence": conf,
            "position_conflict_flag": bool(conflict),
            "arbitration_reason": reason,
            "position_evidence": "; ".join(evidence_bits),
        })
        return result

    # ST / forward split.
    if raw_groups & {"ST", "ST-SS"}:
        if spatial_pos and spatial_group in {"AM-W", "ST", "ST-SS"} and spatial_strength >= 0.70:
            group, pos, lane = spatial_group, spatial_pos, spatial_lane
            reason = f"Forward source label refined by spatial evidence ({spatial_source})."
            conflict = conflict or group == "AM-W"
            spatial_component = spatial_strength
        elif profile_code in WIDE_ATTACK_CODES or winger_style or (wide_score >= central_score + 18 and wide_score >= 60):
            group = "AM-W" if winger_style else "ST-W"
            pos = "W" if winger_style else "ST-W"
            lane = "Wide Forward" if winger_style else "Wide/Channel Forward"
            conflict = True
            if winger_style:
                reason = (
                    "ST source label came from weak/generic evidence, but style is winger-like. "
                    f"Signals: {', '.join(winger_reasons)}."
                )
            else:
                reason = f"ST source label but strong wide evidence ({wide_score:.1f}) exceeds central evidence ({central_score:.1f})."
            spatial_component = 0.0
        elif central_score >= wide_score + 15 and central_score >= 55:
            group = "ST-SS"
            pos = "SS"
            lane = "Central Support Forward"
            conflict = False
            reason = f"ST source label with central/link evidence ({central_score:.1f}) above wide evidence ({wide_score:.1f})."
            spatial_component = 0.0
        else:
            group = "ST"
            pos = "ST"
            lane = "Central Forward"
            conflict = False
            reason = f"ST source label retained. Wide evidence={wide_score:.1f}, central evidence={central_score:.1f}."
            spatial_component = 0.0
        conf = confidence_from_evidence(
            base=0.50,
            agreement=agreement,
            evidence_gap=gap_strength,
            spatial_strength=spatial_component,
            profile_bonus=0.03 if profile_code else 0.0,
            conflict_penalty=0.05 if conflict else 0.0,
        )
        result.update({
            "arbitrated_position": pos,
            "arbitrated_role_group": group,
            "arbitrated_lane": lane,
            "arbitrated_confidence": conf,
            "position_conflict_flag": bool(conflict),
            "arbitration_reason": reason,
            "position_evidence": "; ".join(evidence_bits),
        })
        return result

    # Defender split when spatial data exists.
    if spatial_pos and raw_groups & {"CB", "FB", "WB"}:
        conf = confidence_from_evidence(
            base=0.52,
            agreement=agreement,
            spatial_strength=spatial_strength,
            profile_bonus=0.03 if profile_code else 0.0,
            conflict_penalty=0.04 if conflict else 0.0,
        )
        result.update({
            "arbitrated_position": spatial_pos,
            "arbitrated_role_group": spatial_group,
            "arbitrated_lane": spatial_lane,
            "arbitrated_confidence": conf,
            "position_conflict_flag": bool(conflict),
            "arbitration_reason": "Defensive label refined using average-position spatial evidence.",
            "position_evidence": "; ".join(evidence_bits),
        })
        return result

    # Default: trust profile first, then match position, then match role.
    final_code = profile_code or match_pos or match_role
    conf = confidence_from_evidence(
        base=0.46,
        agreement=agreement,
        evidence_gap=gap_strength,
        spatial_strength=spatial_strength if spatial_pos and position_group(spatial_pos) == position_group(final_code) else 0.0,
        profile_bonus=0.05 if profile_code else 0.0,
        conflict_penalty=0.05 if conflict else 0.0,
    )
    result.update({
        "arbitrated_position": final_code,
        "arbitrated_role_group": position_group(final_code),
        "arbitrated_lane": position_side(final_code) or "Central/Unspecified",
        "arbitrated_confidence": conf,
        "position_conflict_flag": bool(conflict),
        "arbitration_reason": "Defaulted to profile position when available, otherwise weighted match-position mode.",
        "position_evidence": "; ".join(evidence_bits),
    })
    return result

def prepare_input(df: pd.DataFrame) -> pd.DataFrame:
    work = ensure_per90(df)

    profile_col = find_first_col(work, PROFILE_POSITION_COLUMNS)
    if profile_col:
        work["profile_position_raw"] = work[profile_col]
    else:
        work["profile_position_raw"] = np.nan

    match_pos_cols = [c for c in MATCH_POSITION_COLUMNS if c in work.columns and c != profile_col]
    match_role_cols = [c for c in MATCH_ROLE_COLUMNS if c in work.columns]

    if "player_id" in work.columns and "season" in work.columns and ("event_id" in work.columns or "match_id" in work.columns):
        # Match-level input: summarize position modes by player-season.
        keys = ["player_id", "season"]
        if "league" in work.columns:
            keys.append("league")

        profile_summaries = []
        for _, group in work.groupby(keys, dropna=False):
            idxs = group.index
            profile_mode, profile_summary = weighted_position_summary(group, ["profile_position_raw"], MINUTES_COL)
            match_pos_mode, match_pos_summary = weighted_position_summary(group, match_pos_cols, MINUTES_COL)
            match_role_mode, match_role_summary = weighted_position_summary(group, match_role_cols, MINUTES_COL)
            minutes = numeric(group[MINUTES_COL]) if MINUTES_COL in group.columns else pd.Series([1.0] * len(group), index=group.index)
            x_col = find_first_col(group, SPATIAL_X_COLUMNS)
            y_col = find_first_col(group, SPATIAL_Y_COLUMNS)
            if x_col and y_col:
                x_vals = numeric(group[x_col])
                y_vals = numeric(group[y_col])
                valid = x_vals.notna() & y_vals.notna() & minutes.notna() & (minutes > 0)
                if valid.any():
                    avg_x = float(np.average(x_vals[valid], weights=minutes[valid]))
                    avg_y = float(np.average(y_vals[valid], weights=minutes[valid]))
                    spatial_n = int(valid.sum())
                else:
                    avg_x, avg_y, spatial_n = np.nan, np.nan, 0
            else:
                avg_x, avg_y, spatial_n = np.nan, np.nan, 0

            for idx in idxs:
                profile_summaries.append((idx, profile_mode, profile_summary, match_pos_mode, match_pos_summary, match_role_mode, match_role_summary, avg_x, avg_y, spatial_n))

        for col in ["profile_position_mode", "profile_positions_played", "match_position_mode", "match_positions_played", "match_role_mode", "match_roles_played", "avg_x", "avg_y", "spatial_matches_used"]:
            if col not in work.columns:
                work[col] = np.nan
        for idx, prof_mode, prof_sum, pos_mode, pos_sum, role_mode, role_sum, avg_x, avg_y, spatial_n in profile_summaries:
            work.at[idx, "profile_position_mode"] = prof_mode
            work.at[idx, "profile_positions_played"] = prof_sum
            work.at[idx, "match_position_mode"] = pos_mode
            work.at[idx, "match_positions_played"] = pos_sum
            work.at[idx, "match_role_mode"] = role_mode
            work.at[idx, "match_roles_played"] = role_sum
            if not pd.isna(avg_x):
                work.at[idx, "avg_x"] = avg_x
            if not pd.isna(avg_y):
                work.at[idx, "avg_y"] = avg_y
            work.at[idx, "spatial_matches_used"] = spatial_n

        # Use profile mode as raw profile for arbitration.
        work["profile_position_raw"] = work["profile_position_mode"]
    else:
        # Season-level input: use existing columns directly.
        if "match_position_mode" not in work.columns:
            pos_source = find_first_col(work, ["primary_role_position", "role_position", "player_position", "base_position"])
            work["match_position_mode"] = work[pos_source] if pos_source else np.nan
        if "match_role_mode" not in work.columns:
            role_source = find_first_col(work, ["primary_role_position", "role_position"])
            work["match_role_mode"] = work[role_source] if role_source else np.nan
        if "profile_positions_played" not in work.columns:
            work["profile_positions_played"] = work["profile_position_raw"]
        if "match_positions_played" not in work.columns:
            work["match_positions_played"] = work["match_position_mode"]
        if "match_roles_played" not in work.columns:
            work["match_roles_played"] = work["match_role_mode"]

    # Base group for evidence cohorts.
    base = []
    for _, row in work.iterrows():
        code = normalize_position(row.get("profile_position_raw")) or normalize_position(row.get("match_position_mode")) or normalize_position(row.get("match_role_mode"))
        base.append(position_group(code))
    work["_base_group"] = base
    return work


def arbitrate_dataset(df: pd.DataFrame, season: str | None, league: str | None, player_id: int | None, min_minutes: float | None) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    work = prepare_input(df)

    if season is not None and "season" in work.columns:
        work = work.loc[work["season"].astype(str) == str(season)].copy()
    if league is not None and "league" in work.columns:
        work = work.loc[work["league"].astype(str).str.lower() == str(league).lower()].copy()
    if min_minutes is not None and MINUTES_COL in work.columns:
        work = work.loc[numeric(work[MINUTES_COL]).fillna(0) >= float(min_minutes)].copy()

    if work.empty:
        raise ValueError("No rows left after filters.")

    target_index = work.index
    if player_id is not None:
        if "player_id" not in work.columns:
            raise ValueError("Cannot use --player-id without player_id column.")
        target_index = work.loc[numeric(work["player_id"]) == int(player_id)].index
        if len(target_index) == 0:
            raise ValueError(f"player_id={player_id} not found after filters.")

    object_cols = [
        "arbitrated_position", "arbitrated_role_group", "arbitrated_lane",
        "arbitration_reason", "position_evidence",
    ]
    for col in object_cols:
        if col not in work.columns:
            work[col] = pd.Series([None] * len(work), index=work.index, dtype="object")
        else:
            work[col] = work[col].astype("object")

    if "arbitrated_confidence" not in work.columns:
        work["arbitrated_confidence"] = np.nan
    if "position_conflict_flag" not in work.columns:
        work["position_conflict_flag"] = pd.Series([None] * len(work), index=work.index, dtype="object")
    else:
        work["position_conflict_flag"] = work["position_conflict_flag"].astype("object")

    json_rows = []
    for idx in target_index:
        row = work.loc[idx]
        arb = arbitrate_row(row, work)
        for key, value in arb.items():
            work.at[idx, key] = value

        record = {
            "player_id": row.get("player_id"),
            "player_name": row.get("player_name") if pd.notna(row.get("player_name")) else row.get("profile_name"),
            "season": row.get("season"),
            "league": row.get("league"),
            "team": row.get("team"),
            "minutes_played": row.get("minutes_played"),
            "profile_position_raw": row.get("profile_position_raw"),
            "profile_positions_played": row.get("profile_positions_played"),
            "match_position_mode": row.get("match_position_mode"),
            "match_positions_played": row.get("match_positions_played"),
            "match_role_mode": row.get("match_role_mode"),
            "match_roles_played": row.get("match_roles_played"),
            "avg_x": row.get("avg_x"),
            "avg_y": row.get("avg_y"),
            "spatial_matches_used": row.get("spatial_matches_used"),
            **arb,
        }
        json_rows.append(record)

    work = work.drop(columns=["_base_group"], errors="ignore")
    return work, json_rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Arbitrate conflicting source positions into final comparison cohorts.")
    ap.add_argument("--input", "-i", default="player_season_totals.csv", help="Input CSV")
    ap.add_argument("--output", "-o", default="player_positions_arbitrated.csv", help="Output path")
    ap.add_argument("--season", "-s", default=None, help="Optional season filter")
    ap.add_argument("--league", "-l", default=None, help="Optional league filter")
    ap.add_argument("--player-id", "-p", type=int, default=None, help="Optional single player output")
    ap.add_argument("--min-minutes", type=float, default=None, help="Optional minimum minutes filter")
    ap.add_argument("--format", choices=["csv", "json", "both"], default="csv")
    ap.add_argument("--position-context", default=None, help="Optional JSON cache of event/player average positions")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    df = apply_position_context(df, args.position_context)
    out, records = arbitrate_dataset(df, args.season, args.league, args.player_id, args.min_minutes)

    output_path = Path(args.output)
    written = []

    if args.format in {"csv", "both"}:
        csv_path = output_path if output_path.suffix.lower() == ".csv" else output_path.with_suffix(".csv")
        out.to_csv(csv_path, index=False)
        written.append(csv_path)

    if args.format in {"json", "both"}:
        json_path = output_path if output_path.suffix.lower() == ".json" else output_path.with_suffix(".json")
        payload = records[0] if args.player_id is not None and len(records) == 1 else records
        json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        written.append(json_path)

    print("\nDone.")
    for p in written:
        print(f"Output: {p}")

    print("\nSample arbitrations:")
    for r in records[:10]:
        print(
            f"  {r.get('player_name')} ({r.get('player_id')}): "
            f"profile={r.get('profile_position_raw')} match={r.get('match_role_mode') or r.get('match_position_mode')} "
            f"→ {r.get('arbitrated_role_group')} / {r.get('arbitrated_position')} "
            f"[strength={r.get('arbitrated_confidence')}, conflict={r.get('position_conflict_flag')}]"
        )


if __name__ == "__main__":
    main()
