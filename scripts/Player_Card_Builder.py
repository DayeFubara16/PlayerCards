
"""
Player_Card_Builder.py
──────────────────────
Build one Observable-ready player-card JSON from the current scouting pipeline.

V2 updates for the new pipeline:
- understands RB/LB/RWB/LWB/CB-FB labels from the arbitrator
- carries season spatial distribution fields into the card
- reads Find_Similar_Players_v4 JSON shape with {target, cohort_info, results}
- uses normalized role-family cohorts for percentiles instead of exact raw label only
- supports visual-only heatmap files from Build_Player_Season_Heatmap_v3_visual_only.py

Recommended run:
  python Player_Card_Builder.py --player-id 994546 --season 2025-26 --league "Premier League"

Expected inputs:
  player_season_totals_arbitrated.csv
  player_roles.csv
  similar_994546.json OR *_994546_*similarities.json
  Elliot Anderson_994546_2025-26.json OR 994546_2025-26.json
  *_994546_*heatmap_position.json

Output:
  player_card_PlayerName_playerID_season.json
"""

from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROLE_COL_PRIORITY = [
    "arbitrated_role_group",
    "role_model_family",
    "role_position_refined",
    "primary_role_position",
    "role_position",
]

DISPLAY_STAT_ALIASES = {
    "minutes": ["minutes_played"],
    "matches": ["matches"],
    "goals": ["goals"],
    "assists": ["assists"],
    "xg": ["xg"],
    "xgot": ["xgot"],
    "xa": ["xa"],
    "shots": ["shots_total", "shots"],
    "shots_on_target": ["shots_on_target"],
    "big_chances_created": ["big_chances_created"],
    "touches": ["touches"],
    "touches_opp_box": ["touches_opp_box"],
    "passes_total": ["passes_total"],
    "passes_accurate": ["passes_accurate"],
    "pass_accuracy_pct": ["pass_accuracy_pct"],
    "passes_opposition_half_total": ["passes_opposition_half_total"],
    "long_balls_total": ["long_balls_total"],
    "long_balls_accurate": ["long_balls_accurate"],
    "crosses_total": ["crosses_total"],
    "crosses_accurate": ["crosses_accurate"],
    "dribbles_attempted": ["dribbles_attempted", "contests_total"],
    "dribbles_won": ["dribbles_won", "contests_won"],
    "carries": ["carries"],
    "carry_distance": ["carry_distance"],
    "progressive_carries": ["progressive_carries"],
    "progressive_carry_distance": ["progressive_carry_distance"],
    "total_progression": ["total_progression"],
    "tackles_total": ["tackles_total"],
    "tackles_won": ["tackles_won"],
    "interceptions": ["interceptions"],
    "clearances": ["clearances"],
    "blocked_shots": ["blocked_shots"],
    "recoveries": ["recoveries"],
    "duels_total": ["duels_total"],
    "duels_won": ["duels_won"],
    "aerial_duels_total": ["aerial_duels_total"],
    "aerial_duels_won": ["aerial_duels_won"],
    "fouls_drawn": ["fouls_drawn"],
    "fouls_committed": ["fouls_committed"],
    "possession_lost": ["possession_lost"],
    "dispossessed": ["dispossessed"],
}

# Dedicated GK metrics are only used for GK profiles. Shared outfielder stats
# that also appear for keepers stay available to keep the card flexible.
GK_ONLY_METRICS = {
    "gk_saves", "gk_saves_inside_box", "gk_xgot_faced", "gk_goals_prevented",
    "gk_goals_prevented_raw", "gk_save_value", "gk_high_claims", "gk_punches",
    "gk_sweeper_total", "gk_sweeper_accurate", "penalties_faced",
}

# Physical / metadata / provider-specific fields are intentionally excluded from
# percentile and grading selection because they are not universal scouting outputs.
EXCLUDED_METRIC_TOKENS = {
    "sofascore", "rating", "height", "weight", "shirt", "jersey", "market_value",
    "contract", "injury", "date_of_birth", "dob", "age_as_of", "distance_walking",
    "distance_jogging", "distance_running", "distance_high_speed", "distance_sprinting",
    "leagues_played", "teams_played", "positions_played", "position_confidence",
    "arbitrated_confidence", "spatial_", "season_avg", "season_median", "season_std",
    "avg_x", "avg_y", "sub_on", "sub_off", "is_substitute", "mw",
}

# Role weights guide ordering and composite role_grade; they do not hard-filter
# outfielder metrics. Any valid, available outfielder stat can still surface.
ROLE_METRIC_WEIGHTS = {
    "ST": {"goal": 1.6, "xg": 1.5, "xgot": 1.4, "shot": 1.3, "touches_opp_box": 1.2, "aerial": 0.9, "key_pass": 0.8, "xa": 0.8},
    "AM": {"key_pass": 1.5, "xa": 1.5, "assist": 1.3, "pass": 1.1, "progressive": 1.1, "carry": 1.1, "dribble": 1.0, "shot": 0.9, "xg": 0.9},
    "W": {"progressive": 1.4, "carry": 1.4, "dribble": 1.3, "cross": 1.3, "key_pass": 1.2, "xa": 1.2, "shot": 1.0, "xg": 1.0, "touches_opp_box": 1.0},
    "CM": {"pass": 1.3, "progressive": 1.3, "progression": 1.3, "carry": 1.1, "key_pass": 1.0, "xa": 0.9, "recover": 1.0, "tackle": 0.9, "interception": 0.9},
    "DM": {"interception": 1.5, "tackle": 1.4, "recover": 1.3, "duel": 1.2, "clearance": 1.0, "block": 1.0, "pass": 1.0, "progressive": 0.9},
    "FB": {"cross": 1.4, "progressive": 1.3, "carry": 1.2, "pass": 1.1, "key_pass": 1.1, "xa": 1.1, "tackle": 1.0, "interception": 1.0, "recover": 1.0},
    "WB": {"cross": 1.4, "progressive": 1.4, "carry": 1.3, "touches_opp_box": 1.1, "key_pass": 1.1, "xa": 1.1, "tackle": 0.9, "recover": 0.9},
    "CB": {"clearance": 1.4, "block": 1.3, "interception": 1.3, "tackle": 1.2, "aerial": 1.2, "duel": 1.1, "pass": 1.0, "long_ball": 0.9},
    "CB-FB": {"interception": 1.3, "tackle": 1.2, "recover": 1.2, "clearance": 1.1, "pass": 1.1, "progressive": 1.0, "carry": 1.0, "cross": 0.8},
    "GK": {"gk_saves": 1.5, "gk_xgot": 1.5, "gk_goals_prevented": 1.5, "gk_high_claims": 1.1, "gk_sweeper": 1.1, "pass": 0.8, "long_ball": 0.8},
}

GRADE_CATEGORIES = {
    "progression": ["progressive", "carry", "progression", "passes_opposition_half", "long_balls", "pass_value"],
    "defense": ["tackle", "interception", "recover", "duel", "clearance", "block", "challenge"],
    "creation": ["key_pass", "xa", "assist", "cross", "big_chances_created"],
    "scoring": ["goal", "xg", "xgot", "shot", "touches_opp_box", "big_chance_missed"],
    "possession": ["passes", "pass_accuracy", "touches", "dribble", "dispossessed", "possession_lost"],
    "goalkeeping": ["gk_", "penalties_faced"],
}


def clean_filename(text: str | None, fallback: str = "Player") -> str:
    text = str(text or fallback).strip()
    text = re.sub(r"[^\w\s.-]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "_", text)
    return text or fallback


def parse_json(path: str | Path | None) -> Any:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def num(x: Any) -> float | None:
    if x in (None, ""):
        return None
    try:
        v = float(x)
        return None if math.isnan(v) else v
    except Exception:
        return None


def round_or_none(x: Any, digits: int = 3):
    v = num(x)
    return None if v is None else round(v, digits)


def value(row: pd.Series | dict[str, Any] | None, names: list[str], default=None):
    if row is None:
        return default
    if isinstance(row, pd.Series):
        lower = {str(c).lower(): c for c in row.index}
        for name in names:
            col = lower.get(name.lower())
            if col is not None:
                v = row.get(col)
                if pd.notna(v):
                    return v
    else:
        lower = {str(c).lower(): c for c in row.keys()}
        for name in names:
            col = lower.get(name.lower())
            if col is not None:
                v = row.get(col)
                if v not in (None, ""):
                    return v
    return default


def choose_role_col(df: pd.DataFrame) -> str | None:
    for c in ROLE_COL_PRIORITY:
        if c in df.columns:
            return c
    return None


def normalize_family(role_value: Any) -> str | None:
    if role_value is None or pd.isna(role_value):
        return None
    text = str(role_value).upper().replace("_", "-").replace(" ", "-").strip()
    text = re.sub(r"-+", "-", text)

    if text in {"GK", "G", "GOALKEEPER"}:
        return "GK"
    if text in {"CB", "RCB", "LCB"}:
        return "CB"
    if text in {"CB-FB", "FB-CB", "WCB", "WIDE-CB", "RB-CB", "LB-CB"}:
        return "CB-FB"
    if text in {"RB", "LB", "FB", "FULLBACK", "FULL-BACK", "RIGHT-BACK", "LEFT-BACK"}:
        return "FB"
    if text.startswith("FB"):
        return "FB"
    if text in {"RWB", "LWB", "WB", "WINGBACK", "WING-BACK"}:
        return "WB"
    if text.startswith("WB"):
        return "WB"
    if text in {"RW", "LW", "W", "AM-W", "AMR", "AML", "WM", "RM", "LM"}:
        return "W"
    if text.startswith("AM"):
        return "AM"
    if text in {"ST", "SS", "CF", "F", "FW", "ST-SS", "ST-W"} or text.startswith("ST"):
        return "ST"
    if text in {"CM", "DM"}:
        return text
    return text


def normalized_family_series(series: pd.Series) -> pd.Series:
    return series.apply(normalize_family)

def find_player_row(df: pd.DataFrame, player_id: int, season: str | None, league: str | None) -> pd.Series:
    work = df.loc[pd.to_numeric(df["player_id"], errors="coerce") == int(player_id)].copy()
    if season and "season" in work.columns:
        work = work.loc[work["season"].astype(str) == str(season)]
    if league and "league" in work.columns:
        work = work.loc[work["league"].astype(str).str.lower() == str(league).lower()]
    if work.empty:
        raise ValueError(f"Could not find player_id={player_id} season={season} league={league}.")
    return work.iloc[0]


def find_role_row(path: str | Path | None, player_id: int, season: str | None, league: str | None) -> dict[str, Any]:
    if not path or not Path(path).exists():
        return {}
    df = pd.read_csv(path)
    if "player_id" not in df.columns:
        return {}
    work = df.loc[pd.to_numeric(df["player_id"], errors="coerce") == int(player_id)].copy()
    if season and "season" in work.columns:
        work = work.loc[work["season"].astype(str) == str(season)]
    if league and "league" in work.columns:
        work = work.loc[work["league"].astype(str).str.lower() == str(league).lower()]
    return {} if work.empty else work.iloc[0].to_dict()


def auto_find_json(player_id: int, season: str, kind: str, search_dir: str | Path = ".") -> str | None:
    d = Path(search_dir)
    season_clean = str(season).replace("/", "-")
    if kind == "event":
        patterns = [f"{player_id}_{season_clean}.json", f"*{player_id}*{season_clean}.json"]
        exclude = ["heatmap", "similar", "card"]
    elif kind == "heatmap":
        patterns = [f"*{player_id}*{season_clean}*heatmap_position.json", f"*{player_id}*heatmap*.json"]
        exclude = ["card"]
    elif kind == "similarity":
        patterns = [f"similar_{player_id}.json", f"*{player_id}*{season_clean}*similarit*.json", f"*{player_id}*similar*.json"]
        exclude = ["card", "heatmap"]
    else:
        return None

    for pat in patterns:
        for p in d.glob(pat):
            name = p.name.lower()
            if any(e in name for e in exclude):
                continue
            return str(p)
    return None


def metric_col(df: pd.DataFrame, metric: str) -> str | None:
    lower = {c.lower(): c for c in df.columns}
    candidates = [metric] if metric.endswith(("_per90", "_p90")) else [f"{metric}_per90", f"{metric}_p90", metric]
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def base_metric_name(metric: str) -> str:
    return re.sub(r"_(per90|p90)$", "", str(metric).lower())


def is_excluded_metric(metric: str, family: str | None) -> bool:
    m = base_metric_name(metric)
    if family != "GK" and any(m == g or m.startswith(f"{g}_") for g in GK_ONLY_METRICS):
        return True
    if family == "GK":
        # Keep GK-only metrics plus normal football stats that also apply to keepers.
        pass
    return any(tok in m for tok in EXCLUDED_METRIC_TOKENS)


def metric_weight(metric: str, family: str | None) -> float:
    m = base_metric_name(metric)
    weights = ROLE_METRIC_WEIGHTS.get(family or "", {})
    best = 1.0
    for token, weight in weights.items():
        if token in m:
            best = max(best, float(weight))
    return best


def available_percentile_metric_cols(df: pd.DataFrame, family: str | None) -> list[str]:
    cols: list[str] = []
    seen: set[str] = set()
    for col in df.columns:
        cl = str(col).lower()
        if not cl.endswith(("_per90", "_p90")):
            continue
        base = base_metric_name(cl)
        if base in seen or is_excluded_metric(cl, family):
            continue
        vals = pd.to_numeric(df[col], errors="coerce")
        if vals.notna().sum() < 3:
            continue
        if vals.nunique(dropna=True) <= 1:
            continue
        cols.append(col)
        seen.add(base)
    return cols


def percentile(series: pd.Series, x: Any) -> float | None:
    v = num(x)
    if v is None:
        return None
    vals = pd.to_numeric(series, errors="coerce").dropna()
    if vals.empty:
        return None
    return round(float((vals <= v).mean() * 100.0), 2)


def prettify_metric(metric: str) -> str:
    s = re.sub(r"_(per90|p90)$", "/90", metric)
    return s.replace("_", " ").title().replace("Xg", "xG").replace("Xa", "xA").replace("Xgot", "xGOT")


def build_percentiles(df: pd.DataFrame, row: pd.Series, role_col: str | None, role_value: Any, family: str | None, max_items: int) -> list[dict[str, Any]]:
    if role_col and role_col in df.columns and family is not None:
        fam = normalized_family_series(df[role_col])
        cohort = df.loc[fam == family].copy()
    elif role_col and role_col in df.columns and role_value is not None:
        cohort = df.loc[df[role_col].astype(str) == str(role_value)].copy()
    else:
        cohort = df.copy()

    if cohort.empty or len(cohort) < 3:
        cohort = df.copy()

    metric_cols = available_percentile_metric_cols(df, family)
    out, seen = [], set()
    for col in metric_cols:
        if col in seen or col not in row.index:
            continue
        v = num(row.get(col))
        if v is None:
            continue
        pct = percentile(cohort[col], v)
        if pct is None:
            continue
        weight = metric_weight(col, family)
        out.append({
            "metric": col,
            "label": prettify_metric(col),
            "value": round(v, 4),
            "percentile": pct,
            "weighted_percentile": round(pct * weight, 2),
            "weight": round(weight, 3),
            "bvalue": round((pct - 50) / 50, 4),
            "cohort": family or str(role_value),
            "cohort_size": int(len(cohort)),
        })
        seen.add(col)

    # Sort by role-weighted value first, then raw percentile. This keeps the card
    # flexible while still surfacing the most role-relevant strengths.
    out.sort(key=lambda d: (d["weighted_percentile"], d["percentile"]), reverse=True)
    return out[:max_items]


def build_stat_block(row: pd.Series, aliases: dict[str, list[str]], per90: bool) -> dict[str, Any]:
    out = {}
    for key, names in aliases.items():
        search = []
        for name in names:
            if per90:
                search += [f"{name}_per90", f"{name}_p90"]
            search.append(name)
        v = value(row, search)
        if v is not None:
            out[key] = round_or_none(v, 3)
    return out


def letter_grade(score: float | None) -> str | None:
    if score is None:
        return None
    if score >= 90: return "A+"
    if score >= 82: return "A"
    if score >= 75: return "A-"
    if score >= 68: return "B+"
    if score >= 60: return "B"
    if score >= 52: return "B-"
    if score >= 45: return "C+"
    if score >= 38: return "C"
    if score >= 30: return "C-"
    return "D"


def category_grade(percentiles: list[dict[str, Any]], tokens: list[str], weighted: bool = False) -> str | None:
    vals = []
    weights = []
    for p in percentiles:
        metric = str(p.get("metric", "")).lower()
        if not any(t in metric for t in tokens):
            continue
        pct = num(p.get("percentile"))
        if pct is None:
            continue
        vals.append(pct)
        weights.append(num(p.get("weight")) or 1.0)
    if not vals:
        return None
    score = float(np.average(vals, weights=weights)) if weighted else float(np.mean(vals))
    return letter_grade(score)


def numeric_category_score(percentiles: list[dict[str, Any]], tokens: list[str]) -> float | None:
    vals, weights = [], []
    for p in percentiles:
        metric = str(p.get("metric", "")).lower()
        if any(t in metric for t in tokens):
            pct = num(p.get("percentile"))
            if pct is not None:
                vals.append(pct)
                weights.append(num(p.get("weight")) or 1.0)
    return None if not vals else float(np.average(vals, weights=weights))


def build_grades(percentiles: list[dict[str, Any]], role_row: dict[str, Any], family: str | None = None) -> dict[str, Any]:
    scores = {name: numeric_category_score(percentiles, tokens) for name, tokens in GRADE_CATEGORIES.items()}
    if family != "GK":
        scores.pop("goalkeeping", None)

    category_grades = {name: letter_grade(score) for name, score in scores.items()}
    valid_scores = [s for s in scores.values() if s is not None]
    role_output_score = float(np.mean(valid_scores)) if valid_scores else None

    return {
        "role_fit": letter_grade(num(role_row.get("role_score"))),
        "role_output": letter_grade(role_output_score),
        **category_grades,
    }


def extract_similarity(sim_json: Any) -> list[dict[str, Any]]:
    if not sim_json:
        return []

    cohort_info = {}
    if isinstance(sim_json, dict):
        cohort_info = sim_json.get("cohort_info") or {}
        rows = sim_json.get("results") or sim_json.get("similar_players") or []
    else:
        rows = sim_json

    rows = rows or []
    out = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        out.append({
            "player_id": r.get("player_id"),
            "name": r.get("player_name") or r.get("name"),
            "team": r.get("team"),
            "league": r.get("league"),
            "age": round_or_none(r.get("age"), 1),
            "similarity": round_or_none(r.get("similarity_score") or r.get("similarity"), 4),
            "similarity_raw": round_or_none(r.get("similarity_raw"), 4),
            "similarity_core": round_or_none(r.get("similarity_core"), 4),
            "similarity_broad": round_or_none(r.get("similarity_broad"), 4),
            "similarity_spatial": round_or_none(r.get("similarity_spatial"), 4),
            "similarity_role_bonus": round_or_none(r.get("similarity_role_bonus"), 4),
            "similarity_pct": round_or_none(r.get("similarity_pct"), 2),
            "role": r.get("arbitrated_role_group") or r.get("role_model_family") or r.get("role_position") or r.get("primary_role_position"),
            "position": r.get("arbitrated_position"),
            "lane": r.get("arbitrated_lane"),
            "closest_metrics": r.get("closest_metrics"),
            "largest_metric_gaps": r.get("largest_metric_gaps"),
            "primary_role": r.get("primary_role"),
            "secondary_role": r.get("secondary_role"),
            "archetype": r.get("similarity_archetype"),
            "family": r.get("similarity_family") or r.get("_family"),
            "side": r.get("similarity_side"),
            "display_role": r.get("similarity_display_role") or r.get("primary_role") or r.get("arbitrated_role_group"),
            "color_key": r.get("similarity_color_key") or r.get("similarity_archetype") or r.get("primary_role") or r.get("arbitrated_role_group"),
        })

    return out


def extract_similarity_meta(sim_json: Any) -> dict[str, Any]:
    if not isinstance(sim_json, dict):
        return {}
    info = sim_json.get("cohort_info") or {}
    target = sim_json.get("target") or {}
    return {
        "target_family": target.get("normalized_family"),
        "target_side": target.get("side"),
        "cohort_strategy": info.get("cohort_strategy"),
        "cohort_size": info.get("cohort_size"),
        "feature_count": info.get("feature_count"),
        "core_feature_count": info.get("core_feature_count"),
        "broad_feature_count": info.get("broad_feature_count"),
        "score_blend": info.get("score_blend"),
    }

def extract_shotmap(event_json: Any) -> dict[str, Any]:
    block = event_json.get("shotmap", {}) if isinstance(event_json, dict) else {}
    shots = block.get("shots") or []
    clean = [{
        "event_id": s.get("event_id"),
        "x": round_or_none(s.get("x"), 3),
        "y": round_or_none(s.get("y"), 3),
        "xg": round_or_none(s.get("xg"), 4),
        "xgot": round_or_none(s.get("xgot"), 4),
        "result": s.get("shot_type"),
        "is_goal": bool(s.get("is_goal")),
        "body_part": s.get("body_part"),
        "situation": s.get("situation"),
        "minute": s.get("minute"),
    } for s in shots if isinstance(s, dict)]
    return {"count": len(clean), "summary": block.get("summary") or {}, "shots": clean}


def extract_heatmap(heat_json: Any) -> dict[str, Any]:
    if not isinstance(heat_json, dict):
        return {
            "cell_count": 0,
            "raw_point_count": 0,
            "points": [],
            "visual_summary": {},
            "position_estimate": None,
            "position_note": "No heatmap file supplied.",
        }

    heat = heat_json.get("heatmap", {}) if isinstance(heat_json.get("heatmap"), dict) else {}
    points = heat.get("points") or []
    clean = [
        {
            "x": round_or_none(p.get("x"), 3),
            "y": round_or_none(p.get("y"), 3),
            "value": round_or_none(p.get("value"), 5),
        }
        for p in points
        if isinstance(p, dict)
    ]

    visual_summary = heat_json.get("visual_summary") or {}
    position_note = heat_json.get("position_note") or (
        "Visual heatmap only. Tactical position is intentionally not estimated here; use Position_Arbitrator output."
    )

    return {
        "cell_count": len(clean),
        "raw_point_count": heat.get("raw_point_count"),
        "precision": heat.get("precision"),
        "points": clean,
        "visual_summary": visual_summary,
        "position_estimate": None,
        "position_note": position_note,
        "source_position_labels": heat_json.get("source_position_labels", {}),
    }




def build_spatial_profile(row: pd.Series) -> dict[str, Any]:
    keys = [
        "season_avg_x", "season_avg_y", "season_median_x", "season_median_y",
        "season_std_x", "season_std_y", "season_position_zone",
        "spatial_matches_used", "spatial_wide_pct", "spatial_right_pct",
        "spatial_left_pct", "spatial_central_pct", "spatial_high_pct",
        "spatial_mid_pct", "spatial_deep_pct", "spatial_high_wide_pct",
        "spatial_mid_wide_pct", "spatial_deep_central_pct",
        "spatial_dominant_side",
    ]
    out = {}
    for k in keys:
        if k not in row.index:
            continue
        v = row.get(k)
        if pd.isna(v):
            continue
        if isinstance(v, (int, float, np.number)):
            out[k] = round_or_none(v, 4)
        else:
            out[k] = v
    return out




def profile_from_sources(row: pd.Series, event_json: Any) -> dict[str, Any]:
    """
    Build display-only player profile metadata.

    These fields are useful context for the UI/card, but they are not used in
    percentile or grading calculations. Calculation exclusions are handled by
    EXCLUDED_METRIC_TOKENS and available_percentile_metric_cols().
    """
    ep = event_json.get("profile", {}) if isinstance(event_json, dict) else {}
    return {
        "player_id": int(value(row, ["player_id"], ep.get("player_id"))),
        "name": value(row, ["player_name", "profile_name"], ep.get("name")),
        "short_name": ep.get("short_name") or value(row, ["short_name"]),
        "team": value(row, ["team"], ep.get("team")),
        "league": value(row, ["league"]),
        "season": value(row, ["season"]),
        "age": round_or_none(value(row, ["age"])),
        "date_of_birth": value(row, ["date_of_birth", "dob"], ep.get("date_of_birth")),
        "nationality": value(row, ["nationality"], ep.get("nationality")),
        "height_cm": round_or_none(value(row, ["height_cm"], ep.get("height_cm")), 0),
        "preferred_foot": value(row, ["preferred_foot"], ep.get("preferred_foot")),
        "profile_position": ep.get("position") or value(row, ["profile_position", "player_position", "position"]),
        "jersey_number": ep.get("jersey_number") or value(row, ["jersey_number", "shirt_number"]),
        "market_value_eur": round_or_none(ep.get("market_value_eur") or value(row, ["market_value_eur"]), 0),
        "contract_until": ep.get("contract_until") or value(row, ["contract_until"]),
        "injury_status": ep.get("injury_status"),
    }

def build_summary(profile: dict[str, Any], position: dict[str, Any], role: dict[str, Any], grades: dict[str, Any], percentiles: list[dict[str, Any]]) -> dict[str, Any]:
    name = profile.get("name") or "Player"
    role_name = role.get("primary") or "role pending"
    pos = position.get("arbitrated_role_group") or position.get("listed_role")
    top_labels = [p["label"] for p in percentiles[:4]]
    return {
        "headline": f"{name} profiles as a {pos} with strongest value in {', '.join(top_labels[:2]).lower() if top_labels else 'role-specific outputs'}.",
        "one_liner": f"{name}: {role_name} profile with {grades.get('progression') or 'N/A'} progression and {grades.get('defense') or 'N/A'} defensive indicators.",
        "strengths": top_labels,
        "watchpoints": [],
        "model_caveat": "Role, similarity, and position labels are model outputs. Season average-position distribution is the primary spatial source; heatmaps are visual/action-density layers only.",
    }


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    season_df = pd.read_csv(args.season_totals)
    row = find_player_row(season_df, args.player_id, args.season, args.league)

    event_path = args.event_data or auto_find_json(args.player_id, args.season, "event", args.search_dir)
    heat_path = args.heatmap or auto_find_json(args.player_id, args.season, "heatmap", args.search_dir)
    sim_path = args.similarity or auto_find_json(args.player_id, args.season, "similarity", args.search_dir)

    event_json = parse_json(event_path)
    heat_json = parse_json(heat_path)
    sim_json = parse_json(sim_path)
    role_row = find_role_row(args.roles, args.player_id, args.season, args.league)

    role_col = choose_role_col(season_df)
    role_value = value(row, [role_col]) if role_col else None
    family = normalize_family(role_value)
    percentiles = build_percentiles(season_df, row, role_col, role_value, family, args.max_percentiles)

    profile = profile_from_sources(row, event_json)
    heatmap = extract_heatmap(heat_json)

    position = {
        "listed_role": value(row, ["primary_role_position", "role_position"]),
        "listed_position": value(row, ["player_position", "base_position"]),
        "arbitrated_position": value(row, ["arbitrated_position"]),
        "arbitrated_role_group": value(row, ["arbitrated_role_group", "role_position_refined"]),
        "arbitrated_lane": value(row, ["arbitrated_lane"]),
        "arbitrated_confidence": round_or_none(value(row, ["arbitrated_confidence"]), 3),
        "position_conflict_flag": value(row, ["position_conflict_flag"]),
        "arbitration_reason": value(row, ["arbitration_reason"]),
        "season_spatial": build_spatial_profile(row),

        # Heatmaps are now visual/action-density only.
        "heatmap_estimate": None,
        "heatmap_role_group": None,
        "heatmap_lane": None,
        "heatmap_confidence": None,
        "heatmap_position_note": heatmap.get("position_note"),
        "heatmap_visual_summary": heatmap.get("visual_summary") or {},
    }

    role = {
        "primary": role_row.get("primary_role"),
        "secondary": role_row.get("secondary_role"),
        "role_score": round_or_none(role_row.get("role_score"), 2),
        "role_bvalue": round_or_none(role_row.get("role_bvalue"), 4),
        "confidence": round_or_none(role_row.get("confidence"), 3),
        "role_source_column": role_row.get("role_source_column"),
        "role_model_family": role_row.get("role_model_family") or family,
        "measured_inputs": role_row.get("measured_inputs"),
        "partial_proxies": role_row.get("partial_proxies"),
        "unmeasured_traits": role_row.get("unmeasured_traits"),
    }

    season_stats = build_stat_block(row, DISPLAY_STAT_ALIASES, per90=False)
    per90 = build_stat_block(row, DISPLAY_STAT_ALIASES, per90=True)
    grades = build_grades(percentiles, role_row, family)

    card = {
        "meta": {
            "player_id": int(args.player_id),
            "season": args.season,
            "league": args.league,
            "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "input_files": {
                "season_totals": str(args.season_totals),
                "roles": str(args.roles) if args.roles else None,
                "similarity": str(sim_path) if sim_path else None,
                "event_data": str(event_path) if event_path else None,
                "heatmap": str(heat_path) if heat_path else None,
            },
        },
        "profile": profile,
        "position": position,
        "role": role,
        "grades": grades,
        "season_stats": season_stats,
        "per90": per90,
        "percentiles": percentiles,
        "similarity_meta": extract_similarity_meta(sim_json),
        "similar_players": extract_similarity(sim_json),
        "shotmap": extract_shotmap(event_json),
        "heatmap": heatmap,
    }
    card["summary"] = build_summary(profile, position, role, grades, percentiles)
    return card

def clean_json(obj):
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
    if isinstance(obj, (np.floating, np.integer)):
        v = obj.item()
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v
    if pd.isna(obj):
        return None
    return obj


def main() -> None:
    ap = argparse.ArgumentParser(description="Build Observable-ready player-card JSON.")
    ap.add_argument("--player-id", "-p", type=int, required=True)
    ap.add_argument("--season", "-s", default="2025-26")
    ap.add_argument("--league", "-l", default=None)
    ap.add_argument("--season-totals", default="player_season_totals_arbitrated.csv")
    ap.add_argument("--roles", default="player_roles.csv")
    ap.add_argument("--similarity", default=None)
    ap.add_argument("--event-data", default=None)
    ap.add_argument("--heatmap", default=None)
    ap.add_argument("--search-dir", default=".")
    ap.add_argument("--out", "-o", default=None)
    ap.add_argument("--max-percentiles", type=int, default=18)
    args = ap.parse_args()

    card = build_card(args)
    card = clean_json(card)

    name = card["profile"].get("name") or str(args.player_id)
    out_path = Path(args.out or f"player_card_{clean_filename(name)}_{args.player_id}_{str(args.season).replace('/', '-')}.json")
    out_path.write_text(json.dumps(card, indent=2, ensure_ascii=False, allow_nan=False, default=str), encoding="utf-8")

    print(f"Saved: {out_path}")
    print(f"Player: {card['profile'].get('name')}")
    print(f"Role:   {card['role'].get('primary')} / {card['role'].get('secondary')}")
    print(f"Pos:    {card['position'].get('arbitrated_role_group')} | heatmap=visual-only")
    print(f"Shots:  {card['shotmap'].get('count')}")
    print(f"Heat:   {card['heatmap'].get('cell_count')} cells")
    print(f"Comps:  {len(card['similar_players'])}")


if __name__ == "__main__":
    main()
