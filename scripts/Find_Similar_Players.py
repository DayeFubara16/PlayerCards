"""
Find_Similar_Players_v4.py
──────────────────────────

Broad-stat, arbitration-aware, in-house z-score + percentile similarity engine.

Designed for:
  player_season_totals_arbitrated.csv from Position_Arbitrator_v13_fb_guarded.py

Core philosophy:
- Keep many football-stat categories because weak/medium signals matter stylistically.
- Drop true junk/id/category columns.
- Build the cohort first.
- Compute z-scores and percentiles inside the cohort.
- Blend:
    role-core z/percentile similarity
    broad-style z/percentile similarity
    spatial similarity
    role/source similarity
into an intuitive 0-1 final score.

Usage:
  python Find_Similar_Players_v4.py --player-id 159665 --season 2025-26 --input player_season_totals_arbitrated.csv --top 25 --min-minutes 450 --format both

Useful options:
  --side-mode auto|strict|off
  --cohort-mode family|exact|adjacent|all
  --age-weight 0.25
  --spatial-weight 0.12
  --role-bonus-weight 0.08
  --core-share 0.65
"""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity


ROLE_PREF = [
    "role_model_family",
    "arbitrated_role_group",
    "role_position_refined",
    "primary_role_position",
    "role_position",
]

IDENTITY_COLUMNS = {
    "player_id", "event_id", "match_id", "player_name", "profile_name",
    "date_of_birth", "age_as_of", "team", "opponent", "teams_played_list",
    "league", "leagues_played_list", "season", "nationality", "preferred_foot",
    "player_position", "base_position", "role_family", "primary_role_position",
    "secondary_role_position", "positions_played_list", "position_confidence",
    "position_source", "profile_position_raw", "profile_position_group",
    "profile_position_mode", "profile_positions_played", "match_position_mode",
    "match_positions_played", "match_role_mode", "match_roles_played",
    "arbitrated_position", "arbitrated_role_group", "arbitrated_lane",
    "arbitration_reason", "position_evidence", "season_position_zone",
    "spatial_dominant_side", "flags", "warning", "role_source_column",
    "role_source_value", "primary_role", "secondary_role", "measured_inputs",
    "partial_proxies", "unmeasured_traits",
}

JUNK_NUMERIC_COLUMNS = {
    "shirt_number",
    "matches",
    "teams_played_count",
    "leagues_played_count",
    "positions_played_count",
    "spatial_matches_used",
    "spatial_wide_matches",
    "spatial_right_matches",
    "spatial_left_matches",
    "spatial_central_matches",
    "spatial_high_matches",
    "spatial_mid_matches",
    "spatial_deep_matches",
    "spatial_high_wide_matches",
    "spatial_mid_wide_matches",
    "spatial_deep_central_matches",
    "arbitrated_confidence",
    "position_conflict_flag",
}

# Keep minutes only as filter, not similarity.
JUNK_NUMERIC_COLUMNS.add("minutes_played")

VALUE_COLUMNS = {
    "sofascore_rating",
    "pass_value",
    "dribble_value",
    "defensive_value",
    "shot_value",
    "goalkeeper_value",
}

SPATIAL_COLUMNS = {
    "season_avg_x",
    "season_avg_y",
    "season_std_x",
    "season_std_y",
    "spatial_wide_pct",
    "spatial_right_pct",
    "spatial_left_pct",
    "spatial_central_pct",
    "spatial_high_pct",
    "spatial_mid_pct",
    "spatial_deep_pct",
    "spatial_high_wide_pct",
    "spatial_mid_wide_pct",
    "spatial_deep_central_pct",
}

BIO_COLUMNS = {
    "age",
    "height_cm",
}

ADJACENT_FAMILIES = {
    "GK": ["GK"],
    "CB": ["CB", "CB-FB", "DM"],
    "CB-FB": ["CB-FB", "CB", "FB"],
    "FB": ["FB", "WB", "CB-FB"],
    "WB": ["WB", "FB", "W"],
    "DM": ["DM", "CM", "CB"],
    "CM": ["CM", "DM", "AM"],
    "AM": ["AM", "CM", "W", "ST"],
    "W": ["W", "WB", "AM", "ST"],
    "ST": ["ST", "W", "AM"],
}

ROLE_WEIGHTS: dict[str, dict[str, float]] = {
    "W": {
        "dribble": 1.45, "contest": 1.35, "carry": 1.35, "progressive": 1.25,
        "shot": 1.15, "xg": 1.10, "xa": 1.15, "key": 1.15, "cross": 1.10,
        "touches_opp_box": 1.10, "spatial": 1.25, "defensive": 0.65, "gk": 0.0,
    },
    "ST": {
        "goal": 1.45, "shot": 1.35, "xg": 1.45, "xgot": 1.25,
        "touches_opp_box": 1.25, "aerial": 0.90, "key": 0.80, "xa": 0.75,
        "carry": 0.80, "spatial": 1.10, "defensive": 0.45, "gk": 0.0,
    },
    "FB": {
        "cross": 1.30, "tackle": 1.20, "interception": 1.10, "recovery": 1.15,
        "carry": 1.10, "progressive": 1.10, "pass": 1.00, "spatial": 1.35,
        "shot": 0.45, "xg": 0.40, "gk": 0.0,
    },
    "WB": {
        "cross": 1.35, "carry": 1.25, "progressive": 1.25, "xa": 1.15,
        "key": 1.10, "tackle": 0.95, "recovery": 1.00, "spatial": 1.45,
        "shot": 0.70, "gk": 0.0,
    },
    "CB": {
        "clearance": 1.35, "block": 1.25, "aerial": 1.30, "duel": 1.15,
        "interception": 1.10, "tackle": 0.95, "pass": 1.05, "long": 1.00,
        "spatial": 1.05, "shot": 0.20, "dribble": 0.25, "gk": 0.0,
    },
    "CB-FB": {
        "clearance": 1.05, "block": 0.95, "aerial": 0.90, "duel": 1.05,
        "interception": 1.15, "tackle": 1.15, "pass": 1.15, "carry": 1.00,
        "progressive": 1.00, "cross": 0.75, "spatial": 1.20, "gk": 0.0,
    },
    "CM": {
        "pass": 1.25, "long": 0.95, "key": 0.95, "xa": 0.85,
        "tackle": 0.95, "interception": 0.95, "recovery": 1.15,
        "carry": 1.00, "progressive": 1.00, "spatial": 1.10, "gk": 0.0,
    },
    "DM": {
        "tackle": 1.25, "interception": 1.30, "recovery": 1.25,
        "duel": 1.00, "pass": 1.10, "long": 1.05, "clearance": 0.85,
        "shot": 0.35, "xg": 0.30, "spatial": 1.05, "gk": 0.0,
    },
    "AM": {
        "key": 1.35, "xa": 1.35, "pass_value": 1.25, "pass": 1.00,
        "shot": 1.00, "xg": 0.95, "dribble": 1.10, "carry": 1.10,
        "touches_opp_box": 1.00, "spatial": 1.15, "defensive": 0.55, "gk": 0.0,
    },
    "GK": {
        "gk": 1.80, "pass": 0.70, "long": 0.80, "spatial": 0.20,
        "shot": 0.0, "xg": 0.0, "dribble": 0.0,
    },
}


CORE_BUCKETS: dict[str, set[str]] = {
    "W": {"dribble", "carry", "progressive", "shot", "xg", "xa", "key", "cross", "touches_opp_box", "spatial"},
    "ST": {"goal", "shot", "xg", "xgot", "touches_opp_box", "aerial", "key", "xa", "spatial"},
    "FB": {"cross", "tackle", "interception", "recovery", "carry", "progressive", "pass", "spatial"},
    "WB": {"cross", "carry", "progressive", "xa", "key", "tackle", "recovery", "spatial"},
    "CB": {"clearance", "block", "aerial", "duel", "interception", "tackle", "pass", "long", "spatial"},
    "CB-FB": {"clearance", "aerial", "duel", "interception", "tackle", "pass", "carry", "progressive", "spatial"},
    "CM": {"pass", "long", "key", "xa", "tackle", "interception", "recovery", "carry", "progressive", "spatial"},
    "DM": {"tackle", "interception", "recovery", "duel", "pass", "long", "clearance", "spatial"},
    "AM": {"key", "xa", "pass_value", "pass", "shot", "xg", "dribble", "carry", "touches_opp_box", "spatial"},
    "GK": {"gk", "pass", "long"},
}


def split_core_broad_features(cols: list[str], family: str | None) -> tuple[list[str], list[str]]:
    core_buckets = CORE_BUCKETS.get(family or "", set())
    core = [c for c in cols if feature_bucket(c) in core_buckets]
    broad = [c for c in cols if c not in core]
    # Safety: if a family has too few core features in the available data, avoid a brittle split.
    if len(core) < 8:
        return cols, cols
    if len(broad) < 8:
        return core, cols
    return core, broad


def subset_matrix(cols_all: list[str], matrix: np.ndarray, wanted: list[str]) -> np.ndarray:
    idx = [cols_all.index(c) for c in wanted if c in cols_all]
    if not idx:
        return matrix
    return matrix[:, idx]



def normalize_family(value: Any) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    text = text.replace("_", "-").replace(" ", "-")
    text = re.sub(r"-+", "-", text)

    if text in {"GK", "G", "GOALKEEPER"}:
        return "GK"
    if text in {"CB", "RCB", "LCB"}:
        return "CB"
    if text in {"CB-FB", "FB-CB", "WCB", "RB-CB", "LB-CB", "WIDE-CB"}:
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
    if text in {"AM-C", "AM", "AMC", "AM-HYB", "AM-W-C", "AM-C-W"}:
        return "AM"
    if text in {"ST", "SS", "CF", "F", "FW", "ST-SS", "ST-W"}:
        return "ST"
    if text in {"DM", "CM"}:
        return text
    return text


def choose_role_col(df: pd.DataFrame, forced: str | None = None) -> str | None:
    if forced:
        if forced not in df.columns:
            raise ValueError(f"Requested role column '{forced}' not found.")
        return forced
    for col in ROLE_PREF:
        if col in df.columns:
            return col
    return None


def normalize_side(row: pd.Series) -> str | None:
    for col in ["arbitrated_position", "arbitrated_lane", "spatial_dominant_side"]:
        if col not in row.index:
            continue
        val = row.get(col)
        if pd.isna(val):
            continue
        text = str(val).upper()
        if text.startswith("R") or "RIGHT" in text:
            return "Right"
        if text.startswith("L") or "LEFT" in text:
            return "Left"
        if "CENTRAL" in text or text in {"CM", "DM", "AM", "ST", "CB"}:
            return "Central"

    right = pd.to_numeric(pd.Series([row.get("spatial_right_pct")]), errors="coerce").iloc[0] if "spatial_right_pct" in row.index else np.nan
    left = pd.to_numeric(pd.Series([row.get("spatial_left_pct")]), errors="coerce").iloc[0] if "spatial_left_pct" in row.index else np.nan
    if pd.notna(right) and pd.notna(left):
        if right >= 0.45 and right >= left:
            return "Right"
        if left >= 0.45 and left > right:
            return "Left"
    return None


def feature_bucket(col: str) -> str:
    c = col.lower()
    if c in SPATIAL_COLUMNS:
        return "spatial"
    if c in BIO_COLUMNS:
        return "bio"
    if "goalkeeper" in c or c.startswith("gk_"):
        return "gk"
    if "goal" in c and "prevented" not in c:
        return "goal"
    if "xgot" in c:
        return "xgot"
    if c == "xg" or c.startswith("xg_") or "expectedgoals" in c:
        return "xg"
    if c == "xa" or "assist" in c or "key_pass" in c:
        return "xa" if c == "xa" or "assist" in c else "key"
    if "shot" in c or "woodwork" in c:
        return "shot"
    if "touches_opp_box" in c or "touches_in_box" in c:
        return "touches_opp_box"
    if "cross" in c:
        return "cross"
    if "dribble" in c or "contest" in c:
        return "dribble"
    if "carry" in c or "carries" in c:
        return "carry"
    if "progressive" in c or "progression" in c:
        return "progressive"
    if "pass_value" in c:
        return "pass_value"
    if "pass" in c or "accurate" in c:
        return "pass"
    if "long_ball" in c or "long_balls" in c:
        return "long"
    if "tackle" in c:
        return "tackle"
    if "interception" in c:
        return "interception"
    if "recover" in c:
        return "recovery"
    if "clearance" in c:
        return "clearance"
    if "block" in c:
        return "block"
    if "aerial" in c:
        return "aerial"
    if "duel" in c:
        return "duel"
    if "foul" in c or "card" in c or "error" in c:
        return "defensive"
    return "other"

def style_archetype(row: pd.Series) -> str:
    family = normalize_family(row.get("arbitrated_role_group") or row.get("role_model_family"))

    def n(col):
        return pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0]

    drib = n("dribbles_attempted_per90")
    carry = n("progressive_carries_per90")
    xa = n("xa_per90")
    key = n("key_passes_per90")
    xg = n("xg_per90")
    shots = n("shots_total_per90")
    rec = n("recoveries_per90")

    if family == "W":
        if pd.notna(xa) and pd.notna(key) and xa >= 0.22 and key >= 1.4:
            return "Playmaking Winger"
        if pd.notna(drib) and drib >= 5.0:
            return "1v1 Winger"
        if pd.notna(xg) and pd.notna(shots) and xg >= 0.25 and shots >= 2.0:
            return "Goal Threat Winger"
        if pd.notna(carry) and carry >= 2.2:
            return "Ball-Carrying Winger"
        if pd.notna(rec) and rec >= 5.0:
            return "Two-Way Winger"
        return "Winger"

    if family == "AM":
        if pd.notna(xa) and xa >= 0.20:
            return "Creator"
        if pd.notna(xg) and xg >= 0.20:
            return "Second Striker"
        return "Attacking Midfielder"

    if family == "ST":
        if pd.notna(xg) and xg >= 0.35:
            return "Penalty Box Forward"
        if pd.notna(key) and key >= 1.0:
            return "Link Forward"
        return "Striker"

    if family in {"CM", "DM"}:
        if pd.notna(carry) and carry >= 1.8:
            return "Ball Progressor"
        if pd.notna(rec) and rec >= 6.0:
            return "Ball Winner"
        return "Midfielder"

    if family in {"FB", "WB"}:
        if pd.notna(xa) and xa >= 0.15:
            return "Attacking Fullback"
        if pd.notna(carry) and carry >= 1.8:
            return "Progressive Fullback"
        return "Fullback"

    return family or "Other"


def feature_weight(col: str, family: str | None, age_weight: float) -> float:
    if col in BIO_COLUMNS:
        return float(age_weight)
    bucket = feature_bucket(col)
    weights = ROLE_WEIGHTS.get(family or "", {})
    return float(weights.get(bucket, 1.0))


def is_candidate_feature(df: pd.DataFrame, col: str) -> bool:
    if col in IDENTITY_COLUMNS or col in JUNK_NUMERIC_COLUMNS:
        return False
    if col.endswith("_id") or col.endswith("_count") or col.endswith("_matches"):
        return False
    if col.startswith("_"):
        return False

    # Keep broad football stats:
    # - per90/p90
    # - value metrics
    # - spatial metrics
    # - selected bio
    # - numeric raw football totals if not junk
    if col.endswith("_per90") or col.endswith("_p90"):
        return True
    if col in VALUE_COLUMNS or col in SPATIAL_COLUMNS or col in BIO_COLUMNS:
        return True

    # raw numeric football columns are okay if not categorical junk
    return pd.api.types.is_numeric_dtype(df[col]) or pd.to_numeric(df[col], errors="coerce").notna().sum() > 0


def build_features(cohort: pd.DataFrame, family: str | None, age_weight: float, min_non_null: int) -> tuple[list[str], np.ndarray, np.ndarray, dict[str, float]]:
    cols: list[str] = []
    weights: dict[str, float] = {}

    for col in cohort.columns:
        if not is_candidate_feature(cohort, col):
            continue

        vals = pd.to_numeric(cohort[col], errors="coerce")
        if vals.notna().sum() < min_non_null:
            continue
        if vals.nunique(dropna=True) <= 1:
            continue

        w = feature_weight(col, family, age_weight)
        if w <= 0:
            continue

        cols.append(col)
        weights[col] = w

    if not cols:
        raise ValueError("No usable numeric features for similarity.")

    X = cohort[cols].apply(pd.to_numeric, errors="coerce")
    med = X.median(numeric_only=True)
    X = X.fillna(med).fillna(0.0)

    # Winsorize in-house to reduce outlier domination.
    q_low = X.quantile(0.01)
    q_high = X.quantile(0.99)
    Xw = X.clip(lower=q_low, upper=q_high, axis=1)

    mean = Xw.mean(axis=0)
    std = Xw.std(axis=0, ddof=0).replace(0, np.nan)
    Z = ((Xw - mean) / std).fillna(0.0).to_numpy(dtype=float)

    # Percentiles inside cohort. 0-1 scale, centered to -1..1 for cosine.
    P = Xw.rank(pct=True, axis=0).fillna(0.5)
    P = (P * 2.0 - 1.0).to_numpy(dtype=float)

    # Apply sqrt weights so cosine is weighted without exploding dimensions.
    w_vec = np.array([math.sqrt(weights[c]) for c in cols], dtype=float)
    Z *= w_vec
    P *= w_vec

    return cols, Z, P, weights


def spatial_similarity(target: pd.Series, comp: pd.Series) -> float:
    cols = [c for c in SPATIAL_COLUMNS if c in target.index and c in comp.index]
    if not cols:
        return 0.0

    t = pd.to_numeric(pd.Series([target.get(c) for c in cols]), errors="coerce")
    c = pd.to_numeric(pd.Series([comp.get(c) for c in cols]), errors="coerce")
    valid = t.notna() & c.notna()
    if not valid.any():
        return 0.0

    # Normalize approximate spatial columns to 0..1.
    diffs = []
    for col, tv, cv in zip(cols, t, c):
        if pd.isna(tv) or pd.isna(cv):
            continue
        scale = 100.0 if col in {"season_avg_x", "season_avg_y", "season_std_x", "season_std_y"} else 1.0
        diffs.append(abs(float(tv) - float(cv)) / scale)

    if not diffs:
        return 0.0

    dist = float(np.mean(diffs))
    return max(0.0, min(1.0, 1.0 - dist))


def role_bonus(target: pd.Series, comp: pd.Series, role_col: str | None) -> float:
    bonus = 0.0

    if role_col and role_col in target.index and role_col in comp.index:
        tf = normalize_family(target.get(role_col))
        cf = normalize_family(comp.get(role_col))
        if tf and cf:
            if tf == cf:
                bonus += 0.65
            elif cf in ADJACENT_FAMILIES.get(tf, []):
                bonus += 0.35

    # Exact arbitrated role/lane/side add signal.
    if "arbitrated_role_group" in target.index and target.get("arbitrated_role_group") == comp.get("arbitrated_role_group"):
        bonus += 0.15
    if normalize_side(target) and normalize_side(target) == normalize_side(comp):
        bonus += 0.20

    return max(0.0, min(1.0, bonus))


def build_cohort(
    df: pd.DataFrame,
    target: pd.Series,
    role_col: str | None,
    cohort_mode: str,
    side_mode: str,
    min_cohort_size: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    work = df.copy()

    if role_col:
        work["_family"] = work[role_col].apply(normalize_family)
        target_family = normalize_family(target.get(role_col))
    else:
        work["_family"] = None
        target_family = None

    info = {
        "role_column": role_col,
        "target_role_value": target.get(role_col) if role_col else None,
        "target_family": target_family,
        "target_side": normalize_side(target),
        "cohort_mode": cohort_mode,
        "side_mode": side_mode,
        "cohort_strategy": None,
    }

    if cohort_mode == "all" or not role_col:
        cohort = work.copy()
        info["cohort_strategy"] = "all"
    elif cohort_mode == "exact":
        cohort = work.loc[work[role_col].astype(str) == str(target.get(role_col))].copy()
        info["cohort_strategy"] = f"exact={target.get(role_col)}"
    elif cohort_mode == "adjacent":
        families = ADJACENT_FAMILIES.get(target_family, [target_family])
        cohort = work.loc[work["_family"].isin(families)].copy()
        info["cohort_strategy"] = f"adjacent={'+'.join([str(f) for f in families])}"
    else:  # family
        cohort = work.loc[work["_family"] == target_family].copy()
        info["cohort_strategy"] = f"family={target_family}"

    # Hybrid defender pool is more meaningful with adjacent defenders.
    if target_family == "CB-FB" and cohort_mode in {"family", "exact"}:
        cohort = work.loc[work["_family"].isin(["CB-FB", "CB", "FB"])].copy()
        info["cohort_strategy"] = "hybrid_defender_pool=CB-FB+CB+FB"

    side_sensitive = target_family in {"FB", "WB", "W"}
    target_side = info["target_side"]

    if side_mode != "off" and side_sensitive and target_side in {"Right", "Left"}:
        sides = cohort.apply(normalize_side, axis=1)
        side_cohort = cohort.loc[sides == target_side].copy()
        if side_mode == "strict":
            if len(side_cohort) >= max(5, min_cohort_size):
                cohort = side_cohort
                info["cohort_strategy"] += f"+side={target_side}"
        elif side_mode == "auto":
            if len(side_cohort) >= min_cohort_size:
                cohort = side_cohort
                info["cohort_strategy"] += f"+side={target_side}"
            else:
                info["cohort_strategy"] += "+side_relaxed_small_cohort"

    if len(cohort) < min_cohort_size and cohort_mode != "all":
        families = ADJACENT_FAMILIES.get(target_family, [target_family])
        cohort = work.loc[work["_family"].isin(families)].copy()
        info["cohort_strategy"] = f"fallback_adjacent={'+'.join([str(f) for f in families])}"

    return cohort, info


def rescale_similarity(raw: pd.Series) -> pd.Series:
    """
    Convert raw blend into an intuitive 0-1 score.

    The ranking still comes from raw similarity, but this improves readability.
    Uses within-result min/max plus a gentle sigmoid-like compression.
    """
    vals = pd.to_numeric(raw, errors="coerce")
    if vals.notna().sum() <= 1:
        return vals.clip(0, 1)

    # raw can be negative. Convert to 0..1 first.
    base = ((vals + 1.0) / 2.0).clip(0, 1)

    # Make good matches feel good without changing order.
    return (base ** 0.65).clip(0, 1)


def main() -> None:
    ap = argparse.ArgumentParser(description="Find similar players with broad stats, z-scores, percentiles, and spatial context.")
    ap.add_argument("--input", default="player_season_totals_arbitrated.csv")
    ap.add_argument("--player-id", type=int, required=True)
    ap.add_argument("--season", default=None)
    ap.add_argument("--league", default=None)
    ap.add_argument("--role-column", default=None)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--min-minutes", type=float, default=450)
    ap.add_argument("--format", choices=["csv", "json", "both"], default="both")
    ap.add_argument("--output-prefix", default=None)
    ap.add_argument("--side-mode", choices=["auto", "strict", "off"], default="auto")
    ap.add_argument("--cohort-mode", choices=["family", "exact", "adjacent", "all"], default="family")
    ap.add_argument("--min-cohort-size", type=int, default=25)
    ap.add_argument("--z-weight", type=float, default=0.55)
    ap.add_argument("--percentile-weight", type=float, default=0.25)
    ap.add_argument("--spatial-weight", type=float, default=0.12)
    ap.add_argument("--role-bonus-weight", type=float, default=0.08)
    ap.add_argument("--core-share", type=float, default=0.65, help="Share of statistical similarity from role-core features; remainder comes from broad-style features.")
    ap.add_argument("--age-weight", type=float, default=0.25)
    ap.add_argument("--raw-score", action="store_true", help="Do not readability-rescale final score.")
    args = ap.parse_args()

    df = pd.read_csv(args.input)

    if args.season and "season" in df.columns:
        df = df.loc[df["season"].astype(str) == str(args.season)].copy()
    if args.league and "league" in df.columns:
        df = df.loc[df["league"].astype(str).str.lower() == str(args.league).lower()].copy()
    if "minutes_played" in df.columns and args.min_minutes is not None:
        df = df.loc[pd.to_numeric(df["minutes_played"], errors="coerce").fillna(0) >= float(args.min_minutes)].copy()

    if df.empty:
        raise SystemExit("No rows left after filters.")
    if "player_id" not in df.columns:
        raise SystemExit("Input must contain player_id.")

    role_col = choose_role_col(df, args.role_column)

    target_rows = df.loc[pd.to_numeric(df["player_id"], errors="coerce") == int(args.player_id)]
    if target_rows.empty:
        raise SystemExit("Player not found.")
    target = target_rows.iloc[0]

    cohort, info = build_cohort(df, target, role_col, args.cohort_mode, args.side_mode, args.min_cohort_size)

    if target.name not in cohort.index:
        cohort = pd.concat([cohort, target.to_frame().T], axis=0)
        cohort = cohort.loc[~cohort.index.duplicated(keep="last")]

    min_non_null = min(10, max(4, len(cohort) // 5))
    family = info.get("target_family")
    feature_cols, Z, P, feature_weights = build_features(cohort, family, args.age_weight, min_non_null=min_non_null)

    loc = cohort.index.get_loc(target.name)

    core_cols, broad_cols = split_core_broad_features(feature_cols, family)
    Z_core = subset_matrix(feature_cols, Z, core_cols)
    P_core = subset_matrix(feature_cols, P, core_cols)
    Z_broad = subset_matrix(feature_cols, Z, broad_cols)
    P_broad = subset_matrix(feature_cols, P, broad_cols)

    z_core_sim = cosine_similarity(Z_core)[loc]
    p_core_sim = cosine_similarity(P_core)[loc]
    z_broad_sim = cosine_similarity(Z_broad)[loc]
    p_broad_sim = cosine_similarity(P_broad)[loc]

    stat_core = (
        args.z_weight * z_core_sim
        + args.percentile_weight * p_core_sim
    ) / (args.z_weight + args.percentile_weight)

    stat_broad = (
        args.z_weight * z_broad_sim
        + args.percentile_weight * p_broad_sim
    ) / (args.z_weight + args.percentile_weight)

    core_share = max(0.0, min(1.0, float(args.core_share)))
    stat_similarity = core_share * stat_core + (1.0 - core_share) * stat_broad

    out = cohort.copy()
    out["similarity_core"] = stat_core
    out["similarity_broad"] = stat_broad
    out["similarity_z_core"] = z_core_sim
    out["similarity_percentile_core"] = p_core_sim
    out["similarity_z_broad"] = z_broad_sim
    out["similarity_percentile_broad"] = p_broad_sim

    # Backward-compatible aliases.
    out["similarity_z"] = out["similarity_z_core"]
    out["similarity_percentile"] = out["similarity_percentile_core"]

    spatial_scores = []
    role_scores = []
    for _, row in out.iterrows():
        spatial_scores.append(spatial_similarity(target, row))
        role_scores.append(role_bonus(target, row, role_col))

    out["similarity_spatial"] = spatial_scores
    out["similarity_role_bonus"] = role_scores

    total_w = 1.0 + args.spatial_weight + args.role_bonus_weight
    raw = (
        stat_similarity
        + args.spatial_weight * out["similarity_spatial"]
        + args.role_bonus_weight * out["similarity_role_bonus"]
    ) / total_w

    out["similarity_raw"] = raw
    out["similarity"] = raw.clip(-1, 1) if args.raw_score else rescale_similarity(raw)

    out["similarity_archetype"] = out.apply(style_archetype, axis=1)
    out["similarity_family"] = out[role_col].apply(normalize_family) if role_col else None
    out["similarity_side"] = out.apply(normalize_side, axis=1)

    out["similarity_display_role"] = out.get("primary_role", pd.Series(index=out.index, dtype=object))
    out["similarity_display_role"] = out["similarity_display_role"].fillna(out["similarity_archetype"])

    out["similarity_color_key"] = out["similarity_archetype"]

    out = (
        out.loc[pd.to_numeric(out["player_id"], errors="coerce") != int(args.player_id)]
        .sort_values("similarity_raw", ascending=False)
        .head(args.top)
        .copy()
    )

    stem = args.output_prefix or f"similar_{int(args.player_id)}"
    written = []

    if args.format in {"csv", "both"}:
        csv_path = Path(stem).with_suffix(".csv")
        out.to_csv(csv_path, index=False)
        written.append(csv_path)

    if args.format in {"json", "both"}:
        json_path = Path(stem).with_suffix(".json")
        payload = {
            "target": {
                "player_id": target.get("player_id"),
                "player_name": target.get("player_name", target.get("profile_name")),
                "team": target.get("team"),
                "role_column": role_col,
                "role_value": target.get(role_col) if role_col else None,
                "normalized_family": family,
                "side": info.get("target_side"),
            },
            "cohort_info": {
                **info,
                "cohort_size": len(cohort),
                "features_used": feature_cols,
                "feature_count": len(feature_cols),
                "core_features_used": core_cols,
                "core_feature_count": len(core_cols),
                "broad_features_used": broad_cols,
                "broad_feature_count": len(broad_cols),
                "feature_weights": feature_weights,
                "score_blend": {
                    "core_share": core_share,
                    "broad_share": 1.0 - core_share,
                    "z_weight": args.z_weight,
                    "percentile_weight": args.percentile_weight,
                    "spatial_weight": args.spatial_weight,
                    "role_bonus_weight": args.role_bonus_weight,
                    "readability_rescaled": not args.raw_score,
                },
            },
            "results": out.to_dict(orient="records"),
        }
        json_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8"
        )

    display_cols = [
        "player_name", "player_id", "team", "league",
        "arbitrated_position", "arbitrated_role_group", "arbitrated_lane",
        "similarity", "similarity_raw", "similarity_core", "similarity_broad",
        "similarity_spatial", "similarity_role_bonus",
    ]
    display_cols = [c for c in display_cols if c in out.columns]

    print(f"Using role column: {role_col}")
    print(f"Cohort strategy: {info.get('cohort_strategy')}")
    print(f"Cohort size: {len(cohort)}")
    print(f"Features used: {len(feature_cols)} | core={len(core_cols)} broad={len(broad_cols)} | core_share={core_share:.2f}")
    for p in written:
        print(f"Output: {p}")

    print(out[display_cols].head(args.top).to_string(index=False))


if __name__ == "__main__":
    main()
