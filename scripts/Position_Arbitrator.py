'''
Position_Arbitrator.py  —  V14 (Possession-Corrected Rewrite)
═══════════════════════════════════════════════════════════════

Core architecture change from V13
──────────────────────────────────
Every previous version used raw spatial coordinates (avg_x) as-is.
This means a wingback at a 70%-possession team (Grimaldo at Leverkusen)
averaged x=54 — indistinguishable from an AM in raw space — while the same
player at a 35%-possession team would sit at x=46, looking like a DM.

V14 corrects avg_x for possession before any zone-based decision:

    team_poss_pct  = 100 - oppo_poss_pct
    adj_avg_x      = avg_x - (team_poss_pct - 50.0) * POSS_SLOPE_X

POSS_SLOPE_X = 0.25 is empirically derived from this dataset:
  - Regressing avg_x on team_poss_pct per role group gives slopes of
    0.197 (WB) to 0.299 (AM-W); 0.25 is the weighted cross-role median.
  - The lateral axis (avg_y) is not adjusted — possession does not
    meaningfully shift where wide players operate laterally.

Possession-adjusted zone thresholds (calibrated to adj_avg_x distributions):
    Deep:  adj_avg_x < 38    (CB median=36.3, DM median=41.8)
    Mid:   38 ≤ adj_avg_x < 58   (WB median=51.5, CM median=50.7)
    High:  adj_avg_x ≥ 58    (AM-W median=60.5, ST median=64.5)

    The old thresholds were 35/62 on raw coords. After correction the
    midpoint between WB p90 (58.9) and AM-W p10 (54.7) is ~57; we use 58.

New role: WM (Wide Midfielder / RM archetype)
──────────────────────────────────────────────
The RM role — Giuliano Simeone, classic Atlético wide players — was
previously absorbed into AM-W (too attacking) or WB (too defensive).
The defining profile:
    adj_avg_x in [48, 60]
    spatial_wide_pct ≥ 0.65
    spatial_mid_wide_pct > spatial_high_wide_pct
    meaningful P-Adj defensive output (tracks back, wins ball)

After possession correction, Simeone sits at adj_avg_x=57.5 with
mid_wide=0.645 > high_wide=0.355 — cleanly in WM territory rather than
the high attacking zone of a true AM-W.

Possession adjustment of role-score stats
──────────────────────────────────────────
Two passing stats have empirically large possession contamination
(slope > 0.3 per 1% possession, r > 0.38 across role groups):
    passes_own_half_total_per90   slope ≈ +0.308/pct
    passes_total_per90            slope ≈ +1.304/pct
    passes_opposition_half_per90  slope ≈ +0.840/pct

These are adjusted in role_scores before scoring:
    pa_stat = raw_stat - (team_poss_pct - 50.0) * slope

This prevents a DM or WB at a high-possession team from scoring
artificially high on passing volume relative to their true contribution.

CB hard gate
────────────
Simplified from V13. The gate only fires when:
    1. primary_role_position is CB AND
    2. secondary_role_position is NOT an explicit FB/WB code.
The spatial-veto was tested on all 1056 CBs and found unsafe (released
Koundé, Ben White, Walker). The secondary-role veto releases only players
whose own pipeline explicitly labelled them FB/WB as a secondary role.
profile_position_raw is also checked but profile_position (dead column) is not.

Output columns added
────────────────────
    arbitrated_position        short code (e.g. LB, RW, WM, CM)
    arbitrated_role_group      family bucket (FB, WB, AM-W, WM, CM, ...)
    arbitrated_lane            human label (Left Fullback, Wide Midfielder, ...)
    arbitrated_confidence      evidence-strength float 0.35–0.95
    position_conflict_flag     bool
    arbitration_reason         text
    position_evidence          diagnostic text
    adj_avg_x                  possession-corrected average x coordinate
    team_poss_pct              derived team possession share

Usage
─────
  python Position_Arbitrator.py \\
      --input  player_season_totals.csv \\
      --output player_season_totals_arbitrated.csv

  python Position_Arbitrator.py \\
      --input player_season_totals.csv \\
      --player-id 12345 --format both

  python Position_Arbitrator.py \\
      --input player_season_totals.csv \\
      --position-context position_context_by_event.json \\
      --output player_season_totals_arbitrated.csv
'''

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


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

# Empirical slope: avg_x shifts this many units per 1% above/below 50% possession.
# Derived by regressing season_avg_x on team_poss_pct per role group;
# cross-role weighted median is 0.25.
POSS_SLOPE_X: float = 0.25

# Possession-adjustment slopes for contaminated passing stats.
# Slope = units of stat per 1% possession (derived from dataset regression).
POSS_STAT_SLOPES: dict[str, float] = {
    "passes_own_half_total_per90":      0.308,
    "passes_own_half_accurate_per90":   0.313,
    "passes_total_per90":               1.304,
    "passes_accurate_per90":            1.285,
    "passes_opposition_half_total_per90":  0.840,
    "passes_opposition_half_accurate_per90": 0.723,
}

# adj_avg_x zone boundaries (calibrated to possession-corrected distributions)
ADJ_X_HIGH: float  = 58.0   # above = high zone  (AM-W p10=54.7, WB p90=58.9)
ADJ_X_DEEP: float  = 38.0   # below = deep zone  (DM median=41.8, CB median=36.3)
# mid zone: [38, 58]

# adj_avg_x sub-thresholds for WM archetype gate
ADJ_X_WM_LO: float = 48.0
ADJ_X_WM_HI: float = 60.0

PROFILE_POSITION_COLUMNS = [
    "profile_position_raw",
    "canonical_position",
    "sofascore_position",
    "position",
    "player_position",
]
MATCH_ROLE_COLUMNS   = ["role_position", "primary_role_position", "secondary_role_position"]
MATCH_POSITION_COLUMNS = ["player_position", "position", "lineup_position", "match_position", "base_position"]
MINUTES_COL = "minutes_played"

SPATIAL_X_COLUMNS = [
    "adj_avg_x", "avg_x", "average_x", "averageX",
    "position_x", "positionX", "average_position_x", "heatmap_avg_x",
]
SPATIAL_Y_COLUMNS = [
    "avg_y", "average_y", "averageY",
    "position_y", "positionY", "average_position_y", "heatmap_avg_y",
]
POSITION_CONTEXT_SOURCE_COL = "position_context_source"

POSITION_ALIASES = {
    "G": "GK", "GK": "GK", "GOALKEEPER": "GK",
    "CB": "CB", "DC": "CB", "D C": "CB", "DEFENDER CENTRE": "CB", "CENTRE BACK": "CB", "CENTER BACK": "CB",
    "RB": "RB", "DR": "RB", "D R": "RB", "RIGHT BACK": "RB",
    "LB": "LB", "DL": "LB", "D L": "LB", "LEFT BACK": "LB",
    "FB": "FB", "FULL BACK": "FB",
    "RWB": "RWB", "WBR": "RWB", "WB R": "RWB", "D/WB R": "RWB",
    "LWB": "LWB", "WBL": "LWB", "WB L": "LWB", "D/WB L": "LWB",
    "WB": "WB", "WING BACK": "WB",
    "DM": "DM", "DMC": "DM", "DM C": "DM", "DEFENSIVE MIDFIELDER": "DM",
    "CM": "CM", "MC": "CM", "M C": "CM", "MIDFIELDER CENTRE": "CM", "LCM": "CM", "RCM": "CM",
    "RM": "RM", "MR": "RM", "M R": "RM", "RIGHT MIDFIELDER": "RM",
    "LM": "LM", "ML": "LM", "M L": "LM", "LEFT MIDFIELDER": "LM",
    "WM": "WM",
    "AM": "AM", "AMC": "AMC", "AM C": "AMC", "ATTACKING MIDFIELDER": "AM", "CAM": "AMC",
    "RAM": "AMR", "AMR": "AMR", "AM R": "AMR",
    "LAM": "AML", "AML": "AML", "AM L": "AML",
    "RW": "RW", "RIGHT WINGER": "RW", "RIGHT FORWARD": "RW", "RF": "RW",
    "LW": "LW", "LEFT WINGER": "LW", "LEFT FORWARD": "LW", "LF": "LW",
    "W": "W", "WINGER": "W",
    "ST": "ST", "CF": "ST", "F": "ST", "FW": "ST", "FORWARD": "ST",
    "STRIKER": "ST", "CENTRE FORWARD": "ST", "CENTER FORWARD": "ST",
    "SS": "SS", "SECOND STRIKER": "SS",
}

SIDE_MAP = {
    "RW": "Right", "AMR": "Right", "RM": "Right", "RWB": "Right", "RB": "Right",
    "LW": "Left",  "AML": "Left",  "LM": "Left",  "LWB": "Left",  "LB": "Left",
}

GROUP_MAP = {
    "GK": "GK",
    "CB": "CB",
    "RB": "FB", "LB": "FB", "FB": "FB",
    "RWB": "WB", "LWB": "WB", "WB": "WB",
    "DM": "DM",
    "CM": "CM",
    "RM": "WM", "LM": "WM", "WM": "WM",   # WM = wide midfielder family
    "AMC": "AM-C", "AM": "AM-C",
    "AMR": "AM-W", "AML": "AM-W", "RW": "AM-W", "LW": "AM-W", "W": "AM-W",
    "ST": "ST", "SS": "ST-SS",
}

WIDE_ATTACK_CODES   = {"RW", "LW", "AMR", "AML", "RM", "LM", "W"}
WIDE_DEF_CODES      = {"RB", "LB", "RWB", "LWB"}
CENTRAL_ATTACK_CODES = {"AMC", "AM", "SS"}
STRIKER_CODES       = {"ST", "CF"}
GENERIC_POSITION_CODES = {"D", "M", "F", "FW", "DEFENDER", "MIDFIELDER", "FORWARD", None}


# ═══════════════════════════════════════════════════════════════════════════
# Utility
# ═══════════════════════════════════════════════════════════════════════════

def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def normalize_position(value: Any) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    clean = text.upper().replace("-", " ").replace("_", " ").replace("/", " / ")
    clean = re.sub(r"\s+", " ", clean).strip()
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


def is_heuristic_position_source(row: pd.Series) -> bool:
    src = str(row.get("position_source") or "").strip().lower()
    return any(tok in src for tok in {"heuristic_stats", "heuristic_base"})


def is_generic_position(code: str | None) -> bool:
    return code in GENERIC_POSITION_CODES


def score_gap_strength(a: float, b: float, scale: float = 35.0) -> float:
    return clamp(abs(float(a) - float(b)) / scale)


def confidence_from_evidence(
    *,
    base: float,
    agreement: float = 0.0,
    evidence_gap: float = 0.0,
    spatial_strength: float = 0.0,
    profile_bonus: float = 0.0,
    conflict_penalty: float = 0.0,
) -> float:
    score = (
        base
        + 0.16 * clamp(agreement)
        + 0.14 * clamp(evidence_gap)
        + 0.18 * clamp(spatial_strength)
        + profile_bonus
        - conflict_penalty
    )
    return round(clamp(score, 0.35, 0.95), 2)


def weighted_mode(values: list[tuple[Any, float]]) -> Any:
    weights: dict[Any, float] = defaultdict(float)
    for value, weight in values:
        if value is None or pd.isna(value) or str(value).strip() == "":
            continue
        weights[value] += float(weight or 0.0)
    if not weights:
        return None
    return max(weights.items(), key=lambda kv: kv[1])[0]


def weighted_position_summary(
    group: pd.DataFrame, cols: list[str], minutes_col: str = MINUTES_COL
) -> tuple[str | None, str]:
    minutes = numeric(group[minutes_col]) if minutes_col in group.columns else pd.Series(
        [1.0] * len(group), index=group.index
    )
    pos_minutes: dict[str, float] = defaultdict(float)
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
    summary = ", ".join(
        f"{k}:{round(v, 1)}"
        for k, v in sorted(pos_minutes.items(), key=lambda kv: kv[1], reverse=True)
    )
    return mode, summary


# ═══════════════════════════════════════════════════════════════════════════
# Possession correction
# ═══════════════════════════════════════════════════════════════════════════

def compute_adj_avg_x(row: pd.Series) -> float | None:
    """
    Possession-correct avg_x.

    team_poss_pct = 100 - oppo_poss_pct.
    adj_avg_x = avg_x - (team_poss_pct - 50.0) * POSS_SLOPE_X

    Returns None when avg_x is not available.
    """
    avg_x = row_number(row, ["avg_x", "season_avg_x", "average_x", "averageX"])
    if avg_x is None:
        return None
    oppo_poss = row_number(row, ["oppo_poss_pct"])
    if oppo_poss is None:
        return avg_x   # no possession data — use raw
    team_poss = 100.0 - oppo_poss
    return avg_x - (team_poss - 50.0) * POSS_SLOPE_X


def adj_vertical_zone(adj_x: float | None) -> str | None:
    """Return 'high', 'mid', or 'deep' based on possession-adjusted x."""
    if adj_x is None:
        return None
    if adj_x >= ADJ_X_HIGH:
        return "high"
    if adj_x < ADJ_X_DEEP:
        return "deep"
    return "mid"


def poss_adjust_stat(row: pd.Series, col: str) -> float:
    """
    Return a possession-corrected per-90 stat.
    For passing-volume stats contaminated by possession, subtract the
    expected inflation above the 50% possession baseline.
    Falls back gracefully to 0.0 if the column is missing.
    """
    raw = row_number(row, [col]) or 0.0
    slope = POSS_STAT_SLOPES.get(col, 0.0)
    if slope == 0.0:
        return raw
    oppo_poss = row_number(row, ["oppo_poss_pct"])
    if oppo_poss is None:
        return raw
    team_poss = 100.0 - oppo_poss
    return raw - (team_poss - 50.0) * slope


# ═══════════════════════════════════════════════════════════════════════════
# Per-90 helpers (possession-aware)
# ═══════════════════════════════════════════════════════════════════════════

def metric_value(row: pd.Series, names: list[str]) -> float:
    """Prefer per90 variants, then raw names. Returns 0.0 if not found."""
    candidates: list[str] = []
    for n in names:
        candidates.extend([f"{n}_per90", f"{n}_p90"])
    candidates.extend(names)
    return row_number(row, candidates) or 0.0


def poss_metric(row: pd.Series, col: str) -> float:
    """Like metric_value but applies possession correction for known contaminated stats."""
    if col in POSS_STAT_SLOPES:
        return poss_adjust_stat(row, col)
    return metric_value(row, [col])


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


# ═══════════════════════════════════════════════════════════════════════════
# Role scores  (V14 — possession-aware)
# ═══════════════════════════════════════════════════════════════════════════

def role_scores_v14(row: pd.Series) -> dict[str, float]:
    """
    Possession-corrected role fingerprints.

    Changes from V6:
    - passes_own_half, passes_total, passes_opp_half are possession-adjusted
      before entering the DM/CM/WB scores.
    - P-Adj_ defensive columns are used for tackles/interceptions/recoveries
      where available (they are already possession-normalised by definition).
    - WM score added for the wide-midfielder / RM archetype.
    - adj_avg_x used in spatial zone contributions where relevant.
    """
    # Attacking
    crosses      = metric_value(row, ["crosses_total"])
    takeons      = metric_value(row, ["dribbles_attempted", "contests_total"])
    takeons_won  = metric_value(row, ["dribbles_completed", "contests_won"])
    prog_carries = metric_value(row, ["progressive_carries"])
    prog_dist    = metric_value(row, ["progressive_carry_distance", "carry_distance"])
    touches_box  = metric_value(row, ["touches_opp_box"])
    shots        = metric_value(row, ["shots_total"])
    xg           = metric_value(row, ["xg"])
    xa           = metric_value(row, ["xa"])
    key_passes   = metric_value(row, ["key_passes"])
    dribble_val  = row_number(row, ["dribble_value"]) or 0.0
    shot_val     = row_number(row, ["shot_value"]) or 0.0
    pass_val     = row_number(row, ["pass_value"]) or 0.0

    # Defensive — use P-Adj where available, fall back to raw
    tackles       = metric_value(row, ["P-Adj_tackles_total", "tackles_total"])
    tackles_won   = metric_value(row, ["P-Adj_tackles_won", "tackles_won"])
    interceptions = metric_value(row, ["P-Adj_interceptions", "interceptions"])
    recoveries    = metric_value(row, ["P-Adj_recoveries", "recoveries"])
    padj_clear    = metric_value(row, ["P-Adj_clearances"])
    aerial_won    = metric_value(row, ["aerial_duels_won"])
    aerial_total  = metric_value(row, ["aerial_duels_total"])
    blocks        = metric_value(row, ["blocked_shots"])
    duels_won     = metric_value(row, ["duels_won"])

    # Passing — possession-corrected for volume-contaminated columns
    passes     = poss_metric(row, "passes_total_per90")
    own_half   = poss_metric(row, "passes_own_half_total_per90")
    passes_opp = poss_metric(row, "passes_opposition_half_total_per90")
    long_balls = metric_value(row, ["long_balls_total"])

    # Wide score
    wide = (
        min(crosses / 3.0, 1)      * 1.3
        + min(takeons / 5.0, 1)    * 1.4
        + min(takeons_won / 2.5, 1)* 0.9
        + min(prog_carries / 3.0, 1)* 1.0
        + min(prog_dist / 60.0, 1) * 0.7
        + min(dribble_val / 0.35, 1)* 0.8
    )

    # Striker score
    st = (
        min(shots / 3.5, 1)        * 1.4
        + min(xg / 0.45, 1)        * 1.5
        + min(touches_box / 5.0, 1)* 1.1
        + min(shot_val / 0.25, 1)  * 0.6
        - min(crosses / 4.0, 1)    * 0.4
    )

    # SS/AM score
    ss_am = (
        min(key_passes / 2.2, 1) * 1.1
        + min(xa / 0.25, 1)      * 1.2
        + min(passes_opp / 25.0, 1)* 0.8
        + min(pass_val / 0.35, 1)* 0.7
        + min(shots / 2.2, 1)    * 0.4
    )

    # DM score — possession-adjusted passing volume
    dm = (
        min(tackles / 3.0, 1)        * 1.0
        + min(interceptions / 1.7, 1)* 1.1
        + min(recoveries / 7.0, 1)   * 1.0
        + min(own_half / 20.0, 1)    * 0.7   # PA-adjusted; threshold lowered accordingly
        + min(long_balls / 4.0, 1)   * 0.4
        - min(touches_box / 3.0, 1)  * 0.5
        - min(shots / 2.0, 1)        * 0.3
    )

    # CM score — possession-adjusted passing volume
    cm = (
        min(passes / 40.0, 1)       * 1.1  # PA-adjusted threshold lowered from 55
        + min(recoveries / 6.0, 1)  * 0.7
        + min(prog_carries / 2.2, 1)* 0.7
        + min(passes_opp / 18.0, 1) * 0.7  # PA-adjusted
        + min(tackles / 2.2, 1)     * 0.5
        + min(pass_val / 0.30, 1)   * 0.5
    )

    # AM score
    am = (
        min(key_passes / 2.0, 1)    * 1.1
        + min(xa / 0.25, 1)         * 1.0
        + min(touches_box / 3.5, 1) * 0.8
        + min(shots / 2.0, 1)       * 0.7
        + min(passes_opp / 18.0, 1) * 0.5  # PA-adjusted
        - min(own_half / 22.0, 1)   * 0.3  # PA-adjusted
    )

    # CB score  — note: clearances col is P-Adj_clearances in this dataset
    cb = (
        min(padj_clear / 2.5, 1)    * 1.5
        + min(aerial_won / 2.5, 1)  * 1.1
        + min(aerial_total / 4.0, 1)* 0.6
        + min(blocks / 1.0, 1)      * 0.8
        + min(interceptions / 1.5, 1)* 0.5
        - min(crosses / 1.5, 1)     * 0.7
        - min(takeons / 2.0, 1)     * 0.4
    )

    # FB score
    fb = (
        min(crosses / 2.2, 1)       * 1.0
        + min(tackles / 2.5, 1)     * 0.8
        + min(recoveries / 6.0, 1)  * 0.7
        + min(prog_carries / 2.0, 1)* 0.8
        + min(passes_opp / 14.0, 1) * 0.4  # PA-adjusted
        - min(shots / 1.5, 1)       * 0.4
    )

    # WB score
    wb = (
        min(crosses / 3.0, 1)       * 1.0
        + min(prog_carries / 3.0, 1)* 1.0
        + min(prog_dist / 60.0, 1)  * 0.7
        + min(takeons / 3.0, 1)     * 0.5
        + min(tackles / 2.0, 1)     * 0.4
    )

    # WM score — wide midfielder (RM archetype)
    # High crossing volume + meaningful defensive output + mid-channel progression
    # but NOT pure ball-carrier/dribbler like a winger, NOT a pure CB/FB defender
    wm = (
        min(crosses / 2.5, 1)       * 1.2
        + min(tackles / 2.5, 1)     * 1.0
        + min(recoveries / 6.0, 1)  * 1.0
        + min(interceptions / 1.4, 1)* 0.9
        + min(passes_opp / 14.0, 1) * 0.8  # PA-adjusted
        + min(key_passes / 1.2, 1)  * 0.7
        + min(prog_carries / 2.0, 1)* 0.6
        - min(shots / 2.5, 1)       * 0.4  # not a shooter
        - min(padj_clear / 1.5, 1)  * 0.5  # not a CB
    )

    return {
        "W":  round(wide,  3),
        "ST": round(st,    3),
        "SS": round(ss_am, 3),
        "DM": round(dm,    3),
        "CM": round(cm,    3),
        "AM": round(am,    3),
        "CB": round(cb,    3),
        "FB": round(fb,    3),
        "WB": round(wb,    3),
        "WM": round(wm,    3),
    }


def best_role(scores: dict[str, float], roles: list[str]) -> tuple[str, float, float]:
    ordered = sorted(((r, scores.get(r, 0.0)) for r in roles), key=lambda x: x[1], reverse=True)
    best, best_score = ordered[0]
    second = ordered[1][1] if len(ordered) > 1 else 0.0
    return best, best_score, second


# ═══════════════════════════════════════════════════════════════════════════
# Spatial helpers (possession-aware)
# ═══════════════════════════════════════════════════════════════════════════

def spatial_role_from_adj_xy(
    base_code: str | None,
    adj_x: float | None,
    y: float | None,
) -> tuple[str | None, str | None, str | None, float, str]:
    """
    Convert possession-adjusted average position to a role guess.

    Uses adj_x (possession-corrected) with updated thresholds.
    y is not possession-adjusted (lateral position is possession-independent).
    """
    if adj_x is None or y is None:
        return None, None, None, 0.0, "no_spatial_data"
    if not (0 <= adj_x <= 100 and 0 <= y <= 100):
        return None, None, None, 0.0, "spatial_out_of_range"

    base = normalize_position(base_code)
    if base in {"GK"}:
        return "GK", "GK", "Central", 0.85, "spatial_adj"

    side = "Right" if y <= 33 else "Left" if y >= 67 else "Central"
    wide = side in {"Left", "Right"}
    sc   = "L" if side == "Left" else "R" if side == "Right" else ""
    zone = adj_vertical_zone(adj_x)

    # Defenders
    if position_group(base) in {"CB", "FB", "WB"} or base in {"D", "DEFENDER"}:
        if wide:
            if zone == "high":
                role, group, lane, strength = f"{sc}WB", "WB", f"{side} Wing Back", 0.78
            elif zone == "mid":
                role, group, lane, strength = f"{sc}WB", "WB", f"{side} Wing Back", 0.76
            else:
                role, group, lane, strength = f"{sc}B", "FB", f"{side} Fullback", 0.82
        else:
            role, group, lane, strength = "CB", "CB", "Central Defence", 0.82
        return role, group, lane, strength, "spatial_adj"

    # Midfielders
    if position_group(base) in {"DM", "CM", "AM-C", "AM-W", "WM"} or base in {"M", "MIDFIELDER"}:
        if wide:
            if zone == "high":
                role, group, lane, strength = f"{sc}W", "AM-W", f"{side} Wide Forward", 0.72
            elif zone == "mid":
                role, group, lane, strength = f"{sc}M", "WM",   f"{side} Wide Midfielder", 0.72
            else:
                role, group, lane, strength = f"{sc}M", "WM",   f"{side} Wide Midfielder", 0.60
        else:
            if zone == "deep":
                role, group, lane, strength = "DM",  "DM",   "Defensive Midfield", 0.75
            elif zone == "high":
                role, group, lane, strength = "AMC", "AM-C", "Central Attacking Midfield", 0.72
            else:
                role, group, lane, strength = "CM",  "CM",   "Central Midfield", 0.72
        return role, group, lane, strength, "spatial_adj"

    # Forwards
    if position_group(base) in {"ST", "ST-SS", "AM-W", "AM-C"} or base in {"F", "FW", "FORWARD"}:
        if wide:
            role, group, lane, strength = f"{sc}W", "AM-W", f"{side} Wide Forward", 0.78
        else:
            if zone == "mid":
                role, group, lane, strength = "SS", "ST-SS", "Central Support Forward", 0.65
            else:
                role, group, lane, strength = "ST", "ST",    "Central Forward", 0.76
        return role, group, lane, strength, "spatial_adj"

    return None, None, None, 0.0, "spatial_base_unknown"


def _position_zone_from_adj_xy(adj_x: float | None, y: float | None) -> str | None:
    if adj_x is None or y is None:
        return None
    try:
        adj_x = float(adj_x); y = float(y)
    except Exception:
        return None
    if not (0 <= adj_x <= 100 and 0 <= y <= 100):
        return None
    vertical = "deep" if adj_x < ADJ_X_DEEP else "high" if adj_x >= ADJ_X_HIGH else "mid"
    lateral  = "right" if y < 33 else "left" if y > 67 else "central"
    return f"{vertical}_{lateral}"


# ═══════════════════════════════════════════════════════════════════════════
# Position-context merge (from external JSON — unchanged from V11/V13)
# ═══════════════════════════════════════════════════════════════════════════

def _extract_position_context_records(context_path: str | None) -> list[dict[str, Any]]:
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
                "event_id":   r.get("event_id"),
                "player_id":  r.get("player_id") or r.get("id"),
                "avg_x":      r.get("avg_x") or r.get("average_x") or r.get("averageX") or r.get("x"),
                "avg_y":      r.get("avg_y") or r.get("average_y") or r.get("averageY") or r.get("y"),
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
        items = players.items() if isinstance(players, dict) else [(None, pb) for pb in players]
        for pid, pb in items:
            if not isinstance(pb, dict):
                continue
            records.append({
                "event_id":   event_id,
                "player_id":  pid or pb.get("player_id") or pb.get("id"),
                "avg_x":      pb.get("avg_x") or pb.get("average_x") or pb.get("averageX") or pb.get("x"),
                "avg_y":      pb.get("avg_y") or pb.get("average_y") or pb.get("averageY") or pb.get("y"),
                POSITION_CONTEXT_SOURCE_COL: pb.get("source") or event_block.get("source") or "position_context_json",
            })
    return records


def _season_position_summary_from_context(records: list[dict[str, Any]]) -> pd.DataFrame:
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
    ctx = ctx.loc[ctx["avg_x"].between(0, 100) & ctx["avg_y"].between(0, 100)].copy()
    if ctx.empty:
        return pd.DataFrame()
    ctx["player_id"] = ctx["player_id"].astype(int)
    ctx["is_wide"]        = (ctx["avg_y"] < 33) | (ctx["avg_y"] > 67)
    ctx["is_right"]       = ctx["avg_y"] < 33
    ctx["is_left"]        = ctx["avg_y"] > 67
    ctx["is_central"]     = ~ctx["is_wide"]
    ctx["is_high"]        = ctx["avg_x"] >= ADJ_X_HIGH
    ctx["is_deep"]        = ctx["avg_x"] < ADJ_X_DEEP
    ctx["is_mid"]         = (ctx["avg_x"] >= ADJ_X_DEEP) & (ctx["avg_x"] < ADJ_X_HIGH)
    ctx["is_high_wide"]   = ctx["is_high"] & ctx["is_wide"]
    ctx["is_mid_wide"]    = ctx["is_mid"]  & ctx["is_wide"]
    ctx["is_deep_central"]= ctx["is_deep"] & ctx["is_central"]

    grouped = ctx.groupby("player_id", dropna=False)
    count_col = "nunique" if "event_id" in ctx.columns else "count"
    summary = grouped.agg(
        season_avg_x     = ("avg_x", "mean"),
        season_avg_y     = ("avg_y", "mean"),
        season_std_x     = ("avg_x", "std"),
        season_std_y     = ("avg_y", "std"),
        spatial_matches_used   = ("event_id", count_col) if count_col == "nunique" else ("avg_x", "count"),
        spatial_wide_matches   = ("is_wide",         "sum"),
        spatial_right_matches  = ("is_right",        "sum"),
        spatial_left_matches   = ("is_left",         "sum"),
        spatial_central_matches= ("is_central",      "sum"),
        spatial_high_matches   = ("is_high",         "sum"),
        spatial_mid_matches    = ("is_mid",          "sum"),
        spatial_deep_matches   = ("is_deep",         "sum"),
        spatial_high_wide_matches = ("is_high_wide", "sum"),
        spatial_mid_wide_matches  = ("is_mid_wide",  "sum"),
        spatial_deep_central_matches = ("is_deep_central", "sum"),
    ).reset_index()
    summary["season_std_x"] = summary["season_std_x"].fillna(0.0)
    summary["season_std_y"] = summary["season_std_y"].fillna(0.0)
    denom = summary["spatial_matches_used"].replace(0, np.nan)
    for col in ["spatial_wide","spatial_right","spatial_left","spatial_central",
                "spatial_high","spatial_mid","spatial_deep","spatial_high_wide",
                "spatial_mid_wide","spatial_deep_central"]:
        summary[f"{col}_pct"] = (summary[f"{col}_matches"] / denom).fillna(0.0).round(4)

    summary["season_position_zone"] = [
        _position_zone_from_adj_xy(x, y)
        for x, y in zip(summary["season_avg_x"], summary["season_avg_y"])
    ]

    def dominant_side(row: pd.Series) -> str:
        rp = row.get("spatial_right_pct", 0)
        lp = row.get("spatial_left_pct", 0)
        if rp >= 0.45 and rp >= lp:
            return "Right"
        if lp >= 0.45 and lp > rp:
            return "Left"
        return "Central/Mixed"

    summary["spatial_dominant_side"] = summary.apply(dominant_side, axis=1)
    summary[POSITION_CONTEXT_SOURCE_COL] = "season_collapsed_average_positions_v14"
    return summary


def apply_position_context(df: pd.DataFrame, context_path: str | None) -> pd.DataFrame:
    records = _extract_position_context_records(context_path)
    if not records:
        return df
    out = df.copy()
    if "event_id" in out.columns and "player_id" in out.columns:
        ctx = pd.DataFrame(records)
        if ctx.empty:
            return out
        for col in ["event_id", "player_id"]:
            if col in ctx.columns:
                ctx[col] = ctx[col].astype(str)
        out["_event_id_str"]  = out["event_id"].astype(str)
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

    if "player_id" not in out.columns:
        return out
    summary = _season_position_summary_from_context(records)
    if summary.empty:
        return out
    out["_player_id_int"]     = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    summary["_player_id_int"] = pd.to_numeric(summary["player_id"], errors="coerce").astype("Int64")
    summary = summary.drop(columns=["player_id"], errors="ignore")
    out = out.merge(summary, on="_player_id_int", how="left", suffixes=("", "_ctx"))
    if "avg_x" not in out.columns:
        out["avg_x"] = np.nan
    if "avg_y" not in out.columns:
        out["avg_y"] = np.nan
    out["avg_x"] = out["avg_x"].where(out["avg_x"].notna(), out.get("season_avg_x"))
    out["avg_y"] = out["avg_y"].where(out["avg_y"].notna(), out.get("season_avg_y"))
    ctx_src = f"{POSITION_CONTEXT_SOURCE_COL}_ctx"
    if ctx_src in out.columns:
        if POSITION_CONTEXT_SOURCE_COL in out.columns:
            out[POSITION_CONTEXT_SOURCE_COL] = out[POSITION_CONTEXT_SOURCE_COL].where(
                out[POSITION_CONTEXT_SOURCE_COL].notna(), out[ctx_src]
            )
            out = out.drop(columns=[ctx_src])
        else:
            out = out.rename(columns={ctx_src: POSITION_CONTEXT_SOURCE_COL})
    out = out.drop(columns=["_player_id_int"], errors="ignore")
    return out


# ═══════════════════════════════════════════════════════════════════════════
# Source family detection
# ═══════════════════════════════════════════════════════════════════════════

def family_hint_from_sources(
    profile_code: str | None, match_pos: str | None, match_role: str | None
) -> str | None:
    """
    Determine broad positional family from the available source labels.

    Priority: DEF > ATT > MID.

    ATT is checked before MID so that a player declared as F/ST/FW is treated
    as an attacker even when match data also shows AM. This resolves the
    Raphinha-type case (F + AM → ATT, not MID → correct attacker splitter path).
    """
    groups = [position_group(c) for c in [profile_code, match_pos, match_role] if c]
    if any(g in {"CB", "FB", "WB"} for g in groups):
        return "DEF"
    if any(g in {"ST", "ST-SS"} for g in groups):
        return "ATT"
    if any(g in {"DM", "CM", "AM-C", "AM-W", "WM"} for g in groups):
        return "MID"
    return None


# ═══════════════════════════════════════════════════════════════════════════
# CB hard gate  (simplified V14)
# ═══════════════════════════════════════════════════════════════════════════

def enforce_cb_hard_gate(row: pd.Series, current_arbitrated_group: str | None) -> str | None:
    """
    Prevent CBs from drifting into FB/WB labels solely from spatial or style signals.

    V14 simplification:
    - Only checks primary_role_position for CB identity (the only column that
      reliably carries CB for this dataset; player_position/base_position are
      always generic 'D').
    - Also checks profile_position_raw (NOT the defunct 'profile_position').
    - Secondary-role veto: if secondary_role_position is an explicit wide-defensive
      code, the player is a fullback/wingback regardless of primary label.
      Tested against all 1056 CBs — releases only genuine FB/WB players,
      does not touch Koundé, Ben White, Walker, Gvardiol (secondary=D).
    """
    wide_defensive_groups = {"LB", "RB", "FB", "WB", "LWB", "RWB", "CB-FB"}
    if current_arbitrated_group not in wide_defensive_groups:
        return current_arbitrated_group

    primary_role  = str(row.get("primary_role_position", "")).upper()
    profile_raw   = str(row.get("profile_position_raw",  "")).upper()

    cb_sources = [primary_role, profile_raw]
    is_cb_profile = any(
        x in {"CB", "DC", "CENTRE BACK", "CENTER BACK"}
        for x in cb_sources
    )
    if not is_cb_profile:
        return current_arbitrated_group

    # Secondary-role veto: explicit FB/WB secondary label overrides the primary CB
    secondary_role = str(row.get("secondary_role_position", "")).upper()
    if secondary_role in {"FB", "LB", "RB", "LWB", "RWB", "WB"}:
        return current_arbitrated_group

    # Check genuine wide deployment in positions_played_list
    import re as _re
    pos_played = (
        str(row.get("positions_played_list",  "")) + " " +
        str(row.get("match_positions_played", ""))
    ).upper()

    short_wide = ["LB", "RB", "LWB", "RWB", "FB", "WB"]
    long_wide  = ["LEFT BACK", "RIGHT BACK", "FULLBACK", "WINGBACK"]
    has_wide_deployment = (
        any(_re.search(rf"\b{r}\b", pos_played) for r in short_wide)
        or any(r in pos_played for r in long_wide)
    )

    prog_carries = metric_value(row, ["progressive_carries"])
    crosses      = metric_value(row, ["crosses_total"])
    takeons      = metric_value(row, ["dribbles_attempted", "contests_total"])

    spatial_wide_pct = row_number(row, ["spatial_wide_pct"]) or 0.0
    _sc = row_number(row, ["spatial_central_pct"])
    spatial_central_pct = _sc if _sc is not None else 1.0
    spatial_n = row_number(row, ["spatial_matches_used"]) or 0.0

    spatial_overwhelming = (
            spatial_n >= 8
            and spatial_wide_pct >= 0.85
            and spatial_central_pct <= 0.10
    )

    genuine_hybrid = (
        has_wide_deployment
        and (crosses >= 1.8 or takeons >= 2.0 or prog_carries >= 2.2) or
        spatial_overwhelming
    )

    if not genuine_hybrid:
        return "CB"

    return current_arbitrated_group


# ═══════════════════════════════════════════════════════════════════════════
# Zone-distribution rescue  (possession-aware)
# ═══════════════════════════════════════════════════════════════════════════

def zone_distribution_rescue_v14(
    row: pd.Series,
    scores: dict[str, float],
    source_family: str | None,
    adj_x: float | None,
    y: float | None,
    heuristic_source: bool,
) -> dict[str, Any] | None:
    """
    Use possession-corrected spatial distribution to classify players whose
    repeated positional appearances clearly place them in a specific zone.

    Replaces the V13 zone-distribution rescue with possession-aware thresholds.

    Three rescue paths:
    1. Wide defender  — wide_pct ≥ 0.55, source_family DEF or MID
    2. WM/RM          — wide_pct ≥ 0.65, mid_wide > high_wide, adj_x in WM band
    3. Wide forward   — wide_pct ≥ 0.38, source_family MID or ATT, W score competitive
    """
    wide_pct      = row_number(row, ["spatial_wide_pct"])     or 0.0
    high_wide_pct = row_number(row, ["spatial_high_wide_pct"])or 0.0
    mid_wide_pct  = row_number(row, ["spatial_mid_wide_pct"]) or 0.0
    right_pct     = row_number(row, ["spatial_right_pct"])    or 0.0
    left_pct      = row_number(row, ["spatial_left_pct"])     or 0.0
    spatial_n     = row_number(row, ["spatial_matches_used"]) or 0.0
    side_code     = "R" if right_pct >= left_pct else "L"
    side_lane     = "Right" if right_pct >= left_pct else "Left"

    evidence = (
        f"wide_pct={wide_pct:.2f}; mid_wide_pct={mid_wide_pct:.2f}; "
        f"high_wide_pct={high_wide_pct:.2f}; "
        f"adj_x={'N/A' if adj_x is None else f'{adj_x:.1f}'}; "
        f"right_pct={right_pct:.2f}; left_pct={left_pct:.2f}; "
        f"spatial_n={spatial_n:.0f}"
    )

    # ── Path 1: Wide defender ──────────────────────────────────────────
    if spatial_n >= 6 and wide_pct >= 0.55 and source_family in {"MID", "DEF"}:
        wide_def_score = max(scores.get("FB", 0), scores.get("WB", 0))
        mid_score      = max(scores.get("CM", 0), scores.get("DM", 0), scores.get("AM", 0))
        winger_score   = scores.get("W", 0)

        # creative_wb_exception: very wide AND mid-range high_wide (inverted WB pattern)
        creative_wb = wide_pct >= 0.85 and 0.12 <= high_wide_pct <= 0.45 and wide_def_score >= 1.90

        # Shield for high-touch wingers with genuine attacking wide profile
        winger_immune = high_wide_pct >= 0.35 and winger_score >= wide_def_score - 0.10

        if (
            wide_def_score >= mid_score - 1.15
            and (winger_score <= wide_def_score + 0.65 or creative_wb)
            and not winger_immune
        ):
            if source_family == "DEF":
                wb_or_fb = "WB" if (scores.get("WB", 0) >= scores.get("FB", 0) + 0.20 or high_wide_pct >= 0.30) else "FB"
            else:
                wb_or_fb = "WB" if (scores.get("WB", 0) >= scores.get("FB", 0) - 0.35 or high_wide_pct >= 0.14 or wide_pct >= 0.85) else "FB"

            return {
                "arbitrated_position":   f"{side_code}{'WB' if wb_or_fb == 'WB' else 'B'}",
                "arbitrated_role_group": wb_or_fb,
                "arbitrated_lane":       f"{side_lane} {'Wing Back' if wb_or_fb == 'WB' else 'Fullback'}",
                "arbitrated_confidence": 0.81,
                "position_conflict_flag": True,
                "arbitration_reason": "V14 zone rescue: wide defender from repeated wide spatial positions.",
                "position_evidence": f"v14_scores={scores}; {evidence}",
            }

    # ── Path 2: WM / RM archetype ──────────────────────────────────────
    # wide + mid-channel dominant + adj_x in WM band + defensive contribution
    if (
        spatial_n >= 6
        and wide_pct >= 0.65
        and mid_wide_pct > high_wide_pct
        and adj_x is not None
        and ADJ_X_WM_LO <= adj_x <= ADJ_X_WM_HI
        and source_family in {"MID", "ATT"}
    ):
        wm_score     = scores.get("WM", 0)
        winger_score = scores.get("W",  0)
        # Only reclassify if WM score is at least competitive with W
        if wm_score >= winger_score - 0.45:
            return {
                "arbitrated_position":   f"{side_code}M",
                "arbitrated_role_group": "WM",
                "arbitrated_lane":       f"{side_lane} Wide Midfielder",
                "arbitrated_confidence": 0.79,
                "position_conflict_flag": bool(heuristic_source),
                "arbitration_reason": "V14 zone rescue: WM/RM archetype — wide, mid-channel, possession-adjusted position.",
                "position_evidence": f"v14_scores={scores}; {evidence}",
            }

    # ── Path 3: Wide forward / winger ─────────────────────────────────
    if spatial_n >= 6 and wide_pct >= 0.38 and source_family in {"MID", "ATT"}:
        attacking_best = max(
            scores.get("CM", 0), scores.get("DM", 0), scores.get("AM", 0),
            scores.get("ST", 0), scores.get("SS", 0),
        )
        if scores.get("W", 0) >= attacking_best - 0.45:
            return {
                "arbitrated_position":   f"{side_code}W",
                "arbitrated_role_group": "AM-W",
                "arbitrated_lane":       f"{side_lane} Wide Forward",
                "arbitrated_confidence": 0.82,
                "position_conflict_flag": bool(heuristic_source),
                "arbitration_reason": "V14 zone rescue: wide forward from repeated wide spatial positions.",
                "position_evidence": f"v14_scores={scores}; {evidence}",
            }

    return None


# ═══════════════════════════════════════════════════════════════════════════
# Defender sub-classification
# ═══════════════════════════════════════════════════════════════════════════

def defender_refinement_v14(row: pd.Series, scores: dict[str, float]) -> tuple[str, str, str, str]:
    """Refine generic DEF into CB / FB / WB / CB-FB using style signals."""
    crosses      = metric_value(row, ["crosses_total"])
    prog_carries = metric_value(row, ["progressive_carries"])
    prog_dist    = metric_value(row, ["progressive_carry_distance", "carry_distance"])
    takeons      = metric_value(row, ["dribbles_attempted", "contests_total"])
    passes_opp   = poss_metric(row, "passes_opposition_half_total_per90")
    key_passes   = metric_value(row, ["key_passes"])
    tackles      = metric_value(row, ["P-Adj_tackles_total", "tackles_total"])
    recoveries   = metric_value(row, ["P-Adj_recoveries", "recoveries"])
    interceptions= metric_value(row, ["P-Adj_interceptions", "interceptions"])
    padj_clear   = metric_value(row, ["P-Adj_clearances"])
    aerial_won   = metric_value(row, ["aerial_duels_won"])
    aerial_total = metric_value(row, ["aerial_duels_total"])
    blocks       = metric_value(row, ["blocked_shots"])

    cb_signal = (
        min(padj_clear / 2.0, 1) * 1.4
        + min(aerial_won / 2.3, 1) * 1.0
        + min(aerial_total / 3.8, 1) * 0.6
        + min(blocks / 0.9, 1) * 0.7
        + min(interceptions / 1.4, 1) * 0.5
    )
    wide_def_signal = (
        min(crosses / 2.0, 1)       * 1.0
        + min(prog_carries / 2.2, 1)* 0.9
        + min(prog_dist / 50.0, 1)  * 0.7
        + min(passes_opp / 14.0, 1) * 0.5  # PA-adjusted
        + min(tackles / 2.4, 1)     * 0.6
        + min(recoveries / 5.5, 1)  * 0.5
    )
    wingback_signal = (
        min(crosses / 3.0, 1)       * 1.1
        + min(prog_carries / 3.0, 1)* 1.0
        + min(prog_dist / 65.0, 1)  * 0.8
        + min(takeons / 2.5, 1)     * 0.5
        + min(key_passes / 1.0, 1)  * 0.5
    )
    detail = (
        f"cb_signal={cb_signal:.2f}, wide_def_signal={wide_def_signal:.2f}, "
        f"wingback_signal={wingback_signal:.2f}"
    )

    # CB-FB hybrid
    if cb_signal >= 2.45 and wide_def_signal >= 2.15 and crosses >= 1.8 and prog_carries >= 1.8:
        return "CB-FB", "CB-FB", "Hybrid Defensive", detail

    # Wingback
    if wingback_signal >= 2.75 and wide_def_signal >= 2.30 and scores.get("WB", 0) >= scores.get("FB", 0) - 0.25:
        return "WB", "WB", "Wide Wing Back", detail

    role, _, _ = best_role(scores, ["CB", "FB", "WB"])
    if role == "CB":
        return "CB", "CB", "Central Defence", detail
    if role == "WB":
        return "WB", "WB", "Wide Wing Back", detail
    return "FB", "FB", "Wide Defensive", detail


# ═══════════════════════════════════════════════════════════════════════════
# Wide-defender gate  (catches WB/FB-style players misread as CM)
# ═══════════════════════════════════════════════════════════════════════════

def wide_defender_gate_v14(row: pd.Series, scores: dict[str, float]) -> tuple[bool, str, str, str, float, str]:
    crosses      = metric_value(row, ["crosses_total"])
    prog_carries = metric_value(row, ["progressive_carries"])
    prog_dist    = metric_value(row, ["progressive_carry_distance", "carry_distance"])
    takeons      = metric_value(row, ["dribbles_attempted", "contests_total"])
    passes_opp   = poss_metric(row, "passes_opposition_half_total_per90")
    key_passes   = metric_value(row, ["key_passes"])
    tackles      = metric_value(row, ["P-Adj_tackles_total", "tackles_total"])
    interceptions= metric_value(row, ["P-Adj_interceptions", "interceptions"])
    recoveries   = metric_value(row, ["P-Adj_recoveries", "recoveries"])
    duels_won    = metric_value(row, ["duels_won"])
    shots        = metric_value(row, ["shots_total"])
    xg           = metric_value(row, ["xg"])
    touches_box  = metric_value(row, ["touches_opp_box"])

    wide_lane = (
        min(crosses / 2.2, 1)       * 1.2
        + min(prog_carries / 2.4, 1)* 1.0
        + min(prog_dist / 55.0, 1)  * 0.8
        + min(takeons / 2.4, 1)     * 0.6
        + min(passes_opp / 14.0, 1) * 0.5  # PA-adjusted
        + min(key_passes / 1.0, 1)  * 0.4
    )
    def_work = (
        min(tackles / 2.2, 1)       * 1.0
        + min(interceptions / 1.2, 1)* 0.8
        + min(recoveries / 5.5, 1)  * 0.8
        + min(duels_won / 4.0, 1)   * 0.5
    )
    striker_pen = (
        min(shots / 2.5, 1)         * 0.6
        + min(xg / 0.30, 1)         * 0.7
        + min(touches_box / 4.0, 1) * 0.4
    )
    wb_score = wide_lane + def_work - striker_pen
    evidence = (
        f"wide_lane={wide_lane:.2f}, def_work={def_work:.2f}, "
        f"striker_pen={striker_pen:.2f}, wb_score={wb_score:.2f}"
    )

    def_score    = max(scores.get("FB", 0), scores.get("WB", 0))
    mid_score    = max(scores.get("DM", 0), scores.get("CM", 0), scores.get("AM", 0))
    winger_score = scores.get("W", 0)
    defender_competitive  = def_score >= mid_score - 0.35
    is_high_touch_winger  = touches_box >= 3.5 and takeons >= 4.0
    not_obvious_winger    = (winger_score <= def_score + 0.90) and not is_high_touch_winger

    evidence += (
        f", def_score={def_score:.2f}, mid_score={mid_score:.2f}, "
        f"winger_score={winger_score:.2f}, defender_competitive={defender_competitive}, "
        f"not_obvious_winger={not_obvious_winger}"
    )

    if (
        wide_lane >= 2.55
        and def_work >= 1.85
        and wb_score >= 2.75
        and defender_competitive
        and not_obvious_winger
    ):
        if wide_lane >= 3.35 or scores.get("WB", 0) >= scores.get("FB", 0) - 0.15:
            return True, "WB", "WB", "Wide Wing Back", round(clamp(0.58 + min(wb_score / 8.0, 0.16), 0.56, 0.78), 2), evidence
        return True, "FB", "FB", "Wide Defensive", round(clamp(0.56 + min(wb_score / 8.0, 0.15), 0.54, 0.76), 2), evidence

    return False, "", "", "", 0.0, evidence


# ═══════════════════════════════════════════════════════════════════════════
# Creative wide-defender rescue  (Grimaldo / Dimarco / TAA archetype)
# ═══════════════════════════════════════════════════════════════════════════

def creative_wide_defender_v14(row: pd.Series, scores: dict[str, float]) -> tuple[bool, str, str, str, float, str]:
    crosses      = metric_value(row, ["crosses_total"])
    key_passes   = metric_value(row, ["key_passes"])
    xa           = metric_value(row, ["xa"])
    passes_opp   = poss_metric(row, "passes_opposition_half_total_per90")
    prog_carries = metric_value(row, ["progressive_carries"])
    prog_dist    = metric_value(row, ["progressive_carry_distance", "carry_distance"])
    tackles      = metric_value(row, ["P-Adj_tackles_total", "tackles_total"])
    recoveries   = metric_value(row, ["P-Adj_recoveries", "recoveries"])
    interceptions= metric_value(row, ["P-Adj_interceptions", "interceptions"])
    shots        = metric_value(row, ["shots_total"])
    xg           = metric_value(row, ["xg"])

    creativity = (
        min(crosses / 3.0, 1)       * 1.1
        + min(key_passes / 1.8, 1)  * 1.0
        + min(xa / 0.22, 1)         * 1.0
        + min(passes_opp / 16.0, 1) * 0.8  # PA-adjusted
    )
    wide_prog = (
        min(prog_carries / 2.6, 1)  * 0.9
        + min(prog_dist / 55.0, 1)  * 0.8
        + min(crosses / 2.6, 1)     * 0.6
    )
    def_base = (
        min(tackles / 1.8, 1)       * 0.8
        + min(recoveries / 5.0, 1)  * 0.8
        + min(interceptions / 1.0, 1)* 0.5
    )
    striker_pen = min(shots / 2.6, 1) * 0.6 + min(xg / 0.28, 1) * 0.7
    total = creativity + wide_prog + def_base - striker_pen
    evidence = (
        f"creativity={creativity:.2f}, wide_prog={wide_prog:.2f}, "
        f"def_base={def_base:.2f}, striker_pen={striker_pen:.2f}, total={total:.2f}"
    )

    def_score    = max(scores.get("FB", 0), scores.get("WB", 0))
    mid_score    = max(scores.get("CM", 0), scores.get("AM", 0))
    winger_score = scores.get("W", 0)

    if (
        total >= 3.6
        and def_score >= mid_score - 0.25
        and def_score >= winger_score - 0.85
        and winger_score <= def_score + 0.85
    ):
        if creativity >= 2.4 or scores.get("WB", 0) >= scores.get("FB", 0) - 0.1:
            return True, "WB", "WB", "Creative Wing Back", 0.74, evidence
        return True, "FB", "FB", "Creative Fullback", 0.72, evidence

    return False, "", "", "", 0.0, evidence


# ═══════════════════════════════════════════════════════════════════════════
# Winger-style style gate  (for heuristic-source forward labels)
# ═══════════════════════════════════════════════════════════════════════════

def winger_like_from_style(
    profile_code: str | None,
    match_pos: str | None,
    match_role: str | None,
    wide_score: float,
    central_score: float,
    row: pd.Series,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    crosses      = row_number(row, ["crosses_total_per90", "crosses_total_p90"]) or 0.0
    takeons      = row_number(row, ["dribbles_attempted_per90","dribbles_attempted_p90","contests_total_per90"]) or 0.0
    takeons_won  = row_number(row, ["dribbles_completed_per90","dribbles_completed_p90","contests_won_per90"]) or 0.0
    prog_carries = row_number(row, ["progressive_carries_per90","progressive_carries_p90"]) or 0.0
    prog_dist    = row_number(row, ["progressive_carry_distance_per90","carry_distance_per90"]) or 0.0
    dribble_val  = row_number(row, ["dribble_value"]) or 0.0

    if wide_score >= 68:        reasons.append(f"wide_score={wide_score:.1f}")
    if crosses >= 1.2:          reasons.append(f"crosses_p90={crosses:.2f}")
    if takeons >= 4.5:          reasons.append(f"takeons_p90={takeons:.2f}")
    if takeons_won >= 2.0:      reasons.append(f"takeons_won_p90={takeons_won:.2f}")
    if prog_carries >= 2.5:     reasons.append(f"prog_carries_p90={prog_carries:.2f}")
    if prog_dist >= 35:         reasons.append(f"prog_carry_dist_p90={prog_dist:.1f}")
    if dribble_val >= 0.15:     reasons.append(f"dribble_value={dribble_val:.2f}")

    generic_or_wide = (
        is_generic_position(profile_code)
        or profile_code in WIDE_ATTACK_CODES
        or match_pos in {"ST", "SS", "AMC", "AM", "F"}
        or match_role in {"ST", "SS", "AMC", "AM", "F"}
    )
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


# ═══════════════════════════════════════════════════════════════════════════
# Wide-attack / central-attack evidence percentiles
# ═══════════════════════════════════════════════════════════════════════════

def percentile(value: float | None, series: pd.Series) -> float | None:
    if value is None:
        return None
    vals = numeric(series).dropna()
    if vals.empty:
        return None
    return float((vals <= value).mean() * 100.0)


def wide_attack_evidence(row: pd.Series, cohort: pd.DataFrame) -> tuple[float, list[str]]:
    metrics = {
        "crosses_total":     ["crosses_total_per90", "crosses_total_p90"],
        "crosses_accurate":  ["crosses_accurate_per90"],
        "dribbles_attempted":["dribbles_attempted_per90", "contests_total_per90"],
        "contests_won":      ["contests_won_per90"],
        "progressive_carries":["progressive_carries_per90"],
        "carry_distance":    ["progressive_carry_distance_per90", "carry_distance_per90"],
        "touches_opp_box":   ["touches_opp_box_per90"],
    }
    weights = {
        "crosses_total": 1.0, "crosses_accurate": 1.0,
        "dribbles_attempted": 1.2, "contests_won": 1.1,
        "progressive_carries": 1.2, "carry_distance": 0.8, "touches_opp_box": 0.6,
    }
    vals, used = [], []
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
        "key_passes":  ["key_passes_per90"],
        "xa":          ["xa_per90"],
        "passes_total":["passes_total_per90"],
        "passes_opp":  ["passes_opposition_half_total_per90"],
        "pass_value":  ["pass_value"],
        "assists":     ["assists_per90"],
    }
    weights = {
        "key_passes": 1.2, "xa": 1.2, "passes_total": 0.8,
        "passes_opp": 0.9, "pass_value": 1.1, "assists": 0.5,
    }
    vals, used = [], []
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


# ═══════════════════════════════════════════════════════════════════════════
# Main arbitration function
# ═══════════════════════════════════════════════════════════════════════════

def arbitrate_row(row: pd.Series, df: pd.DataFrame) -> dict[str, Any]:
    profile_code = normalize_position(row.get("profile_position_raw"))
    match_pos    = normalize_position(row.get("match_position_mode"))
    match_role   = normalize_position(row.get("match_role_mode"))

    candidates = [c for c in [profile_code, match_pos, match_role] if c]
    agreement  = source_agreement_strength(candidates)
    heuristic_source = is_heuristic_position_source(row)
    if heuristic_source:
        agreement = min(agreement, 0.45)

    result: dict[str, Any] = {
        "arbitrated_position":   None,
        "arbitrated_role_group": None,
        "arbitrated_lane":       None,
        "arbitrated_confidence": 0.0,
        "position_conflict_flag": False,
        "arbitration_reason":    "",
        "position_evidence":     "",
    }

    if not candidates:
        result.update({
            "arbitrated_lane":       "Unknown",
            "arbitrated_confidence": 0.35,
            "position_conflict_flag": True,
            "arbitration_reason":    "No usable profile or match position found.",
            "position_evidence":     "none",
        })
        return result

    # Possession-corrected spatial data
    adj_x = compute_adj_avg_x(row)
    y     = row_number(row, SPATIAL_Y_COLUMNS)
    spatial_base = profile_code or match_role or match_pos
    spatial_pos, spatial_group, spatial_lane, spatial_strength, spatial_source = (
        spatial_role_from_adj_xy(spatial_base, adj_x, y)
    )

    # Role scores (possession-aware)
    scores = role_scores_v14(row)
    source_family = family_hint_from_sources(profile_code, match_pos, match_role)

    # Evidence cohort for attacking percentile calculations
    attacking_groups = {"AM-C", "AM-W", "ST", "ST-SS", "WM"}
    attack_cohort = df.loc[df["_base_group"].isin(attacking_groups)].copy()
    if attack_cohort.empty:
        attack_cohort = df
    wide_score,    wide_used    = wide_attack_evidence(row, attack_cohort)
    central_score, central_used = central_attack_evidence(row, attack_cohort)
    gap_strength = score_gap_strength(wide_score, central_score)

    raw_groups = {position_group(c) for c in candidates if c}
    conflict   = len({g for g in raw_groups if g}) > 1

    evidence_bits = [
        f"sources={','.join(candidates)}",
        f"agreement={agreement:.2f}",
        f"wide_score={wide_score:.1f}",
        f"central_score={central_score:.1f}",
        f"adj_avg_x={adj_x:.1f}" if adj_x is not None else "adj_avg_x=N/A",
    ]
    if spatial_pos:
        evidence_bits.append(f"spatial={spatial_pos}@adj_x={adj_x:.1f},y={y:.1f},strength={spatial_strength:.2f}")
    if heuristic_source:
        evidence_bits.append("upstream_position_source=heuristic")

    # ── 1. Zone-distribution rescue (possession-aware) ──────────────────
    zone_result = zone_distribution_rescue_v14(
        row, scores, source_family, adj_x, y, heuristic_source
    )
    if zone_result is not None:
        return zone_result

    # ── 2. V12-style spatial coherence override ─────────────────────────
    if spatial_pos and spatial_strength >= 0.62:
        def_score = max(scores.get("CB", 0), scores.get("FB", 0), scores.get("WB", 0))
        mid_score = max(scores.get("DM", 0), scores.get("CM", 0), scores.get("AM", 0))

        spatial_is_coherent = True

        # Do not let CB build-up positions become FB purely from lateral centroid
        if source_family == "DEF" and spatial_group in {"FB", "WB"}:
            spatial_is_coherent = max(scores.get("FB", 0), scores.get("WB", 0)) >= scores.get("CB", 0) - 0.35

        # Do not let a midfielder become wide purely from y unless wide evidence is strong
        if source_family == "MID" and spatial_group in {"WM", "AM-W"}:
            spatial_is_coherent = (
                scores.get("W", 0)  >= mid_score + 0.10
                or scores.get("WM", 0) >= mid_score - 0.20
                or max(scores.get("FB", 0), scores.get("WB", 0)) >= mid_score - 0.25
            )

        # Do not let attackers become central purely by coordinate if W score dominates
        if source_family == "ATT" and spatial_group in {"ST", "ST-SS"}:
            winger_dominant = (
                scores.get("W", 0) >= scores.get("ST", 0) + 0.55
                and scores.get("W", 0) >= scores.get("SS", 0) + 0.55
                and scores.get("W", 0) >= 3.2
            )
            if winger_dominant:
                spatial_is_coherent = False

        if spatial_is_coherent:
            conf = confidence_from_evidence(
                base=0.58, agreement=agreement, evidence_gap=0.45,
                spatial_strength=spatial_strength,
                profile_bonus=0.03 if profile_code else 0.0,
                conflict_penalty=0.02 if heuristic_source else 0.0,
            )
            return {
                "arbitrated_position":   spatial_pos,
                "arbitrated_role_group": spatial_group,
                "arbitrated_lane":       spatial_lane,
                "arbitrated_confidence": conf,
                "position_conflict_flag": bool(heuristic_source),
                "arbitration_reason":    f"Possession-adjusted spatial evidence coherent with role scores ({spatial_source}).",
                "position_evidence":     "; ".join(evidence_bits),
            }

    # ── 3. Defender family ───────────────────────────────────────────────
    if source_family == "DEF" or profile_code in {"D"} or match_pos in {"D"} or match_role in {"D"}:
        role, top, second = best_role(scores, ["CB", "FB", "WB"])
        gap = top - second
        pos, group, lane, detail = defender_refinement_v14(row, scores)
        hybrid_penalty = 0.04 if group == "CB-FB" else 0.0
        conf = round(clamp(
            0.52 + min(max(gap, 0) / 2.5, 0.18) + min(top / 5.0, 0.12)
            + (0.05 if not heuristic_source else 0) - hybrid_penalty, 0.45, 0.86
        ), 2)
        if top >= 1.4:
            return {
                "arbitrated_position":   pos,
                "arbitrated_role_group": group,
                "arbitrated_lane":       lane,
                "arbitrated_confidence": conf,
                "position_conflict_flag": bool(heuristic_source or gap < 0.6 or group == "CB-FB"),
                "arbitration_reason":    f"Defender refinement: {pos}, top={role} score={top:.2f}, next={second:.2f}.",
                "position_evidence":     f"v14_scores={scores}; {detail}; source_family={source_family}",
            }

    # ── 4. Wide-defender gate (WB/FB disguised as CM) ───────────────────
    wd_hit, wd_pos, wd_group, wd_lane, wd_conf, wd_evidence = wide_defender_gate_v14(row, scores)
    if wd_hit and source_family in {"MID", "DEF"}:
        return {
            "arbitrated_position":   wd_pos,
            "arbitrated_role_group": wd_group,
            "arbitrated_lane":       wd_lane,
            "arbitrated_confidence": wd_conf,
            "position_conflict_flag": True,
            "arbitration_reason":    f"Wide-defender gate: {wd_pos} — player profiled as wingback/fullback, not CM.",
            "position_evidence":     f"v14_scores={scores}; {wd_evidence}; source_family={source_family}",
        }

    # ── 5. Creative wide-defender rescue (Grimaldo / TAA) ───────────────
    cw_hit, cw_pos, cw_group, cw_lane, cw_conf, cw_evidence = creative_wide_defender_v14(row, scores)
    if cw_hit and source_family in {"MID", "DEF"}:
        return {
            "arbitrated_position":   cw_pos,
            "arbitrated_role_group": cw_group,
            "arbitrated_lane":       cw_lane,
            "arbitrated_confidence": cw_conf,
            "position_conflict_flag": True,
            "arbitration_reason":    f"Creative wide-defender rescue: {cw_pos}.",
            "position_evidence":     f"v14_scores={scores}; {cw_evidence}; source_family={source_family}",
        }

    # ── 6. Midfield family ───────────────────────────────────────────────
    if source_family == "MID" or profile_code in {"M"} or match_pos in {"M"} or match_role in {"M"}:
        mid_role, mid_top, mid_second = best_role(scores, ["DM", "CM", "AM", "WM"])

        # WM gate: if WM score is competitive and adj_x is in WM band
        wm_eligible = (
            scores.get("WM", 0) >= mid_top - 0.45
            and adj_x is not None
            and ADJ_X_WM_LO <= adj_x <= ADJ_X_WM_HI
        )

        # Wide-dominant check (e.g. inverted wingers mislabelled as MID)
        wide_dominant = scores["W"] >= 3.7 and scores["W"] >= mid_top + 0.45

        if wm_eligible and not wide_dominant:
            role, top, second = "WM", scores.get("WM", 0), mid_top
            pos, group, lane = "WM", "WM", "Wide Midfielder"
        elif wide_dominant:
            role, top, second = "W", scores["W"], mid_top
            pos, group, lane = "W", "AM-W", "Wide Forward"
        else:
            role, top, second = mid_role, mid_top, mid_second
            if role == "DM":   pos, group, lane = "DM",  "DM",   "Defensive Midfield"
            elif role == "CM": pos, group, lane = "CM",  "CM",   "Central Midfield"
            elif role == "WM": pos, group, lane = "WM",  "WM",   "Wide Midfielder"
            else:              pos, group, lane = "AMC", "AM-C", "Central Attacking Midfield"

        gap = top - second
        conf = round(clamp(0.50 + min(max(gap, 0) / 2.2, 0.17) + min(top / 5.5, 0.13), 0.46, 0.84), 2)
        return {
            "arbitrated_position":   pos,
            "arbitrated_role_group": group,
            "arbitrated_lane":       lane,
            "arbitrated_confidence": conf,
            "position_conflict_flag": bool(heuristic_source or gap < 0.45),
            "arbitration_reason":    f"Midfield splitter: {pos}, top score={top:.2f}, next={second:.2f}.",
            "position_evidence":     f"v14_scores={scores}; source_family={source_family}; wm_eligible={wm_eligible}",
        }

    # ── 7. Heuristic winger style gate ──────────────────────────────────
    winger_style, winger_reasons = winger_like_from_style(
        profile_code, match_pos, match_role, wide_score, central_score, row
    )
    if heuristic_source and winger_style:
        side_hint = spatial_pos if spatial_pos in {"RW", "LW"} else None
        pos = side_hint or "W"
        lane = "Wide Forward" if not side_hint else ("Right Wide Forward" if side_hint == "RW" else "Left Wide Forward")
        conf = confidence_from_evidence(
            base=0.56, agreement=agreement,
            evidence_gap=max(gap_strength, 0.35),
            spatial_strength=spatial_strength if spatial_group == "AM-W" else 0.0,
            conflict_penalty=0.02,
        )
        result.update({
            "arbitrated_position":   pos,
            "arbitrated_role_group": "AM-W",
            "arbitrated_lane":       lane,
            "arbitrated_confidence": conf,
            "position_conflict_flag": True,
            "arbitration_reason":    (
                "Heuristic upstream label; style profile is strongly winger-like. "
                f"Signals: {', '.join(winger_reasons)}."
            ),
            "position_evidence":     "; ".join(evidence_bits),
        })
        return result

    # ── 8. Attacker family ───────────────────────────────────────────────
    if source_family == "ATT" or profile_code in {"F", "FW"} or match_pos in {"F", "FW"}:
        role, top, second = best_role(scores, ["W", "ST", "SS"])

        # Promote SS when close to ST and not a dominant scorer
        if role == "ST" and scores["SS"] >= scores["ST"] - 0.25 and scores["SS"] >= 2.55 and scores["ST"] <= 3.05:
            role, top, second = "SS", scores["SS"], scores["ST"]

        # W wins if it clearly dominates
        if scores["W"] >= 3.2 and scores["W"] >= scores["ST"] + 0.6:
            role, top, second = "W", scores["W"], max(scores["ST"], scores["SS"])

        gap = top - second
        if role == "W":   pos, group, lane = "W",  "AM-W",  "Wide Forward"
        elif role == "ST": pos, group, lane = "ST", "ST",    "Central Forward"
        else:              pos, group, lane = "SS", "ST-SS", "Central Support Forward"

        conf = round(clamp(0.50 + min(max(gap, 0) / 2.2, 0.17) + min(top / 5.5, 0.13), 0.46, 0.84), 2)
        return {
            "arbitrated_position":   pos,
            "arbitrated_role_group": group,
            "arbitrated_lane":       lane,
            "arbitrated_confidence": conf,
            "position_conflict_flag": bool(heuristic_source or gap < 0.45),
            "arbitration_reason":    f"Attacker splitter: {pos}, top score={top:.2f}, next={second:.2f}.",
            "position_evidence":     f"v14_scores={scores}; source_family={source_family}",
        }

    # ── 9. Spatial generic label ─────────────────────────────────────────
    if spatial_pos and (
        profile_code in GENERIC_POSITION_CODES
        or match_pos in GENERIC_POSITION_CODES
        or agreement < 0.6
        or heuristic_source
    ):
        conf = confidence_from_evidence(
            base=0.52, agreement=agreement, evidence_gap=gap_strength,
            spatial_strength=spatial_strength,
            profile_bonus=0.03 if profile_code else 0.0,
            conflict_penalty=0.06 if conflict else 0.0,
        )
        result.update({
            "arbitrated_position":   spatial_pos,
            "arbitrated_role_group": spatial_group,
            "arbitrated_lane":       spatial_lane,
            "arbitrated_confidence": conf,
            "position_conflict_flag": bool(conflict),
            "arbitration_reason":    f"Possession-adjusted spatial evidence resolved generic/conflicting labels.",
            "position_evidence":     "; ".join(evidence_bits),
        })
        return result

    # ── 10. Explicit wide attacker ───────────────────────────────────────
    if profile_code in {"RW", "LW", "AMR", "AML"}:
        side = position_side(profile_code)
        has_st_conflict = any(c in {"ST", "AMC", "AM"} for c in [match_pos, match_role] if c)
        pos  = f"{profile_code}/ST" if (has_st_conflict and wide_score >= 45) else profile_code
        conf = confidence_from_evidence(
            base=0.62 if not has_st_conflict else 0.58,
            agreement=agreement, evidence_gap=gap_strength,
            spatial_strength=spatial_strength if spatial_group == "AM-W" else 0.0,
            profile_bonus=0.07, conflict_penalty=0.05 if has_st_conflict else 0.0,
        )
        result.update({
            "arbitrated_position":   pos,
            "arbitrated_role_group": "AM-W",
            "arbitrated_lane":       f"{side} Wide" if side else "Wide",
            "arbitrated_confidence": conf,
            "position_conflict_flag": bool(has_st_conflict),
            "arbitration_reason":    f"Profile is explicit wide attacker ({profile_code}).",
            "position_evidence":     "; ".join(evidence_bits),
        })
        return result

    # ── 11. Explicit wide fullback ───────────────────────────────────────
    if profile_code in {"RB", "LB", "RWB", "LWB"}:
        side  = position_side(profile_code)
        group = position_group(profile_code)
        has_conflict = any(
            position_group(c) not in {group, None}
            for c in [match_pos, match_role] if c
        )
        conf = confidence_from_evidence(
            base=0.60, agreement=agreement,
            spatial_strength=spatial_strength if spatial_group in {"FB", "WB"} else 0.0,
            profile_bonus=0.07, conflict_penalty=0.05 if has_conflict else 0.0,
        )
        result.update({
            "arbitrated_position":   profile_code,
            "arbitrated_role_group": group,
            "arbitrated_lane":       f"{side} Wide Defensive" if side else "Wide Defensive",
            "arbitrated_confidence": conf,
            "position_conflict_flag": bool(has_conflict),
            "arbitration_reason":    f"Profile is explicit wide defensive ({profile_code}).",
            "position_evidence":     "; ".join(evidence_bits),
        })
        return result

    # ── 12. Default ──────────────────────────────────────────────────────
    final_code = profile_code or match_pos or match_role
    conf = confidence_from_evidence(
        base=0.46, agreement=agreement, evidence_gap=gap_strength,
        spatial_strength=spatial_strength if (
            spatial_pos and position_group(spatial_pos) == position_group(final_code)
        ) else 0.0,
        profile_bonus=0.05 if profile_code else 0.0,
        conflict_penalty=0.05 if conflict else 0.0,
    )
    result.update({
        "arbitrated_position":   final_code,
        "arbitrated_role_group": position_group(final_code),
        "arbitrated_lane":       position_side(final_code) or "Central/Unspecified",
        "arbitrated_confidence": conf,
        "position_conflict_flag": bool(conflict),
        "arbitration_reason":    "Default: profile position or weighted match-position mode.",
        "position_evidence":     "; ".join(evidence_bits),
    })
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Dataset preparation
# ═══════════════════════════════════════════════════════════════════════════

def prepare_input(df: pd.DataFrame) -> pd.DataFrame:
    work = ensure_per90(df)

    # Compute possession-corrected avg_x for every row
    work["team_poss_pct"] = 100.0 - numeric(work.get("oppo_poss_pct", pd.Series(dtype=float)))
    work["adj_avg_x"] = work.apply(compute_adj_avg_x, axis=1)
    work["adj_zone"]  = work["adj_avg_x"].apply(adj_vertical_zone)

    profile_col = find_first_col(work, PROFILE_POSITION_COLUMNS)
    if profile_col:
        work["profile_position_raw"] = work[profile_col]
    else:
        work["profile_position_raw"] = np.nan

    match_pos_cols  = [c for c in MATCH_POSITION_COLUMNS if c in work.columns and c != profile_col]
    match_role_cols = [c for c in MATCH_ROLE_COLUMNS if c in work.columns]

    is_match_level = (
        "player_id" in work.columns
        and "season" in work.columns
        and ("event_id" in work.columns or "match_id" in work.columns)
    )

    if is_match_level:
        keys = ["player_id", "season"] + (["league"] if "league" in work.columns else [])
        profile_summaries = []
        for _, group in work.groupby(keys, dropna=False):
            idxs = group.index
            prof_mode, prof_sum   = weighted_position_summary(group, ["profile_position_raw"], MINUTES_COL)
            pos_mode,  pos_sum    = weighted_position_summary(group, match_pos_cols,            MINUTES_COL)
            role_mode, role_sum   = weighted_position_summary(group, match_role_cols,           MINUTES_COL)
            minutes = numeric(group[MINUTES_COL]) if MINUTES_COL in group.columns else pd.Series([1.0]*len(group), index=group.index)
            x_col = find_first_col(group, ["avg_x", "average_x"])
            y_col = find_first_col(group, ["avg_y", "average_y"])
            if x_col and y_col:
                xv = numeric(group[x_col]); yv = numeric(group[y_col])
                valid = xv.notna() & yv.notna() & minutes.notna() & (minutes > 0)
                if valid.any():
                    avg_x_ = float(np.average(xv[valid], weights=minutes[valid]))
                    avg_y_ = float(np.average(yv[valid], weights=minutes[valid]))
                    spatial_n = int(valid.sum())
                else:
                    avg_x_, avg_y_, spatial_n = np.nan, np.nan, 0
            else:
                avg_x_, avg_y_, spatial_n = np.nan, np.nan, 0
            for idx in idxs:
                profile_summaries.append((idx, prof_mode, prof_sum, pos_mode, pos_sum, role_mode, role_sum, avg_x_, avg_y_, spatial_n))

        for col in ["profile_position_mode","profile_positions_played","match_position_mode",
                    "match_positions_played","match_role_mode","match_roles_played",
                    "avg_x","avg_y","spatial_matches_used"]:
            if col not in work.columns:
                work[col] = np.nan
        for idx, pm, ps, pom, pos_s, rm, rs, ax, ay, sn in profile_summaries:
            work.at[idx, "profile_position_mode"]  = pm
            work.at[idx, "profile_positions_played"]= ps
            work.at[idx, "match_position_mode"]     = pom
            work.at[idx, "match_positions_played"]  = pos_s
            work.at[idx, "match_role_mode"]         = rm
            work.at[idx, "match_roles_played"]      = rs
            if not math.isnan(ax) if isinstance(ax, float) else True:
                work.at[idx, "avg_x"] = ax
            if not math.isnan(ay) if isinstance(ay, float) else True:
                work.at[idx, "avg_y"] = ay
            work.at[idx, "spatial_matches_used"] = sn
        work["profile_position_raw"] = work["profile_position_mode"]
        # Recompute adj_avg_x after match-level spatial averaging
        work["adj_avg_x"] = work.apply(compute_adj_avg_x, axis=1)
    else:
        if "match_position_mode" not in work.columns:
            src = find_first_col(work, ["primary_role_position", "role_position", "player_position", "base_position"])
            work["match_position_mode"] = work[src] if src else np.nan
        if "match_role_mode" not in work.columns:
            src = find_first_col(work, ["primary_role_position", "role_position"])
            work["match_role_mode"] = work[src] if src else np.nan
        for col in ["profile_positions_played", "match_positions_played", "match_roles_played"]:
            if col not in work.columns:
                work[col] = work.get({"profile_positions_played": "profile_position_raw",
                                       "match_positions_played":  "match_position_mode",
                                       "match_roles_played":      "match_role_mode"}[col], np.nan)

    # Base group for evidence cohorts
    base = []
    for _, row in work.iterrows():
        code = (
            normalize_position(row.get("profile_position_raw"))
            or normalize_position(row.get("match_position_mode"))
            or normalize_position(row.get("match_role_mode"))
        )
        base.append(position_group(code))
    work["_base_group"] = base
    return work


# ═══════════════════════════════════════════════════════════════════════════
# Dataset-level arbitration runner
# ═══════════════════════════════════════════════════════════════════════════

def arbitrate_dataset(
    df: pd.DataFrame,
    season: str | None,
    league: str | None,
    player_id: int | None,
    min_minutes: float | None,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
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

    json_rows: list[dict[str, Any]] = []

    for idx in target_index:
        row = work.loc[idx]
        arb = arbitrate_row(row, work)

        # CB hard gate
        original_group = arb.get("arbitrated_role_group")
        final_group    = enforce_cb_hard_gate(row, original_group)
        if final_group == "CB" and original_group != "CB":
            arb["arbitrated_role_group"] = "CB"
            arb["arbitrated_position"]   = "CB"
            arb["arbitrated_lane"]       = "Central Defence"
            arb["arbitrated_confidence"] = max(float(arb.get("arbitrated_confidence", 0.70)), 0.70)
            current_reason = str(arb.get("arbitration_reason", "")).strip()
            arb["arbitration_reason"] = (
                f"{current_reason} | CB hard gate override (insufficient genuine wide deployment)"
            ).strip(" | ")

        for key, value in arb.items():
            work.at[idx, key] = value

        json_rows.append({
            "player_id":   row.get("player_id"),
            "player_name": row.get("player_name") if pd.notna(row.get("player_name")) else row.get("profile_name"),
            "season":      row.get("season"),
            "league":      row.get("league"),
            "adj_avg_x":   row.get("adj_avg_x"),
            "team_poss_pct": row.get("team_poss_pct"),
            **arb,
        })

    work = work.drop(columns=["_base_group"], errors="ignore")
    return work, json_rows


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Arbitrate player positions using possession-corrected spatial data (V14)."
    )
    ap.add_argument("--input",  "-i", default="player_season_totals.csv")
    ap.add_argument("--output", "-o", default="player_positions_arbitrated.csv")
    ap.add_argument("--season", "-s", default=None)
    ap.add_argument("--league", "-l", default=None)
    ap.add_argument("--player-id", "-p", type=int, default=None)
    ap.add_argument("--min-minutes", type=float, default=None)
    ap.add_argument("--format", choices=["csv", "json", "both"], default="csv")
    ap.add_argument("--position-context", default=None)
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    df = apply_position_context(df, args.position_context)
    out, records = arbitrate_dataset(df, args.season, args.league, args.player_id, args.min_minutes)

    output_path = Path(args.output)
    written: list[Path] = []

    if args.format in {"csv", "both"}:
        csv_path = output_path if output_path.suffix.lower() == ".csv" else output_path.with_suffix(".csv")
        out.to_csv(csv_path, index=False)
        written.append(csv_path)

    if args.format in {"json", "both"}:
        json_path = output_path if output_path.suffix.lower() == ".json" else output_path.with_suffix(".json")
        payload = records[0] if (args.player_id is not None and len(records) == 1) else records
        json_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
        )
        written.append(json_path)

    print("\nV14 Arbitration complete.")
    for p in written:
        print(f"Output: {p}")

    print("\nSample arbitrations (first 10):")
    for r in records[:10]:
        print(
            f"  {r.get('player_name')} ({r.get('player_id')}): "
            f"adj_x={r.get('adj_avg_x','N/A')!r}  poss={r.get('team_poss_pct','N/A')!r}  "
            f"→ {r.get('arbitrated_role_group')} / {r.get('arbitrated_position')} "
            f"[conf={r.get('arbitrated_confidence')}, conflict={r.get('position_conflict_flag')}]"
        )


if __name__ == "__main__":
    main()