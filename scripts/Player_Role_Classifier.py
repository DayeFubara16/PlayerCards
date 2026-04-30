"""
Player_Role_Classifier_v2.py
────────────────────────────
Arbitration-aware functional role classifier.

V3 updates for Position_Arbitrator_v13/v13_fb_guarded:
- maps RB/LB to FB and RWB/LWB to WB
- supports CB-FB hybrid defender family
- preserves spatial distribution columns from the arbitrator
- avoids unknown-family warnings for side-specific arbitrated positions

This version is designed to run after Position_Arbitrator.py and prefers:

  arbitrated_role_group
  role_position_refined
  primary_role_position
  role_position

It maps refined/arbitrated labels such as:
  AM-W, AM-C, AM-HYB, ST-W, ST-SS, RB, LB, RWB, LWB, FB, WB, CB-FB

into role-model families while preserving the refined cohort label in output.

Usage
-----
  python Player_Role_Classifier_v2.py --input player_season_totals_arbitrated.csv --output player_roles.csv --season 2025-26 --min-minutes 450
  python Player_Role_Classifier_v2.py --input player_season_totals_arbitrated.csv --player-id 934386 --season 2025-26 --format both
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


ROLE_COLUMN_PRIORITY = [
    "arbitrated_role_group",
    "role_position_refined",
    "primary_role_position",
    "role_position",
]


def choose_role_column(df: pd.DataFrame, forced: str | None = None) -> str:
    if forced:
        if forced not in df.columns:
            raise ValueError(f"Requested role column '{forced}' not found.")
        return forced
    for col in ROLE_COLUMN_PRIORITY:
        if col in df.columns:
            return col
    raise ValueError(
        "No usable role column found. Expected one of: "
        + ", ".join(ROLE_COLUMN_PRIORITY)
    )


def normalize_model_family(value: Any) -> str | None:
    """Map refined/arbitrated labels to the role model family."""
    if pd.isna(value):
        return None
    text = str(value).strip().upper()
    if not text:
        return None

    text = text.replace("_", "-").replace(" ", "-")
    text = re.sub(r"-+", "-", text)

    # Goalkeeper
    if text in {"GK", "G", "GOALKEEPER"}:
        return "GK"

    # Centre-back / hybrid defender labels
    if text in {"CB", "RCB", "LCB"}:
        return "CB"
    if text in {"CB-FB", "FB-CB", "WCB", "WIDE-CB", "RB-CB", "LB-CB"}:
        return "CB-FB"

    # Full-back labels. V13 often outputs side-specific RB/LB while the
    # role family remains FB. This makes forced --role-column arbitrated_position safe.
    if text in {"RB", "LB", "FB", "FULLBACK", "FULL-BACK", "RIGHT-BACK", "LEFT-BACK"}:
        return "FB"
    if text.startswith("FB"):
        return "FB"

    # Wing-back labels. Same safety for RWB/LWB.
    if text in {"RWB", "LWB", "WB", "WINGBACK", "WING-BACK"}:
        return "WB"
    if text.startswith("WB"):
        return "WB"

    # Refined attacking midfield / wide labels
    if text in {"AM-W", "AMR", "AML", "RW", "LW", "W", "AM-W/C"}:
        return "W"
    if text in {"AM-C", "AMC", "AM", "AM-C/W"}:
        return "AM"
    if text in {"AM-HYB", "AM-W-C", "AM-C-W"}:
        return "AM"

    # Striker refinements
    if text in {"ST-W", "ST-SS", "SS", "CF", "F", "FW", "ST"}:
        return "ST"

    # Wide midfield: current role model does not have a separate WM family.
    # Route to W because the available scorecards are wide-role scorecards.
    if text in {"WM", "RM", "LM", "M-R", "M-L"}:
        return "W"

    # Standard midfield families
    if text in {"DM", "CM"}:
        return text

    return text

def clean_filename(text: str | None, fallback: str = "Player") -> str:
    text = str(text or fallback)
    text = re.sub(r"[^\w\s.-]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "_", text.strip())
    return text or fallback


def to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def metric_col(df: pd.DataFrame, names: list[str]) -> str | None:
    lower = {c.lower(): c for c in df.columns}
    candidates = []
    for n in names:
        candidates.extend([f"{n}_per90", f"{n}_p90", n])
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def pct_rank(series: pd.Series, value: float, higher_is_better: bool = True) -> float | None:
    vals = to_num(series).dropna()
    if vals.empty or value is None or pd.isna(value):
        return None
    pct = float((vals <= value).mean() * 100.0)
    return pct if higher_is_better else 100.0 - pct


def weighted_pct(df: pd.DataFrame, row: pd.Series, metrics: dict[str, tuple[list[str], float]]) -> tuple[float | None, list[dict[str, Any]]]:
    values = []
    details = []
    for label, (aliases, weight) in metrics.items():
        col = metric_col(df, aliases)
        if not col:
            continue
        try:
            value = float(row.get(col))
        except Exception:
            continue
        if math.isnan(value):
            continue

        lower_better = label.endswith("_inverse")
        pct = pct_rank(df[col], value, higher_is_better=not lower_better)
        if pct is None:
            continue

        values.append((pct, weight))
        details.append({
            "metric": label.replace("_inverse", ""),
            "column": col,
            "value": round(value, 6),
            "percentile": round(pct, 2),
            "weight": weight,
            "direction": "lower_is_better" if lower_better else "higher_is_better",
        })

    if not values:
        return None, details
    score = sum(v * w for v, w in values) / sum(w for _, w in values)
    return float(score), details


# Role scorecards.
# These are intentionally measurable/proxy-only and keep unmeasured guide traits visible.
ROLE_MODEL: dict[str, dict[str, dict[str, Any]]] = {
    "ST": {
        "Poacher": {
            "measured": {
                "goals": (["goals"], 1.2),
                "xg": (["xg"], 1.2),
                "shots_on_target": (["shots_on_target"], 1.0),
                "shots": (["shots_total", "shots"], 0.9),
                "touches_opp_box": (["touches_opp_box"], 1.1),
            },
            "partial": {
                "offsides": (["offsides"], 0.3),
                "xgot": (["xgot"], 0.6),
            },
            "unmeasured": ["anticipation", "composure", "concentration", "off the ball", "acceleration"],
        },
        "Centre Forward": {
            "measured": {
                "goals": (["goals"], 1.0),
                "xg": (["xg"], 1.1),
                "shots": (["shots_total", "shots"], 0.9),
                "touches_opp_box": (["touches_opp_box"], 0.9),
                "aerial_duels_won": (["aerial_duels_won"], 0.5),
                "key_passes": (["key_passes"], 0.3),
            },
            "partial": {
                "height": (["height_cm"], 0.2),
                "fouls_drawn": (["fouls_drawn"], 0.3),
            },
            "unmeasured": ["finishing technique", "first touch", "composure", "off the ball", "strength"],
        },
        "Target Forward": {
            "measured": {
                "aerial_duels_won": (["aerial_duels_won"], 1.2),
                "aerial_duels_total": (["aerial_duels_total"], 0.9),
                "duels_won": (["duels_won"], 0.8),
                "fouls_drawn": (["fouls_drawn"], 0.7),
                "xg": (["xg"], 0.6),
            },
            "partial": {"height": (["height_cm"], 0.5)},
            "unmeasured": ["strength", "jumping", "balance", "bravery", "hold-up technique"],
        },
        "Deep-Lying Forward": {
            "measured": {
                "key_passes": (["key_passes"], 1.0),
                "xa": (["xa"], 0.9),
                "assists": (["assists"], 0.8),
                "passes_total": (["passes_total"], 0.7),
                "pass_value": (["pass_value"], 0.8),
                "touches": (["touches"], 0.6),
            },
            "partial": {},
            "unmeasured": ["first touch", "technique", "composure", "decisions", "vision"],
        },
        "False Nine": {
            "measured": {
                "key_passes": (["key_passes"], 1.0),
                "xa": (["xa"], 0.9),
                "pass_value": (["pass_value"], 0.9),
                "passes_total": (["passes_total"], 0.8),
                "dribbles_attempted": (["dribbles_attempted", "contests_total"], 0.7),
                "progressive_carries": (["progressive_carries"], 0.6),
            },
            "partial": {},
            "unmeasured": ["first touch", "technique", "composure", "decisions", "vision", "off-ball movement"],
        },
        "Channel Forward": {
            "measured": {
                "progressive_carries": (["progressive_carries"], 1.0),
                "dribbles_attempted": (["dribbles_attempted", "contests_total"], 0.9),
                "shots": (["shots_total", "shots"], 0.8),
                "xg": (["xg"], 0.8),
                "touches_opp_box": (["touches_opp_box"], 0.7),
                "fouls_drawn": (["fouls_drawn"], 0.4),
            },
            "partial": {"distance_sprinting": (["distance_sprinting_km"], 0.3)},
            "unmeasured": ["work rate", "acceleration", "pace", "stamina", "channel movement"],
        },
    },
    "W": {
        "Winger": {
            "measured": {
                "crosses_total": (["crosses_total"], 1.1),
                "crosses_accurate": (["crosses_accurate"], 1.1),
                "dribbles_attempted": (["dribbles_attempted", "contests_total"], 0.9),
                "contests_won": (["contests_won"], 0.8),
                "key_passes": (["key_passes"], 0.7),
                "xa": (["xa"], 0.7),
                "progressive_carries": (["progressive_carries"], 0.8),
            },
            "partial": {"distance_sprinting": (["distance_sprinting_km"], 0.3)},
            "unmeasured": ["pace", "acceleration", "agility", "flair", "off the ball"],
        },
        "Inside Winger": {
            "measured": {
                "dribbles_attempted": (["dribbles_attempted", "contests_total"], 1.0),
                "contests_won": (["contests_won"], 0.9),
                "progressive_carries": (["progressive_carries"], 0.9),
                "shots": (["shots_total", "shots"], 0.7),
                "xg": (["xg"], 0.6),
                "key_passes": (["key_passes"], 0.7),
                "xa": (["xa"], 0.6),
            },
            "partial": {},
            "unmeasured": ["technique", "composure", "teamwork", "acceleration", "agility"],
        },
        "Playmaking Winger": {
            "measured": {
                "key_passes": (["key_passes"], 1.1),
                "xa": (["xa"], 1.2),
                "pass_value": (["pass_value"], 1.0),
                "crosses_accurate": (["crosses_accurate"], 0.8),
                "dribbles_attempted": (["dribbles_attempted", "contests_total"], 0.6),
                "passes_opposition_half": (["passes_opposition_half_total"], 0.6),
            },
            "partial": {},
            "unmeasured": ["vision", "technique", "decisions", "composure", "flair"],
        },
        "Wide Forward": {
            "measured": {
                "shots": (["shots_total", "shots"], 1.0),
                "xg": (["xg"], 1.0),
                "touches_opp_box": (["touches_opp_box"], 1.0),
                "dribbles_attempted": (["dribbles_attempted", "contests_total"], 0.7),
                "contests_won": (["contests_won"], 0.6),
                "progressive_carries": (["progressive_carries"], 0.5),
            },
            "partial": {"distance_sprinting": (["distance_sprinting_km"], 0.3)},
            "unmeasured": ["off the ball", "pace", "acceleration", "agility", "anticipation"],
        },
        "Inside Forward": {
            "measured": {
                "shots": (["shots_total", "shots"], 1.1),
                "xg": (["xg"], 1.1),
                "goals": (["goals"], 0.8),
                "touches_opp_box": (["touches_opp_box"], 0.9),
                "dribbles_attempted": (["dribbles_attempted", "contests_total"], 0.7),
                "contests_won": (["contests_won"], 0.6),
            },
            "partial": {},
            "unmeasured": ["composure", "flair", "vision", "off the ball", "pace"],
        },
        "Tracking Winger": {
            "measured": {
                "recoveries": (["recoveries"], 1.0),
                "tackles_won": (["tackles_won"], 0.9),
                "interceptions": (["interceptions"], 0.7),
                "crosses_total": (["crosses_total"], 0.4),
            },
            "partial": {"distance_running": (["distance_running_km"], 0.3)},
            "unmeasured": ["marking", "work rate", "stamina"],
        },
    },
    "AM": {
        "Attacking Midfielder": {
            "measured": {
                "key_passes": (["key_passes"], 1.0),
                "xa": (["xa"], 1.0),
                "pass_value": (["pass_value"], 0.9),
                "shots": (["shots_total", "shots"], 0.6),
                "xg": (["xg"], 0.6),
                "dribbles_attempted": (["dribbles_attempted", "contests_total"], 0.6),
                "touches_opp_box": (["touches_opp_box"], 0.6),
            },
            "partial": {},
            "unmeasured": ["first touch", "technique", "composure", "flair", "off the ball"],
        },
        "Free Role": {
            "measured": {
                "key_passes": (["key_passes"], 1.1),
                "xa": (["xa"], 1.0),
                "shots": (["shots_total", "shots"], 0.7),
                "xg": (["xg"], 0.6),
                "dribbles_attempted": (["dribbles_attempted", "contests_total"], 0.8),
                "contests_won": (["contests_won"], 0.7),
                "pass_value": (["pass_value"], 0.9),
            },
            "partial": {},
            "unmeasured": ["flair", "vision", "composure", "off the ball", "creative freedom"],
        },
        "Second Striker": {
            "measured": {
                "shots": (["shots_total", "shots"], 1.0),
                "xg": (["xg"], 1.0),
                "goals": (["goals"], 0.8),
                "touches_opp_box": (["touches_opp_box"], 0.8),
                "assists": (["assists"], 0.5),
                "key_passes": (["key_passes"], 0.5),
            },
            "partial": {},
            "unmeasured": ["anticipation", "composure", "off the ball", "acceleration", "first touch"],
        },
        "Channel Midfielder": {
            "measured": {
                "crosses_accurate": (["crosses_accurate"], 0.8),
                "key_passes": (["key_passes"], 0.8),
                "pass_value": (["pass_value"], 0.8),
                "progressive_carries": (["progressive_carries"], 0.8),
                "touches_opp_box": (["touches_opp_box"], 0.6),
                "recoveries": (["recoveries"], 0.4),
            },
            "partial": {"distance_hsr": (["distance_high_speed_running_km"], 0.3)},
            "unmeasured": ["work rate", "acceleration", "off the ball", "vision", "decisions"],
        },
        "Tracking Attacking Midfielder": {
            "measured": {
                "recoveries": (["recoveries"], 1.0),
                "tackles_won": (["tackles_won"], 0.8),
                "interceptions": (["interceptions"], 0.7),
                "key_passes": (["key_passes"], 0.4),
            },
            "partial": {"distance_running": (["distance_running_km"], 0.3)},
            "unmeasured": ["marking", "work rate", "stamina"],
        },
    },
    "FB": {
        "Full-Back": {
            "measured": {
                "tackles_won": (["tackles_won"], 1.0),
                "interceptions": (["interceptions"], 0.9),
                "recoveries": (["recoveries"], 0.8),
                "crosses_accurate": (["crosses_accurate"], 0.7),
                "passes_total": (["passes_total"], 0.5),
                "progressive_carries": (["progressive_carries"], 0.5),
            },
            "partial": {"distance_hsr": (["distance_high_speed_running_km"], 0.3)},
            "unmeasured": ["marking", "anticipation", "concentration", "positioning", "teamwork", "acceleration"],
        },
        "Inside Full-Back": {
            "measured": {
                "passes_total": (["passes_total"], 0.9),
                "passes_accurate": (["passes_accurate"], 0.8),
                "interceptions": (["interceptions"], 0.8),
                "tackles_won": (["tackles_won"], 0.8),
                "recoveries": (["recoveries"], 0.7),
                "aerial_duels_won": (["aerial_duels_won"], 0.5),
            },
            "partial": {},
            "unmeasured": ["positioning", "strength", "decisions", "composure", "concentration"],
        },
        "Pressing Full-Back": {
            "measured": {
                "tackles_total": (["tackles_total"], 1.1),
                "tackles_won": (["tackles_won"], 0.9),
                "recoveries": (["recoveries"], 0.9),
                "interceptions": (["interceptions"], 0.7),
            },
            "partial": {"distance_running": (["distance_running_km"], 0.3)},
            "unmeasured": ["aggression", "work rate", "anticipation"],
        },
        "Holding Full-Back": {
            "measured": {
                "interceptions": (["interceptions"], 1.0),
                "tackles_won": (["tackles_won"], 0.8),
                "recoveries": (["recoveries"], 0.8),
                "clearances": (["clearances"], 0.6),
                "blocked_shots": (["blocked_shots"], 0.5),
            },
            "partial": {},
            "unmeasured": ["positioning", "concentration", "marking"],
        },
    },
    "WB": {
        "Wing-Back": {
            "measured": {
                "crosses_total": (["crosses_total"], 1.0),
                "crosses_accurate": (["crosses_accurate"], 1.1),
                "progressive_carries": (["progressive_carries"], 0.9),
                "tackles_won": (["tackles_won"], 0.7),
                "recoveries": (["recoveries"], 0.7),
                "xa": (["xa"], 0.7),
                "touches": (["touches"], 0.5),
            },
            "partial": {"distance_hsr": (["distance_high_speed_running_km"], 0.3)},
            "unmeasured": ["work rate", "pace", "stamina", "teamwork", "positioning"],
        },
        "Advanced Wing-Back": {
            "measured": {
                "crosses_accurate": (["crosses_accurate"], 1.2),
                "dribbles_attempted": (["dribbles_attempted", "contests_total"], 0.9),
                "contests_won": (["contests_won"], 0.8),
                "progressive_carries": (["progressive_carries"], 1.1),
                "xa": (["xa"], 1.0),
                "touches_opp_box": (["touches_opp_box"], 0.7),
                "key_passes": (["key_passes"], 0.7),
            },
            "partial": {"distance_sprinting": (["distance_sprinting_km"], 0.3)},
            "unmeasured": ["acceleration", "agility", "pace", "stamina", "off the ball"],
        },
        "Inside Wing-Back": {
            "measured": {
                "passes_total": (["passes_total"], 0.9),
                "passes_accurate": (["passes_accurate"], 0.8),
                "pass_value": (["pass_value"], 1.0),
                "interceptions": (["interceptions"], 0.7),
                "tackles_won": (["tackles_won"], 0.7),
                "progressive_carries": (["progressive_carries"], 0.6),
            },
            "partial": {},
            "unmeasured": ["composure", "decisions", "positioning", "teamwork"],
        },
        "Playmaking Wing-Back": {
            "measured": {
                "pass_value": (["pass_value"], 1.2),
                "key_passes": (["key_passes"], 1.0),
                "xa": (["xa"], 1.1),
                "crosses_accurate": (["crosses_accurate"], 0.8),
                "long_balls_accurate": (["long_balls_accurate"], 0.5),
                "passes_total": (["passes_total"], 0.6),
            },
            "partial": {},
            "unmeasured": ["vision", "technique", "decisions", "composure"],
        },
    },
    "CM": {
        "Central Midfielder": {
            "measured": {
                "passes_total": (["passes_total"], 0.9),
                "passes_accurate": (["passes_accurate"], 0.8),
                "tackles_won": (["tackles_won"], 0.7),
                "recoveries": (["recoveries"], 0.8),
                "interceptions": (["interceptions"], 0.6),
                "pass_value": (["pass_value"], 0.6),
            },
            "partial": {},
            "unmeasured": ["first touch", "decisions", "teamwork", "positioning", "concentration"],
        },
        "Advanced Playmaker": {
            "measured": {
                "key_passes": (["key_passes"], 1.1),
                "xa": (["xa"], 1.2),
                "pass_value": (["pass_value"], 1.2),
                "passes_opposition_half": (["passes_opposition_half_total"], 0.7),
                "long_balls_accurate": (["long_balls_accurate"], 0.5),
                "dribbles_attempted": (["dribbles_attempted", "contests_total"], 0.4),
            },
            "partial": {},
            "unmeasured": ["first touch", "technique", "vision", "flair", "decisions", "composure"],
        },
        "Midfield Playmaker": {
            "measured": {
                "passes_total": (["passes_total"], 1.1),
                "passes_accurate": (["passes_accurate"], 0.9),
                "pass_value": (["pass_value"], 1.1),
                "key_passes": (["key_passes"], 0.6),
                "long_balls_accurate": (["long_balls_accurate"], 0.6),
                "recoveries": (["recoveries"], 0.4),
            },
            "partial": {},
            "unmeasured": ["technique", "vision", "composure", "decisions", "first touch"],
        },
        "Pressing Central Midfielder": {
            "measured": {
                "tackles_total": (["tackles_total"], 1.1),
                "tackles_won": (["tackles_won"], 0.9),
                "recoveries": (["recoveries"], 0.9),
                "interceptions": (["interceptions"], 0.6),
            },
            "partial": {"distance_running": (["distance_running_km"], 0.3)},
            "unmeasured": ["aggression", "work rate", "anticipation"],
        },
    },
    "DM": {
        "Defensive Midfielder": {
            "measured": {
                "tackles_won": (["tackles_won"], 1.0),
                "interceptions": (["interceptions"], 1.1),
                "recoveries": (["recoveries"], 1.0),
                "passes_total": (["passes_total"], 0.6),
                "passes_accurate": (["passes_accurate"], 0.5),
                "duels_won": (["duels_won"], 0.6),
            },
            "partial": {},
            "unmeasured": ["positioning", "concentration", "teamwork", "anticipation"],
        },
        "Deep-Lying Playmaker": {
            "measured": {
                "passes_total": (["passes_total"], 1.0),
                "passes_accurate": (["passes_accurate"], 0.9),
                "pass_value": (["pass_value"], 1.2),
                "long_balls_accurate": (["long_balls_accurate"], 0.8),
                "key_passes": (["key_passes"], 0.5),
                "interceptions": (["interceptions"], 0.4),
            },
            "partial": {},
            "unmeasured": ["first touch", "technique", "vision", "decisions", "composure"],
        },
        "Half-Back": {
            "measured": {
                "interceptions": (["interceptions"], 1.1),
                "clearances": (["clearances"], 0.8),
                "aerial_duels_won": (["aerial_duels_won"], 0.8),
                "passes_total": (["passes_total"], 0.7),
                "tackles_won": (["tackles_won"], 0.8),
                "recoveries": (["recoveries"], 0.7),
            },
            "partial": {"height": (["height_cm"], 0.2)},
            "unmeasured": ["positioning", "concentration", "strength", "jumping", "bravery"],
        },
        "Box-to-Box Midfielder": {
            "measured": {
                "tackles_won": (["tackles_won"], 0.8),
                "recoveries": (["recoveries"], 0.8),
                "passes_total": (["passes_total"], 0.7),
                "progressive_carries": (["progressive_carries"], 0.7),
                "shots": (["shots_total", "shots"], 0.5),
                "xa": (["xa"], 0.4),
                "touches_opp_box": (["touches_opp_box"], 0.4),
            },
            "partial": {"distance_running": (["distance_running_km"], 0.3)},
            "unmeasured": ["work rate", "stamina", "off the ball", "strength", "pace"],
        },
    },
    "CB": {
        "Centre-Back": {
            "measured": {
                "clearances": (["clearances"], 1.0),
                "blocked_shots": (["blocked_shots"], 0.9),
                "interceptions": (["interceptions"], 0.8),
                "tackles_won": (["tackles_won"], 0.8),
                "aerial_duels_won": (["aerial_duels_won"], 1.1),
                "duels_won": (["duels_won"], 0.8),
            },
            "partial": {"height": (["height_cm"], 0.2)},
            "unmeasured": ["marking", "anticipation", "positioning", "jumping", "strength", "bravery", "concentration"],
        },
        "Ball-Playing Centre-Back": {
            "measured": {
                "passes_total": (["passes_total"], 0.9),
                "passes_accurate": (["passes_accurate"], 0.8),
                "long_balls_accurate": (["long_balls_accurate"], 1.0),
                "pass_value": (["pass_value"], 1.2),
                "total_progression": (["total_progression"], 0.9),
                "carries": (["carries"], 0.5),
                "interceptions": (["interceptions"], 0.5),
            },
            "partial": {},
            "unmeasured": ["composure", "decisions", "vision", "technique", "first touch", "positioning"],
        },
        "No-Nonsense Centre-Back": {
            "measured": {
                "clearances": (["clearances"], 1.3),
                "blocked_shots": (["blocked_shots"], 1.1),
                "aerial_duels_won": (["aerial_duels_won"], 1.1),
                "tackles_won": (["tackles_won"], 0.7),
            },
            "partial": {},
            "unmeasured": ["marking", "positioning", "jumping", "strength", "bravery"],
        },
        "Wide Centre-Back": {
            "measured": {
                "progressive_carries": (["progressive_carries"], 1.0),
                "carry_distance": (["carry_distance"], 0.8),
                "crosses_total": (["crosses_total"], 0.5),
                "tackles_won": (["tackles_won"], 0.7),
                "interceptions": (["interceptions"], 0.7),
                "duels_won": (["duels_won"], 0.7),
            },
            "partial": {"distance_hsr": (["distance_high_speed_running_km"], 0.3)},
            "unmeasured": ["pace", "stamina", "agility", "work rate", "wide defending"],
        },
    },
    "GK": {
        "Goalkeeper": {
            "measured": {
                "gk_saves": (["gk_saves"], 1.2),
                "gk_goals_prevented": (["gk_goals_prevented"], 1.2),
                "gk_save_value": (["gk_save_value"], 1.0),
                "gk_high_claims": (["gk_high_claims"], 0.7),
            },
            "partial": {"gk_punches": (["gk_punches"], 0.3)},
            "unmeasured": ["aerial reach", "command of area", "communication", "handling", "reflexes", "concentration"],
        },
        "Ball-Playing Goalkeeper": {
            "measured": {
                "passes_total": (["passes_total"], 0.8),
                "passes_accurate": (["passes_accurate"], 0.8),
                "long_balls_accurate": (["long_balls_accurate"], 0.8),
                "pass_value": (["pass_value"], 1.0),
                "gk_sweeper_accurate": (["gk_sweeper_accurate"], 0.8),
            },
            "partial": {"gk_sweeper_total": (["gk_sweeper_total"], 0.5)},
            "unmeasured": ["kicking", "passing technique", "composure", "decisions", "one-on-ones"],
        },
        "Sweeper Keeper": {
            "measured": {
                "gk_sweeper_total": (["gk_sweeper_total"], 1.2),
                "gk_sweeper_accurate": (["gk_sweeper_accurate"], 1.2),
                "passes_total": (["passes_total"], 0.4),
                "long_balls_accurate": (["long_balls_accurate"], 0.4),
            },
            "partial": {},
            "unmeasured": ["rushing out tendency", "anticipation", "decisions", "starting position"],
        },
    },
}


# Hybrid defender family introduced for Position_Arbitrator_v13+.
# It captures players who sit between CB and FB: Ben White, Hincapié, Aké/Koundé-type profiles.
ROLE_MODEL["CB-FB"] = {
    "Hybrid Defender": {
        "measured": {
            "interceptions": (["interceptions"], 1.0),
            "tackles_won": (["tackles_won"], 0.9),
            "recoveries": (["recoveries"], 0.8),
            "clearances": (["clearances"], 0.7),
            "passes_total": (["passes_total"], 0.7),
            "pass_value": (["pass_value"], 0.7),
            "progressive_carries": (["progressive_carries"], 0.5),
        },
        "partial": {"height": (["height_cm"], 0.2)},
        "unmeasured": ["positioning", "concentration", "decisions", "wide defending", "covering pace"],
    },
    "Wide Centre-Back": {
        "measured": {
            "progressive_carries": (["progressive_carries"], 1.0),
            "carry_distance": (["carry_distance"], 0.8),
            "passes_total": (["passes_total"], 0.7),
            "pass_value": (["pass_value"], 0.8),
            "tackles_won": (["tackles_won"], 0.7),
            "interceptions": (["interceptions"], 0.7),
            "duels_won": (["duels_won"], 0.6),
        },
        "partial": {"distance_hsr": (["distance_high_speed_running_km"], 0.3)},
        "unmeasured": ["pace", "stamina", "agility", "work rate", "wide defending"],
    },
    "Defensive Full-Back Hybrid": {
        "measured": {
            "tackles_won": (["tackles_won"], 1.0),
            "interceptions": (["interceptions"], 0.9),
            "recoveries": (["recoveries"], 0.8),
            "clearances": (["clearances"], 0.6),
            "blocked_shots": (["blocked_shots"], 0.5),
            "passes_total": (["passes_total"], 0.5),
        },
        "partial": {},
        "unmeasured": ["positioning", "concentration", "marking", "covering", "decision making"],
    },
    "Ball-Playing Defensive Hybrid": {
        "measured": {
            "passes_total": (["passes_total"], 1.0),
            "passes_accurate": (["passes_accurate"], 0.9),
            "long_balls_accurate": (["long_balls_accurate"], 0.7),
            "pass_value": (["pass_value"], 1.1),
            "total_progression": (["total_progression"], 0.8),
            "interceptions": (["interceptions"], 0.5),
            "recoveries": (["recoveries"], 0.4),
        },
        "partial": {},
        "unmeasured": ["composure", "decisions", "vision", "technique", "first touch", "positioning"],
    },
}


def score_player(row: pd.Series, cohort: pd.DataFrame, family: str) -> dict[str, Any]:
    role_defs = ROLE_MODEL.get(family)
    if not role_defs:
        return {
            "primary_role": None,
            "secondary_role": None,
            "role_score": np.nan,
            "role_bvalue": np.nan,
            "confidence": 0.0,
            "role_scores": [],
            "warning": f"No role model for family={family}",
        }

    scored = []
    for role_name, spec in role_defs.items():
        measured_score, measured_details = weighted_pct(cohort, row, spec.get("measured", {}))
        partial_score, partial_details = weighted_pct(cohort, row, spec.get("partial", {}))

        measured_count = len(measured_details)
        measured_possible = len(spec.get("measured", {}))
        partial_count = len(partial_details)
        partial_possible = len(spec.get("partial", {}))

        all_values = []
        for d in measured_details:
            all_values.append((d["percentile"], d["weight"]))
        for d in partial_details:
            all_values.append((d["percentile"], d["weight"] * 0.55))

        if all_values:
            total_score = sum(v * w for v, w in all_values) / sum(w for _, w in all_values)
        else:
            total_score = np.nan

        coverage = measured_count / measured_possible if measured_possible else 0
        partial_coverage = partial_count / partial_possible if partial_possible else 0
        confidence = min(1.0, 0.85 * coverage + 0.15 * partial_coverage)
        if measured_count < 3:
            confidence *= 0.55

        scored.append({
            "role": role_name,
            "score": None if pd.isna(total_score) else round(float(total_score), 2),
            "bvalue": None if pd.isna(total_score) else round((float(total_score) - 50.0) / 50.0, 4),
            "measured_score": None if measured_score is None else round(float(measured_score), 2),
            "partial_proxy_score": None if partial_score is None else round(float(partial_score), 2),
            "confidence": round(float(confidence), 3),
            "measured_metrics_available": measured_count,
            "measured_metrics_possible": measured_possible,
            "partial_proxies_available": partial_count,
            "partial_proxies_possible": partial_possible,
            "measured_inputs": measured_details,
            "partial_proxies": partial_details,
            "unmeasured_traits": spec.get("unmeasured", []),
        })

    scored.sort(key=lambda x: (-999 if x["score"] is None else -x["score"], -x["confidence"]))
    primary = scored[0] if scored else {}
    secondary = scored[1] if len(scored) > 1 else {}

    return {
        "primary_role": primary.get("role"),
        "secondary_role": secondary.get("role"),
        "role_score": primary.get("score"),
        "role_bvalue": primary.get("bvalue"),
        "confidence": primary.get("confidence", 0.0),
        "role_scores": scored,
        "warning": None,
    }


def classify(df: pd.DataFrame, args: argparse.Namespace) -> tuple[pd.DataFrame, list[dict[str, Any]], str]:
    work = df.copy()

    if args.season and "season" in work.columns:
        work = work.loc[work["season"].astype(str) == str(args.season)].copy()
    if args.league and "league" in work.columns:
        work = work.loc[work["league"].astype(str).str.lower() == str(args.league).lower()].copy()
    if args.min_minutes is not None and "minutes_played" in work.columns:
        work = work.loc[to_num(work["minutes_played"]).fillna(0) >= float(args.min_minutes)].copy()

    if work.empty:
        raise ValueError("No rows left after filters.")

    role_col = choose_role_column(work, args.role_column)
    work["_role_model_family"] = work[role_col].apply(normalize_model_family)

    target = work
    if args.player_id is not None:
        target = work.loc[to_num(work["player_id"]) == int(args.player_id)].copy()
        if target.empty:
            raise ValueError(f"player_id={args.player_id} not found after filters.")

    csv_rows = []
    json_rows = []

    for idx, row in target.iterrows():
        family = row.get("_role_model_family")

        if family == "CB-FB":
            cohort = work.loc[work["_role_model_family"].isin(["CB-FB", "CB", "FB"])].copy()
        else:
            cohort = work.loc[work["_role_model_family"] == family].copy()

        if cohort.empty:
            cohort = work.copy()

        result = score_player(row, cohort, family)

        base = {
            "player_id": row.get("player_id"),
            "player_name": row.get("player_name") if "player_name" in row.index and pd.notna(row.get("player_name")) else row.get("profile_name"),
            "season": row.get("season"),
            "league": row.get("league"),
            "team": row.get("team"),
            "minutes_played": row.get("minutes_played"),
            "age": row.get("age"),
            "role_source_column": role_col,
            "role_source_value": row.get(role_col),
            "role_model_family": family,
            "arbitrated_position": row.get("arbitrated_position"),
            "arbitrated_role_group": row.get("arbitrated_role_group"),
            "arbitrated_lane": row.get("arbitrated_lane"),
            "arbitrated_confidence": row.get("arbitrated_confidence"),
            "position_conflict_flag": row.get("position_conflict_flag"),
            "season_avg_x": row.get("season_avg_x"),
            "season_avg_y": row.get("season_avg_y"),
            "season_position_zone": row.get("season_position_zone"),
            "spatial_matches_used": row.get("spatial_matches_used"),
            "spatial_wide_pct": row.get("spatial_wide_pct"),
            "spatial_right_pct": row.get("spatial_right_pct"),
            "spatial_left_pct": row.get("spatial_left_pct"),
            "spatial_high_wide_pct": row.get("spatial_high_wide_pct"),
            "primary_role": result.get("primary_role"),
            "secondary_role": result.get("secondary_role"),
            "role_score": result.get("role_score"),
            "role_bvalue": result.get("role_bvalue"),
            "confidence": result.get("confidence"),
            "cohort_size": len(cohort),
            "warning": result.get("warning"),
        }

        primary_detail = result["role_scores"][0] if result.get("role_scores") else {}
        base["measured_metrics_available"] = primary_detail.get("measured_metrics_available")
        base["measured_metrics_possible"] = primary_detail.get("measured_metrics_possible")
        base["partial_proxies_available"] = primary_detail.get("partial_proxies_available")
        base["partial_proxies_possible"] = primary_detail.get("partial_proxies_possible")
        base["measured_inputs"] = ", ".join(d["column"] for d in primary_detail.get("measured_inputs", []))
        base["partial_proxies"] = ", ".join(d["column"] for d in primary_detail.get("partial_proxies", []))
        base["unmeasured_traits"] = ", ".join(primary_detail.get("unmeasured_traits", []))

        csv_rows.append(base)
        json_rows.append({
            **base,
            "role_scores": result.get("role_scores", []),
            "method_note": (
                "Arbitration-aware role classification. Role model family is derived from "
                "arbitrated/refined position fields when available. Mental/physical guide traits "
                "are listed as unmeasured or partial proxies rather than directly scored."
            ),
        })

    return pd.DataFrame(csv_rows), json_rows, role_col


def main() -> None:
    ap = argparse.ArgumentParser(description="Arbitration-aware player role classifier.")
    ap.add_argument("--input", "-i", default="player_season_totals_arbitrated.csv")
    ap.add_argument("--output", "-o", default="player_roles.csv")
    ap.add_argument("--season", "-s", default=None)
    ap.add_argument("--league", "-l", default=None)
    ap.add_argument("--player-id", "-p", type=int, default=None)
    ap.add_argument("--role-column", default=None, help="Optional override. Defaults to arbitrated/refined priority.")
    ap.add_argument("--min-minutes", type=float, default=None)
    ap.add_argument("--format", choices=["csv", "json", "both"], default="csv")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    csv_df, json_rows, role_col = classify(df, args)

    out = Path(args.output)
    written = []

    if args.format in {"csv", "both"}:
        csv_path = out if out.suffix.lower() == ".csv" else out.with_suffix(".csv")
        csv_df.to_csv(csv_path, index=False)
        written.append(csv_path)

    if args.format in {"json", "both"}:
        json_path = out if out.suffix.lower() == ".json" else out.with_suffix(".json")
        payload = json_rows[0] if args.player_id is not None and len(json_rows) == 1 else json_rows
        json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        written.append(json_path)

    print(f"Using role source column: {role_col}")
    print(f"Classified rows: {len(csv_df)}")
    for p in written:
        print(f"Output: {p}")

    if len(csv_df) == 1:
        r = csv_df.iloc[0]
        print(f"{r.get('player_name')}: {r.get('primary_role')} / {r.get('secondary_role')} "
              f"score={r.get('role_score')} confidence={r.get('confidence')}")


if __name__ == "__main__":
    main()
