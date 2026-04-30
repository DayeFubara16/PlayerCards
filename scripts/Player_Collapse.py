from __future__ import annotations

from pathlib import Path
import argparse
import pandas as pd
import numpy as np


def safe_mode(series: pd.Series):
    s = series.dropna()
    if s.empty:
        return np.nan
    m = s.mode()
    return m.iloc[0] if not m.empty else s.iloc[0]


def first_non_null(series: pd.Series):
    s = series.dropna()
    return s.iloc[0] if not s.empty else np.nan


def weighted_average(group: pd.DataFrame, value_col: str, weight_col: str = "minutes_played"):
    if value_col not in group.columns:
        return np.nan
    vals = pd.to_numeric(group[value_col], errors="coerce")
    if weight_col not in group.columns:
        return vals.mean()
    weights = pd.to_numeric(group[weight_col], errors="coerce")
    valid = vals.notna() & weights.notna() & (weights > 0)
    if valid.sum() == 0:
        return vals.mean()
    return float(np.average(vals[valid], weights=weights[valid]))


# Sofascore proprietary/in-house model columns.
# Useful diagnostics in match logs, but not season-total scouting inputs.
MODEL_OUTPUT_EXACT = {
    "sofascore_rating",
    "rating",
    "original_rating",
    "alternative_rating",
    "pass_value",
    "dribble_value",
    "defensive_value",
    "shot_value",
    "goalkeeper_value",
    "gk_save_value",
    "keeper_save_value",
}

MODEL_OUTPUT_PATTERNS = (
    "rating",
    "value",
    "valuenormalized",
    "value_normalized",
)

CONTEST_RENAME_MAP = {
    "contests_total": "dribbles_attempted",
    "contests_won": "dribbles_won",
    "total_contest": "dribbles_attempted",
    "won_contest": "dribbles_won",
}


def is_model_output_col(col: str) -> bool:
    c = str(col).strip().lower()
    if c in MODEL_OUTPUT_EXACT:
        return True
    return any(p in c for p in MODEL_OUTPUT_PATTERNS)


def normalize_dribble_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sofascore calls take-ons 'contests'. Normalize those to dribble language.

    If both contest and dribble columns exist, fill missing dribble values from
    contests and drop the contest alias to avoid double-counting.
    """
    out = df.copy()

    for old, new in CONTEST_RENAME_MAP.items():
        if old not in out.columns:
            continue

        old_vals = pd.to_numeric(out[old], errors="coerce")

        if new in out.columns:
            new_vals = pd.to_numeric(out[new], errors="coerce")
            out[new] = new_vals.combine_first(old_vals)
            out = out.drop(columns=[old])
        else:
            out = out.rename(columns={old: new})

    return out


def normalize_role(value):
    if pd.isna(value):
        return np.nan
    text = str(value).strip()
    return text if text else np.nan


def summarize_positions(group: pd.DataFrame, min_minutes_for_position: float = 90.0) -> dict:
    result = {
        "primary_role_position": np.nan,
        "secondary_role_position": np.nan,
        "positions_played_count": 0,
        "positions_played_list": np.nan,
        "base_position": np.nan,
        "role_family": np.nan,
    }

    if "role_position" not in group.columns:
        return result

    tmp = group.copy()
    tmp["role_position"] = tmp["role_position"].apply(normalize_role)
    tmp["minutes_played"] = pd.to_numeric(tmp.get("minutes_played", 0), errors="coerce").fillna(0)

    role_rows = tmp.dropna(subset=["role_position"]).copy()
    if role_rows.empty:
        return result

    role_minutes = (
        role_rows.groupby("role_position", dropna=True)["minutes_played"]
        .sum()
        .sort_values(ascending=False)
    )

    qualifying = role_minutes[role_minutes >= min_minutes_for_position]
    if qualifying.empty:
        qualifying = role_minutes.head(1)

    roles = qualifying.index.tolist()
    result["primary_role_position"] = roles[0] if len(roles) >= 1 else np.nan
    result["secondary_role_position"] = roles[1] if len(roles) >= 2 else np.nan
    result["positions_played_count"] = len(roles)
    result["positions_played_list"] = ", ".join(roles) if roles else np.nan

    if pd.notna(result["primary_role_position"]):
        primary_rows = role_rows[role_rows["role_position"] == result["primary_role_position"]]
        if "base_position" in primary_rows.columns:
            result["base_position"] = safe_mode(primary_rows["base_position"])
        if "role_family" in primary_rows.columns:
            result["role_family"] = safe_mode(primary_rows["role_family"])

    return result


# Columns whose values are IDs, labels, rates, or metadata and should not become totals/per90.
NEVER_SUM = {
    "player_id", "event_id", "match_id", "MW", "shirt_number", "sub_on_minute", "sub_off_minute",
    "age", "height_cm", "age_as_of", "date_of_birth",
    "pass_accuracy_pct", "duel_win_pct", "aerial_win_pct", "dribble_success_pct",
}

TEXT_OR_METADATA = {
    "league", "player_name", "player_position", "base_position", "role_position", "role_family",
    "position_confidence", "position_source", "season", "team", "opponent", "venue", "result",
    "flags", "date_of_birth", "profile_name", "nationality", "preferred_foot",
    "primary_role_position", "secondary_role_position", "positions_played_list", "teams_played_list",
}

WEIGHTED_MEAN_PATTERNS = (
    "accuracy_pct", "win_pct", "success_pct", "age", "height_cm"
)

# Known numeric columns that are single-match totals/counts and should be summed then converted to per90.
KNOWN_SUM_COLS = {
    "minutes_played", "goals", "assists", "shots_total", "shots_on_target", "shots_off_target",
    "xg", "xgot", "xa", "big_chances_created", "big_chance_missed", "touches_opp_box",
    "offsides", "hit_woodwork", "passes_total", "passes_accurate", "passes_own_half_total",
    "passes_own_half_accurate", "passes_opposition_half_total", "passes_opposition_half_accurate",
    "key_passes", "long_balls_total", "long_balls_accurate", "crosses_total", "crosses_accurate",
    "touches", "unsuccessful_touches", "dribbles_attempted", "carries", "carry_distance",
    "progressive_carries", "progressive_carry_distance", "best_carry_progression", "total_progression",
    "dispossessed", "possession_lost", "tackles_total", "tackles_won", "last_man_tackles",
    "interceptions", "clearances", "clearance_off_line", "blocked_shots", "duels_total", "duels_won",
    "duels_lost", "aerial_duels_total", "aerial_duels_won", "aerial_duels_lost", "recoveries",
    "dribbles_won", "challenges_lost", "errors_leading_to_shot",
    "errors_leading_to_goal", "fouls_committed", "fouls_drawn", "yellow_cards", "red_cards",
    "penalties_won", "penalties_conceded", "penalties_faced", "distance_walking_km",
    "distance_jogging_km", "distance_running_km", "distance_high_speed_running_km", "distance_sprinting_km",
    "gk_saves", "gk_saves_inside_box", "gk_xgot_faced", "gk_goals_prevented", "gk_goals_prevented_raw",
    "gk_save_value", "gk_high_claims", "gk_punches", "gk_sweeper_total", "gk_sweeper_accurate",
}


def looks_like_total_col(col: str) -> bool:
    """Heuristic for new future numeric stat columns that should be summed/per90'd."""
    c = col.lower()
    if c in NEVER_SUM or c.endswith("_per90"):
        return False
    if any(p in c for p in WEIGHTED_MEAN_PATTERNS):
        return False
    if c.endswith(("_total", "_accurate", "_won", "_lost", "_km")):
        return True
    if c.startswith(("gk_",)):
        return True
    count_words = (
        "goals", "assists", "shots", "passes", "touches", "carries", "tackles", "interceptions",
        "clearances", "blocks", "duels", "recoveries", "dribbles", "challenges", "errors", "fouls",
        "cards", "penalties", "offsides", "woodwork", "xg", "xgot", "xa", "distance",
        "progression", "dispossessed", "possession_lost"
    )
    return any(w in c for w in count_words)


def classify_columns(df: pd.DataFrame):
    numeric_cols = []
    dropped_model_cols = []

    for col in df.columns:
        if col in TEXT_OR_METADATA:
            continue
        if is_model_output_col(col):
            dropped_model_cols.append(col)
            continue

        coerced = pd.to_numeric(df[col], errors="coerce")
        if coerced.notna().any():
            numeric_cols.append(col)
            df[col] = coerced

    sum_cols = [c for c in numeric_cols if c in KNOWN_SUM_COLS or looks_like_total_col(c)]
    mean_cols = [c for c in numeric_cols if c not in sum_cols and c not in {"player_id", "event_id", "match_id"}]
    return sum_cols, mean_cols, dropped_model_cols


def merge_player_master(out: pd.DataFrame, players_master: str | None) -> pd.DataFrame:
    if not players_master:
        return out
    master_path = Path(players_master)
    if not master_path.exists():
        return out

    master = pd.read_csv(master_path)
    rename_map = {}
    if "dob" in master.columns and "date_of_birth" not in master.columns:
        rename_map["dob"] = "date_of_birth"
    if rename_map:
        master = master.rename(columns=rename_map)

    merge_cols = [c for c in ["player_id", "date_of_birth", "age", "age_as_of", "profile_name", "nationality", "height_cm", "preferred_foot"] if c in master.columns]
    if "player_id" not in merge_cols:
        return out

    master = master[merge_cols].drop_duplicates(subset=["player_id"], keep="first")
    for c in merge_cols:
        if c != "player_id" and c in out.columns:
            master = master.rename(columns={c: f"{c}__master"})

    out = out.merge(master, on="player_id", how="left")

    for c in ["date_of_birth", "age", "age_as_of", "profile_name", "nationality", "height_cm", "preferred_foot"]:
        mc = f"{c}__master"
        if mc in out.columns:
            if c in out.columns:
                out[c] = out[c].combine_first(out[mc])
            else:
                out[c] = out[mc]
            out = out.drop(columns=[mc])

    return out


def main():
    parser = argparse.ArgumentParser(description="Collapse player match logs into player-season totals with future-proof column handling")
    parser.add_argument("--input", default="player_match_logs_with_ages.csv", help="Input match-log CSV")
    parser.add_argument("--output", default="player_season_totals.csv", help="Output season-level CSV")
    parser.add_argument("--min-position-minutes", type=float, default=90.0, help="Minimum minutes at a role for it to count in positions list")
    parser.add_argument("--players-master", default=None, help="Optional player master CSV to merge/fill profile fields")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    if "player_id" not in df.columns or "season" not in df.columns:
        raise ValueError("Input must contain player_id and season columns.")

    df = normalize_dribble_columns(df)

    sum_cols, mean_cols, dropped_model_cols = classify_columns(df)

    rows = []
    for _, group in df.groupby(["player_id", "season"], dropna=False):
        row = {}

        # Core identity fields.
        for col in ["player_id", "player_name", "season", "league", "team", "profile_name", "date_of_birth", "nationality", "preferred_foot"]:
            if col in group.columns:
                row[col] = first_non_null(group[col])

        # Age/height are profile properties; use weighted average as fallback if duplicated consistently.
        for col in ["age", "height_cm", "age_as_of"]:
            if col in group.columns:
                row[col] = first_non_null(group[col])

        if "team" in group.columns:
            teams = sorted({str(t).strip() for t in group["team"].dropna() if str(t).strip()})
            row["teams_played_count"] = len(teams)
            row["teams_played_list"] = ", ".join(teams) if teams else np.nan
            if len(teams) > 1:
                row["team"] = "Multiple"

        if "league" in group.columns:
            leagues = sorted({str(t).strip() for t in group["league"].dropna() if str(t).strip()})
            row["leagues_played_count"] = len(leagues)
            row["leagues_played_list"] = ", ".join(leagues) if leagues else np.nan
            if len(leagues) > 1:
                row["league"] = "Multiple"

        row["matches"] = len(group)

        for col in sum_cols:
            if col in group.columns:
                row[col] = pd.to_numeric(group[col], errors="coerce").sum(min_count=1)

        for col in mean_cols:
            if col in group.columns and col not in row:
                row[col] = weighted_average(group, col)

        # Mode metadata, but position summary overwrites base/role with primary-role-aware values.
        for col in ["player_position", "position_confidence", "position_source", "base_position", "role_family"]:
            if col in group.columns:
                row[col] = safe_mode(group[col])

        row.update(summarize_positions(group, min_minutes_for_position=args.min_position_minutes))

        minutes = row.get("minutes_played", np.nan)
        if pd.notna(minutes) and minutes > 0:
            for col in sum_cols:
                if col != "minutes_played" and col in row and pd.notna(row[col]):
                    row[f"{col}_per90"] = row[col] * 90 / minutes

            if pd.notna(row.get("passes_total")) and row.get("passes_total", 0) > 0:
                row["pass_accuracy_pct"] = 100 * row.get("passes_accurate", 0) / row["passes_total"]
            if pd.notna(row.get("duels_total")) and row.get("duels_total", 0) > 0:
                row["duel_win_pct"] = 100 * row.get("duels_won", 0) / row["duels_total"]
            if pd.notna(row.get("aerial_duels_total")) and row.get("aerial_duels_total", 0) > 0:
                row["aerial_win_pct"] = 100 * row.get("aerial_duels_won", 0) / row["aerial_duels_total"]
            if pd.notna(row.get("dribbles_attempted")) and row.get("dribbles_attempted", 0) > 0:
                row["dribble_success_pct"] = 100 * row.get("dribbles_won", 0) / row["dribbles_attempted"]

        rows.append(row)

    out = pd.DataFrame(rows)
    out = merge_player_master(out, args.players_master)

    preferred_order = [
        "player_id", "player_name", "profile_name", "season", "league", "team",
        "leagues_played_count", "leagues_played_list", "teams_played_count", "teams_played_list",
        "matches", "minutes_played", "date_of_birth", "age", "age_as_of", "nationality", "height_cm", "preferred_foot",
        "player_position", "base_position", "role_family", "primary_role_position", "secondary_role_position",
        "positions_played_count", "positions_played_list", "position_confidence", "position_source",
    ]
    ordered_cols = [c for c in preferred_order if c in out.columns]
    remaining_cols = [c for c in out.columns if c not in ordered_cols]
    out = out[ordered_cols + remaining_cols]

    sort_cols = [c for c in ["season", "league", "team", "minutes_played", "goals", "xg"] if c in out.columns]
    if sort_cols:
        asc = [True, True, True, False, False, False][:len(sort_cols)]
        out = out.sort_values(sort_cols, ascending=asc)

    out.to_csv(args.output, index=False)
    print(f"Input rows:     {len(df)}")
    print(f"Output rows:    {len(out)}")
    print(f"Summed columns: {len(sum_cols)}")
    print(f"Mean columns:   {len(mean_cols)}")
    print(f"Dropped model columns: {len(dropped_model_cols)}")
    if dropped_model_cols:
        print("Dropped model columns:", ", ".join(sorted(dropped_model_cols)))
    print(f"Saved to:       {args.output}")


if __name__ == "__main__":
    main()
