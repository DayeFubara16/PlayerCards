#!/usr/bin/env python3
"""
Player_Finder.py
================
A Stathead-style player search tool for player_season_totals_arbitrated.csv.

Usage examples
--------------
# All players with 10+ goals and 5+ assists:
python Player_Finder.py --goals_min 10 --assists_min 5

# Strikers under 23 in the Premier League with >0.5 xG/90:
python Player_Finder.py --position ST --league "Premier League" --age_max 23 --xg_per90_min 0.5

# CBs who win >60% of aerial duels (add --aerial_duels_total_min to avoid tiny samples):
python Player_Finder.py --position CB --aerial_win_pct_min 60 --aerial_duels_total_min 30 --minutes_played_min 900

# Top 20 playmakers by key passes, showing selected columns:
python Player_Finder.py --key_passes_min 20 --sort key_passes --desc --limit 20 --show player_name,team,league,key_passes,xa,assists

# Goalkeepers with the best goals prevented:
python Player_Finder.py --position GK --gk_goals_prevented_min 1 --sort gk_goals_prevented --desc

# Search by name (partial, case-insensitive):
python Player_Finder.py --name "Messi"

Run with --help for the full list of filters.
"""

import argparse
import sys
import os
import pandas as pd

# ── Constants ──────────────────────────────────────────────────────────────────

DEFAULT_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "player_season_totals_arbitrated.csv")

# Columns always shown unless --show overrides
IDENTITY_COLS = ["player_name", "season", "league", "team", "arbitrated_position", "age", "matches", "minutes_played"]

# Categorical columns that support exact / partial matching
CATEGORICAL_FILTERS = {
    "name":                 "player_name",
    "team":                 "team",
    "league":               "league",
    "nationality":          "nationality",
    "position":             "arbitrated_position",
    "role_group":           "arbitrated_role_group",
    "lane":                 "arbitrated_lane",
    "preferred_foot":       "preferred_foot",
    "season_position_zone": "season_position_zone",
    "profile_position":     "profile_position_raw",
}

# Every numeric column exposed as --STAT_min / --STAT_max pair
NUMERIC_STATS = [
    # Playing time
    "matches", "minutes_played", "age", "height_cm",
    # Attacking
    "goals", "assists", "shots_total", "shots_on_target", "shots_off_target",
    "xg", "xgot", "xa", "big_chances_created", "big_chance_missed",
    "offsides", "hit_woodwork", "penalties_won",
    # Per-90 attacking
    "goals_per90", "assists_per90", "shots_total_per90", "shots_on_target_per90",
    "xg_per90", "xgot_per90", "xa_per90", "big_chances_created_per90",
    # Passing
    "passes_total", "passes_accurate", "pass_accuracy_pct",
    "key_passes", "long_balls_total", "long_balls_accurate",
    "crosses_total", "crosses_accurate",
    "passes_total_per90", "passes_accurate_per90", "key_passes_per90",
    "crosses_total_per90",
    # Carrying / dribbling
    "dribbles_attempted", "dribbles_won", "dribble_success_pct",
    "carries", "carry_distance", "progressive_carries", "progressive_carry_distance",
    "total_progression", "dispossessed", "possession_lost",
    "dribbles_attempted_per90", "progressive_carries_per90",
    # Defending
    "tackles_total", "tackles_won", "last_man_tackles",
    "interceptions", "clearances", "blocked_shots", "clearance_off_line",
    "duels_total", "duels_won", "duels_lost", "duel_win_pct",
    "aerial_duels_total", "aerial_duels_won", "aerial_duels_lost", "aerial_win_pct",
    "recoveries", "challenges_lost",
    "tackles_total_per90", "interceptions_per90", "clearances_per90",
    "duels_total_per90", "aerial_duels_total_per90",
    # Errors / discipline
    "errors_leading_to_shot", "errors_leading_to_goal",
    "fouls_committed", "fouls_drawn", "yellow_cards", "red_cards",
    "penalties_conceded",
    # GK-specific
    "gk_saves", "gk_saves_inside_box", "gk_xgot_faced",
    "gk_goals_prevented", "gk_goals_prevented_raw",
    "gk_high_claims", "gk_punches", "gk_sweeper_total",
    "gk_saves_per90", "gk_xgot_faced_per90", "gk_goals_prevented_per90",
    # Distance
    "distance_walking_km", "distance_jogging_km", "distance_running_km",
    "distance_high_speed_running_km", "distance_sprinting_km",
    # Spatial / positioning
    "avg_x", "avg_y", "season_avg_x", "season_avg_y",
    "spatial_wide_pct", "spatial_central_pct",
    "spatial_high_pct", "spatial_mid_pct", "spatial_deep_pct",
    # Confidence
    "arbitrated_confidence",
]


# ── Argument Parser ────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="Player_Finder.py",
        description="Stathead-style player search for player_season_totals_arbitrated.csv",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Data source
    p.add_argument("--csv", default=DEFAULT_CSV,
                   help="Path to the CSV database (default: same directory as this script)")

    # ── Categorical filters ────────────────────────────────────────────────────
    cat_group = p.add_argument_group("Categorical / identity filters")
    cat_group.add_argument("--name",        metavar="STR",  help="Player name (partial, case-insensitive)")
    cat_group.add_argument("--team",        metavar="STR",  help="Team name (partial, case-insensitive)")
    cat_group.add_argument("--league",      metavar="STR",  help="League name (partial, case-insensitive)")
    cat_group.add_argument("--nationality", metavar="STR",  help="Nationality (partial, case-insensitive)")
    cat_group.add_argument("--position",    metavar="POS",  help="Arbitrated position, e.g. CB, ST, CM, GK (exact)")
    cat_group.add_argument("--role_group",  metavar="STR",  help="Role group, e.g. 'defence', 'midfield' (partial)")
    cat_group.add_argument("--lane",        metavar="STR",  help="Arbitrated lane, e.g. 'Central Defence' (partial)")
    cat_group.add_argument("--preferred_foot", metavar="STR", help="'Left', 'Right', or 'Both'")
    cat_group.add_argument("--season_position_zone", metavar="STR",
                           help="Spatial zone, e.g. 'deep_central', 'mid_left' (partial)")

    # ── Numeric filters (auto-generated) ──────────────────────────────────────
    num_group = p.add_argument_group(
        "Numeric stat filters",
        "For every stat below, add _min and/or _max suffixes.\n"
        "  Example: --goals_min 10 --xg_per90_max 0.8"
    )
    for stat in NUMERIC_STATS:
        num_group.add_argument(f"--{stat}_min", type=float, metavar="N", dest=f"{stat}_min",
                               help=f"Min {stat}")
        num_group.add_argument(f"--{stat}_max", type=float, metavar="N", dest=f"{stat}_max",
                               help=f"Max {stat}")

    # ── Output controls ────────────────────────────────────────────────────────
    out_group = p.add_argument_group("Output / display options")
    out_group.add_argument("--show", metavar="COL1,COL2,...",
                           help="Comma-separated list of columns to display (overrides defaults). "
                                "Use 'all' to show every column.")
    out_group.add_argument("--sort", metavar="COLUMN",
                           help="Column to sort results by")
    out_group.add_argument("--desc", action="store_true",
                           help="Sort descending (default is ascending)")
    out_group.add_argument("--limit", type=int, metavar="N",
                           help="Maximum number of results to display")
    out_group.add_argument("--no_truncate", action="store_true",
                           help="Do not truncate wide output (show all columns fully)")
    out_group.add_argument("--list_columns", action="store_true",
                           help="Print all available column names and exit")

    return p


# ── Core logic ─────────────────────────────────────────────────────────────────

def load_data(csv_path: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        sys.exit(
            f"ERROR: CSV not found at '{csv_path}'.\n"
            f"Place player_season_totals_arbitrated.csv in the same folder as this script,\n"
            f"or pass --csv /path/to/file.csv"
        )
    return pd.read_csv(csv_path, low_memory=False)


def apply_filters(df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    mask = pd.Series([True] * len(df), index=df.index)

    # ── Categorical ────────────────────────────────────────────────────────────
    for arg_name, col_name in CATEGORICAL_FILTERS.items():
        val = getattr(args, arg_name, None)
        if val is None:
            continue
        if col_name not in df.columns:
            print(f"  [warning] Column '{col_name}' not found in CSV, skipping filter '{arg_name}'.")
            continue
        # Position: exact match (case-insensitive); all others: substring
        if arg_name == "position":
            mask &= df[col_name].str.upper() == val.upper()
        else:
            mask &= df[col_name].str.contains(val, case=False, na=False)

    # ── Numeric ───────────────────────────────────────────────────────────────
    for stat in NUMERIC_STATS:
        col = stat  # column name matches stat name directly
        if col not in df.columns:
            continue

        min_val = getattr(args, f"{stat}_min", None)
        max_val = getattr(args, f"{stat}_max", None)

        if min_val is not None:
            mask &= df[col].notna() & (df[col] >= min_val)
        if max_val is not None:
            mask &= df[col].notna() & (df[col] <= max_val)

    return df[mask].copy()


def determine_display_columns(df: pd.DataFrame, args: argparse.Namespace,
                               filtered_stats: list[str]) -> list[str]:
    """Work out which columns to display."""
    if args.show:
        if args.show.lower() == "all":
            return list(df.columns)
        requested = [c.strip() for c in args.show.split(",")]
        missing = [c for c in requested if c not in df.columns]
        if missing:
            print(f"  [warning] Requested columns not found and will be skipped: {missing}")
        return [c for c in requested if c in df.columns]

    # Default: identity cols + any stat columns that were actively filtered on
    display = list(IDENTITY_COLS)
    for stat in filtered_stats:
        if stat not in display and stat in df.columns:
            display.append(stat)

    # Also add the sort column if not already present
    if args.sort and args.sort in df.columns and args.sort not in display:
        display.append(args.sort)

    # Deduplicate while preserving order
    seen = set()
    result = []
    for c in display:
        if c not in seen and c in df.columns:
            seen.add(c)
            result.append(c)
    return result


def detect_filtered_stats(args: argparse.Namespace) -> list[str]:
    """Return the list of numeric stats the user actually provided a bound for."""
    active = []
    for stat in NUMERIC_STATS:
        if getattr(args, f"{stat}_min", None) is not None or \
           getattr(args, f"{stat}_max", None) is not None:
            active.append(stat)
    return active


def print_results(df: pd.DataFrame, display_cols: list[str], args: argparse.Namespace) -> None:
    subset = df[display_cols].copy()

    if args.sort:
        if args.sort in subset.columns:
            subset = subset.sort_values(args.sort, ascending=not args.desc)
        else:
            print(f"  [warning] Sort column '{args.sort}' not in display columns; sorting ignored.")

    if args.limit:
        subset = subset.head(args.limit)

    if len(subset) == 0:
        print("\nNo players matched your criteria.\n")
        return

    print(f"\n{'─'*60}")
    print(f"  {len(subset)} result(s) found")
    print(f"{'─'*60}\n")

    # Pandas display options
    if args.no_truncate:
        with pd.option_context("display.max_columns", None,
                               "display.max_rows", None,
                               "display.width", None,
                               "display.max_colwidth", None):
            print(subset.to_string(index=False))
    else:
        with pd.option_context("display.max_columns", None,
                               "display.max_rows", None,
                               "display.width", 200,
                               "display.max_colwidth", 30,
                               "display.float_format", "{:.3f}".format):
            print(subset.to_string(index=False))

    print()


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = build_parser()
    args = parser.parse_args()

    df = load_data(args.csv)

    # --list_columns shortcut
    if args.list_columns:
        print("\nAvailable columns:\n")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i:>3}. {col}")
        print()
        sys.exit(0)

    # Apply all filters
    filtered = apply_filters(df, args)

    # Determine which stat columns were actually queried
    filtered_stats = detect_filtered_stats(args)

    # Determine display columns
    display_cols = determine_display_columns(filtered, args, filtered_stats)

    # Print
    print_results(filtered, display_cols, args)


if __name__ == "__main__":
    main()
