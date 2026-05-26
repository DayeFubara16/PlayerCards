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

# CBs who win >60% of aerial duels (pair with volume filter to avoid small samples):
python Player_Finder.py --position CB --aerial_win_pct_min 60 --aerial_duels_total_min 30 --minutes_played_min 900

# CBs by possession-adjusted tackles per 90 (P-Adj accounts for how much the opponent had the ball):
python scripts/Player_Finder.py --position CB --P-Adj_tackles_total_per90_min 4.0 --sort P-Adj_tackles_total_per90 --desc

# Top 20 playmakers by key passes, showing selected columns:
python scripts/Player_Finder.py --key_passes_min 20 --sort key_passes --desc --limit 20 --show player_name,team,league,key_passes,xa,assists

# Goalkeepers with the best goals prevented:
python Player_Finder.py --position GK --gk_goals_prevented_min 1 --sort gk_goals_prevented --desc

# Search by name (partial, case-insensitive):
python Player_Finder.py --name "Messi"

# Show all available column names:
python Player_Finder.py --list_columns

Run with --help for the full list of filters.

--export_excel FILENAME allows the use of exporting the file to an excel sheet for later analysis
# Examples
1.) Exporting the strikers with 15+ goals
python Player_Finder.py --position ST --goals_min 15 --export_excel top_strikers

Notes on stat types
-------------------
Counting stats (goals, tackles, etc.) represent raw season totals.
Per-90 stats (_per90 suffix) normalise those totals to a 90-minute rate.
  Every counting stat that has a per-90 version is exposed as both.
Percentage/ratio stats (pass_accuracy_pct, aerial_win_pct, etc.) are
  already rate-normalised and have no per-90 equivalent.
P-Adj stats (P-Adj_ prefix) are possession-adjusted defensive counting
  stats. The adjustment scales each match's figure by (oppo_poss_pct / 50),
  so players who face ball-dominant opponents are rewarded and those who
  face low-possession opponents are scaled down. P-Adj stats also have
  per-90 equivalents (P-Adj_tackles_total_per90, etc.).
  Raw defensive equivalents (tackles_total, interceptions, clearances,
  recoveries) no longer appear in this dataset — use P-Adj versions instead.
  Duels, aerial duels, and blocked shots are NOT possession-adjusted
  (consistent with StatsBomb/Opta/FBref methodology) and remain as-is.
"""

import argparse
import sys
import os
import pandas as pd

# ── Constants ──────────────────────────────────────────────────────────────────

DEFAULT_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "player_season_totals_arbitrated.csv")
EXPORT_DIR = r"C:\Users\DayeF\Documents\PlayerCards\data\queries"

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

# ── Numeric stats ──────────────────────────────────────────────────────────────
# Structure: every entry is either:
#   - A plain stat name   → exposed as --STAT_min / --STAT_max only
#   - Paired with _per90  → also exposed as --STAT_per90_min / --STAT_per90_max
#
# Percentage / ratio stats (already rate-normalised) have no per-90 equivalent.
# Possession-adjusted (P-Adj_*) defensive stats replace the raw versions that
# were dropped from the dataset. Their per-90 variants are also included.
# Spatial coordinate averages (avg_x, season_avg_x, etc.) have per-90 cols in
# the CSV but those are nonsensical for filtering; only the raw avg is exposed.

NUMERIC_STATS = [
    # ── Playing time (no per-90; these ARE the denominator) ───────────────────
    "matches",
    "minutes_played",
    "age",
    "height_cm",

    # ── Attacking — counting + per-90 ─────────────────────────────────────────
    "goals",                            "goals_per90",
    "assists",                          "assists_per90",
    "shots_total",                      "shots_total_per90",
    "shots_on_target",                  "shots_on_target_per90",
    "shots_off_target",                 "shots_off_target_per90",
    "xg",                               "xg_per90",
    "xgot",                             "xgot_per90",
    "xa",                               "xa_per90",
    "big_chances_created",              "big_chances_created_per90",
    "big_chance_missed",                "big_chance_missed_per90",
    "offsides",                         "offsides_per90",
    "hit_woodwork",                     "hit_woodwork_per90",
    "penalties_won",                    "penalties_won_per90",

    # ── Passing — counting + per-90 ───────────────────────────────────────────
    "passes_total",                     "passes_total_per90",
    "passes_accurate",                  "passes_accurate_per90",
    "passes_own_half_total",            "passes_own_half_total_per90",
    "passes_own_half_accurate",         "passes_own_half_accurate_per90",
    "passes_opposition_half_total",     "passes_opposition_half_total_per90",
    "passes_opposition_half_accurate",  "passes_opposition_half_accurate_per90",
    "key_passes",                       "key_passes_per90",
    "long_balls_total",                 "long_balls_total_per90",
    "long_balls_accurate",              "long_balls_accurate_per90",
    "crosses_total",                    "crosses_total_per90",
    "crosses_accurate",                 "crosses_accurate_per90",
    # Passing rate — no per-90 (already a percentage)
    "pass_accuracy_pct",

    # ── Carrying / dribbling — counting + per-90 ──────────────────────────────
    "touches",                          "touches_per90",
    "unsuccessful_touches",             "unsuccessful_touches_per90",
    "dribbles_attempted",               "dribbles_attempted_per90",
    "dribbles_won",                     "dribbles_won_per90",
    "carries",                          "carries_per90",
    "carry_distance",                   "carry_distance_per90",
    "progressive_carries",              "progressive_carries_per90",
    "progressive_carry_distance",       "progressive_carry_distance_per90",
    "best_carry_progression",           "best_carry_progression_per90",
    "total_progression",                "total_progression_per90",
    "dispossessed",                     "dispossessed_per90",
    "possession_lost",                  "possession_lost_per90",
    # Dribble rate — no per-90 (already a percentage)
    "dribble_success_pct",

    # ── Possession-adjusted (P-Adj) defensive stats — counting + per-90 ───────
    # Raw tackles_total, tackles_won, interceptions, clearances, recoveries have
    # been removed from this dataset and replaced by these P-Adj versions.
    # Adjustment formula per match: raw_stat × (oppo_poss_pct / 50).
    # A neutral 50/50 game gives a multiplier of 1.0; facing a ball-dominant
    # opponent (e.g. 65% poss) gives ×1.30; a low-poss opponent gives <1.0.
    "P-Adj_tackles_total",              "P-Adj_tackles_total_per90",
    "P-Adj_tackles_won",                "P-Adj_tackles_won_per90",
    "P-Adj_interceptions",              "P-Adj_interceptions_per90",
    "P-Adj_clearances",                 "P-Adj_clearances_per90",
    "P-Adj_recoveries",                 "P-Adj_recoveries_per90",

    # ── Non-adjusted defensive stats — counting + per-90 ──────────────────────
    # These are NOT possession-adjusted (consistent with StatsBomb/Opta/FBref):
    # duels include offensive actions; aerials are 50/50 contests; blocked shots
    # depend on shots faced; last-man tackles and clearance_off_line are too
    # rare and context-specific to adjust reliably.
    "last_man_tackles",                 "last_man_tackles_per90",
    "clearance_off_line",               "clearance_off_line_per90",
    "blocked_shots",                    "blocked_shots_per90",
    "duels_total",                      "duels_total_per90",
    "duels_won",                        "duels_won_per90",
    "duels_lost",                       "duels_lost_per90",
    "aerial_duels_total",               "aerial_duels_total_per90",
    "aerial_duels_won",                 "aerial_duels_won_per90",
    "aerial_duels_lost",                "aerial_duels_lost_per90",
    "challenges_lost",                  "challenges_lost_per90",
    # Win-rate ratios — no per-90 (already percentages)
    "duel_win_pct",
    "aerial_win_pct",

    # ── Errors & discipline — counting + per-90 ───────────────────────────────
    "errors_leading_to_shot",           "errors_leading_to_shot_per90",
    "errors_leading_to_goal",           "errors_leading_to_goal_per90",
    "fouls_committed",                  "fouls_committed_per90",
    "fouls_drawn",                      "fouls_drawn_per90",
    "yellow_cards",                     "yellow_cards_per90",
    "red_cards",                        "red_cards_per90",
    "penalties_conceded",               "penalties_conceded_per90",

    # ── Goalkeeping — counting + per-90 ───────────────────────────────────────
    "gk_saves",                         "gk_saves_per90",
    "gk_saves_inside_box",              "gk_saves_inside_box_per90",
    "gk_xgot_faced",                    "gk_xgot_faced_per90",
    "gk_goals_prevented",               "gk_goals_prevented_per90",
    "gk_goals_prevented_raw",           "gk_goals_prevented_raw_per90",
    "gk_high_claims",                   "gk_high_claims_per90",
    "gk_punches",                       "gk_punches_per90",
    "gk_sweeper_total",                 "gk_sweeper_total_per90",
    "gk_sweeper_accurate",              "gk_sweeper_accurate_per90",

    # ── Physical / distance — counting + per-90 ───────────────────────────────
    "distance_walking_km",              "distance_walking_km_per90",
    "distance_jogging_km",              "distance_jogging_km_per90",
    "distance_running_km",              "distance_running_km_per90",
    "distance_high_speed_running_km",   "distance_high_speed_running_km_per90",
    "distance_sprinting_km",            "distance_sprinting_km_per90",

    # ── Spatial / positioning ─────────────────────────────────────────────────
    # Average x/y coordinates (pitch position). No per-90 exposed — the per-90
    # versions in the CSV are artefacts of the collapse pipeline, not meaningful.
    "avg_x",
    "avg_y",
    "season_avg_x",
    "season_avg_y",
    # Spatial percentage breakdowns — already proportions, no per-90
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

    # ── Arbitration metadata ───────────────────────────────────────────────────
    # Useful for filtering by how confident the position arbitrator was.
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
                   help="Path to the CSV database (default: ../data/processed/ relative to this script)")

    # ── Categorical filters ────────────────────────────────────────────────────
    cat_group = p.add_argument_group("Categorical / identity filters")
    cat_group.add_argument("--name",        metavar="STR",  help="Player name (partial, case-insensitive)")
    cat_group.add_argument("--team",        metavar="STR",  help="Team name (partial, case-insensitive)")
    cat_group.add_argument("--league",      metavar="STR",  help="League name (partial, case-insensitive)")
    cat_group.add_argument("--nationality", metavar="STR",  help="Nationality (partial, case-insensitive)")
    cat_group.add_argument("--position",    metavar="POS",  help="Arbitrated position, e.g. CB, ST, CM, GK (exact match)")
    cat_group.add_argument("--role_group",  metavar="STR",  help="Role group, e.g. 'defence', 'midfield' (partial)")
    cat_group.add_argument("--lane",        metavar="STR",  help="Arbitrated lane, e.g. 'Central Defence' (partial)")
    cat_group.add_argument("--preferred_foot", metavar="STR", help="'Left', 'Right', or 'Both'")
    cat_group.add_argument("--season_position_zone", metavar="STR",
                           help="Spatial zone, e.g. 'deep_central', 'mid_left' (partial)")

    # ── Numeric filters (auto-generated) ──────────────────────────────────────
    num_group = p.add_argument_group(
        "Numeric stat filters",
        "Every stat listed below is exposed as --STAT_min and/or --STAT_max.\n"
        "Counting stats appear alongside their per-90 equivalent where one exists.\n"
        "P-Adj_ prefixed stats are possession-adjusted defensive totals.\n"
        "  Examples:\n"
        "    --goals_min 10\n"
        "    --xg_per90_min 0.4\n"
        "    --P-Adj_interceptions_per90_min 3.0\n"
        "    --aerial_win_pct_min 60\n"
    )
    for stat in NUMERIC_STATS:
        # argparse dest: replace hyphens with underscores for attribute access
        dest = stat.replace("-", "_")
        num_group.add_argument(f"--{stat}_min", type=float, metavar="N", dest=f"{dest}_min",
                               help=f"Min {stat}")
        num_group.add_argument(f"--{stat}_max", type=float, metavar="N", dest=f"{dest}_max",
                               help=f"Max {stat}")

    # ── Output controls ────────────────────────────────────────────────────────
    out_group = p.add_argument_group("Output / display options")
    out_group.add_argument("--export_excel", metavar="FILENAME",
                           help="Export query results to an Excel file in the queries folder")
    out_group.add_argument("--show", metavar="COL1,COL2,...",
                           help="Comma-separated columns to display (overrides defaults). Use 'all' for every column.")
    out_group.add_argument("--sort", metavar="COLUMN",
                           help="Column to sort results by")
    out_group.add_argument("--desc", action="store_true",
                           help="Sort descending (default is ascending)")
    out_group.add_argument("--limit", type=int, metavar="N",
                           help="Maximum number of results to display")
    out_group.add_argument("--no_truncate", action="store_true",
                           help="Disable column-width truncation in output")
    out_group.add_argument("--list_columns", action="store_true",
                           help="Print all available column names and exit")

    return p


# ── Core logic ─────────────────────────────────────────────────────────────────

def load_data(csv_path: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        sys.exit(
            f"ERROR: CSV not found at '{csv_path}'.\n"
            f"Ensure player_season_totals_arbitrated.csv is at data/processed/,\n"
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
        col = stat                          # actual CSV column name (may contain hyphens)
        dest = stat.replace("-", "_")       # argparse dest attribute name
        if col not in df.columns:
            continue

        min_val = getattr(args, f"{dest}_min", None)
        max_val = getattr(args, f"{dest}_max", None)

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
    """Return the CSV column names of numeric stats the user provided a bound for."""
    active = []
    for stat in NUMERIC_STATS:
        dest = stat.replace("-", "_")
        if getattr(args, f"{dest}_min", None) is not None or \
           getattr(args, f"{dest}_max", None) is not None:
            active.append(stat)   # stat is the real CSV column name
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

    export_to_excel(subset, args)

    print()

def export_to_excel(df: pd.DataFrame, args: argparse.Namespace) -> None:
    """Export results to Excel."""
    if not args.export_excel:
        return

    os.makedirs(EXPORT_DIR, exist_ok=True)

    filename = args.export_excel

    # Ensure .xlsx extension
    if not filename.lower().endswith(".xlsx"):
        filename += ".xlsx"

    output_path = os.path.join(EXPORT_DIR, filename)

    try:
        df.to_excel(output_path, index=False)
        print(f"\n[exported] Results saved to:\n{output_path}\n")
    except Exception as e:
        print(f"\n[error] Failed to export Excel file:\n{e}\n")


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

    filtered = apply_filters(df, args)
    filtered_stats = detect_filtered_stats(args)
    display_cols = determine_display_columns(filtered, args, filtered_stats)
    print_results(filtered, display_cols, args)


if __name__ == "__main__":
    main()