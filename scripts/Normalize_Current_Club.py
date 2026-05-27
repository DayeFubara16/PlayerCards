"""
normalize_current_club.py
─────────────────────────
Resolves team and league fields in player_season_totals_arbitrated.csv so that:

  1. Players listed as "Multiple" in team/league get their most-recent club
     and competition, derived from player_match_logs.csv.

  2. Known same-club name aliases (e.g. "Milan" → "AC Milan", "Roma" → "AS Roma",
     "Liverpool" → "Liverpool FC") are collapsed to a single canonical form
     BEFORE the recency lookup runs. This guarantees all players at the same
     physical club share exactly one name string — even those whose last logged
     appearance happened to carry the older alias. This is critical for any
     club-level grouping or regression analysis.

     To add a new alias in future, append to TEAM_ALIASES below.

  3. Recency is defined league-aware:
     - Compute each league's maximum observed MW for the season.
     - For each player, rank their appearances by (league_max_MW - player_MW),
       then by descending MW, then take the first row. This correctly handles
       players who played in two leagues that finished on different weeks —
       the one that ended most recently wins.

Usage
─────
  python normalize_current_club.py
      [--logs   player_match_logs.csv]
      [--arb    player_season_totals_arbitrated.csv]
      [--output player_season_totals_arbitrated.csv]   # default: overwrite in place
      [--dry-run]   # print summary without writing

File structure (mirrors PlayerCards project):
  scripts/normalize_current_club.py          ← this file
  data/processed/player_match_logs.csv
  data/processed/player_season_totals_arbitrated.csv

Running from the scripts/ folder with no arguments uses the relative paths
above automatically.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Known same-club name aliases → canonical form.
#
# These are brute-force replacements applied to BOTH the match logs and the
# arbitrated file before any other logic runs.  The canonical name is the
# value; all keys that map to it are aliases that should disappear.
#
# Discovered from the 2025-26 dataset by finding player_ids that appear under
# >1 team name within the same (season, league):
#   Milan      → AC Milan      (Serie A,      same club throughout)
#   Roma       → AS Roma       (Serie A,      same club throughout)
#   Napoli     → SSC Napoli    (Serie A,      same club throughout)
#   Liverpool  → Liverpool FC  (Premier League, same club throughout)
#
# To add future aliases: insert additional "alias": "canonical" pairs below.
# ---------------------------------------------------------------------------
TEAM_ALIASES: dict[str, str] = {
    "Milan":    "AC Milan",
    "Roma":     "AS Roma",
    "Napoli":   "SSC Napoli",
    "Liverpool": "Liverpool FC",
}


def apply_team_aliases(df: pd.DataFrame, col: str = "team") -> tuple[pd.DataFrame, list[tuple[str, str]]]:
    """
    Replace every known alias in *col* with its canonical form.
    Returns the modified DataFrame and a list of (alias, canonical) pairs
    that were actually found and replaced.
    """
    out = df.copy()
    applied: list[tuple[str, str]] = []
    for alias, canonical in TEAM_ALIASES.items():
        mask = out[col] == alias
        if mask.any():
            out.loc[mask, col] = canonical
            applied.append((alias, canonical))
    return out, applied


# ---------------------------------------------------------------------------
# Default paths — relative to this script's location so they work from any
# working directory as long as the project structure is intact.
# ---------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_PROCESSED = _SCRIPT_DIR.parent / "data" / "processed"
_DATA_RAW = _SCRIPT_DIR.parent / "data" / "raw"

DEFAULT_LOGS = _DATA_RAW / "player_match_logs.csv"
DEFAULT_ARB  = _DATA_PROCESSED / "player_season_totals_arbitrated.csv"


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def build_current_club_map(logs: pd.DataFrame) -> pd.DataFrame:
    """
    Return a DataFrame indexed by (player_id, season) with columns:
      current_league, current_team

    Aliases in TEAM_ALIASES are collapsed first so that the recency lookup
    operates on canonical names throughout — even if a player's last logged
    appearance used an older alias string.

    Recency rule
    ────────────
    For each (season, league), find the maximum MW across all players.
    Each player row is then ranked by:
      1. mw_lag  = league_max_mw - row_MW   (ascending → smallest lag first)
      2. MW      (descending → latest week first, as tiebreak within same lag)
    The first row after sorting is the player's most-recent appearance.
    """
    # Collapse aliases before any grouping so that e.g. Milan MW-37 and
    # AC Milan MW-38 are treated as the same club, and the higher MW wins.
    logs, _ = apply_team_aliases(logs)

    # League-level ceiling for the season
    league_max = (
        logs.groupby(["season", "league"])["MW"]
        .max()
        .rename("league_max_mw")
        .reset_index()
    )
    enriched = logs.merge(league_max, on=["season", "league"], how="left")
    enriched["mw_lag"] = enriched["league_max_mw"] - enriched["MW"]

    # Sort so that the first row per (player_id, season) is the most-recent
    enriched_sorted = enriched.sort_values(
        ["player_id", "season", "mw_lag", "MW"],
        ascending=[True, True, True, False],
    )

    most_recent = (
        enriched_sorted
        .groupby(["player_id", "season"], sort=False)
        .first()
        .reset_index()
    )[["player_id", "season", "league", "team"]]

    most_recent = most_recent.rename(
        columns={"league": "current_league", "team": "current_team"}
    )
    return most_recent


def normalise(
    arb: pd.DataFrame,
    current_map: pd.DataFrame,
    dry_run: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """
    Merge current club info into the arbitrated DataFrame and overwrite
    the league and team columns.  Returns the updated DataFrame and a
    summary dict.

    Step 1: Apply TEAM_ALIASES directly to the arbitrated file so that
    players who already have a resolved (non-Multiple) team name but carry
    an old alias (e.g. "Milan", "Roma", "Liverpool") are corrected before
    the recency merge runs.

    Step 2: Merge the current_map (built from alias-normalised logs) to
    resolve any remaining Multiple entries.
    """
    # Step 1 — brute-force alias replacement on the arb file itself.
    # Track what changed so we can report it separately.
    arb_aliased, alias_pairs_applied = apply_team_aliases(arb)

    merged = arb_aliased.merge(current_map, on=["player_id", "season"], how="left")

    # -----------------------------------------------------------------------
    # Identify changes for the summary report
    # -----------------------------------------------------------------------
    # Alias changes: rows where arb differed from arb_aliased
    alias_mask = arb["team"] != arb_aliased["team"]
    alias_changes = (
        arb.loc[alias_mask, ["player_id", "player_name", "season", "team"]]
        .copy()
        .rename(columns={"team": "old_team"})
    )
    alias_changes["new_team"]    = arb_aliased.loc[alias_mask, "team"].values
    alias_changes["change_type"] = "standardised_alias"

    # Multiple → resolved: rows still showing Multiple after alias pass
    league_changed_mask = (
        merged["current_league"].notna()
        & (merged["league"] != merged["current_league"])
    )
    team_changed_mask = (
        merged["current_team"].notna()
        & (merged["team"] != merged["current_team"])
    )

    league_changes = (
        merged.loc[league_changed_mask, ["player_id", "player_name", "season", "league", "current_league"]]
        .rename(columns={"league": "old_league", "current_league": "new_league"})
        .copy()
    )
    team_multiple_changes = (
        merged.loc[team_changed_mask, ["player_id", "player_name", "season", "team", "current_team"]]
        .rename(columns={"team": "old_team", "current_team": "new_team"})
        .copy()
    )

    def _bucket_league(row):
        return "resolved_multiple" if str(row["old_league"]).strip().lower() == "multiple" else "standardised_alias"

    league_changes["change_type"] = league_changes.apply(_bucket_league, axis=1)
    team_multiple_changes["change_type"] = "resolved_multiple"

    # Merge alias + multiple changes into one team_changes frame for reporting
    team_changes = pd.concat([alias_changes, team_multiple_changes], ignore_index=True)

    summary = {
        "total_rows": len(arb),
        "league_changes": len(league_changes),
        "team_changes":   len(team_changes),
        "league_multiple_resolved":  int((league_changes["change_type"] == "resolved_multiple").sum()),
        "league_alias_standardised": int((league_changes["change_type"] == "standardised_alias").sum()),
        "team_multiple_resolved":    int(len(team_multiple_changes)),
        "team_alias_standardised":   int(len(alias_changes)),
        "aliases_applied":           alias_pairs_applied,
        "unmatched_in_logs":         int(merged["current_team"].isna().sum()),
        "league_change_detail":      league_changes,
        "team_change_detail":        team_changes,
    }

    if dry_run:
        return arb, summary  # return original unchanged

    # -----------------------------------------------------------------------
    # Apply changes — start from alias-normalised arb, then overlay Multiple fixes
    # -----------------------------------------------------------------------
    out = arb_aliased.copy()
    out.loc[league_changed_mask, "league"] = merged.loc[league_changed_mask, "current_league"].values
    out.loc[team_changed_mask,   "team"]   = merged.loc[team_changed_mask,   "current_team"].values

    return out, summary


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(summary: dict, dry_run: bool) -> None:
    tag = "[DRY RUN] " if dry_run else ""
    print(f"\n{tag}── normalize_current_club summary ──────────────────────────")
    print(f"  Total rows in arbitrated file : {summary['total_rows']:,}")
    print(f"  Players not found in logs     : {summary['unmatched_in_logs']:,}")
    print()
    print(f"  League changes")
    print(f"    Multiple → resolved          : {summary['league_multiple_resolved']:,}")
    print(f"    Alias standardised           : {summary['league_alias_standardised']:,}")
    print(f"    Total                        : {summary['league_changes']:,}")
    print()
    print(f"  Team changes")
    print(f"    Multiple → resolved          : {summary['team_multiple_resolved']:,}")
    print(f"    Alias standardised           : {summary['team_alias_standardised']:,}")
    print(f"    Total                        : {summary['team_changes']:,}")

    # Show which alias rules fired
    if summary["aliases_applied"]:
        print()
        print("  Alias rules applied (brute-force, all rows):")
        for alias, canonical in summary["aliases_applied"]:
            n = int((summary["team_change_detail"]["change_type"] == "standardised_alias")
                    .sum() if not summary["team_change_detail"].empty else 0)
            print(f"    '{alias}' → '{canonical}'")

    alias_teams = summary["team_change_detail"][
        summary["team_change_detail"]["change_type"] == "standardised_alias"
    ]
    if not alias_teams.empty:
        print()
        print(f"  Players affected by alias standardisation ({len(alias_teams)}):")
        for _, r in alias_teams.iterrows():
            print(f"    {r['player_name']} ({r['player_id']}): "
                  f"'{r['old_team']}' → '{r['new_team']}'" ) 

    print("────────────────────────────────────────────────────────────\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Normalise team/league fields in player_season_totals_arbitrated.csv "
                    "using the most-recent appearance in player_match_logs.csv."
    )
    ap.add_argument(
        "--logs", "-l",
        type=Path,
        default=DEFAULT_LOGS,
        help=f"Path to player_match_logs.csv (default: {DEFAULT_LOGS})",
    )
    ap.add_argument(
        "--arb", "-a",
        type=Path,
        default=DEFAULT_ARB,
        help=f"Path to player_season_totals_arbitrated.csv (default: {DEFAULT_ARB})",
    )
    ap.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Output path. Defaults to overwriting --arb in place.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would change without writing any file.",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    output_path = args.output or args.arb  # default: overwrite in place

    # ── Load ────────────────────────────────────────────────────────────────
    if not args.logs.exists():
        print(f"ERROR: match logs not found at {args.logs}", file=sys.stderr)
        sys.exit(1)
    if not args.arb.exists():
        print(f"ERROR: arbitrated file not found at {args.arb}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading match logs  : {args.logs}")
    logs = pd.read_csv(args.logs, low_memory=False)

    print(f"Loading arbitrated  : {args.arb}")
    arb  = pd.read_csv(args.arb,  low_memory=False)

    # Coerce player_id to int in both frames so the merge key is consistent
    logs["player_id"] = pd.to_numeric(logs["player_id"], errors="coerce").astype("Int64")
    arb["player_id"]  = pd.to_numeric(arb["player_id"],  errors="coerce").astype("Int64")

    # ── Build recency map ────────────────────────────────────────────────────
    print("Building most-recent-club map from match logs …")
    current_map = build_current_club_map(logs)

    # ── Normalise ────────────────────────────────────────────────────────────
    updated_arb, summary = normalise(arb, current_map, dry_run=args.dry_run)

    # ── Report ───────────────────────────────────────────────────────────────
    print_summary(summary, dry_run=args.dry_run)

    # ── Write ────────────────────────────────────────────────────────────────
    if not args.dry_run:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        updated_arb.to_csv(output_path, index=False)
        verb = "Overwrote" if output_path == args.arb else "Wrote"
        print(f"{verb}: {output_path}")
    else:
        print("Dry run complete — no files written.")


if __name__ == "__main__":
    main()
