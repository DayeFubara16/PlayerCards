"""
Fetch_WhoScored_Matchweek.py
──────────────────────────────
Network layer only. Fetches full raw matchCentreData for every match in
a date range (a matchweek, typically) and caches it — with NO player
filtering. This is the "raw match lake" builder: run it once per
matchweek per league you care about, and every player who appeared in
those matches becomes a free, offline extraction afterward via
Extract_WhoScored_Player_Actions.py — no repeat network calls no matter
how many players you end up building cards for from that batch.

Intended workflow:
  1. Run match_resolver.py once you have both a Sofascore log and a
     WhoScored fixture list — produces match_id_map.csv.
  2. On whatever cadence suits you (e.g. Monday, after the weekend's
     matches have all finished), run this script scoped to the
     matchweek/date range/competition you need:

       python Fetch_WhoScored_Matchweek.py \
         --match-id-map data/raw/match_id_map.csv \
         --date-from 2026-01-09 --date-to 2026-01-12 \
         --competition "Premier League" \
         --cache-dir cache/whoscored_cache \
         --delay 7.0

  3. Later, any time, for any player who played in that window:

       python Extract_WhoScored_Player_Actions.py \
         --whoscored-player-id 12345 \
         --match-id-map data/raw/match_id_map.csv \
         --cache-dir cache/whoscored_cache

     That step touches disk only — no network, no rate-limit risk,
     reusable indefinitely.

--dry-run lists what would be fetched (and what's already cached)
without making any requests — useful for sanity-checking scope before
committing to a scrape.
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path
from typing import Any

import whoscored_common as wc

DEFAULT_MATCH_ID_MAP = "data/raw/match_id_map.csv"


def read_rows(path: str | Path) -> list[dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Not found: {path.resolve()}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def filter_scope(
    rows: list[dict[str, Any]],
    date_from: str | None,
    date_to: str | None,
    competition: str | None,
    round_filter: str | None,
) -> list[dict[str, Any]]:
    scoped = []
    for row in rows:
        ws_id = wc.first_present(row, ["whoscored_match_id"])
        if not ws_id:
            continue  # nothing to fetch — this game never resolved to a WhoScored ID

        date = wc.first_present(row, ["date"])
        if date_from and (not date or str(date) < date_from):
            continue
        if date_to and (not date or str(date) > date_to):
            continue

        if competition:
            row_comp = str(wc.first_present(row, ["competition"]) or "")
            if row_comp.lower() != competition.lower():
                continue

        if round_filter:
            row_round = str(wc.first_present(row, ["round"]) or "")
            if row_round != round_filter:
                continue

        scoped.append(row)
    return scoped


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch and cache full WhoScored matchCentreData for a matchweek/date range — no player scoping."
    )
    ap.add_argument("--match-id-map", default=DEFAULT_MATCH_ID_MAP)
    ap.add_argument("--date-from", default=None, help="YYYY-MM-DD, inclusive")
    ap.add_argument("--date-to", default=None, help="YYYY-MM-DD, inclusive")
    ap.add_argument("--competition", default=None, help="Filter to one competition, e.g. 'Premier League'")
    ap.add_argument("--round", dest="round_filter", default=None, help="Filter to one round/gameweek value, if present in match_id_map.csv")
    ap.add_argument("--cache-dir", default=wc.DEFAULT_CACHE_DIR)
    ap.add_argument("--delay", type=float, default=wc.DEFAULT_DELAY)
    ap.add_argument("--no-browser-fallback", action="store_true", help="Disable Selenium fallback; HTTP-only")
    ap.add_argument("--refresh-cache", action="store_true", help="Refetch even if already cached")
    ap.add_argument("--dry-run", action="store_true", help="List scope without fetching anything")
    args = ap.parse_args()

    rows = read_rows(args.match_id_map)
    scoped = filter_scope(rows, args.date_from, args.date_to, args.competition, args.round_filter)

    if not scoped:
        print("No matches in scope. Check your date range/competition/round filters against match_id_map.csv.")
        return

    already_cached = sum(
        1 for row in scoped
        if wc.is_cached(args.cache_dir, str(wc.first_present(row, ["whoscored_match_id"])))
    )
    print(f"\nMatches in scope: {len(scoped)}")
    print(f"Already cached:   {already_cached}")
    print(f"To fetch:         {len(scoped) - already_cached if not args.refresh_cache else len(scoped)}\n")

    if args.dry_run:
        for row in scoped:
            ws_id = wc.first_present(row, ["whoscored_match_id"])
            cached = wc.is_cached(args.cache_dir, str(ws_id)) and not args.refresh_cache
            print(f"  [{'cached' if cached else 'pending'}] {ws_id}  {row.get('date','?')}  "
                  f"{row.get('home_team','?')} vs {row.get('away_team','?')}")
        print("\n(dry run — no requests made)")
        return

    ok, failed, skipped = 0, [], 0
    for i, row in enumerate(scoped, start=1):
        match_id = str(wc.first_present(row, ["whoscored_match_id"]))
        label = f"  [{i}/{len(scoped)}] {match_id}  {row.get('date','?')}  {row.get('home_team','?')} vs {row.get('away_team','?')}"

        result = wc.fetch_match(
            match_id=match_id,
            cache_dir=args.cache_dir,
            refresh_cache=args.refresh_cache,
            allow_browser_fallback=not args.no_browser_fallback,
        )

        if result.ok:
            ok += 1
            print(f"{label}: [{result.source}]")
        else:
            failed.append((match_id, result.error))
            print(f"{label}: FAILED ({result.error})")

        if result.source not in ("cache",) and args.delay > 0:
            time.sleep(args.delay)

    print(f"\n{'─' * 50}")
    print(f"  Fetched/verified OK: {ok}")
    print(f"  Failed:              {len(failed)}")
    if failed:
        print("  Failed match_ids (retry these, e.g. after checking selenium install or waiting out a block):")
        for mid, err in failed:
            print(f"    {mid}: {err}")
    print(f"  Cache dir: {Path(args.cache_dir).resolve()}")
    print(f"{'─' * 50}")


if __name__ == "__main__":
    main()
