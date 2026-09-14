"""
Extract_WhoScored_Player_Actions.py
─────────────────────────────────────
Offline layer. Reads matches already cached by Fetch_WhoScored_Matchweek.py
and pulls out one player's passes/dribbles/defensive actions/derived
carries. Makes NO network requests — if a needed match isn't cached yet,
it's reported and skipped rather than fetched on the spot, to keep the
fetch/extract boundary honest (extraction should never silently trigger
a scrape).

This is the step you re-run freely: a new player, a re-cut of an
existing player's season, a different competition slice — all of it is
just re-reading JSON already on disk from Fetch_WhoScored_Matchweek.py.

Usage:
  python Extract_WhoScored_Player_Actions.py \
    --player-id 839956 \
    --whoscored-player-id 12345 \
    --match-id-map data/raw/match_id_map.csv \
    --cache-dir cache/whoscored_cache \
    --out-dir cards/whoscored_actions \
    --format both
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import whoscored_common as wc

DEFAULT_MATCH_ID_MAP = "data/raw/match_id_map.csv"
DEFAULT_OUT_DIR = "cards/whoscored_actions"


def read_rows(path: str | Path) -> list[dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Not found: {path.resolve()}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def matches_with_whoscored_id(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if wc.first_present(row, ["whoscored_match_id"])]


def write_flat_csv(path: str | Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = sorted({k for row in rows for k in row.keys()})
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Offline extraction of one player's WhoScored actions from the cached matchweek lake.")
    ap.add_argument("--player-id", "-p", type=int, required=True, help="Sofascore player_id, used only for output naming")
    ap.add_argument("--whoscored-player-id", type=int, required=True, help="This player's WhoScored player_id")
    ap.add_argument("--player-name", type=str, default=None)
    ap.add_argument("--match-id-map", default=DEFAULT_MATCH_ID_MAP)
    ap.add_argument("--cache-dir", default=wc.DEFAULT_CACHE_DIR)
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--date-from", default=None, help="YYYY-MM-DD, inclusive — optionally narrow the slice")
    ap.add_argument("--date-to", default=None, help="YYYY-MM-DD, inclusive")
    ap.add_argument("--format", choices=["json", "csv", "both"], default="both")
    args = ap.parse_args()

    rows = read_rows(args.match_id_map)
    scoped = matches_with_whoscored_id(rows)

    if args.date_from or args.date_to:
        def in_range(row):
            date = str(wc.first_present(row, ["date"]) or "")
            if args.date_from and date < args.date_from:
                return False
            if args.date_to and date > args.date_to:
                return False
            return True
        scoped = [r for r in scoped if in_range(r)]

    print(f"\nMatches in scope: {len(scoped)}")
    if not scoped:
        raise ValueError("No matches in scope. Check --match-id-map and date filters.")

    all_flat_rows: list[dict[str, Any]] = []
    match_summaries: list[dict[str, Any]] = []
    missing_from_cache: list[str] = []

    for row in scoped:
        match_id = str(wc.first_present(row, ["whoscored_match_id"]))

        if not wc.is_cached(args.cache_dir, match_id):
            missing_from_cache.append(match_id)
            match_summaries.append({"match_id": match_id, "status": "not_cached"})
            continue

        match_centre = wc.load_cached(args.cache_dir, match_id)
        if match_centre is None:
            missing_from_cache.append(match_id)
            match_summaries.append({"match_id": match_id, "status": "cache_read_error"})
            continue

        events = wc.player_events(match_centre, args.whoscored_player_id)
        categorized = wc.categorize(events)
        categorized["carries"] = wc.derive_carries(events)

        flat = wc.flatten_categories(categorized, match_id)
        all_flat_rows.extend(flat)

        print(
            f"  match {match_id} ({row.get('date', '?')}): "
            f"{len(categorized['passes'])} passes, "
            f"{len(categorized['dribbles'])} dribbles, "
            f"{len(categorized['defensive_actions'])} defensive, "
            f"{len(categorized['carries'])} carries (derived)"
        )
        match_summaries.append({
            "match_id": match_id,
            "status": "ok",
            "passes": len(categorized["passes"]),
            "dribbles": len(categorized["dribbles"]),
            "defensive_actions": len(categorized["defensive_actions"]),
            "carries_derived": len(categorized["carries"]),
        })

    if missing_from_cache:
        print(f"\n{len(missing_from_cache)} match(es) not yet cached — run Fetch_WhoScored_Matchweek.py "
              f"for these first, then re-run this extraction (no data was fetched here):")
        for mid in missing_from_cache:
            print(f"    {mid}")

    base = f"{wc.clean_filename(args.player_name, f'player_{args.player_id}')}_whoscored"
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.format in ("json", "both"):
        out_json = out_dir / f"{base}.json"
        out_json.write_text(json.dumps({
            "meta": {
                "player_id": args.player_id,
                "whoscored_player_id": args.whoscored_player_id,
                "match_id_map": str(args.match_id_map),
                "cache_dir": str(args.cache_dir),
                "matches_in_scope": len(scoped),
                "matches_ok": sum(1 for s in match_summaries if s["status"] == "ok"),
                "matches_not_cached": len(missing_from_cache),
                "extracted_at_utc": wc.utc_now(),
                "note": "Offline extraction — no network requests made. Missing matches must be "
                        "fetched via Fetch_WhoScored_Matchweek.py first.",
            },
            "match_summaries": match_summaries,
            "actions": all_flat_rows,
        }, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nWrote {out_json}")

    if args.format in ("csv", "both"):
        out_csv = out_dir / f"{base}.csv"
        write_flat_csv(out_csv, all_flat_rows)
        print(f"Wrote {out_csv}")


if __name__ == "__main__":
    main()
