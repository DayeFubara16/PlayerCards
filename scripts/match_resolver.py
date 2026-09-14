"""
match_resolver.py
──────────────────
Canonical match-ID resolver.

Problem: Sofascore and WhoScored each assign their own internal match ID.
There is no shared key between them. If both sources are scraped
independently, there is no guarantee they cover the *same* set of games
for a given player/season — one might have a postponed fixture the other
doesn't, dates might be off by a day due to timezone handling, etc.

This script builds a canonical match registry: one row per real-world
game, with both sources' IDs attached (where found), keyed on a
normalized (date, home_team, away_team) triple. Downstream scrapers
(Fetch_Player_Event_Data.py, Player_Action_Scraper.py,
Fetch_Player_WhoScored_Events.py) should join against this registry
instead of independently deciding which games are "in scope."

Inputs (flexible column names, same convention as the rest of the
pipeline):
  --sofascore-log     player_match_logs.csv-style file
                        (event_id/eventId, date, home_team, away_team, ...)
  --whoscored-fixtures  a WhoScored fixture/match list CSV
                        (match_id/whoscored_id, date, home, away, ...)

Output:
  match_id_map.csv with columns:
    canonical_key, date, home_team, away_team, competition,
    sofascore_event_id, whoscored_match_id, match_confidence, match_method

Matching strategy (two passes):
  1. Exact pass: same date + both team names normalized identically.
  2. Fuzzy pass (only for rows unmatched in pass 1): same date,
     fuzzy-score team names (rapidfuzz if available, else difflib),
     require both teams above --min-confidence.

Rows that still don't match are kept with an empty ID for the missing
source and match_method="unmatched" so they surface for manual review
rather than silently disappearing.

Usage:
  python match_resolver.py \
    --sofascore-log data/raw/player_match_logs.csv \
    --whoscored-fixtures data/raw/whoscored_fixtures.csv \
    --out data/raw/match_id_map.csv \
    --alias-file data/raw/team_aliases.json \
    --min-confidence 88
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    from rapidfuzz import fuzz as _rf_fuzz
    def _similarity(a: str, b: str) -> float:
        return _rf_fuzz.ratio(a, b)
    _FUZZ_BACKEND = "rapidfuzz"
except ImportError:
    from difflib import SequenceMatcher
    def _similarity(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).ratio() * 100
    _FUZZ_BACKEND = "difflib"


DEFAULT_MIN_CONFIDENCE = 88.0

# Seed aliases for common mismatches between site naming conventions.
# Extend via --alias-file (a flat {"site name": "canonical name"} JSON).
BUILTIN_ALIASES = {
    "man utd": "manchester united",
    "man united": "manchester united",
    "man city": "manchester city",
    "spurs": "tottenham hotspur",
    "tottenham": "tottenham hotspur",
    "wolves": "wolverhampton wanderers",
    "newcastle": "newcastle united",
    "nottm forest": "nottingham forest",
    "psg": "paris saint germain",
    "paris sg": "paris saint germain",
    "inter": "inter milan",
    "atletico madrid": "atletico de madrid",
    "atleti": "atletico de madrid",
    "bayern": "bayern munich",
    "dortmund": "borussia dortmund",
}


# ── generic CSV helpers (same convention used across the pipeline) ─────────

def first_present(row: dict[str, Any], names: list[str]) -> Any:
    lower_map = {k.lower().strip(): k for k in row.keys()}
    for name in names:
        key = lower_map.get(name.lower())
        if key is not None and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def read_rows(path: str | Path) -> list[dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Not found: {path.resolve()}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def normalize_date(raw: Any) -> str | None:
    """Best-effort normalization to YYYY-MM-DD; leaves unparseable values as-is."""
    if raw in (None, ""):
        return None
    s = str(raw).strip()
    # Unix timestamp (Sofascore startTimestamp columns sometimes leak through raw)
    if s.isdigit() and len(s) >= 9:
        from datetime import datetime, timezone
        try:
            return datetime.fromtimestamp(int(s), tz=timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            pass
    # Already ISO-ish
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    # DD/MM/YYYY or MM/DD/YYYY — keep as normalized-ish, don't guess ambiguously
    m = re.match(r"^(\d{2})[/.](\d{2})[/.](\d{4})$", s)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo}-{d}"
    return s


def normalize_team(name: Any, aliases: dict[str, str]) -> str:
    if name in (None, ""):
        return ""
    s = str(name).strip().lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = aliases.get(s, s)
    # Strip common noise words that differ by source (fc, cf, afc, calcio, etc.)
    s = re.sub(r"\b(fc|cf|afc|sc|cd|ud|as|ac)\b", "", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s


@dataclass
class MatchRecord:
    source: str
    match_id: str
    date: str | None
    home: str
    away: str
    competition: str | None
    round_label: str | None = field(default=None)
    home_norm: str = field(default="")
    away_norm: str = field(default="")


def derive_home_away(
    row: dict[str, Any],
    home_cols: list[str],
    away_cols: list[str],
    team_cols: list[str] | None,
    opponent_cols: list[str] | None,
    venue_cols: list[str] | None,
) -> tuple[Any, Any]:
    """Direct home_team/away_team columns win if present. Otherwise, fall
    back to a player-centric schema (team/opponent/venue) — common for
    per-player-per-match logs where there's no standalone fixture table."""
    home = first_present(row, home_cols)
    away = first_present(row, away_cols)
    if home is not None and away is not None:
        return home, away

    if team_cols and opponent_cols and venue_cols:
        team = first_present(row, team_cols)
        opponent = first_present(row, opponent_cols)
        venue = first_present(row, venue_cols)
        if team is not None and opponent is not None and venue is not None:
            v = str(venue).strip().lower()
            if v in ("home", "h"):
                return team, opponent
            if v in ("away", "a"):
                return opponent, team
    return None, None


def normalize_round(raw: Any) -> str | None:
    if raw in (None, ""):
        return None
    s = str(raw).strip()
    return str(int(s)) if s.isdigit() else s


def load_matches(
    rows: list[dict[str, Any]],
    source: str,
    id_cols: list[str],
    home_cols: list[str],
    away_cols: list[str],
    date_cols: list[str],
    comp_cols: list[str],
    aliases: dict[str, str],
    round_cols: list[str] | None = None,
    team_cols: list[str] | None = None,
    opponent_cols: list[str] | None = None,
    venue_cols: list[str] | None = None,
) -> list[MatchRecord]:
    seen: dict[str, MatchRecord] = {}
    for row in rows:
        match_id = first_present(row, id_cols)
        home, away = derive_home_away(row, home_cols, away_cols, team_cols, opponent_cols, venue_cols)
        if match_id is None or home is None or away is None:
            continue
        match_id = str(match_id).strip()
        if match_id in seen:
            continue
        seen[match_id] = MatchRecord(
            source=source,
            match_id=match_id,
            date=normalize_date(first_present(row, date_cols)),
            home=str(home).strip(),
            away=str(away).strip(),
            competition=first_present(row, comp_cols),
            round_label=normalize_round(first_present(row, round_cols)) if round_cols else None,
            home_norm=normalize_team(home, aliases),
            away_norm=normalize_team(away, aliases),
        )
    return list(seen.values())


# ── matching ────────────────────────────────────────────────────────────

def canonical_key(date: str | None, home_norm: str, away_norm: str) -> str:
    return f"{date}|{home_norm}|{away_norm}"


def group_key(rec: MatchRecord) -> str:
    """Group candidates by round/matchweek when available (reliable, and
    doesn't require either source to have a date column); fall back to
    date otherwise. Round beats date when both are present, since a date
    off-by-one (timezone, kickoff-vs-calendar-day) is a more common
    failure mode than a wrong matchweek number."""
    if rec.round_label:
        return f"round:{rec.round_label}"
    return f"date:{rec.date}"


def _round_sort_value(round_label: Any) -> tuple[int, int, str]:
    """Numeric rounds sort in true numeric order (1, 2, ..., 10 — not
    string order, which would put '10' before '2'). Non-numeric or blank
    round values sort after all numeric ones, then alphabetically."""
    s = str(round_label or "").strip()
    if s.isdigit():
        return (0, int(s), s)
    return (1, 0, s)


def sort_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Arrange by competition, then round (numeric-aware), then date/home
    team for stability within a round. whoscored_only rows — WhoScored
    fixtures with no resolved Sofascore counterpart — are sorted the same
    way among themselves but kept trailing as their own block rather than
    interleaved into the main sequence, since they represent leftover,
    not-yet-resolved matches rather than part of the ordered schedule."""
    def key(row: dict[str, Any]) -> tuple:
        return (
            str(row.get("competition") or ""),
            _round_sort_value(row.get("round")),
            str(row.get("date") or ""),
            str(row.get("home_team") or ""),
        )

    primary = sorted((r for r in results if r["match_method"] != "whoscored_only"), key=key)
    ws_only = sorted((r for r in results if r["match_method"] == "whoscored_only"), key=key)
    return primary + ws_only


def resolve(
    sofascore: list[MatchRecord],
    whoscored: list[MatchRecord],
    min_confidence: float,
) -> list[dict[str, Any]]:
    ws_by_group: dict[str, list[MatchRecord]] = {}
    for m in whoscored:
        ws_by_group.setdefault(group_key(m), []).append(m)

    used_ws_ids: set[str] = set()
    results: list[dict[str, Any]] = []

    for sf in sofascore:
        exact_key = canonical_key(sf.date, sf.home_norm, sf.away_norm)
        match: MatchRecord | None = None
        confidence = 0.0
        method = "unmatched"

        candidates = ws_by_group.get(group_key(sf), [])

        # Pass 1: exact, same order (home==home, away==away)
        for cand in candidates:
            if cand.match_id in used_ws_ids:
                continue
            if cand.home_norm == sf.home_norm and cand.away_norm == sf.away_norm:
                match, confidence, method = cand, 100.0, "exact"
                break

        # Pass 2: exact, swapped order — sources disagree on who's home
        if match is None:
            for cand in candidates:
                if cand.match_id in used_ws_ids:
                    continue
                if cand.home_norm == sf.away_norm and cand.away_norm == sf.home_norm:
                    match, confidence, method = cand, 100.0, "exact_swapped"
                    break

        # Pass 3: fuzzy, same order
        if match is None:
            best, best_score = None, 0.0
            for cand in candidates:
                if cand.match_id in used_ws_ids:
                    continue
                score = min(_similarity(sf.home_norm, cand.home_norm), _similarity(sf.away_norm, cand.away_norm))
                if score > best_score:
                    best, best_score = cand, score
            if best is not None and best_score >= min_confidence:
                match, confidence, method = best, round(best_score, 1), "fuzzy"

        # Pass 4: fuzzy, swapped order
        if match is None:
            best, best_score = None, 0.0
            for cand in candidates:
                if cand.match_id in used_ws_ids:
                    continue
                score = min(_similarity(sf.home_norm, cand.away_norm), _similarity(sf.away_norm, cand.home_norm))
                if score > best_score:
                    best, best_score = cand, score
            if best is not None and best_score >= min_confidence:
                match, confidence, method = best, round(best_score, 1), "fuzzy_swapped"

        if match is not None:
            used_ws_ids.add(match.match_id)

        results.append({
            "canonical_key": exact_key,
            "date": sf.date,
            "home_team": sf.home,
            "away_team": sf.away,
            "competition": sf.competition or (match.competition if match else None),
            "round": sf.round_label or "",
            "sofascore_event_id": sf.match_id,
            "whoscored_match_id": match.match_id if match else "",
            "match_confidence": confidence,
            "match_method": method,
        })

    # WhoScored fixtures that never got claimed — keep them visible rather
    # than silently dropping (e.g. WhoScored has a match Sofascore is
    # missing, or a genuine mismatch that needs a manual alias added).
    for ws in whoscored:
        if ws.match_id in used_ws_ids:
            continue
        results.append({
            "canonical_key": canonical_key(ws.date, ws.home_norm, ws.away_norm),
            "date": ws.date,
            "home_team": ws.home,
            "away_team": ws.away,
            "competition": ws.competition,
            "round": ws.round_label or "",
            "sofascore_event_id": "",
            "whoscored_match_id": ws.match_id,
            "match_confidence": 0.0,
            "match_method": "whoscored_only",
        })

    return results


def write_csv(rows: list[dict[str, Any]], out_path: str | Path) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "canonical_key", "date", "home_team", "away_team", "competition", "round",
        "sofascore_event_id", "whoscored_match_id", "match_confidence", "match_method",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_aliases(path: str | Path | None) -> dict[str, str]:
    aliases = dict(BUILTIN_ALIASES)
    if path:
        p = Path(path)
        if p.exists():
            user_aliases = json.loads(p.read_text(encoding="utf-8"))
            aliases.update({k.lower().strip(): v.lower().strip() for k, v in user_aliases.items()})
    return aliases


def main() -> None:
    ap = argparse.ArgumentParser(description="Resolve Sofascore/WhoScored match IDs into a canonical registry")
    ap.add_argument("--sofascore-log", required=True, help="player_match_logs.csv-style file")
    ap.add_argument("--whoscored-fixtures", required=True, help="WhoScored fixture list CSV")
    ap.add_argument("--out", default="data/raw/match_id_map.csv")
    ap.add_argument("--alias-file", default=None, help="Optional JSON {site_name: canonical_name} overrides")
    ap.add_argument("--min-confidence", type=float, default=DEFAULT_MIN_CONFIDENCE)
    args = ap.parse_args()

    aliases = load_aliases(args.alias_file)

    sofascore = load_matches(
        read_rows(args.sofascore_log), "sofascore",
        id_cols=["event_id", "eventId", "match_id", "id"],
        home_cols=["home_team", "homeTeam", "home"],
        away_cols=["away_team", "awayTeam", "away"],
        date_cols=["date", "match_date", "start_date", "startDate", "startTimestamp"],
        comp_cols=["league", "competition", "tournament", "uniqueTournament"],
        round_cols=["round", "roundInfo", "gameweek", "matchweek", "MW"],
        team_cols=["team"],
        opponent_cols=["opponent"],
        venue_cols=["venue"],
        aliases=aliases,
    )
    whoscored = load_matches(
        read_rows(args.whoscored_fixtures), "whoscored",
        id_cols=["match_id", "whoscored_id", "id"],
        home_cols=["home", "home_team", "homeTeam"],
        away_cols=["away", "away_team", "awayTeam"],
        date_cols=["date", "match_date", "start_date"],
        comp_cols=["competition", "league", "tournament"],
        round_cols=["round", "roundInfo", "gameweek", "matchweek"],
        aliases=aliases,
    )

    print(f"Fuzzy backend: {_FUZZ_BACKEND}")
    print(f"Sofascore matches loaded:  {len(sofascore)}")
    print(f"WhoScored matches loaded:  {len(whoscored)}")

    results = resolve(sofascore, whoscored, args.min_confidence)
    results = sort_results(results)
    write_csv(results, args.out)

    exact = sum(1 for r in results if r["match_method"] == "exact")
    exact_swapped = sum(1 for r in results if r["match_method"] == "exact_swapped")
    fuzzy = sum(1 for r in results if r["match_method"] == "fuzzy")
    fuzzy_swapped = sum(1 for r in results if r["match_method"] == "fuzzy_swapped")
    unmatched = sum(1 for r in results if r["match_method"] == "unmatched")
    ws_only = sum(1 for r in results if r["match_method"] == "whoscored_only")

    print(f"\n{'─' * 50}")
    print(f"  Exact matches:            {exact}")
    print(f"  Exact (home/away swapped): {exact_swapped}")
    print(f"  Fuzzy matches:            {fuzzy}  (>= {args.min_confidence} confidence)")
    print(f"  Fuzzy (home/away swapped): {fuzzy_swapped}")
    print(f"  Unmatched (Sofascore only): {unmatched}")
    print(f"  WhoScored-only rows:      {ws_only}")
    print(f"  Output: {Path(args.out).resolve()}")
    print(f"{'─' * 50}")
    if exact_swapped or fuzzy_swapped:
        print(f"\n{exact_swapped + fuzzy_swapped} match(es) resolved with home/away reversed between "
              f"sources — see match_method == '*_swapped' rows in the output. Worth spot-checking "
              f"which source is actually correct for those.")
    if unmatched or ws_only:
        print("\nReview rows with match_method in {unmatched, whoscored_only} — "
              "add aliases to --alias-file or check for round/competition mismatches.")


if __name__ == "__main__":
    main()