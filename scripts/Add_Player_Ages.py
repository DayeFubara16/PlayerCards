"""
Add_Player_Ages.py
──────────────────
Enrich player_match_logs.csv with player date_of_birth and age by fetching each
unique Sofascore player profile once via:

    /player/{player_id}

This is designed as a one-run enrichment utility:
  • reads the full match log
  • deduplicates player_id values
  • fetches DOB once per player
  • calculates age as of a configurable date
  • writes an enriched CSV

Usage:
    python Add_Player_Ages.py
    python Add_Player_Ages.py --input player_match_logs.csv --output player_match_logs_with_ages.csv
    python Add_Player_Ages.py --input player_match_logs.csv --in-place
    python Add_Player_Ages.py --as-of 2026-04-24 --delay 0.4
    python Add_Player_Ages.py --only-missing

Output columns added/updated:
  • date_of_birth
  • age
  • age_as_of
  • profile_name
  • nationality
  • height_cm
  • preferred_foot

A JSON cache is used by default. V2 can also seed the cache from an already-enriched CSV, so new match-log versions reuse existing DOB/profile data and only fetch genuinely new player IDs.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

try:
    from curl_cffi import requests as cf_requests
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: curl_cffi\n"
        "Install it with: pip install curl_cffi"
    ) from exc


API_BASE = "https://api.sofascore.com/api/v1"
DEFAULT_INPUT = "data/raw/player_match_logs.csv"
DEFAULT_OUTPUT = "data/processed/player_match_logs_with_ages.csv"
DEFAULT_CACHE = "cache/player_profile_cache.json"
REQUEST_DELAY = 0.5

_session = cf_requests.Session(impersonate="safari")
_session.headers.update({
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.sofascore.com",
    "Referer": "https://www.sofascore.com/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
})


def _get(path: str, retries: int = 3) -> dict[str, Any] | None:
    url = f"{API_BASE}/{path.lstrip('/')}"
    for attempt in range(retries):
        try:
            response = _session.get(url, timeout=20)

            if response.status_code == 429:
                wait = max(1.0, REQUEST_DELAY * 4) * (2 ** attempt)
                print(f"    [rate limited] sleeping {wait:.1f}s ...")
                time.sleep(wait)
                continue

            if response.status_code == 404:
                return None

            if not response.ok:
                snippet = response.text[:220].replace("\n", " ")
                print(f"    [HTTP {response.status_code}] {path} | {snippet}")
                return None

            if REQUEST_DELAY > 0:
                time.sleep(REQUEST_DELAY)

            data = response.json()
            return data if isinstance(data, dict) else None

        except Exception as exc:
            if attempt < retries - 1:
                print(f"    [retry {attempt + 1}/{retries}] {exc}")
                time.sleep(1.0 * (attempt + 1))
            else:
                print(f"    [error] {url}: {exc}")

    return None


def _as_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def _timestamp_to_date(ts: Any) -> str | None:
    ts_int = _as_int(ts)
    if ts_int is None:
        return None
    try:
        return datetime.fromtimestamp(ts_int, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return None


def parse_date(value: str | None) -> date:
    if not value:
        return datetime.now(timezone.utc).date()
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit("--as-of must use YYYY-MM-DD format, e.g. 2026-04-24") from exc


def calculate_age(dob: str | None, as_of: date) -> int | None:
    if not dob:
        return None
    try:
        born = datetime.strptime(dob, "%Y-%m-%d").date()
    except ValueError:
        return None

    age = as_of.year - born.year
    if (as_of.month, as_of.day) < (born.month, born.day):
        age -= 1
    return age


def load_csv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {path.resolve()}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    if not rows:
        raise ValueError(f"Input CSV is empty: {path.resolve()}")

    if "player_id" not in fieldnames:
        raise ValueError("Input CSV must contain a player_id column.")

    return rows, fieldnames


def load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        print(f"  Warning: could not read cache {path}; starting fresh.")
        return {}


def save_cache(path: Path, cache: dict[str, Any]) -> None:
    path.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")



def profile_from_enriched_row(row: dict[str, Any]) -> dict[str, Any] | None:
    player_id = _as_int(row.get("player_id"))
    if player_id is None:
        return None

    dob = (row.get("date_of_birth") or "").strip() or None
    profile_name = (row.get("profile_name") or row.get("player_name") or "").strip() or None
    nationality = (row.get("nationality") or "").strip() or None
    height_cm = row.get("height_cm") or None
    preferred_foot = (row.get("preferred_foot") or "").strip() or None

    if not any([dob, profile_name, nationality, height_cm, preferred_foot]):
        return None

    return {
        "player_id": player_id,
        "profile_found": True,
        "date_of_birth": dob,
        "profile_name": profile_name,
        "nationality": nationality,
        "height_cm": height_cm,
        "preferred_foot": preferred_foot,
        "cache_seed_source": "enriched_csv",
    }


def seed_cache_from_enriched_csv(cache: dict[str, Any], csv_path: Path) -> tuple[int, int]:
    if not csv_path.exists():
        return 0, 0

    try:
        seed_rows, _ = load_csv(csv_path)
    except Exception as exc:
        print(f"  Warning: could not seed cache from {csv_path}: {exc}")
        return 0, 0

    updates = 0
    dob_seen = 0

    for row in seed_rows:
        profile = profile_from_enriched_row(row)
        if not profile:
            continue

        key = str(profile["player_id"])
        existing = cache.get(key)

        if profile.get("date_of_birth"):
            dob_seen += 1

        if cache_profile_has_dob(existing):
            continue

        if profile.get("date_of_birth") or not isinstance(existing, dict):
            cache[key] = profile
            updates += 1
        elif isinstance(existing, dict):
            merged = dict(existing)
            for field in ["profile_name", "nationality", "height_cm", "preferred_foot"]:
                if not merged.get(field) and profile.get(field):
                    merged[field] = profile[field]
            if merged != existing:
                cache[key] = merged
                updates += 1

    return updates, dob_seen



def cache_profile_has_dob(profile: Any) -> bool:
    """Return True only for positive cache entries that contain a usable DOB."""
    return isinstance(profile, dict) and bool(profile.get("date_of_birth"))


def cache_profile_is_usable(profile: Any, require_dob: bool = True) -> bool:
    """
    Decide whether a cache entry is safe to reuse.

    By default, only positive DOB-bearing entries are reusable. This means
    cached None/{}/not-found/temporary-error results are retried on later runs.
    """
    if not isinstance(profile, dict):
        return False
    if require_dob:
        return cache_profile_has_dob(profile)
    return bool(profile.get("profile_found"))

def fetch_player_profile(player_id: int) -> dict[str, Any]:
    data = _get(f"player/{player_id}")
    if not data:
        return {
            "player_id": player_id,
            "profile_found": False,
            "date_of_birth": None,
            "profile_name": None,
            "nationality": None,
            "height_cm": None,
            "preferred_foot": None,
        }

    player = data.get("player") or data
    country = player.get("country") or player.get("nationality") or {}
    nationality = country.get("name") if isinstance(country, dict) else country

    return {
        "player_id": player_id,
        "profile_found": True,
        "date_of_birth": _timestamp_to_date(player.get("dateOfBirthTimestamp")),
        "profile_name": player.get("name") or player.get("shortName"),
        "nationality": nationality,
        "height_cm": player.get("height"),
        "preferred_foot": player.get("preferredFoot"),
    }


def get_unique_player_ids(rows: list[dict[str, Any]]) -> list[int]:
    seen: set[int] = set()
    player_ids: list[int] = []

    for row in rows:
        player_id = _as_int(row.get("player_id"))
        if player_id is None or player_id in seen:
            continue
        seen.add(player_id)
        player_ids.append(player_id)

    return player_ids


def enrich_rows(
    rows: list[dict[str, Any]],
    profiles: dict[int, dict[str, Any]],
    as_of: date,
) -> None:
    as_of_str = as_of.isoformat()

    for row in rows:
        player_id = _as_int(row.get("player_id"))
        profile = profiles.get(player_id or -1, {})

        dob = profile.get("date_of_birth")
        row["date_of_birth"] = dob or ""
        row["age"] = calculate_age(dob, as_of) if dob else ""
        row["age_as_of"] = as_of_str
        row["profile_name"] = profile.get("profile_name") or ""
        row["nationality"] = profile.get("nationality") or ""
        row["height_cm"] = profile.get("height_cm") or ""
        row["preferred_foot"] = profile.get("preferred_foot") or ""


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    added_columns = [
        "date_of_birth",
        "age",
        "age_as_of",
        "profile_name",
        "nationality",
        "height_cm",
        "preferred_foot",
    ]

    final_fieldnames = list(fieldnames)
    for column in added_columns:
        if column not in final_fieldnames:
            final_fieldnames.append(column)

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=final_fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    global REQUEST_DELAY

    ap = argparse.ArgumentParser(description="Add DOB and age columns to player_match_logs.csv using Sofascore player profiles.")
    ap.add_argument("--input", "-i", default=DEFAULT_INPUT, help="Input match log CSV.")
    ap.add_argument("--output", "-o", default=DEFAULT_OUTPUT, help="Output enriched CSV.")
    ap.add_argument("--in-place", action="store_true", help="Overwrite the input CSV instead of writing a separate output.")
    ap.add_argument("--as-of", default=None, help="Date for age calculation, YYYY-MM-DD. Defaults to today UTC.")
    ap.add_argument("--delay", type=float, default=REQUEST_DELAY, help="Seconds between profile requests.")
    ap.add_argument("--cache", default=DEFAULT_CACHE, help="JSON profile cache path.")
    ap.add_argument("--seed-from", default=None, help="Already-enriched CSV to seed DOB/profile cache before fetching.")
    ap.add_argument("--no-auto-seed-output", action="store_true", help="Do not auto-seed from an existing output CSV.")
    ap.add_argument("--no-cache", action="store_true", help="Disable reading/writing the profile cache.")
    ap.add_argument(
        "--only-missing",
        action="store_true",
        help="Only fetch profiles for rows missing date_of_birth/age; useful if partially enriched.",
    )
    args = ap.parse_args()

    REQUEST_DELAY = max(0.0, args.delay)

    input_path = Path(args.input)
    output_path = input_path if args.in_place else Path(args.output)
    cache_path = Path(args.cache)
    as_of = parse_date(args.as_of)

    print(f"\nReading: {input_path}")
    rows, fieldnames = load_csv(input_path)

    player_ids = get_unique_player_ids(rows)
    if args.only_missing:
        missing_ids = []
        for pid in player_ids:
            player_rows = [r for r in rows if _as_int(r.get("player_id")) == pid]
            if any(not r.get("date_of_birth") or not r.get("age") for r in player_rows):
                missing_ids.append(pid)
        player_ids = missing_ids

    print(f"Rows:              {len(rows)}")
    print(f"Unique player IDs: {len(player_ids)}")
    print(f"Age as of:         {as_of.isoformat()}")

    cache: dict[str, Any] = {} if args.no_cache else load_cache(cache_path)

    seeded_updates = 0
    seeded_dobs_seen = 0

    if not args.no_cache:
        seed_paths: list[Path] = []

        if args.seed_from:
            seed_paths.append(Path(args.seed_from))

        if not args.no_auto_seed_output and output_path.exists() and output_path.resolve() != input_path.resolve():
            seed_paths.append(output_path)

        default_enriched = Path(DEFAULT_OUTPUT)
        if not args.no_auto_seed_output and default_enriched.exists() and default_enriched not in seed_paths:
            seed_paths.append(default_enriched)

        for seed_path in seed_paths:
            updates, dob_seen = seed_cache_from_enriched_csv(cache, seed_path)
            seeded_updates += updates
            seeded_dobs_seen += dob_seen
            if updates or dob_seen:
                print(f"Seeded cache from {seed_path}: updates={updates}, DOB rows seen={dob_seen}")

        if seeded_updates:
            save_cache(cache_path, cache)

    profiles: dict[int, dict[str, Any]] = {}
    cache_hits = 0
    retried_cache_misses = 0
    fetched = 0

    for i, player_id in enumerate(player_ids, start=1):
        cache_key = str(player_id)
        cached_profile = cache.get(cache_key) if not args.no_cache else None

        if not args.no_cache and cache_profile_is_usable(cached_profile, require_dob=True):
            profile = cached_profile
            cache_hits += 1
            print(f"  [{i}/{len(player_ids)}] {player_id}: cache")
        else:
            if not args.no_cache and cache_key in cache:
                retried_cache_misses += 1
                print(f"  [{i}/{len(player_ids)}] {player_id}: retrying missing/null cache ...", end=" ", flush=True)
            else:
                print(f"  [{i}/{len(player_ids)}] {player_id}: fetching ...", end=" ", flush=True)

            fetched += 1
            profile = fetch_player_profile(player_id)
            print("OK" if profile.get("date_of_birth") else "no DOB")

            if not args.no_cache:
                cache[cache_key] = profile
                # Save periodically so progress is not lost on interruption.
                if i % 50 == 0:
                    save_cache(cache_path, cache)

        profiles[player_id] = profile

    # If --only-missing was used, keep existing cached/new profiles for all other IDs too.
    if args.only_missing:
        for row in rows:
            pid = _as_int(row.get("player_id"))
            if pid is None or pid in profiles:
                continue
            cache_profile = cache.get(str(pid)) if not args.no_cache else None
            if isinstance(cache_profile, dict):
                profiles[pid] = cache_profile
            else:
                profiles[pid] = {
                    "player_id": pid,
                    "date_of_birth": row.get("date_of_birth") or None,
                    "profile_name": row.get("profile_name") or row.get("player_name"),
                    "nationality": row.get("nationality") or None,
                    "height_cm": row.get("height_cm") or None,
                    "preferred_foot": row.get("preferred_foot") or None,
                }

    if not args.no_cache:
        save_cache(cache_path, cache)

    enrich_rows(rows, profiles, as_of)
    write_csv(output_path, rows, fieldnames)

    found = sum(1 for p in profiles.values() if p.get("date_of_birth"))
    print(f"\nDone.")
    if not args.no_cache:
        print(f"Cache seeded/updated from CSV: {seeded_updates}")
        print(f"Seed DOB rows seen:            {seeded_dobs_seen}")
        print(f"Cache DOB hits:                {cache_hits}")
        print(f"Cache retried:                 {retried_cache_misses}")
    print(f"Fetched this run:  {fetched}")
    print(f"Profiles with DOB: {found}/{len(profiles)}")
    print(f"Output:            {output_path}")


if __name__ == "__main__":
    main()
