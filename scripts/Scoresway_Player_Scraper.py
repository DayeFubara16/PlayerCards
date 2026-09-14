"""
Scoresway_Player_Scraper.py
───────────────────────────
Season-campaign event data harvester using Scoresway's underlying
api.performfeed.com endpoint (Stats Perform / Opta data).

Verified URL architecture (June 2026)
──────────────────────────────────────
Scoresway organises data by competition-season, not by team.

Competition results page:
  https://www.scoresway.com/en_GB/soccer/{competition-slug}/{COMPETITION_ID}/results

Team page within competition:
  https://www.scoresway.com/en_GB/soccer/{competition-slug}/{COMPETITION_ID}/teams/{team-slug}/{TEAM_ID}

Match URL format:
  https://www.scoresway.com/en_GB/soccer/{competition-slug}/{COMPETITION_ID}/match/{home-vs-away}/{MATCH_ID}/match-summary

Critically: the MATCH_ID in the Scoresway URL IS the performfeed UUID.
No secondary page scrape is needed to extract it.

Performfeed event feed:
  https://api.performfeed.com/feeds/soccerdata/matchevent/{MATCH_ID}?_rt=b&_fmt=json

Known IDs (verified):
  Bundesliga 2024/2025:  73zebisnu1109jix9yoc09yc4
  Bundesliga 2025/2026:  2bchmrj23l9u42d68ntcekob8
  Eintracht Frankfurt:   c5hderjlkcoaze51e5wgvptk

Usage
─────
  # Auto-discover via competition results page:
  python Scoresway_Player_Scraper.py --player "Nathaniel Brown" --team "Eintracht Frankfurt" --season "2024/2025"

  # Explicit competition ID (faster, more reliable):
  python Scoresway_Player_Scraper.py --player "Nathaniel Brown" --team "Eintracht Frankfurt" --season "2024/2025" --competition-id 73zebisnu1109jix9yoc09yc4

  # Skip discovery entirely with pre-collected UUIDs:
  python Scoresway_Player_Scraper.py --player "Nathaniel Brown" --team "Eintracht Frankfurt" --season "2024/2025" --uuid-file match_uuids.txt

  # Filter to Bundesliga only (skips DFB-Pokal, UCL etc.):
  python Scoresway_Player_Scraper.py --player "Nathaniel Brown" --team "Eintracht Frankfurt" --season "2024/2025" --competition-id 73zebisnu1109jix9yoc09yc4 --league "Bundesliga"
"""

from __future__ import annotations

import argparse
import json
import re
import time
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from collections import defaultdict

import requests

# ── Constants ─────────────────────────────────────────────────────────────────

SWAY_BASE     = "https://www.scoresway.com"
FEED_BASE     = "https://api.performfeed.com/feeds/soccerdata"
REQUEST_DELAY = 1.2   # seconds between requests

# Verified Scoresway competition IDs
KNOWN_COMPETITIONS: dict[str, dict[str, str]] = {
    "bundesliga": {
        "2024/2025": "73zebisnu1109jix9yoc09yc4",
        "2025/2026": "2bchmrj23l9u42d68ntcekob8",
    },
    "premier league": {
        "2024/2025": "9n12waklv005j8r32sfjj2eqc",
        "2025/2026": "51r6ph2woavlbbpk8f29nynf8",
    },
    "champions league": {
        "2024/2025": "bam3j6qs1v79osuz7nwgy4rh0",
        "2025/2026": "2mr0u0l78k2gdsm79q56tb2fo",
    },
    "europa league": {
        "2025/2026": "7ttpe5jzya3vjhjadiemjy7mc",
    },
}

# Verified Scoresway team IDs
KNOWN_TEAMS: dict[str, str] = {
    "eintracht frankfurt": "c5hderjlkcoaze51e5wgvptk",
}

# Opta event typeId reference
OPTA_EVENT_TYPES: dict[int, str] = {
    1:  "Pass",
    2:  "Offside Pass",
    3:  "Take On",
    4:  "Foul",
    5:  "Out",
    6:  "Corner Awarded",
    7:  "Tackle",
    8:  "Interception",
    9:  "Turnover",
    10: "Save",
    11: "Claim",
    12: "Clearance",
    13: "Miss",
    14: "Post",
    15: "Attempt Saved",
    16: "Goal",
    17: "Card",
    18: "Player Off",
    19: "Player On",
    20: "Player Changed Position",
    27: "Start",
    28: "Player Off",
    29: "Player On",
    30: "Player Off (injured)",
    34: "End",
    37: "Blocked Shot",
    38: "Offside",
    40: "Failed To Block",
    41: "Pre-Match",
    44: "Ball Recovery",
    45: "Clearance Off Line",
    49: "Ball Touch",
    51: "Clearance",
    52: "Shield Ball",
    57: "Attempt",
    58: "Yellow Card",
    59: "Red Card",
    60: "Yellow/Red Card",
    61: "Blocked Shot",
    63: "Keeper Pick-Up",
    65: "Ball Punched",
    68: "Tackle Won",
    69: "Dribbled Past",
    70: "Dribble Won",
    71: "Error",
    73: "Penalty Saved",
    74: "Keeper Sweeper",
    76: "Ball Recovery",
    77: "Error Leads to Goal",
    78: "Error Leads to Attempt",
    79: "Punch",
}

OPTA_QUALIFIERS: dict[int, str] = {
    1:   "Long Ball",
    2:   "Cross",
    4:   "Through Ball",
    5:   "Free Kick Taken",
    6:   "Corner Taken",
    12:  "Assist",
    13:  "Intentional Assist",
    14:  "Big Chance",
    15:  "Key Pass",
    17:  "Assist",
    19:  "Headed",
    24:  "Right Foot",
    25:  "Left Foot",
    28:  "Own Goal",
    33:  "Penalty Taken",
    40:  "Counter Attack",
    41:  "Penalty",
    42:  "Free Kick",
    44:  "Corner",
    56:  "Goal Mouth Y",
    57:  "Goal Mouth Z",
    59:  "End X",
    60:  "End Y",
    68:  "Pass End X",
    69:  "Pass End Y",
    103: "Head",
    146: "xG",
    321: "xG",
    454: "xG On Target",
}

SHOT_TYPE_IDS = {13, 14, 15, 16, 61}


# ── HTTP helpers ───────────────────────────────────────────────────────────────

_session: requests.Session | None = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        })
    return _session


def _get_html(url: str, retries: int = 3) -> str | None:
    session = _get_session()
    session.headers.update({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": SWAY_BASE,
    })
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=20)
            if r.ok:
                time.sleep(REQUEST_DELAY + random.uniform(0, 0.4))
                return r.text
            print(f"    [HTTP {r.status_code}] {url}")
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
            else:
                return None
        except Exception as e:
            if attempt < retries - 1:
                print(f"    [retry {attempt+1}] {e}")
                time.sleep(2 * (attempt + 1))
            else:
                print(f"    [error] {url}: {e}")
    return None


def _get_json(url: str, retries: int = 3) -> dict | list | None:
    session = _get_session()
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Referer": f"{SWAY_BASE}/en_GB/soccer/",
    })
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=25)
            if r.ok:
                time.sleep(REQUEST_DELAY + random.uniform(0, 0.4))
                return r.json()
            print(f"    [HTTP {r.status_code}] {url}")
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
            else:
                return None
        except Exception as e:
            if attempt < retries - 1:
                print(f"    [retry {attempt+1}] {e}")
                time.sleep(2 * (attempt + 1))
            else:
                print(f"    [error] {url}: {e}")
    return None


# ── Competition ID lookup ──────────────────────────────────────────────────────

def resolve_competition_id(league: str | None, season: str) -> str | None:
    """Return a known competition ID, or None if not found."""
    if not league:
        return None
    key = league.lower().strip()
    season_norm = season.replace("-", "/")
    comp = KNOWN_COMPETITIONS.get(key, {})
    return comp.get(season_norm)


def search_competition_id(league: str, season: str) -> str | None:
    """
    Search Scoresway for a competition ID by fetching the competitions page.
    Falls back to regex extraction from the page HTML.
    """
    # Try competitions page
    url = f"{SWAY_BASE}/en_GB/soccer/competitions"
    html = _get_html(url)
    if not html:
        return None

    season_slug = season.replace("/", "-")
    league_slug = re.sub(r"[^\w]", "-", league.lower()).strip("-")

    # Look for competition links matching the season
    pattern = re.compile(
        rf'href="/en_GB/soccer/[^/]*{re.escape(season_slug)}[^/]*/([a-z0-9]{{18,28}})/(?:results|fixtures)"'
    )
    matches = pattern.findall(html)
    if matches:
        return matches[0]
    return None


# ── Match discovery via competition results page ───────────────────────────────

def get_competition_slug(competition_id: str, season: str) -> str | None:
    """
    Derive the competition slug from the results page redirect or a known mapping.
    We can also infer it from the league name + season.
    """
    # Known slugs
    season_slug = season.replace("/", "-")
    known_slugs = {
        "73zebisnu1109jix9yoc09yc4": f"bundesliga-{season_slug}",
        "2bchmrj23l9u42d68ntcekob8": f"bundesliga-{season_slug}",
        "9n12waklv005j8r32sfjj2eqc": f"premier-league-{season_slug}",
        "51r6ph2woavlbbpk8f29nynf8": f"premier-league-{season_slug}",
        "bam3j6qs1v79osuz7nwgy4rh0": f"uefa-champions-league-{season_slug}",
        "2mr0u0l78k2gdsm79q56tb2fo": f"uefa-champions-league-{season_slug}",
        "7ttpe5jzya3vjhjadiemjy7mc": f"uefa-europa-league-{season_slug}",
    }
    return known_slugs.get(competition_id)


def get_team_matches_from_competition(
    competition_id: str,
    competition_slug: str,
    team_id: str,
    season: str,
) -> list[dict[str, Any]]:
    """
    Fetch match IDs for a specific team from the Scoresway competition results pages.

    Scoresway paginates results — we fetch pages until we find no more matches
    or until matches no longer include the target team.

    Match URLs in the HTML look like:
      /en_GB/soccer/{comp-slug}/{comp-id}/match/{home-slug}-vs-{away-slug}/{MATCH_ID}/match-summary

    The MATCH_ID is directly the performfeed UUID.
    """
    matches: list[dict[str, Any]] = []
    seen: set[str] = set()

    # Pattern to extract match IDs and team slugs from result page HTML
    match_pattern = re.compile(
        r'/en_GB/soccer/[^/]+/[^/]+/match/([^/]+)/([a-z0-9]{18,28})/match-summary'
    )

    # Try both /results and paginated /results?page=N
    for page in range(1, 25):  # max 24 pages (season ~34 matches + cup)
        if page == 1:
            url = f"{SWAY_BASE}/en_GB/soccer/{competition_slug}/{competition_id}/results"
        else:
            url = f"{SWAY_BASE}/en_GB/soccer/{competition_slug}/{competition_id}/results?page={page}"

        print(f"      page {page}: {url}")
        html = _get_html(url)
        if not html:
            break

        page_matches = match_pattern.findall(html)
        if not page_matches:
            break   # No more match links

        new_this_page = 0
        for teams_slug, match_id in page_matches:
            if match_id in seen:
                continue
            seen.add(match_id)
            new_this_page += 1

            # Filter to team if team_id known (team_id appears in team-specific links on results page)
            # We collect all match IDs and filter by team later when we fetch the feed
            home_slug, _, away_slug = teams_slug.partition("-vs-")
            matches.append({
                "match_id": match_id,
                "home_slug": home_slug,
                "away_slug": away_slug,
                "season": season,
                "competition_id": competition_id,
                "scoresway_url": f"{SWAY_BASE}/en_GB/soccer/{competition_slug}/{competition_id}/match/{teams_slug}/{match_id}/match-summary",
            })

        # If Scoresway doesn't paginate (all results on one page), stop after first page
        if new_this_page == 0 or page == 1 and len(page_matches) < 5:
            break

        # Stop if we got no new matches (fully paginated through)
        if new_this_page == 0:
            break

    return matches


def get_team_page_matches(
    competition_id: str,
    competition_slug: str,
    team_id: str,
    team_slug: str,
    season: str,
) -> list[dict[str, Any]]:
    """
    Fetch match list from the team-specific page within a competition.
    URL: /en_GB/soccer/{comp-slug}/{comp-id}/teams/{team-slug}/{team-id}/results
    This is more targeted than the full competition results page.
    """
    url = f"{SWAY_BASE}/en_GB/soccer/{competition_slug}/{competition_id}/teams/{team_slug}/{team_id}/results"
    print(f"      {url}")
    html = _get_html(url)
    if not html:
        return []

    match_pattern = re.compile(
        r'/en_GB/soccer/[^/]+/[^/]+/match/([^/]+)/([a-z0-9]{18,28})/match-summary'
    )
    matches: list[dict[str, Any]] = []
    seen: set[str] = set()

    for teams_slug, match_id in match_pattern.findall(html):
        if match_id in seen:
            continue
        seen.add(match_id)
        home_slug, _, away_slug = teams_slug.partition("-vs-")
        matches.append({
            "match_id": match_id,
            "home_slug": home_slug,
            "away_slug": away_slug,
            "season": season,
            "competition_id": competition_id,
            "scoresway_url": f"{SWAY_BASE}/en_GB/soccer/{competition_slug}/{competition_id}/match/{teams_slug}/{match_id}/match-summary",
        })

    return matches


# ── Performfeed event feed ─────────────────────────────────────────────────────

def fetch_match_events(match_id: str) -> dict | None:
    url = f"{FEED_BASE}/matchevent/{match_id}?_rt=b&_fmt=json"
    return _get_json(url)


def parse_match_info(feed: dict) -> dict[str, Any]:
    mi = feed.get("matchInfo") or {}
    if isinstance(mi, list) and len(mi) > 0:
        mi = mi[0]

    contestants = mi.get("contestant") or []
    home_team = away_team = home_id = away_id = None
    for c in contestants:
        if c.get("position") == "home":
            home_team = c.get("name") or c.get("shortName")
            home_id = c.get("id")
        elif c.get("position") == "away":
            away_team = c.get("name") or c.get("shortName")
            away_id = c.get("id")

    competition = mi.get("competition") or {}
    venue = mi.get("venue") or {}

    return {
        "match_id": mi.get("id"),
        "date": mi.get("date") or mi.get("localDate"),
        "time": mi.get("time") or mi.get("localTime"),
        "competition_id": competition.get("id") if isinstance(competition, dict) else None,
        "competition_name": competition.get("name") if isinstance(competition, dict) else None,
        "venue": venue.get("name") if isinstance(venue, dict) else None,
        "home_team": home_team,
        "home_team_id": home_id,
        "away_team": away_team,
        "away_team_id": away_id,
        "attendance": mi.get("attendance"),
        "referee": (mi.get("referee") or {}).get("name") if isinstance(mi.get("referee"), dict) else None,
    }


def _get_qualifiers(event: dict) -> dict[int, str | None]:
    quals: dict[int, str | None] = {}
    raw = event.get("qualifier") or event.get("qualifiers") or []
    if isinstance(raw, dict):
        raw = [raw]
    for q in raw:
        if isinstance(q, dict):
            qid = q.get("qualifierId") or q.get("id")
            val = q.get("value")
            if qid is not None:
                quals[int(qid)] = val
    return quals


def _coord(event: dict, key: str) -> float | None:
    val = event.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _float_qual(quals: dict, *ids: int) -> float | None:
    for qid in ids:
        v = quals.get(qid)
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                pass
    return None


def parse_player_events(
    feed: dict,
    player_name: str,
    match_meta: dict,
) -> list[dict[str, Any]]:
    """
    Extract all events for a specific player from the Opta MA3 event stream.

    Feed structure:
      feed["liveData"]["event"] → list of event dicts
      Each event:
        typeId, periodId, timeMin, timeSec, x, y, outcome
        playerName (or pName): player name string
        playerId: Opta player ID
        teamId: Opta team ID
        qualifier: [{qualifierId, value}, ...]
    """
    live = feed.get("liveData") or {}
    raw_events = live.get("event") or []
    if isinstance(raw_events, dict):
        raw_events = [raw_events]

    player_norm = _norm_name(player_name)
    player_events: list[dict[str, Any]] = []

    for ev in raw_events:
        if not isinstance(ev, dict):
            continue

        ev_player = (
            ev.get("playerName") or ev.get("pName")
            or ev.get("player_name") or ""
        )
        if not _name_match(ev_player, player_norm):
            continue

        type_id   = int(ev.get("typeId") or 0)
        period_id = int(ev.get("periodId") or 0)
        outcome   = ev.get("outcome")
        quals     = _get_qualifiers(ev)

        # End coordinates from qualifiers
        end_x = _float_qual(quals, 59, 68)
        end_y = _float_qual(quals, 60, 69)

        # Shot
        is_shot  = type_id in SHOT_TYPE_IDS
        is_goal  = (type_id == 16)
        own_goal = is_goal and (28 in quals)

        # Pass attributes
        is_cross       = (2 in quals)
        is_long_ball   = (1 in quals)
        is_through     = (4 in quals or 212 in quals)
        is_key_pass    = (15 in quals)
        is_assist      = (12 in quals or 13 in quals or 17 in quals)
        is_headed      = (19 in quals or 103 in quals)
        is_free_kick   = (42 in quals or 5 in quals)
        is_penalty     = (41 in quals or 33 in quals)
        is_corner      = (44 in quals or 6 in quals)
        is_counter     = (40 in quals)

        xg             = _float_qual(quals, 146, 321)
        goal_mouth_y   = _float_qual(quals, 56)
        goal_mouth_z   = _float_qual(quals, 57)

        player_events.append({
            # Match context
            "match_id":        match_meta.get("match_id"),
            "date":            match_meta.get("date"),
            "competition":     match_meta.get("competition_name"),
            "home_team":       match_meta.get("home_team"),
            "away_team":       match_meta.get("away_team"),
            "venue":           match_meta.get("venue"),
            # Event core
            "event_id":        ev.get("id") or ev.get("event_id"),
            "opta_player_id":  ev.get("playerId") or ev.get("player_id"),
            "player_name":     ev_player,
            "team_id":         ev.get("teamId") or ev.get("team_id"),
            "type_id":         type_id,
            "type_name":       OPTA_EVENT_TYPES.get(type_id, f"unknown_{type_id}"),
            "period":          period_id,
            "minute":          int(ev["timeMin"]) if ev.get("timeMin") is not None else None,
            "second":          int(ev["timeSec"]) if ev.get("timeSec") is not None else None,
            "outcome":         int(outcome) if outcome is not None else None,
            "success":         bool(int(outcome) == 1) if outcome is not None else None,
            # Spatial
            "x":               _coord(ev, "x"),
            "y":               _coord(ev, "y"),
            "end_x":           end_x,
            "end_y":           end_y,
            # Shot
            "is_shot":         is_shot,
            "is_goal":         is_goal,
            "is_own_goal":     own_goal,
            "xg":              xg,
            "goal_mouth_y":    goal_mouth_y,
            "goal_mouth_z":    goal_mouth_z,
            # Pass / action
            "is_cross":        is_cross,
            "is_long_ball":    is_long_ball,
            "is_through_ball": is_through,
            "is_key_pass":     is_key_pass,
            "is_assist":       is_assist,
            "is_headed":       is_headed,
            "is_free_kick":    is_free_kick,
            "is_penalty":      is_penalty,
            "is_corner":       is_corner,
            "is_counter":      is_counter,
            # Raw qualifiers
            "qualifiers":      {str(k): v for k, v in quals.items()},
        })

    return player_events


# ── Name matching ─────────────────────────────────────────────────────────────

def _norm_name(name: str) -> str:
    import unicodedata
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    return name.lower().strip()


def _name_match(event_name: str, player_norm: str) -> bool:
    if not event_name:
        return False
    ev_norm = _norm_name(event_name)
    if ev_norm == player_norm:
        return True
    p_tokens = player_norm.split()
    e_tokens = ev_norm.split()
    if p_tokens and e_tokens:
        if p_tokens[-1] == e_tokens[-1]:
            if len(p_tokens) > 1 and len(e_tokens) > 1:
                return p_tokens[0][0] == e_tokens[0][0]
            return True
    if ev_norm in player_norm or player_norm in ev_norm:
        return True
    return False


# ── Season aggregation ────────────────────────────────────────────────────────

def aggregate_season(events: list[dict]) -> dict:
    if not events:
        return {}

    shots         = [e for e in events if e.get("is_shot")]
    goals         = [e for e in events if e.get("is_goal") and not e.get("is_own_goal")]
    passes        = [e for e in events if e.get("type_id") == 1]
    succ_passes   = [p for p in passes if p.get("success")]
    crosses       = [e for e in events if e.get("is_cross")]
    tackles       = [e for e in events if e.get("type_id") == 7]
    interceptions = [e for e in events if e.get("type_id") == 8]
    clearances    = [e for e in events if e.get("type_id") in (12, 51)]
    fouls         = [e for e in events if e.get("type_id") == 4]
    take_ons      = [e for e in events if e.get("type_id") == 3]
    key_passes    = [e for e in events if e.get("is_key_pass")]
    assists       = [e for e in events if e.get("is_assist")]
    saves         = [e for e in events if e.get("type_id") == 10]
    recoveries    = [e for e in events if e.get("type_id") in (44, 76)]
    touches       = [e for e in events if e.get("x") is not None and e.get("type_id") not in {17,18,19,27,28,29,30,34}]
    matches       = len(set(e["match_id"] for e in events if e.get("match_id")))
    xg_total      = sum(e["xg"] for e in shots if e.get("xg") is not None)

    return {
        "matches_in_feed":    matches,
        "total_events":       len(events),
        "touches_proxy":      len(touches),
        "passes":             len(passes),
        "pass_accuracy_pct":  round(len(succ_passes) / len(passes) * 100, 1) if passes else None,
        "long_balls":         sum(1 for e in passes if e.get("is_long_ball")),
        "through_balls":      sum(1 for e in passes if e.get("is_through_ball")),
        "crosses":            len(crosses),
        "cross_accuracy_pct": round(sum(1 for c in crosses if c.get("success")) / len(crosses) * 100, 1) if crosses else None,
        "shots":              len(shots),
        "shots_on_target":    sum(1 for s in shots if s.get("type_id") == 15),
        "goals":              len(goals),
        "xg_total":           round(xg_total, 4) if xg_total else None,
        "xg_per_shot":        round(xg_total / len(shots), 4) if shots and xg_total else None,
        "key_passes":         len(key_passes),
        "assists":            len(assists),
        "tackles":            len(tackles),
        "tackles_won":        sum(1 for t in tackles if t.get("success")),
        "interceptions":      len(interceptions),
        "clearances":         len(clearances),
        "fouls_committed":    len(fouls),
        "take_ons_attempted": len(take_ons),
        "take_ons_won":       sum(1 for t in take_ons if t.get("success")),
        "saves":              len(saves),
        "recoveries":         len(recoveries),
        "headed_events":      sum(1 for e in events if e.get("is_headed")),
        "counter_attacks":    sum(1 for e in events if e.get("is_counter")),
    }


def build_heatmap(events: list[dict], precision: int = 2) -> dict:
    touches = [e for e in events if e.get("x") is not None and e.get("type_id") not in {17,18,19,27,28,29,30,34}]
    cells: dict[tuple, float] = defaultdict(float)
    for e in touches:
        key = (round(e["x"], precision), round(e["y"], precision))
        cells[key] += 1.0
    points = [{"x": x, "y": y, "value": v} for (x, y), v in sorted(cells.items())]
    return {
        "points": points,
        "raw_touch_count": len(touches),
        "cell_count": len(points),
        "note": (
            "Built from all on-ball events with x/y coordinates. "
            "Opta pitch scale: 0-100 on both axes. "
            "Origin (0,0) = bottom-left corner from home team's perspective."
        ),
    }


def build_shotmap(events: list[dict]) -> dict:
    shots = [e for e in events if e.get("is_shot")]
    xg_total = sum(s["xg"] for s in shots if s.get("xg") is not None)
    return {
        "shots": shots,
        "count": len(shots),
        "goals": sum(1 for s in shots if s.get("is_goal")),
        "xg_total": round(xg_total, 4),
        "by_type": {
            "goal":      sum(1 for s in shots if s.get("type_id") == 16),
            "on_target": sum(1 for s in shots if s.get("type_id") == 15),
            "miss":      sum(1 for s in shots if s.get("type_id") == 13),
            "post":      sum(1 for s in shots if s.get("type_id") == 14),
            "blocked":   sum(1 for s in shots if s.get("type_id") in (37, 61)),
        },
    }


def build_pass_map(events: list[dict]) -> dict:
    passes = [e for e in events if e.get("type_id") == 1]
    return {
        "passes": passes,
        "count": len(passes),
        "successful": sum(1 for p in passes if p.get("success")),
        "by_type": {
            "cross":        sum(1 for p in passes if p.get("is_cross")),
            "long_ball":    sum(1 for p in passes if p.get("is_long_ball")),
            "through_ball": sum(1 for p in passes if p.get("is_through_ball")),
            "key_pass":     sum(1 for p in passes if p.get("is_key_pass")),
            "free_kick":    sum(1 for p in passes if p.get("is_free_kick")),
            "corner":       sum(1 for p in passes if p.get("is_corner")),
        },
    }


# ── UUID file loader ──────────────────────────────────────────────────────────

def load_match_uuids_from_file(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8").strip()
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return [str(u) for u in data]
        if isinstance(data, dict) and "uuids" in data:
            return [str(u) for u in data["uuids"]]
    except json.JSONDecodeError:
        pass
    return [line.strip() for line in text.splitlines() if line.strip()]


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(
    player_name: str,
    team_name: str,
    season: str,
    league: str | None = None,
    competition_id: str | None = None,
    team_id: str | None = None,
    uuid_file: Path | None = None,
    out_path: Path | None = None,
    delay: float = REQUEST_DELAY,
) -> dict[str, Any]:
    global REQUEST_DELAY
    REQUEST_DELAY = delay

    season_norm = season.replace("-", "/")  # Normalise to "2024/2025"

    print(f"\n{'═'*62}")
    print(f"  Scoresway / Opta Player Event Scraper")
    print(f"  Player:  {player_name}")
    print(f"  Team:    {team_name}")
    print(f"  Season:  {season_norm}")
    if league:
        print(f"  League:  {league}")
    print(f"{'═'*62}\n")

    # ── Step 1: Resolve IDs ───────────────────────────────────────────────────
    if not team_id:
        team_id = KNOWN_TEAMS.get(team_name.lower())
        if team_id:
            print(f"[IDs] Team ID: {team_id} (from registry)")

    if not competition_id and league:
        competition_id = resolve_competition_id(league, season_norm)
        if competition_id:
            print(f"[IDs] Competition ID: {competition_id} (from registry)")

    # ── Step 2: Collect match UUIDs ───────────────────────────────────────────
    match_records: list[dict] = []

    if uuid_file:
        if not uuid_file.exists():
            print(f"  ✖  UUID file not found: {uuid_file.resolve()}")
            return {}
        print(f"[1/4] Loading match UUIDs from: {uuid_file}")
        uuids = load_match_uuids_from_file(uuid_file)
        match_records = [{"match_id": u, "season": season_norm} for u in uuids]
        print(f"      Loaded {len(match_records)} UUIDs.")

    elif competition_id:
        comp_slug = get_competition_slug(competition_id, season_norm)
        if not comp_slug:
            season_slug = season_norm.replace("/", "-")
            comp_slug = f"soccer-{season_slug}"  # generic fallback

        team_slug = re.sub(r"[^\w]", "-", team_name.lower()).strip("-")

        print(f"[1/4] Discovering matches ...")
        print(f"      Competition: {comp_slug}/{competition_id}")

        # Try team-specific page first (most targeted)
        if team_id:
            print(f"      Strategy: team results page")
            match_records = get_team_page_matches(
                competition_id, comp_slug, team_id, team_slug, season_norm
            )

        # Fall back to full competition results
        if not match_records:
            print(f"      Strategy: full competition results page")
            match_records = get_team_matches_from_competition(
                competition_id, comp_slug, team_id or "", season_norm
            )

        print(f"      Found {len(match_records)} match IDs.")

    else:
        print(
            "\n  ⚠  No competition ID provided and could not auto-resolve one.\n"
            "\n  Options:\n"
            "  A) Pass --competition-id directly:\n"
            "       Bundesliga 2024/2025: 73zebisnu1109jix9yoc09yc4\n"
            "       Bundesliga 2025/2026: 2bchmrj23l9u42d68ntcekob8\n"
            "     e.g.  --competition-id 73zebisnu1109jix9yoc09yc4\n"
            "\n"
            "  B) Pass --league to auto-resolve:\n"
            "     e.g.  --league Bundesliga\n"
            "\n"
            "  C) Collect match UUIDs manually (DevTools → Network → Player Stats tab)\n"
            "     and pass  --uuid-file match_uuids.txt\n"
        )
        return {}

    if not match_records:
        print(
            "\n  ✖  No match IDs found.\n"
            "     Try passing match UUIDs manually via --uuid-file.\n"
            "     Instructions:\n"
            "       1. Open a Scoresway match page in your browser\n"
            "       2. Open DevTools (F12) → Network tab\n"
            "       3. Click the 'Player Stats' tab on the match page\n"
            "       4. Look for: api.performfeed.com/feeds/soccerdata/matchevent/{UUID}\n"
            "       5. Copy UUID — save one per line into match_uuids.txt\n"
            "       6. Run with: --uuid-file match_uuids.txt\n"
        )
        return {}

    # ── Step 3: Fetch event feeds ─────────────────────────────────────────────
    print(f"\n[3/4] Fetching Opta event data for {len(match_records)} matches ...")
    all_events: list[dict] = []
    match_summaries: list[dict] = []

    for i, record in enumerate(match_records, 1):
        match_id = record["match_id"]
        print(f"  [{i}/{len(match_records)}] {match_id} ...", end=" ", flush=True)

        feed = fetch_match_events(match_id)
        if not feed:
            print("→ failed to fetch")
            continue

        meta = parse_match_info(feed)

        # League filter: skip if competition doesn't match
        if league:
            comp = (meta.get("competition_name") or "").lower()
            if league.lower() not in comp:
                print(f"→ skip ({meta.get('competition_name')})")
                continue

        events = parse_player_events(feed, player_name, meta)
        all_events.extend(events)

        type_counts: dict[str, int] = {}
        for ev in events:
            tn = ev.get("type_name") or "unknown"
            type_counts[tn] = type_counts.get(tn, 0) + 1

        match_summaries.append({
            **meta,
            "scoresway_match_id": match_id,
            "player_events_in_match": len(events),
            "events_by_type": type_counts,
        })

        home = meta.get("home_team") or record.get("home_slug", "?")
        away = meta.get("away_team") or record.get("away_slug", "?")
        date = meta.get("date") or "?"
        print(f"→ {home} vs {away} ({date}) — {len(events)} events")

    print(f"\n  Total player events: {len(all_events)} across {len(match_summaries)} matches")

    # ── Step 4: Build output ──────────────────────────────────────────────────
    print("\n[4/4] Building output ...")

    opta_id = all_events[0].get("opta_player_id") if all_events else None

    output = {
        "meta": {
            "player_name":         player_name,
            "team":                team_name,
            "season":              season_norm,
            "league_filter":       league,
            "opta_player_id":      opta_id,
            "sofascore_bridge_note": (
                "To join with Sofascore data, match via player name + team within "
                "the same match context, or use the Reep register "
                "(https://github.com/dcaribou/transfermarkt-datasets) which maps "
                "opta_player_id ↔ sofascore_player_id for most top-5 league players."
            ),
            "source":              "Scoresway.com / api.performfeed.com (Stats Perform Opta MA3 feed)",
            "coordinate_system":   "Opta 0-100 pitch. Origin (0,0) = bottom-left from home team's perspective. x = horizontal, y = vertical.",
            "matches_processed":   len(match_summaries),
            "total_player_events": len(all_events),
            "fetched_at_utc":      datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        "season_summary": aggregate_season(all_events),
        "match_log": match_summaries,
        "shotmap": build_shotmap(all_events),
        "heatmap": build_heatmap(all_events),
        "pass_map": build_pass_map(all_events),
        "all_events": all_events,
    }

    if not out_path:
        p_slug = re.sub(r"[^\w]", "_", player_name.lower())
        s_slug = season_norm.replace("/", "-")
        out_path = Path(f"{p_slug}_{s_slug}_scoresway.json")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n{'─'*62}")
    print(f"  Player:      {player_name}")
    print(f"  Season:      {season_norm}")
    print(f"  Matches:     {len(match_summaries)}")
    print(f"  Events:      {len(all_events)}")
    print(f"  Shots:       {output['shotmap']['count']}")
    print(f"  Goals:       {output['shotmap']['goals']}")
    print(f"  Passes:      {output['season_summary'].get('passes', 0)}")
    print(f"  Heatmap pts: {output['heatmap']['cell_count']}")
    print(f"  Output:      {out_path}")
    print(f"{'─'*62}\n")

    return output


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch season-long Opta event data for a player via Scoresway / api.performfeed.com"
    )
    ap.add_argument("--player",         "-p", required=True, help='e.g. "Nathaniel Brown"')
    ap.add_argument("--team",           "-t", required=True, help='e.g. "Eintracht Frankfurt"')
    ap.add_argument("--season",         "-s", required=True, help='e.g. "2024/2025"')
    ap.add_argument("--league",         "-l", default=None,  help='e.g. "Bundesliga" — used for competition lookup and match filtering')
    ap.add_argument("--competition-id",       default=None,  help="Scoresway competition ID (overrides --league lookup)")
    ap.add_argument("--team-id",              default=None,  help="Scoresway team ID (auto-resolved for known teams)")
    ap.add_argument("--uuid-file",            default=None,  help="Text/JSON file with performfeed match UUIDs — bypasses discovery")
    ap.add_argument("--out",            "-o", default=None,  help="Output JSON path")
    ap.add_argument("--delay",                default=REQUEST_DELAY, type=float)
    args = ap.parse_args()

    run(
        player_name=args.player,
        team_name=args.team,
        season=args.season,
        league=args.league,
        competition_id=args.competition_id,
        team_id=args.team_id,
        uuid_file=Path(args.uuid_file) if args.uuid_file else None,
        out_path=Path(args.out) if args.out else None,
        delay=args.delay,
    )


if __name__ == "__main__":
    main()