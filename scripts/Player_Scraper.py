
from __future__ import annotations

"""
Player_Scraper_rewrite.py
=========================

Cleaner, diagnostics-first Sofascore scraper.

Goals of this rewrite:
- Keep the production dataset wide but maintainable.
- Track stat-family availability by matchweek so feed changes are obvious.
- Preserve useful enrichments (xGOT, cards, assists, roles, safe CSV upgrades).
- Make physical-data disappearance diagnosable instead of mysterious.
- Keep network usage lean: only events, lineups, shotmap, incidents.

New outputs / options:
- --unmapped-out unmapped_stats.json
- --coverage-out stat_coverage_by_mw.json

The coverage report records, per matchweek and stat family:
- how many rows had a value
- which raw stat keys were observed
"""

import argparse
import csv
import json
import math
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

from curl_cffi import requests as cf_requests

API_BASE = "https://api.sofascore.com/api/v1"

# Supported league/tournament IDs.
# La Liga=8, Premier League=17, Serie A=23, Ligue 1=34, Bundesliga=35
LEAGUE_NAMES: dict[int, str] = {
    8: "La Liga",
    17: "Premier League",
    23: "Serie A",
    34: "Ligue 1",
    35: "Bundesliga",
}
SEASON_IDS: dict[int, dict[str, int]] = {
    8: {"2023-24": 52376, "2024-25": 61643, "2025-26": 77559},
    17: {"2023-24": 52186, "2024-25": 61627, "2025-26": 76986},
    23: {"2023-24": 52760, "2024-25": 63515, "2025-26": 76457},
    34: {"2023-24": 52571, "2024-25": 61736, "2025-26": 77356},
    35: {"2023-24": 52608, "2024-25": 63516, "2025-26": 77333},
}

DEFAULT_TOURNAMENT_ID = 35
TOURNAMENT_ID = DEFAULT_TOURNAMENT_ID
LEAGUE_NAME = LEAGUE_NAMES[TOURNAMENT_ID]
KNOWN_SEASONS: dict[str, int] = SEASON_IDS[TOURNAMENT_ID]
DEFAULT_SEASON = "2025-26"
DEFAULT_REQUEST_DELAY = 0.35

LEAGUE_ALIASES: dict[str, int] = {
    "laliga": 8,
    "la liga": 8,
    "spain": 8,
    "premierleague": 17,
    "premier league": 17,
    "epl": 17,
    "england": 17,
    "seriea": 23,
    "serie a": 23,
    "italy": 23,
    "ligue1": 34,
    "ligue 1": 34,
    "france": 34,
    "bundesliga": 35,
    "germany": 35,
}

CSV_COLUMNS = [
    "league",
    "player_id", "player_name", "player_position",
    "base_position", "role_position", "role_family", "position_confidence", "position_source",
    "event_id", "match_id", "MW", "season",
    "team", "opponent", "venue", "result",
    "shirt_number", "is_substitute", "minutes_played",
    "sub_on_minute", "sub_off_minute",
    "sofascore_rating", "rating_original", "rating_alternative",
    "pass_value", "dribble_value", "defensive_value", "shot_value", "goalkeeper_value",
    "goals", "assists",
    "shots_total", "shots_on_target", "shots_off_target",
    "xg", "xgot", "xa",
    "big_chances_created", "big_chance_missed",
    "touches_opp_box", "offsides", "hit_woodwork",
    "passes_total", "passes_accurate", "pass_accuracy_pct",
    "passes_own_half_total", "passes_own_half_accurate",
    "passes_opposition_half_total", "passes_opposition_half_accurate",
    "key_passes",
    "long_balls_total", "long_balls_accurate",
    "crosses_total", "crosses_accurate",
    "touches", "unsuccessful_touches",
    "dribbles_attempted",
    "carries", "carry_distance",
    "progressive_carries", "progressive_carry_distance", "best_carry_progression",
    "total_progression", "dispossessed", "possession_lost",
    "tackles_total", "tackles_won", "last_man_tackles",
    "interceptions", "clearances", "clearance_off_line",
    "blocked_shots",
    "duels_total", "duels_won", "duels_lost",
    "aerial_duels_total", "aerial_duels_won", "aerial_duels_lost",
    "recoveries", "contests_total", "contests_won", "challenges_lost",
    "errors_leading_to_shot", "errors_leading_to_goal",
    "fouls_committed", "fouls_drawn",
    "yellow_cards", "red_cards",
    "penalties_won", "penalties_conceded", "penalties_faced",
    "distance_walking_km", "distance_jogging_km", "distance_running_km",
    "distance_high_speed_running_km", "distance_sprinting_km",
    "gk_saves", "gk_saves_inside_box",
    "gk_xgot_faced", "gk_goals_prevented", "gk_goals_prevented_raw",
    "gk_save_value", "gk_high_claims", "gk_punches",
    "gk_sweeper_total", "gk_sweeper_accurate",
    "flags",
]

PLAYER_STAT_MAP: list[tuple[str, set[str]]] = [
    ("sofascore_rating", {"rating", "sofascorerating", "sofascore rating"}),
    ("rating_original", {"original"}),
    ("rating_alternative", {"alternative"}),
    ("pass_value", {"passvaluenormalized"}),
    ("dribble_value", {"dribblevaluenormalized"}),
    ("defensive_value", {"defensivevaluenormalized"}),
    ("shot_value", {"shotvaluenormalized"}),
    ("goalkeeper_value", {"goalkeepervaluenormalized"}),
    ("goals", {"goals", "goal"}),
    ("assists", {"assists", "assist", "goal assist", "goalassist"}),
    ("shots_total", {"totalshots", "shots", "shotstotal"}),
    ("shots_on_target", {"shotsontarget", "ontargetscoringattempt"}),
    ("shots_off_target", {"shotofftarget"}),
    ("xg", {"expectedgoals", "xg"}),
    ("xgot", {"xgot", "expectedgoalsontarget"}),
    ("xa", {"expectedassists", "xa"}),
    ("big_chances_created", {"bigchancescreated", "bigchancecreated", "bigchances"}),
    ("big_chance_missed", {"bigchancemissed", "bigchancesmissed"}),
    ("touches_opp_box", {"touchesinpenaltyarea", "touchesinoppositionbox"}),
    ("offsides", {"totaloffside", "offsides", "offside"}),
    ("hit_woodwork", {"hitwoodwork"}),
    ("passes_total", {"totalpasses", "allpasses", "totalpass", "passes"}),
    ("passes_accurate", {"accuratepasses", "successfulpasses", "accuratepass"}),
    ("pass_accuracy_pct", {"passaccuracy", "accuratepassespercentage"}),
    ("passes_own_half_total", {"totalownhalfpasses"}),
    ("passes_own_half_accurate", {"accurateownhalfpasses"}),
    ("passes_opposition_half_total", {"totaloppositionhalfpasses"}),
    ("passes_opposition_half_accurate", {"accurateoppositionhalfpasses"}),
    ("key_passes", {"keypasses", "keypass"}),
    ("long_balls_total", {"totallongballs", "longballs"}),
    ("long_balls_accurate", {"accuratelongballs"}),
    ("crosses_total", {"totalcross", "totalcrosses", "crosses"}),
    ("crosses_accurate", {"accuratecross", "accuratecrosses"}),
    ("touches", {"touches", "totaltouch"}),
    ("unsuccessful_touches", {"unsuccessfultouch"}),
    ("dribbles_attempted", {"attempteddribbles", "totaldribble", "totalcontest"}),
    ("carries", {"ballcarriescount"}),
    ("carry_distance", {"totalballcarriesdistance"}),
    ("progressive_carries", {"progressivecarries", "progressiveruns", "progressiveballcarriescount"}),
    ("progressive_carry_distance", {"totalprogressiveballcarriesdistance"}),
    ("best_carry_progression", {"bestballcarryprogression"}),
    ("total_progression", {"totalprogression"}),
    ("dispossessed", {"dispossessed"}),
    ("possession_lost", {"possessionlostctrl"}),
    ("tackles_total", {"totaltackle", "totaltackles", "tackles"}),
    ("tackles_won", {"wontackle", "successfultackles"}),
    ("last_man_tackles", {"lastmantackle"}),
    ("interceptions", {"interceptions", "interceptionwon"}),
    ("clearances", {"clearances", "totalclearance"}),
    ("clearance_off_line", {"clearanceoffline"}),
    ("blocked_shots", {"blockedshots", "outfielderblock", "blockedscoringattempt"}),
    ("duels_total", {"totalduels", "dueltotal", "duels"}),
    ("duels_won", {"wonduels", "duelwon", "totalwonduels"}),
    ("duels_lost", {"duellost"}),
    ("aerial_duels_total", {"aerialtotal", "totalaerialduels", "aerialstotal"}),
    ("aerial_duels_won", {"aerialwon", "wonaerialduels", "aerialswon"}),
    ("aerial_duels_lost", {"aeriallost"}),
    ("recoveries", {"ballrecovery", "totalballrecovery"}),
    ("contests_total", {"totalcontest"}),
    ("contests_won", {"woncontest"}),
    ("challenges_lost", {"challengelost"}),
    ("errors_leading_to_shot", {"errorleadtoashot"}),
    ("errors_leading_to_goal", {"errorleadtoagoal"}),
    ("fouls_committed", {"foulcommitted", "totalfoulcommitted", "fouls"}),
    ("fouls_drawn", {"fouldrawn", "wasfouled", "totalfouldrawn"}),
    ("yellow_cards", {"yellowcards", "yellowcard"}),
    ("red_cards", {"redcards", "redcard", "secondyellowredcard"}),
    ("penalties_won", {"penaltywon"}),
    ("penalties_conceded", {"penaltyconceded"}),
    ("penalties_faced", {"penaltyfaced"}),
    ("distance_walking_km", {"meterscoveredwalkingkm"}),
    ("distance_jogging_km", {"meterscoveredjoggingkm"}),
    ("distance_running_km", {"meterscoveredrunningkm"}),
    ("distance_high_speed_running_km", {"meterscoveredhighspeedrunningkm"}),
    ("distance_sprinting_km", {"meterscoveredsprintingkm"}),
    ("gk_saves", {"saves", "totalsaves", "goalkeepersave"}),
    ("gk_saves_inside_box", {"savesinsidebox", "savedshotsfrominsidethebox"}),
    ("gk_goals_prevented_raw", {"goalsprevented"}),
    ("gk_save_value", {"keepersavevalue"}),
    ("gk_high_claims", {"goodhighclaim"}),
    ("gk_punches", {"punches"}),
    ("gk_sweeper_total", {"totalkeepersweeper"}),
    ("gk_sweeper_accurate", {"accuratekeepersweeper"}),
    ("minutes_played", {"minutesplayed", "minutessincestat"}),
]

STAT_FAMILIES: dict[str, str] = {
    "sofascore_rating": "ratings", "rating_original": "ratings", "rating_alternative": "ratings",
    "pass_value": "value", "dribble_value": "value", "defensive_value": "value", "shot_value": "value", "goalkeeper_value": "value",
    "goals": "attacking", "assists": "attacking", "shots_total": "attacking", "shots_on_target": "attacking",
    "shots_off_target": "attacking", "xg": "attacking", "xgot": "attacking", "xa": "attacking",
    "big_chances_created": "attacking", "big_chance_missed": "attacking", "touches_opp_box": "attacking",
    "offsides": "attacking", "hit_woodwork": "attacking",
    "passes_total": "passing", "passes_accurate": "passing", "pass_accuracy_pct": "passing",
    "passes_own_half_total": "passing", "passes_own_half_accurate": "passing",
    "passes_opposition_half_total": "passing", "passes_opposition_half_accurate": "passing",
    "key_passes": "passing", "long_balls_total": "passing", "long_balls_accurate": "passing",
    "crosses_total": "passing", "crosses_accurate": "passing",
    "touches": "ball_contact", "unsuccessful_touches": "ball_contact", "dribbles_attempted": "carrying",
    "carries": "carrying", "carry_distance": "carrying", "progressive_carries": "carrying",
    "progressive_carry_distance": "carrying", "best_carry_progression": "carrying", "total_progression": "carrying",
    "dispossessed": "ball_security", "possession_lost": "ball_security",
    "tackles_total": "defending", "tackles_won": "defending", "last_man_tackles": "defending",
    "interceptions": "defending", "clearances": "defending", "clearance_off_line": "defending",
    "blocked_shots": "defending", "duels_total": "defending", "duels_won": "defending", "duels_lost": "defending",
    "aerial_duels_total": "defending", "aerial_duels_won": "defending", "aerial_duels_lost": "defending",
    "recoveries": "defending", "contests_total": "defending", "contests_won": "defending", "challenges_lost": "defending",
    "errors_leading_to_shot": "defending", "errors_leading_to_goal": "defending",
    "fouls_committed": "discipline", "fouls_drawn": "discipline", "yellow_cards": "discipline", "red_cards": "discipline",
    "penalties_won": "discipline", "penalties_conceded": "discipline", "penalties_faced": "discipline",
    "distance_walking_km": "physical", "distance_jogging_km": "physical", "distance_running_km": "physical",
    "distance_high_speed_running_km": "physical", "distance_sprinting_km": "physical",
    "gk_saves": "goalkeeping", "gk_saves_inside_box": "goalkeeping", "gk_xgot_faced": "goalkeeping",
    "gk_goals_prevented": "goalkeeping", "gk_goals_prevented_raw": "goalkeeping", "gk_save_value": "goalkeeping",
    "gk_high_claims": "goalkeeping", "gk_punches": "goalkeeping", "gk_sweeper_total": "goalkeeping", "gk_sweeper_accurate": "goalkeeping",
}

def _norm(s: Any) -> str:
    return " ".join(str(s).strip().lower().replace("_", " ").replace("-", " ").split())

_STAT_LOOKUP: dict[str, str] = {}
for _col, _aliases in PLAYER_STAT_MAP:
    for _alias in _aliases:
        _STAT_LOOKUP[_norm(_alias)] = _col

UNMAPPED_STAT_KEYS: Counter[str] = Counter()
UNMAPPED_STAT_EXAMPLES: dict[str, Any] = {}
UNMAPPED_STAT_RECORDS: list[dict[str, Any]] = []
RAW_KEY_COUNTS: Counter[str] = Counter()
RAW_KEY_EXAMPLES: dict[str, Any] = {}
STAT_COVERAGE: dict[str, dict[str, dict[str, Any]]] = defaultdict(lambda: defaultdict(lambda: {
    "rows_with_value": 0,
    "raw_keys_seen": Counter(),
}))

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

REQUEST_DELAY = DEFAULT_REQUEST_DELAY

def set_active_league(tournament_id: int) -> None:
    """Switch active league context before fetching."""
    global TOURNAMENT_ID, LEAGUE_NAME, KNOWN_SEASONS
    if tournament_id not in LEAGUE_NAMES:
        raise ValueError(f"Unknown tournament ID: {tournament_id}. Known IDs: {sorted(LEAGUE_NAMES)}")
    TOURNAMENT_ID = tournament_id
    LEAGUE_NAME = LEAGUE_NAMES[tournament_id]
    KNOWN_SEASONS = SEASON_IDS[tournament_id]


def parse_league_token(token: str) -> int:
    raw = str(token).strip()
    if not raw:
        raise ValueError("Empty league token.")
    if raw.isdigit():
        tid = int(raw)
        if tid not in LEAGUE_NAMES:
            raise ValueError(f"Unknown tournament ID: {tid}. Known IDs: {sorted(LEAGUE_NAMES)}")
        return tid
    key = raw.lower().replace("-", " ").replace("_", " ")
    compact = key.replace(" ", "")
    if key in LEAGUE_ALIASES:
        return LEAGUE_ALIASES[key]
    if compact in LEAGUE_ALIASES:
        return LEAGUE_ALIASES[compact]
    raise ValueError(f"Unknown league '{token}'. Known leagues: {', '.join(LEAGUE_NAMES.values())}")


def resolve_leagues(league_arg: str | None, all_leagues: bool) -> list[int]:
    if all_leagues:
        return list(LEAGUE_NAMES.keys())
    if league_arg:
        out: list[int] = []
        for part in str(league_arg).split(","):
            tid = parse_league_token(part)
            if tid not in out:
                out.append(tid)
        return out
    return [DEFAULT_TOURNAMENT_ID]


def league_safe_name(league_name: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in league_name).strip("_")


def maybe_league_path(path_str: str | None, tournament_id: int) -> str | None:
    if not path_str:
        return None
    safe = league_safe_name(LEAGUE_NAMES[tournament_id])
    return (
        path_str
        .replace("{league}", safe)
        .replace("{league_name}", safe)
        .replace("{league_id}", str(tournament_id))
    )


def _sleep_if_needed() -> None:
    if REQUEST_DELAY > 0:
        time.sleep(REQUEST_DELAY)

def _get(url: str, retries: int = 3, quiet_404: bool = False) -> dict[str, Any] | list[Any] | None:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            r = _session.get(url, timeout=20)
            if r.status_code == 404 and quiet_404:
                return None
            if r.status_code == 429:
                wait = max(1.0, REQUEST_DELAY * 4) * (2 ** attempt)
                print(f"  [rate limited] sleeping {wait:.1f}s ...")
                time.sleep(wait)
                continue
            if not r.ok:
                snippet = r.text[:300].replace("\n", " ")
                raise RuntimeError(f"HTTP {r.status_code} for {url} | {snippet}")
            data = r.json()
            _sleep_if_needed()
            return data
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                print(f"  [retry {attempt + 1}/{retries}] {e}")
                time.sleep(1.0 * (attempt + 1))
            else:
                raise RuntimeError(f"Failed after {retries} attempts: {url}\n  {e}") from e
    raise RuntimeError(f"Unreachable: {url}") from last_err

def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return None if (isinstance(value, float) and math.isnan(value)) else float(value)
    text = str(value).strip().replace("%", "").replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None

def _round(v: float | None, dp: int = 4) -> float | None:
    return round(v, dp) if v is not None else None

def _int_or_none(v: Any) -> int | None:
    try:
        if v is None or v == "":
            return None
        return int(float(v))
    except Exception:
        return None

def _repair_text(text: Any) -> Any:
    if not isinstance(text, str):
        return text
    repaired = text.strip()
    suspicious = ("Ã", "â", "€", "œ", "�")
    if any(ch in repaired for ch in suspicious):
        for src, dst in (("latin-1", "utf-8"), ("cp1252", "utf-8")):
            try:
                candidate = repaired.encode(src, errors="strict").decode(dst, errors="strict")
                if candidate:
                    repaired = candidate
                    break
            except Exception:
                pass
    return repaired

def build_match_id(home_team: str, away_team: str, mw: int) -> str:
    a, b = sorted([home_team, away_team])
    return f"{mw}|{a}|{b}"

def _first_present(d: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in d and d[key] not in (None, ""):
            return d[key]
    return None

def _to_pct(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return _round(100.0 * numerator / denominator, 2)

def _extract_coordinate(obj: dict[str, Any] | None, keys: list[str]) -> float | None:
    if not isinstance(obj, dict):
        return None
    for key in keys:
        value = obj.get(key)
        if value is not None:
            parsed = _parse_float(value)
            if parsed is not None:
                return parsed
    return None

def _extract_nested_coordinate(obj: dict[str, Any] | None, paths: list[tuple[str, ...]]) -> float | None:
    if not isinstance(obj, dict):
        return None
    for path in paths:
        cur: Any = obj
        ok = True
        for part in path:
            if not isinstance(cur, dict) or part not in cur:
                ok = False
                break
            cur = cur[part]
        if ok:
            parsed = _parse_float(cur)
            if parsed is not None:
                return parsed
    return None

def _normalize_base_position(pos: Any) -> str | None:
    p = _norm(pos)
    mapping = {
        'g': 'GK', 'gk': 'GK', 'goalkeeper': 'GK', 'keeper': 'GK',
        'd': 'D', 'defender': 'D', 'defence': 'D', 'defense': 'D',
        'm': 'M', 'midfielder': 'M', 'midfield': 'M',
        'f': 'F', 'fw': 'F', 'forward': 'F', 'attacker': 'F', 'striker': 'F',
    }
    if p in mapping:
        return mapping[p]
    if p in {'cb','rcb','lcb','rb','lb','rwb','lwb'}:
        return 'D'
    if p in {'dm','cdm','cm','rcm','lcm','am','cam','rm','lm','rw','lw'}:
        return 'M'
    if p in {'cf','st','ss','rf','lf'}:
        return 'F'
    return None

def _heuristic_role_from_stats(base_pos: str | None, stats: dict[str, float | None]) -> tuple[str | None, str, str]:
    if base_pos == 'GK':
        return 'GK', 'low', 'heuristic_base'
    crosses = stats.get('crosses_total') or 0
    long_balls = stats.get('long_balls_total') or 0
    touches_box = stats.get('touches_opp_box') or 0
    clearances = stats.get('clearances') or 0
    aerials = stats.get('aerial_duels_total') or 0
    key_passes = stats.get('key_passes') or 0
    xa = stats.get('xa') or 0
    tackles = stats.get('tackles_total') or 0
    interceptions = stats.get('interceptions') or 0
    progressive = stats.get('progressive_carries') or 0
    passes = stats.get('passes_total') or 0
    shots = stats.get('shots_total') or 0
    dispossessed = stats.get('dispossessed') or 0
    if base_pos == 'D':
        if clearances >= 4 or aerials >= 4 or long_balls >= 5:
            return 'CB', 'medium', 'heuristic_stats'
        if crosses >= 2 or progressive >= 2:
            return 'FB', 'medium', 'heuristic_stats'
        return 'D', 'low', 'heuristic_base'
    if base_pos == 'M':
        if key_passes >= 2 or xa >= 0.15 or touches_box >= 3:
            return 'AM', 'medium', 'heuristic_stats'
        if tackles >= 3 or interceptions >= 2 or long_balls >= 4:
            return 'DM', 'medium', 'heuristic_stats'
        if passes >= 35 or progressive >= 2:
            return 'CM', 'low', 'heuristic_stats'
        if crosses >= 3:
            return 'WM', 'low', 'heuristic_stats'
        return 'M', 'low', 'heuristic_base'
    if base_pos == 'F':
        if crosses >= 3 and shots <= 2:
            return 'W', 'medium', 'heuristic_stats'
        if touches_box >= 4 or shots >= 3 or dispossessed >= 2:
            return 'ST', 'medium', 'heuristic_stats'
        if key_passes >= 2 or xa >= 0.12:
            return 'SS', 'low', 'heuristic_stats'
        return 'F', 'low', 'heuristic_base'
    return base_pos, 'low', 'heuristic_base'

def infer_role_position(player_block: dict[str, Any], stats: dict[str, float | None]) -> tuple[str | None, str | None, str | None, str, str]:
    p = player_block.get('player', {}) or {}
    raw_pos = player_block.get('position') or p.get('position') or p.get('positionName')
    base_pos = _normalize_base_position(raw_pos)
    detailed_candidates = [
        player_block.get('positionName'), p.get('positionName'),
        player_block.get('positionCode'), p.get('positionCode'), player_block.get('slug'),
    ]
    direct_map = {
        'rb':'RB','right back':'RB','lb':'LB','left back':'LB','cb':'CB','centre back':'CB','center back':'CB',
        'rcb':'RCB','lcb':'LCB','rwb':'RWB','lwb':'LWB',
        'dm':'DM','cdm':'DM','defensive midfielder':'DM',
        'cm':'CM','central midfielder':'CM','midfielder':'CM',
        'am':'AM','cam':'AM','attacking midfielder':'AM',
        'rm':'RM','lm':'LM','rw':'RW','lw':'LW',
        'winger':'W','right winger':'RW','left winger':'LW',
        'cf':'CF','st':'ST','striker':'ST','centre forward':'CF','center forward':'CF',
        'ss':'SS','second striker':'SS','goalkeeper':'GK','gk':'GK'
    }
    for cand in detailed_candidates:
        norm = _norm(cand)
        if norm in direct_map:
            role = direct_map[norm]
            fam = 'wide' if role in {'RB','LB','RWB','LWB','RM','LM','RW','LW','W'} else 'central'
            if role == 'GK':
                fam = 'goalkeeper'
            elif role in {'ST','CF','SS'}:
                fam = 'attack'
            elif role in {'DM','CM','AM'}:
                fam = 'midfield'
            elif role in {'CB','RCB','LCB'}:
                fam = 'defence'
            return base_pos or _normalize_base_position(role), role, fam, 'high', 'explicit_position_field'
    x = (_extract_coordinate(player_block, ['averageX', 'x', 'positionX'])
         or _extract_coordinate(p, ['averageX', 'x', 'positionX'])
         or _extract_nested_coordinate(player_block, [('averagePosition','x'), ('heatmap','averageX')]))
    y = (_extract_coordinate(player_block, ['averageY', 'y', 'positionY'])
         or _extract_coordinate(p, ['averageY', 'y', 'positionY'])
         or _extract_nested_coordinate(player_block, [('averagePosition','y'), ('heatmap','averageY')]))
    if base_pos == 'GK':
        return 'GK', 'GK', 'goalkeeper', 'high', 'base_position'
    if y is not None and 0 <= y <= 100 and base_pos in {'D','M','F'}:
        side = 'left' if y <= 33 else 'right' if y >= 67 else 'center'
        if base_pos == 'D':
            return base_pos, {'left':'LB','right':'RB','center':'CB'}[side], 'defence', 'medium', 'average_position'
        if base_pos == 'M':
            role = {'left':'LM','right':'RM','center':'CM'}[side]
            if x is not None and 0 <= x <= 100:
                if x >= 68:
                    role = 'AM' if side == 'center' else ('LW' if side == 'left' else 'RW')
                elif x <= 40:
                    role = 'DM' if side == 'center' else role
            fam = 'midfield' if role in {'DM','CM','AM','LM','RM'} else 'attack'
            return base_pos, role, fam, 'medium', 'average_position'
        if base_pos == 'F':
            return base_pos, {'left':'LW','right':'RW','center':'ST'}[side], 'attack', 'medium', 'average_position'
    role, confidence, source = _heuristic_role_from_stats(base_pos, stats)
    family_map = {
        'GK':'goalkeeper','CB':'defence','RCB':'defence','LCB':'defence','RB':'wide_defence','LB':'wide_defence','RWB':'wide_defence','LWB':'wide_defence','FB':'wide_defence','D':'defence',
        'DM':'midfield','CM':'midfield','AM':'midfield','LM':'wide_midfield','RM':'wide_midfield','WM':'wide_midfield','M':'midfield',
        'RW':'attack','LW':'attack','W':'attack','ST':'attack','CF':'attack','SS':'attack','F':'attack'
    }
    return base_pos, role, family_map.get(role or base_pos or '', None), confidence, source

def _iter_named_values(node: Any, context_name: str | None = None):
    if isinstance(node, dict):
        candidate_name = node.get("name") or node.get("title") or node.get("key") or node.get("label") or context_name
        scalar_value = _first_present(node, ["value", "statistics", "stat", "displayValue"])
        if candidate_name is not None and scalar_value is not None and not isinstance(scalar_value, (dict, list)):
            yield str(candidate_name), scalar_value
        for k, v in node.items():
            if k in {"name", "title", "key", "label", "value", "statistics", "stat", "displayValue"}:
                continue
            if isinstance(v, (int, float, str)) and not isinstance(v, bool):
                yield str(k), v
            else:
                yield from _iter_named_values(v, context_name=str(k))
    elif isinstance(node, list):
        for item in node:
            yield from _iter_named_values(item, context_name=context_name)

def _record_stat_coverage(mw: int | None, stats: dict[str, float | None], matched_raw_keys: dict[str, set[str]]) -> None:
    mw_key = f"{LEAGUE_NAME}|MW{mw}" if mw is not None else f"{LEAGUE_NAME}|unknown"
    for col, value in stats.items():
        if value is None:
            continue
        family = STAT_FAMILIES.get(col, "other")
        bucket = STAT_COVERAGE[mw_key][family]
        bucket["rows_with_value"] += 1
        for raw_key in matched_raw_keys.get(col, set()):
            bucket["raw_keys_seen"][raw_key] += 1

def _extract_player_stats(
    statistics: dict | list | None,
    mw: int | None = None,
    context: dict[str, Any] | None = None,
) -> tuple[dict[str, float | None], list[str]]:
    resolved: dict[str, float | None] = {col: None for col, _ in PLAYER_STAT_MAP}
    unmapped_local: list[str] = []
    matched_raw_keys: dict[str, set[str]] = defaultdict(set)
    context = context or {}
    if statistics is None:
        return resolved, unmapped_local
    for raw_name, raw_val in _iter_named_values(statistics):
        normalized = _norm(raw_name)
        parsed = _parse_float(raw_val)
        RAW_KEY_COUNTS[normalized] += 1
        RAW_KEY_EXAMPLES.setdefault(normalized, raw_val)
        col = _STAT_LOOKUP.get(normalized)
        if col:
            matched_raw_keys[col].add(normalized)
            if resolved[col] is None and parsed is not None:
                resolved[col] = parsed
        else:
            if parsed is not None and normalized and len(normalized) <= 80:
                UNMAPPED_STAT_KEYS[normalized] += 1
                unmapped_local.append(normalized)
                UNMAPPED_STAT_EXAMPLES.setdefault(normalized, raw_val)
                UNMAPPED_STAT_RECORDS.append({
                    **context,
                    "raw_key": raw_name,
                    "normalized_key": normalized,
                    "raw_value": raw_val,
                    "parsed_value": parsed,
                })
    _record_stat_coverage(mw, resolved, matched_raw_keys)
    return resolved, sorted(set(unmapped_local))

def _extract_shot_team_side(shot: dict[str, Any], home_team_id: int | None, away_team_id: int | None) -> str | None:
    if isinstance(shot.get("isHome"), bool):
        return "home" if shot["isHome"] else "away"
    team_block = shot.get("team") or {}
    team_id = _int_or_none(team_block.get("id") or shot.get("teamId"))
    if team_id is not None:
        if home_team_id is not None and team_id == home_team_id:
            return "home"
        if away_team_id is not None and team_id == away_team_id:
            return "away"
    team_side = _norm(_first_present(shot, ["teamSide", "side"]))
    if team_side in {"home", "away"}:
        return team_side
    return None

def fetch_shotmap_context(event_id: int, home_team_id: int | None, away_team_id: int | None) -> dict[str, Any]:
    data = _get(f"{API_BASE}/event/{event_id}/shotmap") or {}
    shots = data.get("shotmap") or data.get("shots") or []
    player_xgot: dict[int, float] = defaultdict(float)
    team_xgot_faced: dict[str, float] = {"home": 0.0, "away": 0.0}
    for shot in shots:
        if not isinstance(shot, dict):
            continue
        is_on_target = bool(shot.get("isOnTarget", False)) or shot.get("goalType") is not None
        if not is_on_target:
            continue
        xgot_raw = _first_present(shot, ["xgot", "xGoTot", "xGOT", "expectedGoalsOnTarget"])
        xgot = _parse_float(xgot_raw)
        if xgot is None:
            continue
        pid = _int_or_none((shot.get("player") or {}).get("id") or shot.get("playerId"))
        if pid is not None:
            player_xgot[pid] += xgot
        side = _extract_shot_team_side(shot, home_team_id, away_team_id)
        if side == "home":
            team_xgot_faced["away"] += xgot
        elif side == "away":
            team_xgot_faced["home"] += xgot
    return {
        "player_xgot": {pid: _round(v) for pid, v in player_xgot.items()},
        "team_xgot_faced": {k: _round(v) for k, v in team_xgot_faced.items()},
    }

def fetch_incidents(event_id: int) -> dict[str, Any]:
    return _get(f"{API_BASE}/event/{event_id}/incidents", quiet_404=True) or {}

def _incident_team_side(incident: dict[str, Any], home_team_id: int | None, away_team_id: int | None) -> str | None:
    side = _norm(_first_present(incident, ["teamSide", "side"]))
    if side in {"home", "away"}:
        return side
    team_id = _int_or_none(_first_present(incident, ["teamId"]))
    if team_id is None and isinstance(incident.get("team"), dict):
        team_id = _int_or_none(incident["team"].get("id"))
    if team_id is not None:
        if home_team_id is not None and team_id == home_team_id:
            return "home"
        if away_team_id is not None and team_id == away_team_id:
            return "away"
    return None

def _extract_person_id(obj: Any) -> int | None:
    if isinstance(obj, dict):
        return _int_or_none(obj.get("id") or obj.get("playerId"))
    return None

def build_incident_context(incidents_payload: dict[str, Any], home_team_id: int | None, away_team_id: int | None) -> dict[str, Any]:
    incidents = incidents_payload.get("incidents") or incidents_payload.get("events") or []
    cards: dict[int, dict[str, int]] = defaultdict(lambda: {"yellow": 0, "red": 0})
    assists: Counter[int] = Counter()
    subs_on_min: dict[int, int] = {}
    subs_off_min: dict[int, int] = {}
    side_starting_gk: dict[str, int | None] = {"home": None, "away": None}
    for inc in incidents:
        if not isinstance(inc, dict):
            continue
        incident_type = _norm(_first_present(inc, ["incidentType", "type", "text"]))
        incident_class = _norm(_first_present(inc, ["incidentClass", "incidentClassName", "reason"]))
        minute = _int_or_none(_first_present(inc, ["time", "minute"]))
        side = _incident_team_side(inc, home_team_id, away_team_id)
        player = inc.get("player") or {}
        assist_player = inc.get("assist1") or inc.get("assist") or inc.get("assistPlayer") or {}
        in_player = inc.get("playerIn") or inc.get("substitutionIn") or inc.get("inPlayer") or {}
        out_player = inc.get("playerOut") or inc.get("substitutionOut") or inc.get("outPlayer") or {}
        pid = _extract_person_id(player)
        apid = _extract_person_id(assist_player)
        in_pid = _extract_person_id(in_player)
        out_pid = _extract_person_id(out_player)
        if incident_type == "substitution" or (in_pid is not None and out_pid is not None):
            if in_pid is not None and minute is not None:
                subs_on_min[in_pid] = minute
            if out_pid is not None and minute is not None:
                subs_off_min[out_pid] = minute
            continue
        if incident_type == "card" and pid is not None:
            if "yellow" in incident_class:
                cards[pid]["yellow"] += 1
                if "second" in incident_class:
                    cards[pid]["red"] += 1
            elif "red" in incident_class:
                cards[pid]["red"] += 1
            continue
        if incident_type == "goal" and apid is not None:
            assists[apid] += 1
            continue
        if side in {"home", "away"} and side_starting_gk[side] is None and pid is not None:
            role = _norm((player.get("position") or inc.get("position") or ""))
            if role in {"gk", "goalkeeper"}:
                side_starting_gk[side] = pid
    return {
        "cards": cards,
        "assists": assists,
        "subs_on_min": subs_on_min,
        "subs_off_min": subs_off_min,
        "starting_gk": side_starting_gk,
    }

def fetch_lineups(event_id: int) -> dict[str, Any]:
    return _get(f"{API_BASE}/event/{event_id}/lineups") or {}

def _derive_minutes_played(player_block: dict[str, Any], stats: dict[str, float | None], inc_ctx: dict[str, Any]) -> tuple[float | None, int | None, int | None, list[str]]:
    flags: list[str] = []
    pid = _int_or_none((player_block.get("player") or {}).get("id"))
    minutes = stats.get("minutes_played")
    on_min = inc_ctx["subs_on_min"].get(pid) if pid is not None else None
    off_min = inc_ctx["subs_off_min"].get(pid) if pid is not None else None
    if minutes is not None:
        return minutes, on_min, off_min, flags
    is_sub = bool(player_block.get("substitute", False))
    if pid is not None:
        if on_min is not None and off_min is not None and off_min >= on_min:
            return float(off_min - on_min), on_min, off_min, ["minutes_from_incidents"]
        if on_min is not None:
            return float(max(0, 90 - on_min)), on_min, off_min, ["minutes_from_sub_on"]
        if off_min is not None and not is_sub:
            return float(off_min), on_min, off_min, ["minutes_from_sub_off"]
    return None, on_min, off_min, ["no_minutes"]

def _finalize_derived_metrics(stats: dict[str, float | None]) -> list[str]:
    flags: list[str] = []
    if stats.get("duels_total") is None and stats.get("duels_won") is not None and stats.get("duels_lost") is not None:
        stats["duels_total"] = stats["duels_won"] + stats["duels_lost"]
        flags.append("duels_total_derived")
    if stats.get("aerial_duels_total") is None and stats.get("aerial_duels_won") is not None and stats.get("aerial_duels_lost") is not None:
        stats["aerial_duels_total"] = stats["aerial_duels_won"] + stats["aerial_duels_lost"]
        flags.append("aerial_duels_total_derived")
    if stats.get("pass_accuracy_pct") is None:
        pct = _to_pct(stats.get("passes_accurate"), stats.get("passes_total"))
        if pct is not None:
            stats["pass_accuracy_pct"] = pct
            flags.append("pass_accuracy_derived")
    if stats.get("gk_goals_prevented") is None and stats.get("gk_goals_prevented_raw") is not None:
        stats["gk_goals_prevented"] = stats["gk_goals_prevented_raw"]
        flags.append("gk_goals_prevented_from_raw")
    return flags

def process_player(player_block: dict[str, Any], match_context: dict[str, Any], player_xgot_map: dict[int, float], gk_xgot_faced: float | None, goals_conceded: int, inc_ctx: dict[str, Any]) -> dict[str, Any]:
    p = player_block.get("player", {})
    pid = _int_or_none(p.get("id"))
    name = _repair_text(p.get("name") or p.get("shortName"))
    pos = player_block.get("position") or p.get("position") or p.get("positionName")
    shirt = player_block.get("shirtNumber")
    is_sub = bool(player_block.get("substitute", False))
    raw_stats = player_block.get("statistics")
    stats, unmapped_local = _extract_player_stats(
        raw_stats,
        match_context.get("MW"),
        context={
            "league": match_context.get("league") or LEAGUE_NAME,
            "season": match_context.get("season"),
            "MW": match_context.get("MW"),
            "event_id": match_context.get("event_id"),
            "match_id": match_context.get("match_id"),
            "team": match_context.get("team"),
            "opponent": match_context.get("opponent"),
            "venue": match_context.get("venue"),
            "player_id": pid,
            "player_name": name,
            "player_position": pos,
        },
    )
    derived_flags = _finalize_derived_metrics(stats)
    base_position, role_position, role_family, position_confidence, position_source = infer_role_position(player_block, stats)
    xgot_from_map = player_xgot_map.get(pid) if pid is not None else None
    xgot_final = xgot_from_map if xgot_from_map is not None else stats.get("xgot")
    minutes_played, sub_on_minute, sub_off_minute, minute_flags = _derive_minutes_played(player_block, stats, inc_ctx)
    derived_flags.extend(minute_flags)
    incident_assists = inc_ctx["assists"].get(pid, 0) if pid is not None else 0
    incident_cards = inc_ctx["cards"].get(pid, {"yellow": 0, "red": 0}) if pid is not None else {"yellow": 0, "red": 0}
    assists = stats.get("assists")
    if assists is None and incident_assists:
        assists = float(incident_assists)
        derived_flags.append("assists_from_incidents")
    yellow_cards = stats.get("yellow_cards")
    if yellow_cards is None and incident_cards["yellow"]:
        yellow_cards = float(incident_cards["yellow"])
        derived_flags.append("yellow_from_incidents")
    red_cards = stats.get("red_cards")
    if red_cards is None and incident_cards["red"]:
        red_cards = float(incident_cards["red"])
        derived_flags.append("red_from_incidents")
    is_gk = str(pos).upper() in {"G", "GK", "GOALKEEPER", "PORTERO"}
    gk_faced = gk_xgot_faced if is_gk else None
    gk_prevented = stats.get("gk_goals_prevented")
    if gk_prevented is None and gk_faced is not None:
        gk_prevented = _round(gk_faced - goals_conceded)
        derived_flags.append("gk_xgot_from_shotmap")
    flags: list[str] = []
    if raw_stats in (None, {}, []):
        flags.append("no_stats")
    if unmapped_local:
        flags.append(f"unmapped_stats:{len(unmapped_local)}")
    flags.extend(derived_flags)
    row = {col: None for col in CSV_COLUMNS}
    row.update({
        "league": LEAGUE_NAME,
        "player_id": pid,
        "player_name": name,
        "player_position": pos,
        "base_position": base_position,
        "role_position": role_position,
        "role_family": role_family,
        "position_confidence": position_confidence,
        "position_source": position_source,
        **match_context,
        "shirt_number": shirt,
        "is_substitute": is_sub,
        "minutes_played": minutes_played,
        "sub_on_minute": sub_on_minute,
        "sub_off_minute": sub_off_minute,
        "xgot": xgot_final,
        "assists": assists,
        "yellow_cards": yellow_cards,
        "red_cards": red_cards,
        "gk_xgot_faced": gk_faced,
        "gk_goals_prevented": gk_prevented,
        "flags": ";".join(sorted(set(filter(None, flags)))) or None,
    })
    for col in row:
        if col in stats and row[col] is None:
            row[col] = stats[col]
    return row

def _find_starting_keeper_id(players: list[dict[str, Any]], fallback_side_gk: int | None) -> int | None:
    for pb in players:
        p = pb.get("player", {})
        pos = str(pb.get("position") or p.get("position") or p.get("positionName") or "").upper()
        if pos in {"G", "GK", "GOALKEEPER", "PORTERO"} and not bool(pb.get("substitute", False)):
            return _int_or_none(p.get("id"))
    return fallback_side_gk

def process_event(event: dict, season: str) -> list[dict[str, Any]]:
    eid = event["id"]
    home_team = _repair_text(event["homeTeam"]["name"])
    away_team = _repair_text(event["awayTeam"]["name"])
    home_team_id = _int_or_none(event["homeTeam"].get("id"))
    away_team_id = _int_or_none(event["awayTeam"].get("id"))
    home_score = event["homeScore"].get("current", 0)
    away_score = event["awayScore"].get("current", 0)
    mw = event.get("roundInfo", {}).get("round")
    match_id = build_match_id(home_team, away_team, mw)
    print(f"  [{eid}] {home_team} {home_score}-{away_score} {away_team}")
    shot_ctx = fetch_shotmap_context(eid, home_team_id, away_team_id)
    incidents_payload = fetch_incidents(eid)
    inc_ctx = build_incident_context(incidents_payload, home_team_id, away_team_id)
    lineups = fetch_lineups(eid)
    rows: list[dict[str, Any]] = []
    sides = {
        "home": (home_team, away_team, home_score, away_score),
        "away": (away_team, home_team, away_score, home_score),
    }
    team_players: dict[str, list[dict[str, Any]]] = {
        "home": (lineups.get("home") or {}).get("players", []),
        "away": (lineups.get("away") or {}).get("players", []),
    }
    gk_for_side = {
        "home": _find_starting_keeper_id(team_players["home"], inc_ctx["starting_gk"].get("home")),
        "away": _find_starting_keeper_id(team_players["away"], inc_ctx["starting_gk"].get("away")),
    }
    for venue, (team, opponent, scored, conceded) in sides.items():
        result = "W" if scored > conceded else "L" if scored < conceded else "D"
        match_context = {
            "league": LEAGUE_NAME,
            "event_id": eid, "match_id": match_id, "MW": mw, "season": season,
            "team": team, "opponent": opponent, "venue": venue, "result": result,
        }
        players = team_players[venue]
        if not players:
            print(f"    [WARN] No players found for {venue} ({team})")
            continue
        team_gk_pid = gk_for_side[venue]
        team_gk_xgot_faced = shot_ctx["team_xgot_faced"].get(venue)
        for pb in players:
            try:
                pid = _int_or_none((pb.get("player") or {}).get("id"))
                row = process_player(
                    pb, match_context, shot_ctx["player_xgot"],
                    team_gk_xgot_faced if pid == team_gk_pid else None,
                    conceded, inc_ctx
                )
                rows.append(row)
            except Exception as e:
                pname = _repair_text((pb.get("player") or {}).get("name", "?"))
                print(f"    [ERROR] {team} / {pname}: {e}")
    print(f"    → {len(rows)} player rows")
    return rows

def fetch_matchweek(matchweek: int, season: str = DEFAULT_SEASON, tournament_id: int | None = None) -> list[dict[str, Any]]:
    if tournament_id is not None:
        set_active_league(tournament_id)

    if season not in KNOWN_SEASONS:
        raise ValueError(f"Unknown season '{season}' for {LEAGUE_NAME}. Add it to SEASON_IDS[{TOURNAMENT_ID}].")

    print(f"\nFetching player logs — {LEAGUE_NAME} MW{matchweek} {season}")
    url = f"{API_BASE}/unique-tournament/{TOURNAMENT_ID}/season/{KNOWN_SEASONS[season]}/events/round/{matchweek}"
    data = _get(url) or {}
    events = [e for e in data.get("events", []) if e.get("status", {}).get("code") == 100]
    if not events:
        print("  No completed matches found.")
        return []

    print(f"  {len(events)} match(es). Pulling player data ...\n")
    all_rows: list[dict[str, Any]] = []
    for event in events:
        try:
            all_rows.extend(process_event(event, season))
        except Exception as e:
            name = f"{event.get('homeTeam', {}).get('name', '?')} vs {event.get('awayTeam', {}).get('name', '?')}"
            print(f"  [ERROR] {LEAGUE_NAME} / {name}: {e}")
    return all_rows

def _player_match_key(row: dict[str, Any]) -> str:
    return f"{row.get('league')}|{row.get('player_id')}|{row.get('event_id') or row.get('match_id')}"

def read_existing_rows(csv_path: str) -> list[dict[str, Any]]:
    path = Path(csv_path)
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_full_csv(rows: list[dict[str, Any]], csv_path: str) -> None:
    path = Path(csv_path)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

def append_to_csv(rows: list[dict[str, Any]], csv_path: str) -> None:
    existing_rows = read_existing_rows(csv_path)
    existing_keys = {_player_match_key(r) for r in existing_rows}
    new_rows = [r for r in rows if _player_match_key(r) not in existing_keys]
    skipped = len(rows) - len(new_rows)
    if not new_rows and existing_rows:
        print(f"\nNothing to write — all {len(rows)} row(s) already in CSV.")
        return
    merged_rows: list[dict[str, Any]] = []
    for r in existing_rows + new_rows:
        cleaned = {col: r.get(col) for col in CSV_COLUMNS}
        cleaned["player_name"] = _repair_text(cleaned.get("player_name"))
        merged_rows.append(cleaned)
    write_full_csv(merged_rows, csv_path)
    print(f"\nWrote {len(new_rows)} new player-match rows to {csv_path}")
    if existing_rows:
        print(f"Preserved {len(existing_rows)} existing row(s) with current schema.")
    if skipped:
        print(f"Skipped {skipped} duplicate(s).")

def dump_unmapped_stats(path: str | None) -> None:
    if not path:
        return

    by_key: dict[str, dict[str, Any]] = {}
    for key, count in UNMAPPED_STAT_KEYS.most_common():
        records = [r for r in UNMAPPED_STAT_RECORDS if r.get("normalized_key") == key]
        by_key[key] = {
            "count": count,
            "example": UNMAPPED_STAT_EXAMPLES.get(key),
            "sample_records": records[:25],
        }

    payload = {
        "summary": {
            "unique_unmapped_keys": len(UNMAPPED_STAT_KEYS),
            "total_unmapped_values": sum(UNMAPPED_STAT_KEYS.values()),
        },
        "by_key": by_key,
        "records": UNMAPPED_STAT_RECORDS,
    }

    Path(path).write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Unmapped stat report written to {path}")

def dump_coverage_report(path: str | None) -> None:
    if not path:
        return
    payload: dict[str, Any] = {}
    for mw, families in sorted(STAT_COVERAGE.items(), key=lambda x: (x[0] == "unknown", x[0])):
        payload[mw] = {}
        for family, info in families.items():
            payload[mw][family] = {
                "rows_with_value": info["rows_with_value"],
                "raw_keys_seen": dict(info["raw_keys_seen"].most_common()),
            }
    Path(path).write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Coverage report written to {path}")

def main() -> None:
    global REQUEST_DELAY

    p = argparse.ArgumentParser(description="Diagnostics-first Sofascore player scraper")
    p.add_argument("--matchweek", "-mw", type=int, required=True)
    p.add_argument("--season", "-s", default=DEFAULT_SEASON, help=f"e.g. 2023-24 (default: {DEFAULT_SEASON})")
    p.add_argument("--csv", "-o", default=None, help="Path to player_match_logs.csv. Omit to print summary only.")
    p.add_argument("--unmapped-out", default=None, help="Optional path to dump unmapped stat keys and row-level examples JSON.")
    p.add_argument("--coverage-out", default=None, help="Optional path to dump stat-family coverage JSON.")
    p.add_argument("--delay", type=float, default=DEFAULT_REQUEST_DELAY, help=f"Delay after successful requests in seconds (default: {DEFAULT_REQUEST_DELAY})")

    lg = p.add_mutually_exclusive_group()
    lg.add_argument(
        "--league",
        default=None,
        help="League name/id or comma-list. Examples: 'Premier League', '17', 'Premier League,Bundesliga'. Default remains Bundesliga.",
    )
    lg.add_argument("--all-leagues", action="store_true", help="Fetch this matchweek for all configured leagues.")

    args = p.parse_args()
    REQUEST_DELAY = max(0.0, args.delay)

    league_ids = resolve_leagues(args.league, args.all_leagues)
    all_rows: list[dict[str, Any]] = []

    for tid in league_ids:
        try:
            rows = fetch_matchweek(args.matchweek, args.season, tournament_id=tid)
            all_rows.extend(rows)

            # Optional per-league CSV output when path includes placeholders.
            if args.csv and ("{league" in args.csv or "{league_id}" in args.csv):
                league_csv = maybe_league_path(args.csv, tid)
                if rows:
                    append_to_csv(rows, league_csv)
                else:
                    print(f"No rows to write for {LEAGUE_NAME}.")
        except Exception as e:
            print(f"\n[LEAGUE ERROR] {LEAGUE_NAMES.get(tid, tid)}: {e}")

    if not all_rows:
        dump_unmapped_stats(args.unmapped_out)
        dump_coverage_report(args.coverage_out)
        return

    print(f"\n{'─' * 50}")
    print(f"  Total player-match rows across run: {len(all_rows)}")
    by_league = Counter(r.get("league") or "unknown" for r in all_rows)
    print("  Rows by league:")
    for league, count in by_league.most_common():
        print(f"    {league:<22} {count:>4}")

    filled = {col: sum(1 for r in all_rows if r.get(col) not in (None, "")) for col in CSV_COLUMNS}
    print(f"\n  Field coverage (out of {len(all_rows)} rows):")
    for col, count in filled.items():
        pct = count / len(all_rows) * 100 if all_rows else 0
        bar = "█" * int(pct / 5)
        print(f"    {col:<30} {count:>4}  {bar} {pct:.0f}%")
    print(f"{'─' * 50}")

    if args.csv and not ("{league" in args.csv or "{league_id}" in args.csv):
        append_to_csv(all_rows, args.csv)
    elif not args.csv:
        print("\n[dry run] Pass --csv <path> to write to your database.")

    dump_unmapped_stats(args.unmapped_out)
    dump_coverage_report(args.coverage_out)

if __name__ == "__main__":
    main()
