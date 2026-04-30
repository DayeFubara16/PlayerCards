from __future__ import annotations

"""
Optimized wide per-player match-stat scraper for Sofascore.

Changes in this production build:
- Fixes common mojibake in player names (e.g. Hugo EkitikÃ© -> Hugo Ekitiké)
- Correctly parses card incidents from incidentType='card' + incidentClass
- Removes low-value / unrecovered columns from the schema
- Removes expensive per-player probe endpoints
- Keeps wide useful metrics, shotmap xGOT, incident assists/cards, and safe CSV upgrading
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
TOURNAMENT_ID = 17
KNOWN_SEASONS: dict[str, int] = {
    "2023-24": 52186,
    "2024-25": 61627,
    "2025-26": 76986,
}
DEFAULT_SEASON = "2025-26"
DEFAULT_REQUEST_DELAY = 0.35

CSV_COLUMNS = [
    "player_id", "player_name", "player_position",
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


def _norm(s: Any) -> str:
    return " ".join(str(s).strip().lower().replace("_", " ").replace("-", " ").split())


_STAT_LOOKUP: dict[str, str] = {}
for _col, _aliases in PLAYER_STAT_MAP:
    for _alias in _aliases:
        _STAT_LOOKUP[_norm(_alias)] = _col

UNMAPPED_STAT_KEYS: Counter[str] = Counter()
UNMAPPED_STAT_EXAMPLES: dict[str, Any] = {}

_session = cf_requests.Session(impersonate="chrome124")
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
            r.raise_for_status()
            data = r.json()
            _sleep_if_needed()
            return data
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
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


def _extract_player_stats(statistics: dict | list | None) -> tuple[dict[str, float | None], list[str]]:
    resolved: dict[str, float | None] = {col: None for col, _ in PLAYER_STAT_MAP}
    unmapped_local: list[str] = []
    if statistics is None:
        return resolved, unmapped_local
    for raw_name, raw_val in _iter_named_values(statistics):
        normalized = _norm(raw_name)
        col = _STAT_LOOKUP.get(normalized)
        parsed = _parse_float(raw_val)
        if col:
            if resolved[col] is None and parsed is not None:
                resolved[col] = parsed
        else:
            if parsed is not None and normalized and len(normalized) <= 80:
                UNMAPPED_STAT_KEYS[normalized] += 1
                unmapped_local.append(normalized)
                UNMAPPED_STAT_EXAMPLES.setdefault(normalized, raw_val)
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
    stats, unmapped_local = _extract_player_stats(raw_stats)
    derived_flags = _finalize_derived_metrics(stats)

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
        "player_id": pid,
        "player_name": name,
        "player_position": pos,
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
            "event_id": eid,
            "match_id": match_id,
            "MW": mw,
            "season": season,
            "team": team,
            "opponent": opponent,
            "venue": venue,
            "result": result,
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
                row = process_player(pb, match_context, shot_ctx["player_xgot"], team_gk_xgot_faced if pid == team_gk_pid else None, conceded, inc_ctx)
                rows.append(row)
            except Exception as e:
                pname = _repair_text((pb.get("player") or {}).get("name", "?"))
                print(f"    [ERROR] {team} / {pname}: {e}")
    print(f"    → {len(rows)} player rows")
    return rows


def fetch_matchweek(matchweek: int, season: str = DEFAULT_SEASON) -> list[dict[str, Any]]:
    if season not in KNOWN_SEASONS:
        raise ValueError(f"Unknown season '{season}'. Add it to KNOWN_SEASONS.")
    print(f"\nFetching player logs — MW{matchweek} {season}")
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
            print(f"  [ERROR] {name}: {e}")
    return all_rows


def _player_match_key(row: dict[str, Any]) -> str:
    return f"{row.get('player_id')}|{row.get('event_id') or row.get('match_id')}"


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
    payload = {key: {"count": count, "example": UNMAPPED_STAT_EXAMPLES.get(key)} for key, count in UNMAPPED_STAT_KEYS.most_common()}
    Path(path).write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Unmapped stat report written to {path}")


def main() -> None:
    global REQUEST_DELAY
    p = argparse.ArgumentParser(description="Fetch optimized per-player match logs from Sofascore")
    p.add_argument("--matchweek", "-mw", type=int, required=True)
    p.add_argument("--season", "-s", default=DEFAULT_SEASON, help=f"e.g. 2023-24 (default: {DEFAULT_SEASON})")
    p.add_argument("--csv", "-o", default=None, help="Path to player_match_logs.csv. Omit to print summary only.")
    p.add_argument("--unmapped-out", default=None, help="Optional path to dump unmapped stat keys JSON.")
    p.add_argument("--delay", type=float, default=DEFAULT_REQUEST_DELAY, help=f"Delay after successful requests in seconds (default: {DEFAULT_REQUEST_DELAY})")
    args = p.parse_args()

    REQUEST_DELAY = max(0.0, args.delay)
    rows = fetch_matchweek(args.matchweek, args.season)
    if not rows:
        dump_unmapped_stats(args.unmapped_out)
        return

    print(f"\n{'─' * 50}")
    print(f"  Total player-match rows: {len(rows)}")
    filled = {col: sum(1 for r in rows if r.get(col) not in (None, "")) for col in CSV_COLUMNS}
    print(f"  Field coverage (out of {len(rows)} rows):")
    for col, count in filled.items():
        pct = count / len(rows) * 100 if rows else 0
        bar = "█" * int(pct / 5)
        print(f"    {col:<30} {count:>4}  {bar} {pct:.0f}%")
    print(f"{'─' * 50}")

    if args.csv:
        append_to_csv(rows, args.csv)
    else:
        print("\n[dry run] Pass --csv <path> to write to your database.")

    dump_unmapped_stats(args.unmapped_out)


if __name__ == "__main__":
    main()
