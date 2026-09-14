"""
whoscored_common.py
────────────────────
Shared logic between:
  - Fetch_WhoScored_Matchweek.py   (network: builds the raw match cache)
  - Extract_WhoScored_Player_Actions.py  (offline: reads the cache)

Deliberately has NO player_id anywhere in its fetch/cache path — a cached
match belongs to the match, not to whichever player you happened to be
building a card for when you fetched it. That's the whole point of the
fetch/extract split: one network fetch per match, reusable by any player
who appeared in it, indefinitely, with zero additional requests.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from curl_cffi import requests as cf_requests

API_MATCH_URL = "https://www.whoscored.com/Matches/{match_id}/Live"
DEFAULT_CACHE_DIR = "cache/whoscored_cache"
DEFAULT_DELAY = 7.0  # WhoScored is stricter than Sofascore; be polite by default.

DEFENSIVE_TYPES = {"Tackle", "Interception", "Clearance", "BallRecovery", "BlockedPass", "Challenge"}
CARRY_MIN_DISTANCE = 5.0     # pitch units (0-100 scale) to count as a meaningful carry
CARRY_MAX_GAP_SECONDS = 8.0  # consecutive touches further apart than this aren't one carry

# Structural/bookkeeping event types — not something a player "did" or "had
# done to them," just match-state markers. Excluded from action extraction.
# Everything else WhoScored reports is kept.
STRUCTURAL_TYPES = {"Start", "End", "SubstitutionOn", "SubstitutionOff", "FormationChange"}

SESSION = cf_requests.Session(impersonate="safari")
SESSION.headers.update({
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.whoscored.com/",
})

_MATCH_CENTRE_RE = re.compile(r"matchCentreData\s*:\s*(\{.*?\})\s*,\s*\n", re.DOTALL)


# ── generic helpers ─────────────────────────────────────────────────────

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def clean_filename(text: str | None, fallback: str = "Player") -> str:
    text = str(text or fallback).strip()
    text = re.sub(r"[^\w\s.-]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "_", text)
    return text or fallback


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def first_present(row: dict[str, Any], names: list[str]) -> Any:
    lower_map = {k.lower().strip(): k for k in row.keys()}
    for name in names:
        key = lower_map.get(name.lower())
        if key is not None and row.get(key) not in (None, ""):
            return row.get(key)
    return None


# ── cache (match-level — no player_id in the path, by design) ─────────────

def cache_path(cache_dir: str | Path, match_id: str) -> Path:
    d = Path(cache_dir) / str(match_id)
    d.mkdir(parents=True, exist_ok=True)
    return d / "match_centre.json"


def is_cached(cache_dir: str | Path, match_id: str) -> bool:
    return cache_path(cache_dir, match_id).exists()


def load_cached(cache_dir: str | Path, match_id: str) -> dict[str, Any] | None:
    cp = cache_path(cache_dir, match_id)
    if not cp.exists():
        return None
    try:
        return json.loads(cp.read_text(encoding="utf-8"))
    except Exception:
        return None


# ── fetch: HTTP first, Selenium fallback only on failure ──────────────────

@dataclass
class FetchResult:
    ok: bool
    data: dict[str, Any] | None
    source: str  # "cache" | "http" | "browser" | "failed"
    error: str | None = None


def _extract_match_centre(html: str) -> dict[str, Any] | None:
    m = _MATCH_CENTRE_RE.search(html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def _fetch_via_http(match_id: str) -> tuple[dict[str, Any] | None, str | None]:
    url = API_MATCH_URL.format(match_id=match_id)
    try:
        r = SESSION.get(url, timeout=25, allow_redirects=True)
    except Exception as e:
        return None, f"request-error: {e}"

    if r.status_code == 403:
        return None, "http-403"
    if not r.ok:
        return None, f"http-{r.status_code}"

    data = _extract_match_centre(r.text)
    if data is None:
        return None, "matchCentreData-not-found"
    return data, None


def _fetch_via_browser(match_id: str) -> tuple[dict[str, Any] | None, str | None]:
    """Fallback for bot-walled pages. Optional: requires selenium + a chromedriver."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError:
        return None, "selenium-not-installed"

    url = API_MATCH_URL.format(match_id=match_id)
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = None
    try:
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        time.sleep(3.0)  # let the page's JS populate matchCentreData
        html = driver.page_source
    except Exception as e:
        return None, f"browser-error: {e}"
    finally:
        if driver is not None:
            driver.quit()

    data = _extract_match_centre(html)
    if data is None:
        return None, "matchCentreData-not-found-in-browser"
    return data, None


def fetch_match(
    match_id: str,
    cache_dir: str | Path,
    refresh_cache: bool = False,
    allow_browser_fallback: bool = True,
) -> FetchResult:
    """Fetch one match's full matchCentreData. No player filtering — this
    is the raw-lake layer, shared by every player who appeared in the match."""
    cp = cache_path(cache_dir, match_id)

    if cp.exists() and not refresh_cache:
        cached = load_cached(cache_dir, match_id)
        if cached is not None:
            return FetchResult(ok=True, data=cached, source="cache")
        # fall through and refetch if the cache file is corrupt

    data, err = _fetch_via_http(match_id)
    source = "http"

    if data is None and allow_browser_fallback and err in ("http-403", "matchCentreData-not-found"):
        data, err2 = _fetch_via_browser(match_id)
        source = "browser"
        err = err2 if data is None else None

    if data is None:
        return FetchResult(ok=False, data=None, source="failed", error=err)

    cp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return FetchResult(ok=True, data=data, source=source)


# ── parsing / categorization (operates on an already-fetched match) ───────

def player_events(match_centre: dict[str, Any], whoscored_player_id: int) -> list[dict[str, Any]]:
    events = match_centre.get("events") or []
    return [ev for ev in events if ev.get("playerId") == whoscored_player_id]


def categorize(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """DEPRECATED — kept only for anything external still importing it.
    Use extract_player_actions() instead, which captures every action type
    rather than collapsing everything down to three buckets."""
    passes, dribbles, defensive = [], [], []
    for ev in events:
        type_name = (ev.get("type") or {}).get("displayName", "")
        outcome = (ev.get("outcomeType") or {}).get("displayName", "")
        record = {
            "minute": ev.get("minute"),
            "second": ev.get("second"),
            "x": to_float(ev.get("x")),
            "y": to_float(ev.get("y")),
            "end_x": to_float(ev.get("endX")),
            "end_y": to_float(ev.get("endY")),
            "outcome": outcome,
            "type": type_name,
        }
        if type_name == "Pass":
            passes.append(record)
        elif type_name == "TakeOn":
            dribbles.append(record)
        elif type_name in DEFENSIVE_TYPES:
            defensive.append(record)
    return {"passes": passes, "dribbles": dribbles, "defensive_actions": defensive}


def _qualifiers_dict(ev: dict[str, Any]) -> dict[str, Any]:
    """Qualifiers are WhoScored's per-event metadata array — things like pass
    length/angle/zone, cross/through-ball/long-ball flags, shot placement
    descriptors, etc. Most are flag-only (no 'value' key present at all,
    e.g. {"type": {"displayName": "BoxCentre"}}); those become True. Ones
    that do carry a value (Length, Angle, PassEndX, GoalMouthY, ...) keep it."""
    out: dict[str, Any] = {}
    for q in ev.get("qualifiers") or []:
        name = (q.get("type") or {}).get("displayName")
        if not name:
            continue
        out[name] = q.get("value", True)
    return out


def extract_player_actions(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Every action a player performed or had performed on them, with nothing
    collapsed and nothing dropped except pure match-bookkeeping (see
    STRUCTURAL_TYPES). Category is the event's own WhoScored type name
    (Pass, Aerial, BallTouch, Save, Challenge, Tackle, ...) — not squeezed
    into a fixed handful of buckets.

    Each record carries:
      - core fields: type, outcome, minute, second, x, y, end_x, end_y
      - shot/save-specific fields when present: goal_mouth_y, goal_mouth_z
        (where the shot was headed on the goal frame), blocked_x/blocked_y
        (where a shot was blocked)
      - qualifiers: full dict of WhoScored's per-event metadata (pass
        length/angle/zone, cross/through-ball/long-ball flags, shot
        placement descriptors, save type, etc.) — nothing summarized away
      - satisfied_event_types: WhoScored's own derived-event ID list
        (unmapped here — see note in whoscored_common docstring history —
        but preserved raw so a future mapping can be applied without
        re-fetching anything)
    """
    out = []
    for ev in events:
        type_name = (ev.get("type") or {}).get("displayName", "")
        if not type_name or type_name in STRUCTURAL_TYPES:
            continue

        record = {
            "type": type_name,
            "outcome": (ev.get("outcomeType") or {}).get("displayName", ""),
            "minute": ev.get("minute"),
            "second": ev.get("second"),
            "x": to_float(ev.get("x")),
            "y": to_float(ev.get("y")),
            "end_x": to_float(ev.get("endX")),
            "end_y": to_float(ev.get("endY")),
            "goal_mouth_y": to_float(ev.get("goalMouthY")),
            "goal_mouth_z": to_float(ev.get("goalMouthZ")),
            "blocked_x": to_float(ev.get("blockedX")),
            "blocked_y": to_float(ev.get("blockedY")),
            "qualifiers": _qualifiers_dict(ev),
            "satisfied_event_types": ev.get("satisfiedEventsTypes") or [],
        }
        out.append(record)
    return out


def derive_carries(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Heuristic, NOT a native WhoScored event type. A "carry" is inferred
    between two consecutive on-ball events by the same player when the
    time gap is short enough to be continuous possession and the ball
    moved a meaningful distance — the same general approach used by
    open-source event-data packages (socceraction / kloppy) for deriving
    carries from touch events. Treat the output as an approximation.

    Shaped to match extract_player_actions()'s record schema (same keys)
    so carries slot into the same flat action list rather than needing
    separate handling downstream.
    """
    on_ball = [
        ev for ev in events
        if (ev.get("type") or {}).get("displayName") not in (None, "")
        and ev.get("x") is not None
    ]
    on_ball.sort(key=lambda e: (e.get("minute", 0), e.get("second", 0)))

    carries = []
    for prev, curr in zip(on_ball, on_ball[1:]):
        t_prev = (prev.get("minute", 0) or 0) * 60 + (prev.get("second", 0) or 0)
        t_curr = (curr.get("minute", 0) or 0) * 60 + (curr.get("second", 0) or 0)
        gap = t_curr - t_prev
        if gap <= 0 or gap > CARRY_MAX_GAP_SECONDS:
            continue

        end_x = to_float(prev.get("endX")) if prev.get("endX") is not None else to_float(prev.get("x"))
        end_y = to_float(prev.get("endY")) if prev.get("endY") is not None else to_float(prev.get("y"))
        start_x, start_y = end_x, end_y
        dest_x, dest_y = to_float(curr.get("x")), to_float(curr.get("y"))
        if None in (start_x, start_y, dest_x, dest_y):
            continue

        distance = ((dest_x - start_x) ** 2 + (dest_y - start_y) ** 2) ** 0.5
        if distance < CARRY_MIN_DISTANCE:
            continue

        carries.append({
            "type": "Carry",
            "outcome": "Successful",
            "minute": prev.get("minute"),
            "second": prev.get("second"),
            "x": start_x, "y": start_y,
            "end_x": dest_x, "end_y": dest_y,
            "goal_mouth_y": None, "goal_mouth_z": None,
            "blocked_x": None, "blocked_y": None,
            "qualifiers": {"distance": round(distance, 2), "gap_seconds": round(gap, 1)},
            "satisfied_event_types": [],
            "note": "derived (gap+distance heuristic), not a native WhoScored event",
        })
    return carries


def flatten_categories(categorized: dict[str, list[dict[str, Any]]], match_id: str) -> list[dict[str, Any]]:
    """DEPRECATED — paired with the old categorize(). Use attach_match_id()
    with extract_player_actions()'s flat list instead."""
    rows = []
    for category, items in categorized.items():
        for item in items:
            rows.append({"match_id": match_id, "category": category, **item})
    return rows


def attach_match_id(actions: list[dict[str, Any]], match_id: str) -> list[dict[str, Any]]:
    """Stamp match_id onto a flat action list, and set 'category' equal to
    each action's own 'type' — every action gets its native WhoScored type
    as its category rather than being folded into a handful of buckets."""
    return [{"match_id": match_id, "category": a.get("type"), **a} for a in actions]