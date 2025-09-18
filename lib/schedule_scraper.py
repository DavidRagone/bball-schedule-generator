#!/usr/bin/env python3
import argparse
import csv
import os
import re
import sys
import time
from typing import Dict, Generator, Iterable, List, Optional, Tuple

import requests

BASE_PUBLIC   = "https://bracketteam.com/api/get-public-tournament"
BASE_SCHEDULE = "https://bracketteam.com/api/get-division-schedule"

MATCHES_PER_PAGE = 20
SLEEP_TIME = 0.5     # polite throttle between pages
TIMEOUT    = 30
MAX_RETRIES = 3
BACKOFF     = 1.5    # exponential backoff factor

# --- Helpers -----------------------------------------------------------------

def parse_event_id(s: str) -> int:
    """
    Accept either a numeric tournament/event id or a schedules page URL like:
      https://bracketteam.com/event/6489/2025_Fall_Tip_Off/schedules
    """
    m = re.search(r"/event/(\d+)/", s)
    if m:
        return int(m.group(1))
    if re.fullmatch(r"\d+", s.strip()):
        return int(s)
    raise ValueError(f"Could not parse event/tournament id from: {s}")

def make_session(token: str) -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "BracketTeamScraper/1.0 (+https://example.org)",
        "Accept": "application/json, */*",
        "X-Authorization": token,
    })
    return sess

def get_json(session: requests.Session, url: str, params: Dict[str, str]) -> Optional[dict]:
    """
    GET with simple retry on 5xx/429 and JSON parse safety.
    Returns parsed JSON dict or None on failure.
    """
    attempt = 0
    delay = BACKOFF
    while True:
        attempt += 1
        try:
            resp = session.get(url, params=params, timeout=TIMEOUT)
            # Retry on rate limit or 5xx
            if resp.status_code in (429,) or 500 <= resp.status_code < 600:
                if attempt < MAX_RETRIES:
                    time.sleep(delay)
                    delay *= BACKOFF
                    continue
                else:
                    sys.stderr.write(f"HTTP {resp.status_code} after {attempt} attempts: {resp.url}\n")
                    return None

            if not resp.ok:
                sys.stderr.write(f"HTTP {resp.status_code}: {resp.url}\n")
                return None
            return resp.json()
        except requests.RequestException as e:
            if attempt < MAX_RETRIES:
                time.sleep(delay)
                delay *= BACKOFF
                continue
            sys.stderr.write(f"Request error after {attempt} attempts: {url} :: {e}\n")
            return None
        except ValueError as e:
            sys.stderr.write(f"JSON parse error: {url} :: {e}\n")
            return None

# --- API wrappers -------------------------------------------------------------


def get_divisions(
        session: requests.Session, tournament_id: int) -> List[Tuple[int, str]]:
    """
    Returns list of (division_id, division_name)
    """
    params = {"tournament_id": tournament_id}
    data = get_json(session, BASE_PUBLIC, params) or {}
    divisions = (data.get("content", {}) \
                     .get("tournament", {}) \
                     .get("divisions", []))
    out: List[Tuple[int, str]] = []
    for d in divisions:
        div_id   = d.get("id")
        div_name = d.get("division_name") or d.get("name") or f"Division {div_id}"
        if div_id is not None:
            out.append((int(div_id), div_name))
    return out


def iter_matches(session: requests.Session, tournament_id: int, division_id: int) -> Generator[dict, None, None]:
    """
    Yields match dicts across pages for a single division.
    """
    page = 1
    while True:
        params = {
            "tournament_id": tournament_id,
            "division_id": division_id,
            "page": page,
            "start_date": "null",
            "end_date": "null",
            "filter_team": "null",
            "pool_id": "null",
            "bracket_id": "null",
            "venue_id": "null",
            "court_id": "null",
            "matches_per_page": MATCHES_PER_PAGE,
            "only_games": "false",
        }
        data = get_json(session, BASE_SCHEDULE, params) or {}
        matches = (data.get("content", {}) or {}).get("matches", []) or []
        if not matches:
            break
        for m in matches:
            yield m
        if len(matches) < MATCHES_PER_PAGE:
            break
        page += 1
        time.sleep(SLEEP_TIME)

# --- Field extraction (defensive) --------------------------------------------


def first_nonempty(*values) -> str:
    for v in values:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def get_nested(d: dict, *path, default: str = "") -> str:
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    if isinstance(cur, str):
        return cur
    return default


def extract_time(m: dict) -> str:
    # Common fields seen across BracketTeam payloads
    return first_nonempty(
        m.get("start_date_time"),
        m.get("date"),
        m.get("start_time"),
        m.get("start_datetime"),
        # Sometimes “start_time” is separate and needs the date
    )


def extract_location(m: dict) -> str:
    return first_nonempty(
            get_nested(m, "court", "venue", "name", default=""),
            m.get("venue_name"),
            m.get("facility_name"),
            m.get("location"),
            get_nested(m, "venue", "name", default=""),
            get_nested(m, "facility", "name", default=""),
            get_nested(m, "site", "name", default=""),
    )


def extract_court(m: dict) -> str:
    return first_nonempty(
            get_nested(m, "court", "court_name", default=""),
            m.get("court_name"),
            m.get("court"),
            m.get("field"),
            get_nested(m, "court", "name", default=""),
            get_nested(m, "resource", "name", default=""),
            )


def extract_team_name(obj: Optional[dict], fallback_keys: Iterable[str]) -> str:
    if isinstance(obj, dict):
        name = obj.get("name") or obj.get("team_name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    # fallback flat keys on the match
    for k in fallback_keys:
        v = obj.get(k) if isinstance(obj, dict) else None
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def extract_home(m: dict) -> str:
    # Try nested home_team first; then some flat variants if present
    if isinstance(m.get("home_team"), dict):
        v = m["home_team"].get("name") or m["home_team"].get("team_name")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return first_nonempty(
        m.get("home_name"),
        m.get("homeTeamName"),
        m.get("team_home_name"),
    )


def extract_away(m: dict) -> str:
    if isinstance(m.get("away_team"), dict):
        v = m["away_team"].get("name") or m["away_team"].get("team_name")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return first_nonempty(
        m.get("away_name"),
        m.get("awayTeamName"),
        m.get("team_away_name"),
    )

# --- Main --------------------------------------------------------------------


def run(event_ref: str, out_csv: str, token: Optional[str]) -> int:
    tournament_id = parse_event_id(event_ref)
    token = token or os.environ.get("BRACKETTEAM_TOKEN") or "fB0SC4jghlUrszbzgFmHyAPeWvzwc5kWV3yhdP9xhs8ysLRkDDGpomh5gmqoZdAc"
    if not token:
        sys.stderr.write("Missing API token. Provide --token or set BRACKETTEAM_TOKEN.\n")
        return 2

    session = make_session(token)

    divisions = get_divisions(session, tournament_id)
    if not divisions:
        sys.stderr.write(f"No divisions found for tournament {tournament_id}\n")
        return 1

    # Write CSV
    fieldnames = ["game_start_time", "location", "court", "division", "home_team", "away_team"]
    rows_written = 0
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for div_id, div_name in divisions:
            for m in iter_matches(session, tournament_id, div_id):
                w.writerow({
                    "game_start_time": extract_time(m),
                    "location":        extract_location(m),
                    "court":           extract_court(m),
                    "division":        div_name,
                    "home_team":       extract_home(m),
                    "away_team":       extract_away(m),
                })
                rows_written += 1
            # be polite between divisions
            time.sleep(SLEEP_TIME)

    print(f"Wrote {rows_written} games to {out_csv}")
    return 0

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Export a BracketTeam event's schedules to CSV.")
    ap.add_argument("event", help="Event URL like https://bracketteam.com/event/6489/... or just the numeric id (e.g., 6489)")
    ap.add_argument("-o", "--output", default="schedules.csv", help="Output CSV file (default: schedules.csv)")
    ap.add_argument("--token", default=None, help="X-Authorization token (or set BRACKETTEAM_TOKEN)")
    args = ap.parse_args()
    sys.exit(run(args.event, args.output, args.token))
