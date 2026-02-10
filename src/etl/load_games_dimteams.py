from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from nba_api.stats.endpoints import boxscoresummaryv3

from src.etl.nba_utils import call_with_retries

from src.etl.nba_utils import call_with_retries
from src.etl.parsing_utils import parse_int, parse_bool  
from src.utils.logger import setup_logger  

# Create logger for this module
logger = setup_logger(__name__)  



# Helper

def _safe_get(d: Dict[str, Any], path: List[str], default=None):
    """
    Safely get nested keys from a dictionary
    """
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


@dataclass
class GameInsert:
    game_id: str
    season: str
    game_date: str  # YYYY-MM-DD 
    game_datetime_utc: Optional[str]  # ISO timestamp string
    home_team_id: int
    away_team_id: int
    game_status: Optional[str]
    arena_name: Optional[str]
    arena_city: Optional[str]
    arena_state: Optional[str]


@dataclass
class TeamDimRow:
    team_id: int
    abbreviation: Optional[str]
    team_name: Optional[str]
    city: Optional[str]
    full_name: Optional[str]


# -----------------------------
# DB queries
# -----------------------------

SQL_SELECT_MISSING_GAMES = """
SELECT s.game_id, s.season
FROM nba.spine s
LEFT JOIN nba.fact_games g ON g.game_id = s.game_id
WHERE s.season = :season
    AND g.game_id IS NULL
ORDER BY s.game_id;
"""

SQL_UPSERT_FACT_GAMES = """
INSERT INTO nba.fact_games (
  game_id, season, game_date, game_datetime_utc,
  home_team_id, away_team_id,
  game_status, arena_name, arena_city, arena_state,
  last_updated_at
)
VALUES (
  :game_id, :season, :game_date, :game_datetime_utc,
  :home_team_id, :away_team_id,
  :game_status, :arena_name, :arena_city, :arena_state,
  NOW()
)
ON CONFLICT (game_id) DO UPDATE SET
  season = EXCLUDED.season,
  game_date = EXCLUDED.game_date,
  game_datetime_utc = EXCLUDED.game_datetime_utc,
  home_team_id = EXCLUDED.home_team_id,
  away_team_id = EXCLUDED.away_team_id,
  game_status = EXCLUDED.game_status,
  arena_name = EXCLUDED.arena_name,
  arena_city = EXCLUDED.arena_city,
  arena_state = EXCLUDED.arena_state,
  last_updated_at = NOW();
"""

SQL_UPSERT_DIM_TEAMS = """
INSERT INTO nba.dim_teams (
  team_id, abbreviation, team_name, city, full_name, last_updated_at
)
VALUES (
  :team_id, :abbreviation, :team_name, :city, :full_name, NOW()
)
ON CONFLICT (team_id) DO UPDATE SET
  abbreviation = COALESCE(EXCLUDED.abbreviation, nba.dim_teams.abbreviation),
  team_name    = COALESCE(EXCLUDED.team_name,    nba.dim_teams.team_name),
  city         = COALESCE(EXCLUDED.city,         nba.dim_teams.city),
  full_name    = COALESCE(EXCLUDED.full_name,    nba.dim_teams.full_name),
  last_updated_at = NOW();
"""


# -----------------------------
# NBA API extraction (V3)
# -----------------------------

def extract_game_structure(
    game_id: str,
    season: str
) -> Tuple[GameInsert, List[TeamDimRow]]:
    """
    Calls BoxScoreSummaryV3 for a game_id and returns:
      - one GameInsert for fact_games
      - up to two TeamDimRow rows for dim_teams
    """
    resp = call_with_retries(lambda: boxscoresummaryv3.BoxScoreSummaryV3(game_id=game_id))
    d = resp.get_dict()

    bss = d.get("boxScoreSummary")
    if not isinstance(bss, dict) or not bss:
        raise ValueError(
            f"BoxScoreSummaryV3 returned no 'boxScoreSummary' object for game_id={game_id}"
        )

    # --- team ids ---
    away_team_id = parse_int(bss.get("awayTeamId"))
    home_team_id = parse_int(bss.get("homeTeamId"))
    if home_team_id is None or away_team_id is None:
        raise ValueError(
            f"Missing homeTeamId/awayTeamId in boxScoreSummary for game_id={game_id}"
        )

    # --- times / date ---
    game_time_utc = bss.get("gameTimeUTC")
    game_et = bss.get("gameEt")

    raw_date = game_time_utc or game_et
    if raw_date is None:
        raise ValueError(
            f"Missing gameTimeUTC/gameEt in boxScoreSummary for game_id={game_id}"
        )
    game_date = str(raw_date)[:10]

    # --- arena ---
    arena = bss.get("arena") if isinstance(bss.get("arena"), dict) else {}
    arena_name = arena.get("arenaName")
    arena_city = arena.get("arenaCity")
    arena_state = arena.get("arenaState")

    # --- status ---
    status_text = bss.get("gameStatusText")

    # --- team identity objects ---
    def dim_from_team_obj(team_obj: Dict[str, Any], fallback_id: int) -> TeamDimRow:
        tid = parse_int(team_obj.get("teamId") or team_obj.get("id")) or fallback_id
        abbrev = team_obj.get("teamTricode") or team_obj.get("abbreviation")
        city = team_obj.get("teamCity") or team_obj.get("city")
        name = team_obj.get("teamName") or team_obj.get("name") or team_obj.get("nickname")
        full_name = team_obj.get("teamName") or (f"{city} {name}" if city and name else None)
        return TeamDimRow(
            team_id=tid,
            abbreviation=abbrev,
            team_name=name,
            city=city,
            full_name=full_name,
        )

    away_team_obj = bss.get("awayTeam") if isinstance(bss.get("awayTeam"), dict) else {}
    home_team_obj = bss.get("homeTeam") if isinstance(bss.get("homeTeam"), dict) else {}

    team_dim_rows: List[TeamDimRow] = [
        dim_from_team_obj(home_team_obj, home_team_id),
        dim_from_team_obj(away_team_obj, away_team_id),
    ]

    game_insert = GameInsert(
        game_id=game_id,
        season=season,
        game_date=game_date,
        game_datetime_utc=str(game_time_utc) if game_time_utc is not None else None,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        game_status=status_text,
        arena_name=arena_name,
        arena_city=arena_city,
        arena_state=arena_state,
    )

    return game_insert, team_dim_rows


# -----------------------------
# Main loader
# -----------------------------

def load_game_structure(
    engine: Engine,
    season: str,
    sleep_seconds: float = 1.2,
    limit: Optional[int] = None
) -> None:
    """
    Stage 2 loader:
      - Find spine games missing from fact_games
      - For each game_id:
          * call BoxScoreSummaryV3
          * upsert dim_teams
          * upsert fact_games
    """
    with engine.begin() as conn:
        rows = conn.execute(
            text(SQL_SELECT_MISSING_GAMES),
            {"season": season},
        ).mappings().all()

    if limit is not None:
        rows = rows[:limit]

    if not rows:
        logger.info("No missing games found. Game structure is up to date.")
        return

    logger.info(f"Found {len(rows)} spine games missing from nba.fact_games.")

    success = 0
    failed = 0

    for i, r in enumerate(rows, start=1):
        game_id = r["game_id"]
        season = r["season"]

        try:
            game_insert, team_dim_rows = extract_game_structure(
                game_id=game_id,
                season=season,
            )

            with engine.begin() as conn:
                for t in team_dim_rows:
                    conn.execute(
                        text(SQL_UPSERT_DIM_TEAMS),
                        {
                            "team_id": t.team_id,
                            "abbreviation": t.abbreviation,
                            "team_name": t.team_name,
                            "city": t.city,
                            "full_name": t.full_name,
                        },
                    )

                conn.execute(
                    text(SQL_UPSERT_FACT_GAMES),
                    {
                        "game_id": game_insert.game_id,
                        "season": game_insert.season,
                        "game_date": game_insert.game_date,
                        "game_datetime_utc": game_insert.game_datetime_utc,
                        "home_team_id": game_insert.home_team_id,
                        "away_team_id": game_insert.away_team_id,
                        "game_status": game_insert.game_status,
                        "arena_name": game_insert.arena_name,
                        "arena_city": game_insert.arena_city,
                        "arena_state": game_insert.arena_state,
                    },
                )

            success += 1
            logger.info(f"[{i}/{len(rows)}] Loaded game structure for game_id={game_id}")


        except Exception as e:
            failed += 1
            logger.error(f"ERROR loading game_id={game_id}: {e}")

        time.sleep(sleep_seconds)

    logger.info(
    f"Game structure complete. Success={success}, Failed={failed}, Attempted={len(rows)}"
)