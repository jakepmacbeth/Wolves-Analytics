from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, List, Dict

from sqlalchemy import text
from sqlalchemy.engine import Engine

from nba_api.stats.endpoints import boxscoretraditionalv3

from src.etl.nba_utils import call_with_retries

from src.etl.nba_utils import call_with_retries
from src.etl.parsing_utils import parse_int, parse_bool  
from src.utils.logger import setup_logger 

logger = setup_logger(__name__)  

# -----------------------------
# Failure logging
# -----------------------------

FAILED_LOG = Path("logs/failed_playerbox.txt")
FAILED_LOG.parent.mkdir(parents=True, exist_ok=True)


def log_failed_playerbox(game_id: str, err: Exception) -> None:
    FAILED_LOG.parent.mkdir(parents=True, exist_ok=True)
    with FAILED_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{game_id}\t{type(err).__name__}\t{err}\n")


# -----------------------------
# Helpers
# -----------------------------


@dataclass
class PlayerDimSeed:
    player_id: int
    full_name: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    position: Optional[str]


@dataclass
class PlayerBoxRow:
    game_id: str
    player_id: int
    team_id: int
    season: str
    is_home: Optional[bool]
    opponent_team_id: Optional[int]
    starter_flag: Optional[bool]
    minutes: Optional[str]
    pts: Optional[int]
    reb: Optional[int]
    ast: Optional[int]
    stl: Optional[int]
    blk: Optional[int]
    tov: Optional[int]
    pf: Optional[int]
    fgm: Optional[int]
    fga: Optional[int]
    fg3m: Optional[int]
    fg3a: Optional[int]
    ftm: Optional[int]
    fta: Optional[int]
    plus_minus: Optional[int]


# -----------------------------
# DB queries
# -----------------------------

SQL_SELECT_GAMES_MISSING_PLAYERBOX = """
SELECT
  g.game_id,
  g.season,
  g.home_team_id,
  g.away_team_id
FROM nba.fact_games g
LEFT JOIN nba.playerbox_pergame p
  ON p.game_id = g.game_id
WHERE g.season = :season
  AND p.game_id IS NULL
ORDER BY g.game_date, g.game_id;
"""

SQL_UPSERT_DIM_PLAYERS_SEED = """
INSERT INTO nba.dim_players (
  player_id,
  full_name, first_name, last_name,
  position,
  last_updated_at
)
VALUES (
  :player_id,
  :full_name, :first_name, :last_name,
  :position,
  NOW()
)
ON CONFLICT (player_id) DO UPDATE SET
  full_name = COALESCE(EXCLUDED.full_name, nba.dim_players.full_name),
  first_name = COALESCE(EXCLUDED.first_name, nba.dim_players.first_name),
  last_name = COALESCE(EXCLUDED.last_name, nba.dim_players.last_name),
  position = COALESCE(EXCLUDED.position, nba.dim_players.position),
  last_updated_at = NOW();
"""

SQL_UPSERT_PLAYERBOX = """
INSERT INTO nba.playerbox_pergame (
  game_id, player_id,
  team_id, season,
  is_home, opponent_team_id,
  starter_flag, minutes,
  pts, reb, ast, stl, blk, tov, pf,
  fgm, fga, fg3m, fg3a, ftm, fta,
  plus_minus,
  last_updated_at
)
VALUES (
  :game_id, :player_id,
  :team_id, :season,
  :is_home, :opponent_team_id,
  :starter_flag, :minutes,
  :pts, :reb, :ast, :stl, :blk, :tov, :pf,
  :fgm, :fga, :fg3m, :fg3a, :ftm, :fta,
  :plus_minus,
  NOW()
)
ON CONFLICT (game_id, player_id) DO UPDATE SET
  team_id = EXCLUDED.team_id,
  season = EXCLUDED.season,
  is_home = EXCLUDED.is_home,
  opponent_team_id = EXCLUDED.opponent_team_id,
  starter_flag = EXCLUDED.starter_flag,
  minutes = EXCLUDED.minutes,
  pts = EXCLUDED.pts,
  reb = EXCLUDED.reb,
  ast = EXCLUDED.ast,
  stl = EXCLUDED.stl,
  blk = EXCLUDED.blk,
  tov = EXCLUDED.tov,
  pf = EXCLUDED.pf,
  fgm = EXCLUDED.fgm,
  fga = EXCLUDED.fga,
  fg3m = EXCLUDED.fg3m,
  fg3a = EXCLUDED.fg3a,
  ftm = EXCLUDED.ftm,
  fta = EXCLUDED.fta,
  plus_minus = EXCLUDED.plus_minus,
  last_updated_at = NOW();
"""


# -----------------------------
# Parsing (Option A: PlayerStats via get_data_frames)
# -----------------------------

def extract_dimplayer_boxscores(
    game_id: str,
    season: str,
    home_team_id: int,
    away_team_id: int
) -> tuple[list[PlayerDimSeed], list[PlayerBoxRow]]:
    # Call endpoint once; use PlayerStats dataframe (tabular) instead of nested dict parsing
    endpoint = call_with_retries(lambda: boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id))
    dfs = endpoint.get_data_frames()
    if not dfs:
        raise ValueError(f"No data frames returned for game_id={game_id}")

    # In BoxScoreTraditionalV3, df[0] is typically PlayerStats
    df_players = dfs[0]
    if df_players is None or df_players.empty:
        raise ValueError(f"Empty PlayerStats data for game_id={game_id}")

    # Defensive: ensure required columns exist
    required_cols = {"personId", "teamId"}
    missing = [c for c in required_cols if c not in df_players.columns]
    if missing:
        raise ValueError(f"Missing required columns {missing} in PlayerStats for game_id={game_id}. Got: {list(df_players.columns)}")

    dim_seeds: Dict[int, PlayerDimSeed] = {}
    box_rows: List[PlayerBoxRow] = []

    for _, r in df_players.iterrows():
        pid = parse_int(r.get("personId"))
        tid = parse_int(r.get("teamId"))
        if pid is None or tid is None:
            continue

        is_home = True if tid == home_team_id else False if tid == away_team_id else None
        opp_id = away_team_id if is_home is True else home_team_id if is_home is False else None

        first_name = r.get("firstName")
        last_name = r.get("familyName")
        position = r.get("position")

        # If you want a "full name", the dataset doesn't provide it directly; construct it.
        full_name = None
        if first_name is not None or last_name is not None:
            fn = str(first_name) if first_name is not None else ""
            ln = str(last_name) if last_name is not None else ""
            full = (fn + " " + ln).strip()
            full_name = full if full else None

        if pid not in dim_seeds:
            dim_seeds[pid] = PlayerDimSeed(
                player_id=pid,
                full_name=full_name,
                first_name=str(first_name) if first_name is not None else None,
                last_name=str(last_name) if last_name is not None else None,
                position=str(position) if position is not None else None,
            )

        minutes = r.get("minutes")
        minutes_str = str(minutes) if minutes is not None else None

        # Starter flag is not in the PlayerStats list you provided; keep None unless you add logic later
        starter_flag = None

        row = PlayerBoxRow(
            game_id=game_id,
            player_id=pid,
            team_id=tid,
            season=season,
            is_home=is_home,
            opponent_team_id=opp_id,
            starter_flag=starter_flag,
            minutes=minutes_str,

            pts=parse_int(r.get("points")),
            reb=parse_int(r.get("reboundsTotal")),
            ast=parse_int(r.get("assists")),
            stl=parse_int(r.get("steals")),
            blk=parse_int(r.get("blocks")),
            tov=parse_int(r.get("turnovers")),
            pf=parse_int(r.get("foulsPersonal")),

            fgm=parse_int(r.get("fieldGoalsMade")),
            fga=parse_int(r.get("fieldGoalsAttempted")),
            fg3m=parse_int(r.get("threePointersMade")),
            fg3a=parse_int(r.get("threePointersAttempted")),
            ftm=parse_int(r.get("freeThrowsMade")),
            fta=parse_int(r.get("freeThrowsAttempted")),

            plus_minus=parse_int(r.get("plusMinusPoints")),
        )

        box_rows.append(row)

    if not box_rows:
        raise ValueError(f"No PlayerStats rows parsed into box_rows for game_id={game_id}")

    return list(dim_seeds.values()), box_rows


# -----------------------------
# Loader
# -----------------------------

def load_dimplayer_boxscores(
    engine: Engine,
    season: str,
    sleep_seconds: float = 0.8,
    limit: Optional[int] = None
) -> None:
    with engine.begin() as conn:
        rows = conn.execute(
            text(SQL_SELECT_GAMES_MISSING_PLAYERBOX),
            {"season": season},
        ).mappings().all()

    if limit is not None:
        rows = rows[:limit]

    if not rows:
        logger.info("No missing player boxscores found. playerbox_pergame is up to date.")
        return

    logger.info(f"Found {len(rows)} games missing playerbox_pergame.")

    success = 0
    failed = 0

    for i, r in enumerate(rows, start=1):
        game_id = r["game_id"]
        season = r["season"]
        home_team_id = int(r["home_team_id"])
        away_team_id = int(r["away_team_id"])

        try:
            dim_seeds, box_rows = extract_dimplayer_boxscores(game_id, season, home_team_id, away_team_id)

            with engine.begin() as conn:
                for drow in dim_seeds:
                    conn.execute(
                        text(SQL_UPSERT_DIM_PLAYERS_SEED),
                        {
                            "player_id": drow.player_id,
                            "full_name": drow.full_name,
                            "first_name": drow.first_name,
                            "last_name": drow.last_name,
                            "position": drow.position,
                        },
                    )

                for row in box_rows:
                    conn.execute(
                        text(SQL_UPSERT_PLAYERBOX),
                        {
                            "game_id": row.game_id,
                            "player_id": row.player_id,
                            "team_id": row.team_id,
                            "season": row.season,
                            "is_home": row.is_home,
                            "opponent_team_id": row.opponent_team_id,
                            "starter_flag": row.starter_flag,
                            "minutes": row.minutes,
                            "pts": row.pts,
                            "reb": row.reb,
                            "ast": row.ast,
                            "stl": row.stl,
                            "blk": row.blk,
                            "tov": row.tov,
                            "pf": row.pf,
                            "fgm": row.fgm,
                            "fga": row.fga,
                            "fg3m": row.fg3m,
                            "fg3a": row.fg3a,
                            "ftm": row.ftm,
                            "fta": row.fta,
                            "plus_minus": row.plus_minus,
                        },
                    )

            success += 1
            logger.info(f"[{i}/{len(rows)}] Loaded playerbox_pergame for game_id={game_id} (rows={len(box_rows)})")

        except Exception as e:
            failed += 1
            logger.info(f"ERROR loading playerbox_pergame for game_id={game_id}: {e}")
            log_failed_playerbox(game_id, e)

        time.sleep(sleep_seconds)

    logger.info(f"Player boxscore load complete. Success={success}, Failed={failed}, Attempted={len(rows)}")
