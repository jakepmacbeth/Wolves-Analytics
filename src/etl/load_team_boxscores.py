from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from nba_api.stats.endpoints import boxscoretraditionalv3, boxscoreadvancedv3

from src.etl.nba_utils import call_with_retries

from src.etl.nba_utils import call_with_retries
from src.etl.parsing_utils import parse_int, parse_float  
from src.utils.logger import setup_logger  

logger = setup_logger(__name__)  


# -----------------------------
# Failure logging
# -----------------------------

FAILED_LOG = Path("logs/failed_teambox.txt")
FAILED_LOG.parent.mkdir(parents=True, exist_ok=True)


def log_failed_teambox(game_id: str, err: Exception) -> None:
    FAILED_LOG.parent.mkdir(parents=True, exist_ok=True)
    with FAILED_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{game_id}\t{type(err).__name__}\t{err}\n")




@dataclass
class TeamBoxRow:
    game_id: str
    team_id: int
    season: str
    is_home: Optional[bool]
    opponent_team_id: Optional[int]

    minutes: Optional[str]
    pts: Optional[int]
    fgm: Optional[int]
    fga: Optional[int]
    fg3m: Optional[int]
    fg3a: Optional[int]
    ftm: Optional[int]
    fta: Optional[int]
    oreb: Optional[int]
    dreb: Optional[int]
    reb: Optional[int]
    ast: Optional[int]
    stl: Optional[int]
    blk: Optional[int]
    tov: Optional[int]
    pf: Optional[int]

    off_rating: Optional[float]
    def_rating: Optional[float]
    net_rating: Optional[float]
    pace: Optional[float]
    ts_pct: Optional[float]


# -----------------------------
# DB queries
# -----------------------------

SQL_SELECT_GAMES_MISSING_TEAMBOX = """
SELECT
  g.game_id,
  g.season,
  g.home_team_id,
  g.away_team_id
FROM nba.fact_games g
LEFT JOIN nba.teambox_pergame t
  ON t.game_id = g.game_id
WHERE g.season = :season
    AND t.game_id IS NULL
ORDER BY g.game_date, g.game_id;
"""

SQL_UPSERT_TEAMBOX = """
INSERT INTO nba.teambox_pergame (
  game_id, team_id, season,
  is_home, opponent_team_id,
  minutes, pts,
  fgm, fga, fg3m, fg3a, ftm, fta,
  oreb, dreb, reb, ast, stl, blk, tov, pf,
  off_rating, def_rating, net_rating, pace, ts_pct,
  last_updated_at
)
VALUES (
  :game_id, :team_id, :season,
  :is_home, :opponent_team_id,
  :minutes, :pts,
  :fgm, :fga, :fg3m, :fg3a, :ftm, :fta,
  :oreb, :dreb, :reb, :ast, :stl, :blk, :tov, :pf,
  :off_rating, :def_rating, :net_rating, :pace, :ts_pct,
  NOW()
)
ON CONFLICT (game_id, team_id) DO UPDATE SET
  season = EXCLUDED.season,
  is_home = EXCLUDED.is_home,
  opponent_team_id = EXCLUDED.opponent_team_id,
  minutes = EXCLUDED.minutes,
  pts = EXCLUDED.pts,
  fgm = EXCLUDED.fgm,
  fga = EXCLUDED.fga,
  fg3m = EXCLUDED.fg3m,
  fg3a = EXCLUDED.fg3a,
  ftm = EXCLUDED.ftm,
  fta = EXCLUDED.fta,
  oreb = EXCLUDED.oreb,
  dreb = EXCLUDED.dreb,
  reb = EXCLUDED.reb,
  ast = EXCLUDED.ast,
  stl = EXCLUDED.stl,
  blk = EXCLUDED.blk,
  tov = EXCLUDED.tov,
  pf = EXCLUDED.pf,
  off_rating = EXCLUDED.off_rating,
  def_rating = EXCLUDED.def_rating,
  net_rating = EXCLUDED.net_rating,
  pace = EXCLUDED.pace,
  ts_pct = EXCLUDED.ts_pct,
  last_updated_at = NOW();
"""


# -----------------------------
# Parsing (defensive)
# -----------------------------

def extract_team_stats_traditional(d: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    """
    Return {team_id: stats_dict} from BoxScoreTraditionalV3.
    Shape can drift; we defensive-check common patterns.
    """
    root = None
    for k in ("boxScoreTraditional", "boxScoreTraditionalV3", "boxScoreTraditionalv3"):
        if isinstance(d.get(k), dict):
            root = d.get(k)
            break
    if root is None:
        root = d

    out: Dict[int, Dict[str, Any]] = {}

    for side in ("homeTeam", "awayTeam"):
        team_obj = root.get(side)
        if not isinstance(team_obj, dict):
            continue
        tid = parse_int(team_obj.get("teamId") or team_obj.get("teamID") or team_obj.get("id"))
        stats = team_obj.get("statistics") if isinstance(team_obj.get("statistics"), dict) else None
        if tid is not None and isinstance(stats, dict):
            out[tid] = stats

    if not out and isinstance(root.get("teams"), list):
        for t in root["teams"]:
            if not isinstance(t, dict):
                continue
            tid = parse_int(t.get("teamId") or t.get("teamID") or t.get("id"))
            stats = t.get("statistics") if isinstance(t.get("statistics"), dict) else None
            if tid is not None and isinstance(stats, dict):
                out[tid] = stats

    return out


def extract_team_stats_advanced(d: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    """
    Return {team_id: stats_dict} from BoxScoreAdvancedV3.
    """
    root = None
    for k in ("boxScoreAdvanced", "boxScoreAdvancedV3", "boxScoreAdvancedv3"):
        if isinstance(d.get(k), dict):
            root = d.get(k)
            break
    if root is None:
        root = d

    out: Dict[int, Dict[str, Any]] = {}

    for side in ("homeTeam", "awayTeam"):
        team_obj = root.get(side)
        if not isinstance(team_obj, dict):
            continue
        tid = parse_int(team_obj.get("teamId") or team_obj.get("teamID") or team_obj.get("id"))
        stats = team_obj.get("statistics") if isinstance(team_obj.get("statistics"), dict) else None
        if tid is not None and isinstance(stats, dict):
            out[tid] = stats

    if not out and isinstance(root.get("teams"), list):
        for t in root["teams"]:
            if not isinstance(t, dict):
                continue
            tid = parse_int(t.get("teamId") or t.get("teamID") or t.get("id"))
            stats = t.get("statistics") if isinstance(t.get("statistics"), dict) else None
            if tid is not None and isinstance(stats, dict):
                out[tid] = stats

    return out


def fetch_teambox_rows(
    game_id: str,
    season: str,
    home_team_id: int,
    away_team_id: int
) -> Tuple[TeamBoxRow, TeamBoxRow]:
    trad = call_with_retries(lambda: boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)).get_dict()
    adv = call_with_retries(lambda: boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id)).get_dict()

    trad_by_team = extract_team_stats_traditional(trad)
    adv_by_team = extract_team_stats_advanced(adv)

    def make_row(team_id: int, is_home: bool, opp_id: int) -> TeamBoxRow:
        tstats = trad_by_team.get(team_id, {})
        astats = adv_by_team.get(team_id, {})

        return TeamBoxRow(
            game_id=game_id,
            team_id=team_id,
            season=season,
            is_home=is_home,
            opponent_team_id=opp_id,

            minutes=str(tstats.get("minutes")) if tstats.get("minutes") is not None else None,
            pts=parse_int(tstats.get("points") or tstats.get("pts")),
            fgm=parse_int(tstats.get("fieldGoalsMade") or tstats.get("fgm")),
            fga=parse_int(tstats.get("fieldGoalsAttempted") or tstats.get("fga")),
            fg3m=parse_int(tstats.get("threePointersMade") or tstats.get("fg3m")),
            fg3a=parse_int(tstats.get("threePointersAttempted") or tstats.get("fg3a")),
            ftm=parse_int(tstats.get("freeThrowsMade") or tstats.get("ftm")),
            fta=parse_int(tstats.get("freeThrowsAttempted") or tstats.get("fta")),
            oreb=parse_int(tstats.get("reboundsOffensive") or tstats.get("oreb")),
            dreb=parse_int(tstats.get("reboundsDefensive") or tstats.get("dreb")),
            reb=parse_int(tstats.get("reboundsTotal") or tstats.get("reb")),
            ast=parse_int(tstats.get("assists") or tstats.get("ast")),
            stl=parse_int(tstats.get("steals") or tstats.get("stl")),
            blk=parse_int(tstats.get("blocks") or tstats.get("blk")),
            tov=parse_int(tstats.get("turnovers") or tstats.get("tov")),
            pf=parse_int(tstats.get("foulsPersonal") or tstats.get("pf")),

            off_rating=parse_float(astats.get("offensiveRating") or astats.get("offRating")),
            def_rating=parse_float(astats.get("defensiveRating") or astats.get("defRating")),
            net_rating=parse_float(astats.get("netRating")),
            pace=parse_float(astats.get("pace")),
            ts_pct=parse_float(astats.get("trueShootingPercentage") or astats.get("tsPct")),
        )

    home_row = make_row(home_team_id, True, away_team_id)
    away_row = make_row(away_team_id, False, home_team_id)
    return home_row, away_row


# -----------------------------
# Loader
# -----------------------------

def load_teambox_scores(
    engine: Engine,
    season: str, 
    sleep_seconds: float = 0.8,
    limit: Optional[int] = None
) -> None:
    with engine.begin() as conn:
        rows = conn.execute(
    text(SQL_SELECT_GAMES_MISSING_TEAMBOX),
    {"season": season},
).mappings().all()

    if limit is not None:
        rows = rows[:limit]

    if not rows:
        logger.info("No missing team boxscores found. teambox_pergame is up to date.")
        return

    logger.info(f"Found {len(rows)} games missing teambox_pergame.")

    success = 0
    failed = 0

    for i, r in enumerate(rows, start=1):
        game_id = r["game_id"]
        season = r["season"]
        home_team_id = int(r["home_team_id"])
        away_team_id = int(r["away_team_id"])

        try:
            home_row, away_row = fetch_teambox_rows(game_id, season, home_team_id, away_team_id)

            with engine.begin() as conn:
                for row in (home_row, away_row):
                    conn.execute(
                        text(SQL_UPSERT_TEAMBOX),
                        {
                            "game_id": row.game_id,
                            "team_id": row.team_id,
                            "season": row.season,
                            "is_home": row.is_home,
                            "opponent_team_id": row.opponent_team_id,
                            "minutes": row.minutes,
                            "pts": row.pts,
                            "fgm": row.fgm,
                            "fga": row.fga,
                            "fg3m": row.fg3m,
                            "fg3a": row.fg3a,
                            "ftm": row.ftm,
                            "fta": row.fta,
                            "oreb": row.oreb,
                            "dreb": row.dreb,
                            "reb": row.reb,
                            "ast": row.ast,
                            "stl": row.stl,
                            "blk": row.blk,
                            "tov": row.tov,
                            "pf": row.pf,
                            "off_rating": row.off_rating,
                            "def_rating": row.def_rating,
                            "net_rating": row.net_rating,
                            "pace": row.pace,
                            "ts_pct": row.ts_pct,
                        },
                    )

            success += 1
            logger.info(f"[{i}/{len(rows)}] Loaded teambox_pergame for game_id={game_id}")

        except Exception as e:
            failed += 1
            logger.error(f"ERROR loading teambox_pergame for game_id={game_id}: {e}")
            log_failed_teambox(game_id, e)

        time.sleep(sleep_seconds)

    logger.info(f"Team boxscore load complete. Success={success}, Failed={failed}, Attempted={len(rows)}")