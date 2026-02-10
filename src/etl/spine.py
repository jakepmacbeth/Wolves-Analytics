from __future__ import annotations

from typing import Iterable
from sqlalchemy import text
from nba_api.stats.endpoints import LeagueGameFinder

from src.db.engine import get_engine


def extract_gameids(season: str) -> list[str]:
    """
    LeagueGameFinder returns one row per TEAM per game (so each game appears twice).
    We dedupe to unique GAME_IDs.
    """
    lgf = LeagueGameFinder(
        season_nullable=season,
        league_id_nullable="00",
        season_type_nullable="Regular Season"
    )
    df = lgf.get_data_frames()[0]
    df = df[df["WL"].notna()]

    if "GAME_ID" not in df.columns:
        raise ValueError(f"Expected GAME_ID column. Got columns: {list(df.columns)}")

    game_ids = (
        df["GAME_ID"]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .tolist()
    )
    return game_ids


def upsert_game_ids(game_ids: Iterable[str], season: str) -> tuple[int, int]:
    """
    Insert new game_ids into nba.spine and return:
      (attempted_count, inserted_count)
    Uses a single INSERT ... SELECT FROM unnest(...) so RETURNING works reliably.
    """
    ids = [str(gid) for gid in game_ids]
    attempted = len(ids)
    if attempted == 0:
        return 0, 0

    engine = get_engine()

    insert_sql = text("""
        WITH incoming AS (
            SELECT unnest(:game_ids) AS game_id
        )
        INSERT INTO nba.spine (game_id, season)
        SELECT game_id, :season
        FROM incoming
        ON CONFLICT (game_id) DO NOTHING
        RETURNING game_id;
    """)

    with engine.begin() as conn:
        result = conn.execute(insert_sql, {"game_ids": ids, "season": season})
        inserted = len(result.fetchall())

    return attempted, inserted



def load_seasons(season: str) -> None:
        print(f"\n=== Season {season} ===")
        game_ids = extract_gameids(season)
        print(f"Fetched unique game_ids: {len(game_ids)}")

        attempted, inserted = upsert_game_ids(game_ids, season=season)
        print(f"Attempted inserts: {attempted}")
        print(f"New games inserted: {inserted}")

