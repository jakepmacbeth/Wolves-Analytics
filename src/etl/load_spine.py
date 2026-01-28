from __future__ import annotations
from typing import Iterable
import pandas as pd
from sqlalchemy import text
from nba_api.stats.endpoints import LeagueGameFinder
from src.db.engine import get_engine


def fetch_game_ids_for_season(season: str) -> list[str]:
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


def upsert_game_ids(game_ids: Iterable[str], season: str, batch_size: int = 1000) -> int:
    """
    Insert new game_ids into spine.
    ON CONFLICT DO NOTHING makes it safe to re-run.
    Returns number of attempted inserts (not exact inserted count).
    """
    engine = get_engine()

    insert_sql = text("""
        INSERT INTO nba.spine (game_id, season)
        VALUES (:game_id, :season)
        ON CONFLICT (game_id) DO NOTHING
    """)

    attempted = 0
    batch: list[dict] = []

    with engine.begin() as conn:
        for gid in game_ids:
            batch.append({"game_id": gid, "season": season})
            attempted += 1

            if len(batch) >= batch_size:
                conn.execute(insert_sql, batch)
                batch = []

        if batch:
            conn.execute(insert_sql, batch)

    return attempted


def load_seasons(seasons: list[str]) -> None:
    for season in seasons:
        print(f"\n=== Season {season} ===")
        game_ids = fetch_game_ids_for_season(season)
        print(f"Fetched unique game_ids: {len(game_ids)}")

        attempted = upsert_game_ids(game_ids, season=season)
        print(f"Attempted inserts: {attempted} ")


if __name__ == "__main__":
    seasons = ["2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]
    load_seasons(seasons)
    print("\n Spine complete")
