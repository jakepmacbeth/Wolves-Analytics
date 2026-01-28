import pandas as pd
from sqlalchemy import text
from src.db.engine import get_engine

UPSERT_SQL = text("""
INSERT INTO nba.games (
  game_id, game_date, season, season_type,
  home_team_id, home_team_abbr, away_team_id, away_team_abbr,
  home_points, away_points, game_status
)
VALUES (
  :game_id, :game_date, :season, :season_type,
  :home_team_id, :home_team_abbr, :away_team_id, :away_team_abbr,
  :home_points, :away_points, :game_status
)
ON CONFLICT (game_id) DO UPDATE SET
  game_date = EXCLUDED.game_date,
  season = EXCLUDED.season,
  season_type = EXCLUDED.season_type,
  home_team_id = EXCLUDED.home_team_id,
  home_team_abbr = EXCLUDED.home_team_abbr,
  away_team_id = EXCLUDED.away_team_id,
  away_team_abbr = EXCLUDED.away_team_abbr,
  home_points = EXCLUDED.home_points,
  away_points = EXCLUDED.away_points,
  game_status = EXCLUDED.game_status,
  last_updated = NOW();
""")

def upsert_games(gameheader: pd.DataFrame, game_date: str, season: str, season_type: str) -> int:
    """
    game_date here should be 'YYYY-MM-DD' (we convert it to DATE cleanly)
    """
    engine = get_engine()

    # Map scoreboard columns -> our table fields
    df = pd.DataFrame({
        "game_id": gameheader["game_id"].astype(str),
        "game_date": pd.to_datetime(game_date).date(),
        "season": season,
        "season_type": season_type,
        "home_team_id": gameheader.get("home_team_id"),
        "home_team_abbr": gameheader.get("home_team_abbreviation"),
        "away_team_id": gameheader.get("visitor_team_id"),
        "away_team_abbr": gameheader.get("visitor_team_abbreviation"),
        "home_points": gameheader.get("home_team_score"),
        "away_points": gameheader.get("visitor_team_score"),
        "game_status": gameheader.get("game_status_text"),
    })

    records = df.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, records)

    return len(records)
