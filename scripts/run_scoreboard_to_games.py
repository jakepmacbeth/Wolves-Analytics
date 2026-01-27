from src.etl.pull_scoreboard import fetch_scoreboard_gameheader
from src.etl.load_games import upsert_games

if __name__ == "__main__":
    # NBA API expects MM/DD/YYYY
    api_game_date = "01/10/2026"

    # Store date as ISO in DB
    db_game_date = "2026-01-10"

    # Adjust season labels as you prefer; keep consistent
    season = "2025-26"
    season_type = "Regular Season"

    gameheader = fetch_scoreboard_gameheader(api_game_date)
    n = upsert_games(gameheader, game_date=db_game_date, season=season, season_type=season_type)

    print(f"Upserted {n} games into nba.games for {db_game_date}")
