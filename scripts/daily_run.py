from src.db.engine import get_engine
from src.etl.spine import load_seasons
from src.etl.load_games_dimteams import load_game_structure
from src.etl.load_team_boxscores import load_teambox_scores
from src.etl.load_dimplayers_boxscores import load_dimplayer_boxscores


def main() -> None:
    # 1) update spine (new completed games)
    current_season = "2025-26"
    load_seasons(current_season)

    engine = get_engine()

    # 2) ensure fact_games + dim_teams exist for any new spine games
    load_game_structure(engine=engine, season= current_season, sleep_seconds=0.6, limit=None)

    # 3) fill team boxscores for games missing teambox rows
    load_teambox_scores(engine=engine, season= current_season, sleep_seconds=0.8, limit=None)

    # 4) fill player dimension + player boxscores for games missing playerbox rows
    load_dimplayer_boxscores(engine=engine, season= current_season, sleep_seconds=0.8, limit=None)


if __name__ == "__main__":
    main()
