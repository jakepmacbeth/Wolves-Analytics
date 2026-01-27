import pandas as pd
from nba_api.stats.endpoints import scoreboardv2

def fetch_scoreboard_gameheader(game_date: str) -> pd.DataFrame:
    """
    game_date must be 'MM/DD/YYYY'
    Returns the GameHeader dataframe from ScoreboardV2.
    """
    sb = scoreboardv2.ScoreboardV2(game_date=game_date, timeout=60)
    df = sb.game_header.get_data_frame()
    df.columns = [c.lower() for c in df.columns]
    return df
