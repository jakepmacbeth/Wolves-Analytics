"""
Pytest configuration and shared fixtures.

Provides reusable test fixtures for database connections,
sample data, and common test utilities.
"""
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os


@pytest.fixture(scope="session")
def test_db_engine() -> Engine:
    """
    Create a test database engine.
    
    Uses environment variables or defaults for test database.
    The test database should be separate from production.
    """
    db_url = os.getenv(
        "TEST_DATABASE_URL",
        "postgresql+psycopg2://test_user:test_password@localhost:5432/nba_test"
    )
    
    engine = create_engine(db_url, pool_pre_ping=True)
    
    # Verify connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    
    yield engine
    
    # Cleanup
    engine.dispose()


@pytest.fixture
def clean_tables(test_db_engine: Engine):
    """
    Clean all tables before each test.
    
    Ensures tests start with a clean slate.
    """
    with test_db_engine.begin() as conn:
        # Disable foreign key checks temporarily
        conn.execute(text("SET CONSTRAINTS ALL DEFERRED"))
        
        # Truncate tables in reverse dependency order
        tables = [
            "nba.playerbox_pergame",
            "nba.teambox_pergame",
            "nba.dim_players",
            "nba.fact_games",
            "nba.dim_teams",
            "nba.spine",
            "nba.etl_errors",
        ]
        
        for table in tables:
            conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
    
    yield
    
    # Cleanup after test
    with test_db_engine.begin() as conn:
        for table in tables:
            conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))


@pytest.fixture
def sample_game_data() -> dict:
    """Sample game data for testing."""
    return {
        "game_id": "0022400123",
        "season": "2024-25",
        "game_date": "2024-11-15",
        "game_datetime_utc": "2024-11-15T19:00:00Z",
        "home_team_id": 1610612750,  # Timberwolves
        "away_team_id": 1610612751,  # Lakers
        "game_status": "Final",
        "arena_name": "Target Center",
        "arena_city": "Minneapolis",
        "arena_state": "MN",
    }


@pytest.fixture
def sample_team_data() -> dict:
    """Sample team data for testing."""
    return {
        "team_id": 1610612750,
        "abbreviation": "MIN",
        "team_name": "Timberwolves",
        "city": "Minnesota",
        "full_name": "Minnesota Timberwolves",
    }


@pytest.fixture
def sample_player_data() -> dict:
    """Sample player data for testing."""
    return {
        "player_id": 1630162,
        "full_name": "Anthony Edwards",
        "first_name": "Anthony",
        "last_name": "Edwards",
        "position": "SG",
    }


@pytest.fixture
def sample_teambox_data() -> dict:
    """Sample team boxscore data for testing."""
    return {
        "game_id": "0022400123",
        "team_id": 1610612750,
        "season": "2024-25",
        "is_home": True,
        "opponent_team_id": 1610612751,
        "minutes": "240",
        "pts": 115,
        "fgm": 42,
        "fga": 88,
        "fg3m": 12,
        "fg3a": 35,
        "ftm": 19,
        "fta": 22,
        "oreb": 10,
        "dreb": 32,
        "reb": 42,
        "ast": 25,
        "stl": 8,
        "blk": 5,
        "tov": 12,
        "pf": 18,
        "off_rating": 115.5,
        "def_rating": 108.2,
        "net_rating": 7.3,
        "pace": 98.5,
        "ts_pct": 0.58,
    }


@pytest.fixture
def sample_playerbox_data() -> dict:
    """Sample player boxscore data for testing."""
    return {
        "game_id": "0022400123",
        "player_id": 1630162,
        "team_id": 1610612750,
        "season": "2024-25",
        "is_home": True,
        "opponent_team_id": 1610612751,
        "starter_flag": True,
        "minutes": "35:24",
        "pts": 28,
        "reb": 6,
        "ast": 5,
        "stl": 2,
        "blk": 1,
        "tov": 3,
        "pf": 2,
        "fgm": 10,
        "fga": 18,
        "fg3m": 4,
        "fg3a": 9,
        "ftm": 4,
        "fta": 5,
        "plus_minus": 12,
    }