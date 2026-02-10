"""
Unit tests for data validation.

Tests validation logic for game, team, and player data.
"""
import pytest
from src.etl.validators import DataValidator, _is_valid_season_format


class TestValidateGameInsert:
    """Test game data validation."""
    
    def test_valid_game(self):
        """Should pass validation for valid game data."""
        game = {
            "game_id": "0022400123",
            "season": "2024-25",
            "game_date": "2024-11-15",
            "home_team_id": 1610612750,
            "away_team_id": 1610612751,
        }
        errors = DataValidator.validate_game_insert(game)
        assert len(errors) == 0
    
    def test_missing_game_id(self):
        """Should fail when game_id is missing."""
        game = {
            "season": "2024-25",
            "game_date": "2024-11-15",
            "home_team_id": 1610612750,
            "away_team_id": 1610612751,
        }
        errors = DataValidator.validate_game_insert(game)
        assert len(errors) > 0
        assert any("game_id" in e.field for e in errors)
    
    def test_invalid_game_id_length(self):
        """Should fail when game_id is wrong length."""
        game = {
            "game_id": "123",  # Too short
            "season": "2024-25",
            "game_date": "2024-11-15",
            "home_team_id": 1610612750,
            "away_team_id": 1610612751,
        }
        errors = DataValidator.validate_game_insert(game)
        assert len(errors) > 0
        assert any("game_id" in e.field for e in errors)
    
    def test_same_home_away_team(self):
        """Should fail when home and away teams are the same."""
        game = {
            "game_id": "0022400123",
            "season": "2024-25",
            "game_date": "2024-11-15",
            "home_team_id": 1610612750,
            "away_team_id": 1610612750,  # Same as home!
        }
        errors = DataValidator.validate_game_insert(game)
        assert len(errors) > 0
        assert any("team_ids" in e.field for e in errors)
    
    def test_missing_game_date(self):
        """Should fail when game_date is missing."""
        game = {
            "game_id": "0022400123",
            "season": "2024-25",
            "home_team_id": 1610612750,
            "away_team_id": 1610612751,
        }
        errors = DataValidator.validate_game_insert(game)
        assert len(errors) > 0
        assert any("game_date" in e.field for e in errors)
    
    def test_invalid_season_format(self):
        """Should fail for invalid season format."""
        game = {
            "game_id": "0022400123",
            "season": "2024-2025",  # Wrong format
            "game_date": "2024-11-15",
            "home_team_id": 1610612750,
            "away_team_id": 1610612751,
        }
        errors = DataValidator.validate_game_insert(game)
        assert len(errors) > 0
        assert any("season" in e.field for e in errors)


class TestValidateTeamBoxscore:
    """Test team boxscore validation."""
    
    def test_valid_teambox(self):
        """Should pass validation for valid team boxscore."""
        box = {
            "game_id": "0022400123",
            "team_id": 1610612750,
            "fgm": 40,
            "fga": 85,
            "fg3m": 12,
            "fg3a": 30,
            "ftm": 20,
            "fta": 25,
            "pts": 112,
            "oreb": 10,
            "dreb": 32,
            "reb": 42,
        }
        errors = DataValidator.validate_team_boxscore(box)
        assert len(errors) == 0
    
    def test_fgm_exceeds_fga(self):
        """Should fail when field goals made exceeds attempted."""
        box = {
            "game_id": "0022400123",
            "team_id": 1610612750,
            "fgm": 50,  # More than attempts
            "fga": 40,
        }
        errors = DataValidator.validate_team_boxscore(box)
        assert len(errors) > 0
        assert any("field_goals" in e.field for e in errors)
    
    def test_fg3m_exceeds_fg3a(self):
        """Should fail when 3PM exceeds 3PA."""
        box = {
            "game_id": "0022400123",
            "team_id": 1610612750,
            "fg3m": 20,
            "fg3a": 15,
        }
        errors = DataValidator.validate_team_boxscore(box)
        assert len(errors) > 0
        assert any("three_pointers" in e.field for e in errors)
    
    def test_negative_points(self):
        """Should fail when points are negative."""
        box = {
            "game_id": "0022400123",
            "team_id": 1610612750,
            "pts": -10,
        }
        errors = DataValidator.validate_team_boxscore(box)
        assert len(errors) > 0
        assert any("pts" in e.field for e in errors)
    
    def test_rebound_consistency(self):
        """Should warn when total rebounds differs significantly from oreb + dreb."""
        box = {
            "game_id": "0022400123",
            "team_id": 1610612750,
            "oreb": 10,
            "dreb": 30,
            "reb": 50,  # Significantly more than 40
        }
        errors = DataValidator.validate_team_boxscore(box)
        assert len(errors) > 0
        assert any("rebounds" in e.field for e in errors)


class TestValidatePlayerBoxscore:
    """Test player boxscore validation."""
    
    def test_valid_playerbox(self):
        """Should pass validation for valid player boxscore."""
        box = {
            "game_id": "0022400123",
            "player_id": 1630162,
            "team_id": 1610612750,
            "fgm": 10,
            "fga": 18,
            "fg3m": 3,
            "fg3a": 8,
            "ftm": 5,
            "fta": 6,
            "pts": 28,
            "reb": 6,
            "ast": 4,
        }
        errors = DataValidator.validate_player_boxscore(box)
        assert len(errors) == 0
    
    def test_fg3m_exceeds_fgm(self):
        """Should fail when 3PM exceeds total FGM."""
        box = {
            "game_id": "0022400123",
            "player_id": 1630162,
            "team_id": 1610612750,
            "fgm": 5,
            "fg3m": 8,  # More than total FGM!
        }
        errors = DataValidator.validate_player_boxscore(box)
        assert len(errors) > 0
        assert any("shooting" in e.field for e in errors)
    
    def test_negative_stats(self):
        """Should fail when stats are negative."""
        box = {
            "game_id": "0022400123",
            "player_id": 1630162,
            "team_id": 1610612750,
            "pts": -5,
            "reb": -2,
        }
        errors = DataValidator.validate_player_boxscore(box)
        assert len(errors) >= 2  # At least 2 errors (pts and reb)
    
    def test_missing_required_fields(self):
        """Should fail when required fields are missing."""
        box = {
            "pts": 10,
            # Missing game_id, player_id, team_id
        }
        errors = DataValidator.validate_player_boxscore(box)
        assert len(errors) >= 3


class TestSeasonFormatValidation:
    """Test season format validation helper."""
    
    @pytest.mark.parametrize("season,expected", [
        ("2024-25", True),
        ("2023-24", True),
        ("1999-00", True),
        ("2024-2025", False),  # Wrong format
        ("24-25", False),  # Missing century
        ("2024-26", False),  # Year2 doesn't follow year1
        ("2024-24", False),  # Same year
        ("", False),
        (None, False),
    ])
    def test_season_format(self, season, expected):
        """Should validate season format correctly."""
        assert _is_valid_season_format(season) == expected