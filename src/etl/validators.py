"""
Data validation utilities for NBA Analytics.

Validates data before database insertion to catch:
- Missing required fields
- Out-of-range values
- Logical inconsistencies
- Data type mismatches
"""
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class ValidationError:
    """Represents a single validation failure."""
    
    field: str
    value: Any
    rule: str
    message: str
    
    def __str__(self) -> str:
        return f"{self.field}: {self.message} (value={self.value}, rule={self.rule})"


class DataValidator:
    """Validate NBA data before database insertion."""
    
    @staticmethod
    def validate_game_insert(game_data: Dict[str, Any]) -> List[ValidationError]:
        """
        Validate game data meets business rules.
        
        Args:
            game_data: Dictionary with game fields
        
        Returns:
            List of validation errors (empty if valid)
        
        Example:
            >>> game = {"game_id": "0022400123", "home_team_id": 1, "away_team_id": 1}
            >>> errors = DataValidator.validate_game_insert(game)
            >>> if errors:
            ...     for error in errors:
            ...         print(error)
        """
        errors = []
        
        # Game ID validation
        game_id = game_data.get("game_id")
        if not game_id:
            errors.append(ValidationError(
                field="game_id",
                value=game_id,
                rule="required",
                message="Game ID is required"
            ))
        elif len(str(game_id)) != 10:
            errors.append(ValidationError(
                field="game_id",
                value=game_id,
                rule="length_check",
                message="Game ID must be exactly 10 characters"
            ))
        
        # Team ID validation
        home_id = game_data.get("home_team_id")
        away_id = game_data.get("away_team_id")
        
        if not home_id:
            errors.append(ValidationError(
                field="home_team_id",
                value=home_id,
                rule="required",
                message="Home team ID is required"
            ))
        
        if not away_id:
            errors.append(ValidationError(
                field="away_team_id",
                value=away_id,
                rule="required",
                message="Away team ID is required"
            ))
        
        if home_id and away_id and home_id == away_id:
            errors.append(ValidationError(
                field="team_ids",
                value=f"home={home_id}, away={away_id}",
                rule="uniqueness",
                message="Home and away team IDs must be different"
            ))
        
        # Date validation
        game_date = game_data.get("game_date")
        if not game_date:
            errors.append(ValidationError(
                field="game_date",
                value=game_date,
                rule="required",
                message="Game date is required"
            ))
        
        # Season format validation (YYYY-YY)
        season = game_data.get("season")
        if season and not _is_valid_season_format(season):
            errors.append(ValidationError(
                field="season",
                value=season,
                rule="format_check",
                message="Season must be in format YYYY-YY (e.g., 2024-25)"
            ))
        
        return errors
    
    @staticmethod
    def validate_team_boxscore(box_data: Dict[str, Any]) -> List[ValidationError]:
        """
        Validate team boxscore data.
        
        Args:
            box_data: Dictionary with team stats
        
        Returns:
            List of validation errors
        """
        errors = []
        
        # Required fields
        game_id = box_data.get("game_id")
        team_id = box_data.get("team_id")
        
        if not game_id:
            errors.append(ValidationError(
                field="game_id",
                value=game_id,
                rule="required",
                message="Game ID is required"
            ))
        
        if not team_id:
            errors.append(ValidationError(
                field="team_id",
                value=team_id,
                rule="required",
                message="Team ID is required"
            ))
        
        # Shot validation: made should not exceed attempted
        fgm = box_data.get("fgm", 0) or 0
        fga = box_data.get("fga", 0) or 0
        if fgm > fga:
            errors.append(ValidationError(
                field="field_goals",
                value=f"made={fgm}, attempted={fga}",
                rule="logic_check",
                message="Field goals made cannot exceed attempts"
            ))
        
        fg3m = box_data.get("fg3m", 0) or 0
        fg3a = box_data.get("fg3a", 0) or 0
        if fg3m > fg3a:
            errors.append(ValidationError(
                field="three_pointers",
                value=f"made={fg3m}, attempted={fg3a}",
                rule="logic_check",
                message="Three pointers made cannot exceed attempts"
            ))
        
        ftm = box_data.get("ftm", 0) or 0
        fta = box_data.get("fta", 0) or 0
        if ftm > fta:
            errors.append(ValidationError(
                field="free_throws",
                value=f"made={ftm}, attempted={fta}",
                rule="logic_check",
                message="Free throws made cannot exceed attempts"
            ))
        
        # Points should be non-negative
        pts = box_data.get("pts")
        if pts is not None and pts < 0:
            errors.append(ValidationError(
                field="pts",
                value=pts,
                rule="range_check",
                message="Points cannot be negative"
            ))
        
        # Rebound consistency
        oreb = box_data.get("oreb", 0) or 0
        dreb = box_data.get("dreb", 0) or 0
        reb = box_data.get("reb", 0) or 0
        
        # Total rebounds should approximately equal offensive + defensive
        # Allow small discrepancies due to team rebounds
        if reb > 0 and abs(reb - (oreb + dreb)) > 5:
            errors.append(ValidationError(
                field="rebounds",
                value=f"total={reb}, oreb={oreb}, dreb={dreb}",
                rule="consistency_check",
                message="Total rebounds significantly differs from oreb + dreb"
            ))
        
        return errors
    
    @staticmethod
    def validate_player_boxscore(box_data: Dict[str, Any]) -> List[ValidationError]:
        """
        Validate player boxscore data.
        
        Args:
            box_data: Dictionary with player stats
        
        Returns:
            List of validation errors
        """
        errors = []
        
        # Required fields
        game_id = box_data.get("game_id")
        player_id = box_data.get("player_id")
        team_id = box_data.get("team_id")
        
        if not game_id:
            errors.append(ValidationError(
                field="game_id",
                value=game_id,
                rule="required",
                message="Game ID is required"
            ))
        
        if not player_id:
            errors.append(ValidationError(
                field="player_id",
                value=player_id,
                rule="required",
                message="Player ID is required"
            ))
        
        if not team_id:
            errors.append(ValidationError(
                field="team_id",
                value=team_id,
                rule="required",
                message="Team ID is required"
            ))
        
        # Shot validation
        fgm = box_data.get("fgm", 0) or 0
        fga = box_data.get("fga", 0) or 0
        if fgm > fga:
            errors.append(ValidationError(
                field="field_goals",
                value=f"made={fgm}, attempted={fga}",
                rule="logic_check",
                message="Field goals made cannot exceed attempts"
            ))
        
        fg3m = box_data.get("fg3m", 0) or 0
        fg3a = box_data.get("fg3a", 0) or 0
        if fg3m > fg3a:
            errors.append(ValidationError(
                field="three_pointers",
                value=f"made={fg3m}, attempted={fg3a}",
                rule="logic_check",
                message="Three pointers made cannot exceed attempts"
            ))
        
        ftm = box_data.get("ftm", 0) or 0
        fta = box_data.get("fta", 0) or 0
        if ftm > fta:
            errors.append(ValidationError(
                field="free_throws",
                value=f"made={ftm}, attempted={fta}",
                rule="logic_check",
                message="Free throws made cannot exceed attempts"
            ))
        
        # 3-pointers cannot exceed total field goals
        if fg3m > fgm:
            errors.append(ValidationError(
                field="shooting",
                value=f"fg3m={fg3m}, fgm={fgm}",
                rule="logic_check",
                message="Three pointers made cannot exceed total field goals made"
            ))
        
        if fg3a > fga:
            errors.append(ValidationError(
                field="shooting",
                value=f"fg3a={fg3a}, fga={fga}",
                rule="logic_check",
                message="Three pointers attempted cannot exceed total field goals attempted"
            ))
        
        # Negative stats validation
        for stat in ["pts", "reb", "ast", "stl", "blk", "pf"]:
            value = box_data.get(stat)
            if value is not None and value < 0:
                errors.append(ValidationError(
                    field=stat,
                    value=value,
                    rule="range_check",
                    message=f"{stat.upper()} cannot be negative"
                ))
        
        return errors


def _is_valid_season_format(season: str) -> bool:
    """
    Check if season string matches YYYY-YY format.
    
    Args:
        season: Season string to validate
    
    Returns:
        True if valid format
    
    Examples:
        >>> _is_valid_season_format("2024-25")
        True
        >>> _is_valid_season_format("2024-2025")
        False
    """
    if not season or len(season) != 7:
        return False
    
    parts = season.split("-")
    if len(parts) != 2:
        return False
    
    try:
        year1 = int(parts[0])
        year2 = int(parts[1])
        
        # Check year1 is 4 digits and year2 is 2 digits
        if year1 < 1000 or year1 > 9999:
            return False
        if year2 < 0 or year2 > 99:
            return False
        
        # Check year2 is year1 + 1 (modulo 100)
        expected_year2 = (year1 + 1) % 100
        if year2 != expected_year2:
            return False
        
        return True
    except ValueError:
        return False