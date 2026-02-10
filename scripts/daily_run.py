"""
Daily ETL pipeline runner for Wolves Analytics.

Orchestrates the full ETL pipeline:
1. Load spine (game IDs for season)
2. Load game structure and team dimensions
3. Load team boxscores
4. Load player dimensions and boxscores

Features:
- Automatic season detection
- Comprehensive logging
- Error tracking
- Performance metrics
"""
import sys
from datetime import datetime
from pathlib import Path

from config import get_config
from src.db.engine import get_engine
from src.etl.spine import load_seasons
from src.etl.load_games_dimteams import load_game_structure
from src.etl.load_team_boxscores import load_teambox_scores
from src.etl.load_dimplayers_boxscores import load_dimplayer_boxscores
from src.utils.logger import setup_logger, get_default_log_file

# Set up logging
log_file = get_default_log_file("daily_run")
logger = setup_logger(__name__, log_file=log_file)


def get_current_season() -> str:
    """
    Determine current NBA season based on date.
    
    NBA season runs from October to June.
    If before October, we're in the previous season.
    
    Can be overridden via NBA_SEASON environment variable.
    
    Returns:
        Season string in YYYY-YY format (e.g., "2024-25")
    
    Examples:
        Date: March 2025 → Season: "2024-25"
        Date: September 2025 → Season: "2024-25"  
        Date: October 2025 → Season: "2025-26"
    """
    config = get_config()
    
    # Check for environment variable override
    if config.current_season:
        logger.info(f"Using season from environment: {config.current_season}")
        return config.current_season
    
    # Auto-detect based on date
    now = datetime.now()
    year = now.year
    month = now.month
    
    # If before October, we're still in the previous season
    if month < 10:
        season = f"{year-1}-{str(year)[-2:]}"
    else:
        season = f"{year}-{str(year+1)[-2:]}"
    
    logger.info(f"Auto-detected current season: {season}")
    return season


def main() -> int:
    """
    Run the daily ETL pipeline.
    
    Returns:
        Exit code: 0 for success, 1 for failure
    """
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info("Starting Wolves Analytics Daily ETL Pipeline")
    logger.info("=" * 80)
    
    try:
        # Get configuration
        config = get_config()
        current_season = get_current_season()
        
        logger.info(f"Season: {current_season}")
        logger.info(f"Sleep between API calls: {config.etl.default_sleep_seconds}s")
        logger.info(f"Max retries: {config.etl.max_retries}")
        
        # Get database engine
        engine = get_engine()
        logger.info("Database engine initialized")
        
        # Step 1: Update spine with new completed games
        logger.info("")
        logger.info("-" * 80)
        logger.info("STEP 1: Loading game spine")
        logger.info("-" * 80)
        load_seasons(current_season)
        
        # Step 2: Ensure fact_games + dim_teams exist for any new spine games
        logger.info("")
        logger.info("-" * 80)
        logger.info("STEP 2: Loading game structure and team dimensions")
        logger.info("-" * 80)
        load_game_structure(
            engine=engine,
            season=current_season,
            sleep_seconds=config.etl.default_sleep_seconds,
            limit=None
        )
        
        # Step 3: Fill team boxscores for games missing teambox rows
        logger.info("")
        logger.info("-" * 80)
        logger.info("STEP 3: Loading team boxscores")
        logger.info("-" * 80)
        load_teambox_scores(
            engine=engine,
            season=current_season,
            sleep_seconds=config.etl.default_sleep_seconds,
            limit=None
        )
        
        # Step 4: Fill player dimension + player boxscores for games missing playerbox rows
        logger.info("")
        logger.info("-" * 80)
        logger.info("STEP 4: Loading player dimensions and boxscores")
        logger.info("-" * 80)
        load_dimplayer_boxscores(
            engine=engine,
            season=current_season,
            sleep_seconds=config.etl.default_sleep_seconds,
            limit=None
        )
        
        # Success summary
        duration = (datetime.now() - start_time).total_seconds()
        logger.info("")
        logger.info("=" * 80)
        logger.info("Daily ETL Pipeline Completed Successfully")
        logger.info(f"Total duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        logger.info("=" * 80)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        return 1
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error("")
        logger.error("=" * 80)
        logger.error("Daily ETL Pipeline FAILED")
        logger.error(f"Duration before failure: {duration:.2f} seconds")
        logger.error(f"Error: {e}")
        logger.error("=" * 80)
        logger.exception("Full traceback:")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)