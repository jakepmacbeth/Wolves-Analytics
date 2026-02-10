"""
Backfill utility for historical NBA seasons.

Allows loading multiple past seasons in bulk with proper error handling
and progress tracking.

Usage:
    python scripts/backfill_seasons.py 2022-23 2023-24 2024-25
    python scripts/backfill_seasons.py 2023-24 --sleep 1.0 --limit 50
"""
import argparse
import sys
from datetime import datetime
from typing import List

from config import get_config
from src.db.engine import get_engine
from src.etl.spine import load_seasons
from src.etl.load_games_dimteams import load_game_structure
from src.etl.load_team_boxscores import load_teambox_scores
from src.etl.load_dimplayers_boxscores import load_dimplayer_boxscores
from src.utils.logger import setup_logger, get_default_log_file

log_file = get_default_log_file("backfill")
logger = setup_logger(__name__, log_file=log_file)


def backfill_season(
    season: str,
    sleep_seconds: float = 0.6,
    limit: int = None
) -> bool:
    """
    Backfill a single NBA season.
    
    Runs the complete ETL pipeline for one season:
    1. Load spine (game IDs)
    2. Load game structure and teams
    3. Load team boxscores
    4. Load player boxscores
    
    Args:
        season: Season in YYYY-YY format (e.g., "2023-24")
        sleep_seconds: Sleep time between API calls
        limit: Maximum games to process (for testing)
    
    Returns:
        True if successful, False if failed
    """
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info(f"Starting backfill for season {season}")
    logger.info("=" * 80)
    
    try:
        # Step 1: Load spine
        logger.info(f"[1/4] Loading game spine for {season}")
        load_seasons(season)
        
        engine = get_engine()
        
        # Step 2: Load game structure
        logger.info(f"[2/4] Loading game structure for {season}")
        load_game_structure(
            engine=engine,
            season=season,
            sleep_seconds=sleep_seconds,
            limit=limit
        )
        
        # Step 3: Load team boxscores
        logger.info(f"[3/4] Loading team boxscores for {season}")
        load_teambox_scores(
            engine=engine,
            season=season,
            sleep_seconds=sleep_seconds,
            limit=limit
        )
        
        # Step 4: Load player boxscores
        logger.info(f"[4/4] Loading player boxscores for {season}")
        load_dimplayer_boxscores(
            engine=engine,
            season=season,
            sleep_seconds=sleep_seconds,
            limit=limit
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info("=" * 80)
        logger.info(f"Completed backfill for season {season}")
        logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error("=" * 80)
        logger.error(f"FAILED to backfill season {season}")
        logger.error(f"Duration before failure: {duration:.2f} seconds")
        logger.error(f"Error: {e}")
        logger.error("=" * 80)
        logger.exception("Full traceback:")
        return False


def validate_season_format(season: str) -> bool:
    """
    Validate season format.
    
    Args:
        season: Season string to validate
    
    Returns:
        True if valid
    """
    if len(season) != 7:
        return False
    
    parts = season.split("-")
    if len(parts) != 2:
        return False
    
    try:
        year1 = int(parts[0])
        year2 = int(parts[1])
        
        # Basic sanity checks
        if year1 < 1946 or year1 > 2100:  # NBA started in 1946
            return False
        if year2 < 0 or year2 > 99:
            return False
        
        # year2 should be year1 + 1 (last 2 digits)
        expected = (year1 + 1) % 100
        if year2 != expected:
            return False
        
        return True
    except ValueError:
        return False


def main() -> int:
    """
    Main backfill script.
    
    Returns:
        Exit code: 0 if all seasons succeeded, 1 if any failed
    """
    parser = argparse.ArgumentParser(
        description="Backfill historical NBA seasons",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill multiple seasons
  python scripts/backfill_seasons.py 2021-22 2022-23 2023-24
  
  # Backfill with custom sleep time
  python scripts/backfill_seasons.py 2023-24 --sleep 1.0
  
  # Test with limited games
  python scripts/backfill_seasons.py 2024-25 --limit 10
        """
    )
    
    parser.add_argument(
        "seasons",
        nargs="+",
        help="Seasons to backfill (e.g., 2022-23 2023-24)"
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=None,
        help="Sleep seconds between API calls (default: from config)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of games per season (for testing)"
    )
    
    args = parser.parse_args()
    
    # Validate season formats
    invalid_seasons = [s for s in args.seasons if not validate_season_format(s)]
    if invalid_seasons:
        logger.error(f"Invalid season format(s): {invalid_seasons}")
        logger.error("Seasons must be in format YYYY-YY (e.g., 2023-24)")
        return 1
    
    # Get sleep time from args or config
    config = get_config()
    sleep_seconds = args.sleep if args.sleep is not None else config.etl.default_sleep_seconds
    
    logger.info("=" * 80)
    logger.info("WOLVES ANALYTICS - SEASON BACKFILL")
    logger.info("=" * 80)
    logger.info(f"Seasons to backfill: {', '.join(args.seasons)}")
    logger.info(f"Sleep between API calls: {sleep_seconds}s")
    if args.limit:
        logger.info(f"Limit: {args.limit} games per season")
    logger.info("")
    
    # Track results
    results = {}
    
    # Process each season
    for i, season in enumerate(args.seasons, 1):
        logger.info("")
        logger.info(f"Processing season {i}/{len(args.seasons)}: {season}")
        logger.info("")
        
        success = backfill_season(
            season=season,
            sleep_seconds=sleep_seconds,
            limit=args.limit
        )
        
        results[season] = success
    
    # Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("BACKFILL SUMMARY")
    logger.info("=" * 80)
    
    successful = [s for s, success in results.items() if success]
    failed = [s for s, success in results.items() if not success]
    
    logger.info(f"Total seasons: {len(args.seasons)}")
    logger.info(f"Successful: {len(successful)}")
    logger.info(f"Failed: {len(failed)}")
    
    if successful:
        logger.info(f"✓ Success: {', '.join(successful)}")
    
    if failed:
        logger.error(f"✗ Failed: {', '.join(failed)}")
        return 1
    
    logger.info("=" * 80)
    logger.info("All seasons completed successfully!")
    logger.info("=" * 80)
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)