"""
Error tracking and handling for ETL pipeline.

Provides structured error logging to database for monitoring,
debugging, and retry capabilities.
"""
import traceback
from typing import Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def log_etl_error(
    engine: Engine,
    process_name: str,
    error: Exception,
    game_id: Optional[str] = None,
    retry_count: int = 0,
) -> None:
    """
    Log ETL error to database for tracking and retry.
    
    Args:
        engine: SQLAlchemy engine
        process_name: Name of the process that failed (e.g., 'load_teambox')
        error: Exception that occurred
        game_id: Game ID if error is specific to a game
        retry_count: Number of retry attempts made
    
    Example:
        >>> try:
        ...     process_game(game_id)
        ... except Exception as e:
        ...     log_etl_error(engine, "load_teambox", e, game_id="0022400123")
    """
    error_sql = text("""
        INSERT INTO nba.etl_errors (
            process_name, game_id, error_type, 
            error_message, stack_trace, retry_count
        )
        VALUES (
            :process_name, :game_id, :error_type,
            :error_message, :stack_trace, :retry_count
        )
    """)
    
    try:
        with engine.begin() as conn:
            conn.execute(error_sql, {
                "process_name": process_name,
                "game_id": game_id,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "stack_trace": traceback.format_exc(),
                "retry_count": retry_count,
            })
        logger.debug(f"Logged error to database: {process_name} - {game_id}")
    except Exception as db_error:
        # If we can't log to DB, at least log to console
        logger.error(f"Failed to log error to database: {db_error}")
        logger.error(f"Original error: {error}")


def mark_error_resolved(
    engine: Engine,
    process_name: str,
    game_id: str,
) -> int:
    """
    Mark errors as resolved for a specific game/process.
    
    Call this after successfully processing a previously failed item.
    
    Args:
        engine: SQLAlchemy engine
        process_name: Name of the process
        game_id: Game ID that was successfully processed
    
    Returns:
        Number of errors marked as resolved
    
    Example:
        >>> # After successfully retrying a failed game
        >>> mark_error_resolved(engine, "load_teambox", "0022400123")
        2  # Marked 2 previous errors as resolved
    """
    resolve_sql = text("""
        UPDATE nba.etl_errors
        SET is_resolved = TRUE,
            resolved_at = NOW()
        WHERE process_name = :process_name
          AND game_id = :game_id
          AND is_resolved = FALSE
    """)
    
    with engine.begin() as conn:
        result = conn.execute(resolve_sql, {
            "process_name": process_name,
            "game_id": game_id,
        })
        count = result.rowcount
    
    if count > 0:
        logger.info(f"Marked {count} error(s) as resolved: {process_name} - {game_id}")
    
    return count


def get_failed_game_ids(
    engine: Engine,
    process_name: str,
    limit: Optional[int] = None,
) -> list[str]:
    """
    Get list of game IDs that have unresolved errors for a process.
    
    Use this to retry previously failed items.
    
    Args:
        engine: SQLAlchemy engine
        process_name: Name of the process to get failures for
        limit: Maximum number of game IDs to return
    
    Returns:
        List of game IDs with unresolved errors
    
    Example:
        >>> failed_games = get_failed_game_ids(engine, "load_teambox", limit=10)
        >>> for game_id in failed_games:
        ...     retry_game(game_id)
    """
    query = text("""
        SELECT DISTINCT game_id
        FROM nba.etl_errors
        WHERE process_name = :process_name
          AND is_resolved = FALSE
          AND game_id IS NOT NULL
        ORDER BY created_at DESC
        LIMIT :limit
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {
            "process_name": process_name,
            "limit": limit or 1000,
        })
        game_ids = [row[0] for row in result]
    
    return game_ids


def get_error_summary(engine: Engine, days: int = 7) -> dict:
    """
    Get summary of recent errors grouped by process and error type.
    
    Args:
        engine: SQLAlchemy engine
        days: Number of days to look back
    
    Returns:
        Dictionary with error summary statistics
    
    Example:
        >>> summary = get_error_summary(engine, days=1)
        >>> print(summary)
        {
            'total_errors': 15,
            'unresolved': 8,
            'by_process': {'load_teambox': 10, 'load_playerbox': 5},
            'by_type': {'JSONDecodeError': 12, 'Timeout': 3}
        }
    """
    summary_sql = text("""
        SELECT 
            COUNT(*) as total_errors,
            COUNT(*) FILTER (WHERE is_resolved = FALSE) as unresolved,
            json_object_agg(
                DISTINCT process_name, 
                COUNT(*) FILTER (WHERE process_name = nba.etl_errors.process_name)
            ) as by_process,
            json_object_agg(
                DISTINCT error_type,
                COUNT(*) FILTER (WHERE error_type = nba.etl_errors.error_type)
            ) as by_type
        FROM nba.etl_errors
        WHERE created_at >= NOW() - INTERVAL ':days days'
    """)
    
    with engine.connect() as conn:
        result = conn.execute(summary_sql, {"days": days}).fetchone()
    
    return {
        "total_errors": result[0] or 0,
        "unresolved": result[1] or 0,
        "by_process": result[2] or {},
        "by_type": result[3] or {},
    }