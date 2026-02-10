"""
Database engine management with connection pooling.

Provides singleton engine with proper configuration for production use:
- Connection pooling with pre-ping health checks
- Automatic connection recycling
- Configurable pool size and overflow
- Centralized connection management
"""
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import Pool
from typing import Optional

from config import get_config
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Global engine instance (singleton)
_engine: Optional[Engine] = None


def get_engine(force_reload: bool = False) -> Engine:
    """
    Get or create database engine singleton.
    
    The engine is configured with:
    - Connection pooling (5 connections + 10 overflow)
    - Pre-ping health checks
    - Automatic connection recycling (every hour)
    - UTC timezone for all connections
    
    Args:
        force_reload: Force recreation of engine (useful for testing)
    
    Returns:
        SQLAlchemy Engine instance
    
    Raises:
        ValueError: If required database environment variables are missing
    
    Example:
        >>> engine = get_engine()
        >>> with engine.connect() as conn:
        ...     result = conn.execute(text("SELECT 1"))
    """
    global _engine
    
    if _engine is not None and not force_reload:
        return _engine
    
    config = get_config()
    db_config = config.db
    
    logger.info(f"Creating database engine for {db_config.host}:{db_config.port}/{db_config.name}")
    
    _engine = create_engine(
        db_config.connection_url,
        
        # Connection pool settings
        pool_size=db_config.pool_size,
        max_overflow=db_config.max_overflow,
        pool_recycle=db_config.pool_recycle,
        pool_pre_ping=db_config.pool_pre_ping,
        
        # Connection arguments
        connect_args={
            "connect_timeout": 10,
            "options": "-c timezone=utc",
        },
        
        # Echo SQL for debugging (set via environment)
        echo=False,
        
        # Use QueuePool for better concurrency
        poolclass=None,  # Uses QueuePool by default
    )
    
    # Set up event listeners for logging
    _setup_engine_events(_engine)
    
    logger.info("Database engine created successfully")
    
    return _engine


def _setup_engine_events(engine: Engine) -> None:
    """
    Set up SQLAlchemy event listeners for monitoring.
    
    Args:
        engine: SQLAlchemy engine to attach listeners to
    """
    
    @event.listens_for(engine, "connect")
    def receive_connect(dbapi_conn, connection_record):
        """Log new database connections."""
        logger.debug("New database connection established")
    
    @event.listens_for(engine, "close")
    def receive_close(dbapi_conn, connection_record):
        """Log database connection closures."""
        logger.debug("Database connection closed")
    
    @event.listens_for(Pool, "checkout")
    def receive_checkout(dbapi_conn, connection_record, connection_proxy):
        """Log connection checkouts from pool."""
        logger.debug("Connection checked out from pool")
    
    @event.listens_for(Pool, "checkin")
    def receive_checkin(dbapi_conn, connection_record):
        """Log connection returns to pool."""
        logger.debug("Connection returned to pool")


def dispose_engine() -> None:
    """
    Dispose of the current engine and close all connections.
    
    Useful for testing or when reconfiguring the database.
    
    Example:
        >>> dispose_engine()
        >>> # Engine will be recreated on next get_engine() call
    """
    global _engine
    
    if _engine is not None:
        logger.info("Disposing database engine")
        _engine.dispose()
        _engine = None


def get_pool_status() -> dict:
    """
    Get current connection pool status.
    
    Returns:
        Dictionary with pool statistics
    
    Example:
        >>> status = get_pool_status()
        >>> print(f"Active connections: {status['checked_out']}")
        >>> print(f"Available connections: {status['checked_in']}")
    """
    engine = get_engine()
    pool = engine.pool
    
    return {
        "size": pool.size(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
        "checked_in": pool.size() - pool.checkedout(),
    }