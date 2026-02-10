"""
Central configuration management for Wolves Analytics.

This module provides type-safe configuration loading from environment variables
with sensible defaults for all settings.
"""
from dataclasses import dataclass, field
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    
    host: str
    port: int
    name: str
    user: str
    password: str
    
    # Connection pool settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_recycle: int = 3600
    pool_pre_ping: bool = True
    
    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """
        Load database configuration from environment variables.
        
        Raises:
            ValueError: If required environment variables are missing.
        """
        name = os.getenv("DB_NAME", "")
        user = os.getenv("DB_USER", "")
        password = os.getenv("DB_PASSWORD", "")
        
        if not all([name, user, password]):
            raise ValueError(
                "Missing required database environment variables. "
                "Please set DB_NAME, DB_USER, and DB_PASSWORD in .env file. "
                "See .env.example for reference."
            )
        
        return cls(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            name=name,
            user=user,
            password=password,
        )
    
    @property
    def connection_url(self) -> str:
        """Generate SQLAlchemy connection URL."""
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


@dataclass
class ETLConfig:
    """ETL pipeline configuration."""
    
    default_sleep_seconds: float = 0.6
    max_retries: int = 3
    retry_backoff_seconds: list[int] = field(default_factory=lambda: [120, 200, 250])
    max_total_wait_seconds: int = 600
    batch_size: int = 100
    
    @classmethod
    def from_env(cls) -> "ETLConfig":
        """Load ETL configuration from environment variables."""
        return cls(
            default_sleep_seconds=float(os.getenv("ETL_SLEEP_SECONDS", "0.6")),
            max_retries=int(os.getenv("ETL_MAX_RETRIES", "3")),
            batch_size=int(os.getenv("ETL_BATCH_SIZE", "100")),
        )


@dataclass
class AppConfig:
    """Application-wide configuration."""
    
    db: DatabaseConfig
    etl: ETLConfig
    log_level: str = "INFO"
    current_season: Optional[str] = None
    
    @classmethod
    def load(cls) -> "AppConfig":
        """
        Load complete application configuration.
        
        Returns:
            AppConfig: Fully configured application settings.
        """
        return cls(
            db=DatabaseConfig.from_env(),
            etl=ETLConfig.from_env(),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            current_season=os.getenv("NBA_SEASON"),
        )


# Global config instance (lazy-loaded)
_config: Optional[AppConfig] = None


def get_config(reload: bool = False) -> AppConfig:
    """
    Get application configuration singleton.
    
    Args:
        reload: Force reload configuration from environment.
    
    Returns:
        AppConfig: Application configuration instance.
    """
    global _config
    
    if _config is None or reload:
        _config = AppConfig.load()
    
    return _config