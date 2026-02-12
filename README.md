# Wolves Analytics

**NBA analytics ETL pipeline for Minnesota Timberwolves data analysis, statistical modeling, and machine learning.**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-14+-blue.svg)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Overview

Wolves Analytics is an end-to-end ETL (Extract, Transform, Load) pipeline that:
- Extracts NBA game data from the official NBA Stats API
- Transforms it into a clean, dimensional data model
- Loads it into PostgreSQL for analytics and reporting
- Supports Power BI dashboards for performance tracking
- Enables statistical testing and machine learning workflows

### Key Features

 **Automated Daily Updates** - Incremental loading of new games  
 **Idempotent Operations** - Safe to re-run without duplicates  
 **Robust Error Handling** - Automatic retries with exponential backoff  
 **Data Quality Validation** - Catches bad data before insertion  
 **Comprehensive Logging** - Structured logs for debugging and monitoring  
 **Full Test Coverage** - Unit and integration tests with pytest  


---

##  Architecture

```
┌─────────────────┐
│    nba_api      │  Official NBA Stats API
└────────┬────────┘
         │
         ▼
┌──────────────────────────────────────┐
│  ETL Pipeline (Python + SQLAlchemy)  │
│  ┌───────────────────────────────┐   │
│  │ 1. Spine Loader               │   │  Extract game IDs
│  ├───────────────────────────────┤   │
│  │ 2. Game Structure Loader      │   │  Load games + teams
│  ├───────────────────────────────┤   │
│  │ 3. Team Boxscore Loader       │   │  Load team stats
│  ├───────────────────────────────┤   │
│  │ 4. Player Boxscore Loader     │   │  Load player stats
│  └───────────────────────────────┘   │
└──────────────┬───────────────────────┘
               │
               ▼
        ┌──────────────┐
        │  PostgreSQL  │  Data Warehouse
        │  (Star Schema)│
        └──────┬───────┘
               │
               ├─────────────────
               ▼                 ▼            
        ┌──────────┐      ┌──────────┐  
        │ Power BI │      │ Jupyter  │   
        │Dashboard │      │Notebooks │    
        └──────────┘      └──────────┘    
```

---

## Data Model

### Star Schema Design

**Fact Tables:**
- `fact_games` - Game-level information (date, teams, score, arena)
- `teambox_pergame` - Team statistics per game
- `playerbox_pergame` - Player statistics per game

**Dimension Tables:**
- `dim_teams` - Team master data (name, city, abbreviation)
- `dim_players` - Player master data (name, position, physical attributes)
- `spine` - Master game registry (all game IDs by season)

**Operational Tables:**
- `etl_errors` - Error tracking for failed operations

### Entity Relationship

```
                    ┌─────────────┐
                    │    spine    │
                    │  (game_id)  │
                    └──────┬──────┘
                           │
                           ▼
                    ┌──────────────┐
          ┌─────────│  fact_games  │─────────┐
          │         │   (game_id)  │         │
          │         └───────┬──────┘         │
          │                 │                │
          ▼                 ▼                ▼
   ┌──────────┐      ┌────────────┐   ┌─────────────┐
   │dim_teams │◄─────│teambox_    │   │playerbox_   │
   │(team_id) │      │pergame     │   │pergame      │
   └──────────┘      └────────────┘   └──────┬──────┘
                                              │
                                              ▼
                                       ┌────────────┐
                                       │dim_players │
                                       │(player_id) │
                                       └────────────┘
```

---

### Prerequisites

- **Python 3.11+**
- **PostgreSQL 14+**
- **Power BI Desktop** (optional, for dashboards)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/Wolves-Analytics.git
   cd Wolves-Analytics
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv venv
   
   # On Windows:
   venv\Scripts\activate
   
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up database**
   
   Create a PostgreSQL database:
   ```sql
   CREATE DATABASE nba_analytics;
   ```

5. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

6. **Initialize database schema**
   ```bash
   python scripts/create_core_tables.py
   ```

---

### Daily Pipeline

Run the daily ETL to load new games:

```bash
python scripts/daily_run.py
```

This automatically:
1. Detects the current NBA season
2. Loads new game IDs
3. Extracts game data from NBA API
4. Validates and loads to database
5. Logs all operations

### Backfill Historical Seasons

Load multiple past seasons:

```bash
# Single season
python scripts/backfill_seasons.py 2024-25

# Multiple seasons
python scripts/backfill_seasons.py 2022-23 2023-24 2024-25

# With custom API throttling
python scripts/backfill_seasons.py 2023-24 --sleep 1.0

# Test with limited games
python scripts/backfill_seasons.py 2024-25 --limit 10
```


---

## Testing

Run the test suite:

```bash
# All tests
pytest

# With coverage report
pytest --cov=src --cov-report=html

# Specific test file
pytest tests/test_etl/test_parsing_utils.py

# Verbose output
pytest -v
```

---

## Power BI Dashboard

The included `Wolves_Performance_Tracking.pbix` dashboard provides:

- **Team Performance** - Win/loss trends, offensive/defensive ratings
- **Player Analytics** - Per-game averages, shooting efficiency
- **Game Breakdowns** - Box scores, quarter-by-quarter analysis
- **Season Comparisons** - Year-over-year metrics

**To use:**
1. Open `reports/Wolves_Performance_Tracking.pbix` in Power BI Desktop
2. Update the database connection to your PostgreSQL instance
3. Refresh the data

---

## Configuration

All configuration is managed through environment variables in `.env`:

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nba_analytics
DB_USER=your_username
DB_PASSWORD=your_password

# Application
NBA_SEASON=2024-25       # Override auto-detection
LOG_LEVEL=INFO           # DEBUG, INFO, WARNING, ERROR

# ETL Settings
ETL_SLEEP_SECONDS=0.6    # API throttling
ETL_MAX_RETRIES=3        # Retry attempts
```

---

## Project Structure

```
Wolves-Analytics/
├── config.py                   # Configuration management
├── requirements.txt            # Python dependencies
├── .env.example               # Environment template
├── README.md
├── LICENSE
│
├── src/
│   ├── db/
│   │   ├── engine.py          # Database connection pooling
│   │   └── schema/            # SQL DDL scripts
│   │       ├── create_spine_table.sql
│   │       ├── games_boxscores_tables.sql
│   │       └── error_tracking.sql
│   │
│   ├── etl/
│   │   ├── spine.py           # Game ID extraction
│   │   ├── load_games_dimteams.py
│   │   ├── load_team_boxscores.py
│   │   ├── load_dimplayers_boxscores.py
│   │   ├── nba_utils.py       # API retry logic
│   │   ├── parsing_utils.py   # Safe type parsing
│   │   ├── validators.py      # Data validation
│   │   └── error_handler.py   # Error tracking
│   │
│   └── utils/
│       └── logger.py          # Logging configuration
│
├── scripts/
│   ├── daily_run.py           # Main ETL orchestration
│   ├── backfill_seasons.py    # Historical data loader
│   └── create_core_tables.py  # Schema initialization
│
├── tests/
│   ├── conftest.py            # Pytest fixtures
│   └── test_etl/
│       ├── test_parsing_utils.py
│       └── test_validators.py
│
├── logs/                      # Execution logs
├── reports/
│   └── Wolves_Performance_Tracking.pbix
│
└── sql/                       # Ad-hoc analysis queries
```

---

## Development

### Code Quality

```bash
# Format code
black src/ scripts/ tests/

# Lint
flake8 src/ scripts/ tests/ --max-line-length=100

# Type checking
mypy src/


### Error Tracking

All ETL errors are logged to:
1. **Console** - Immediate visibility
2. **Log files** - `logs/daily_run_YYYY-MM-DD.log`
3. **Database** - `nba.etl_errors` table

Query recent errors:
```sql
SELECT 
    process_name,
    game_id,
    error_type,
    error_message,
    created_at
FROM nba.etl_errors
WHERE is_resolved = FALSE
ORDER BY created_at DESC
LIMIT 10;
```

---


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

