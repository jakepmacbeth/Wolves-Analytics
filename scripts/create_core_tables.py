from pathlib import Path
from sqlalchemy import text
from src.db.engine import get_engine


def _read_sql(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Could not find schema file at: {path}")
    return path.read_text(encoding="utf-8")


def main() -> None:
    """
    Create core NBA schema tables.
    Safe to re-run: uses CREATE TABLE IF NOT EXISTS.
      1) create_spine_table.sql
      2) games_boxscores_tables.sql
    """
    engine = get_engine()

    spine_path = Path("src/db/schema/create_spine_table.sql")
    core_path = Path("src/db/schema/games_boxscores_tables.sql")
    lineups_path = Path("src/db/schema/lineups_tables.sql")

    spine_ddl = _read_sql(spine_path)
    core_ddl = _read_sql(core_path)
    lineups_ddl = _read_sql(lineups_path)

    # One transaction: either all DDL applies, or none.
    with engine.begin() as conn:
        conn.execute(text(spine_ddl)) 
        conn.execute(text(core_ddl))   
        conn.execute(text(lineups_ddl))
    print("tables created (spine, core and lineups)")


if __name__ == "__main__":
    main()
