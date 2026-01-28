from pathlib import Path
from sqlalchemy import text
from src.db.engine import get_engine

if __name__ == "__main__":
    sql_path = Path("src/db/schema/create_spine_table.sql")

    ddl = sql_path.read_text(encoding="utf-8")

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(ddl))

    print("verified")