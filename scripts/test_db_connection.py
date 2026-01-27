from sqlalchemy import text
from src.db.engine import get_engine

if __name__ == "__main__":
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1;")).scalar()
    print("DB connection OK:", result)
