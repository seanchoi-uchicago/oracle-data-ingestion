from pathlib import Path
import duckdb


def init_db(db_path: str = None, schema_path: str = None) -> Path:
    base = Path(__file__).parent
    db_file = Path(db_path) if db_path else base / "data.duckdb"
    schema_file = Path(schema_path) if schema_path else base / "schema.sql"
    conn = duckdb.connect(str(db_file))
    with open(schema_file, "r", encoding="utf-8") as f:
        sql = f.read()
    conn.execute(sql)
    conn.close()
    return db_file


if __name__ == "__main__":
    db = init_db()
    print(f"Initialized DuckDB at {db}")
