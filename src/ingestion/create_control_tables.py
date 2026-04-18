from __future__ import annotations

from pathlib import Path

from src.utils.db import execute_sql
from src.utils.logger import get_logger

logger = get_logger(__name__)

def read_sql_file(file_path: Path) -> str:
    return file_path.read_text(encoding="utf-8")

def create_control_tables() -> None:
    sql_file = Path("sql/ddl/002_create_control_tables.sql")
    
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    
    sql = read_sql_file(sql_file)
    execute_sql(sql)
    logger.info("Control tables created successfully.")
    
if __name__ == "__main__":
    create_control_tables()