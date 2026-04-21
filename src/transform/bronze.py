from __future__ import annotations
from unittest.mock import NonCallableMagicMock

import pandas as pd
from sqlalchemy import table, text

from src.transform.bronze_rules import APPEND_DEDUP, BUSINESS_KEYS, LATEST_BY_UPDATED_AT
from src.utils.db import get_engine
from src.utils.logger import get_logger

logger = get_logger(__name__)

TABLES = [
    "stores",
    "customers",
    "products",
    "employees",
    "promotions",
    "orders",
    "order_items",
    "payments",
    "refunds",
    "inventory_movements",
]

def read_raw_table(table_name: str) -> pd.DataFrame:
    engine = get_engine()
    query = f" SELECT * FROM raw.{table_name}"
    return pd.read_sql(query, engine)

def normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    object_cols = result.select_dtypes(include=["object"]).columns
    
    for col in object_cols:
        result[col] = result[col].apply(
            lambda x: x.strip().upper() if isinstance(x, str) else x
        )
    return result

def dedupe_latest(df: pd.DataFrame, key_cols:list[str]) -> pd.DataFrame:
    result = df.copy()
    
    for col in ["updated_at", "ingested_at"]:
        if col in result.columns:
            result[col] = pd.to_datetime(result[col], errors='coerce')
            
    sort_cols = [c for c in ["updated_at", "ingested_at"] if c in result.columns]
    result = result.sort_values(sort_cols)
    result = result.drop_duplicates(subset = key_cols, keep="last")
    
    return result

def dedupe_append(df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    result = df.copy()

    if "ingested_at" in result.columns:
        result["ingested_at"] = pd.to_datetime(result["ingested_at"], errors="coerce")
        result = result.sort_values(["ingested_at"])

    result = result.drop_duplicates(subset=key_cols, keep="last")
    return result

def truncate_bronze_table(table_name: str) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE bronze.{table_name}"))
        
def write_bronze_table(df: pd.DataFrame, table_name: str) -> None:
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema="bronze",
        if_exists ="append",
        index = False,
        method="multi",
        chunksize=1000,
    )
    
def process_table(table_name: str) -> None:
    logger.info("Processing bronze table: %s", table_name)
    
    df = read_raw_table(table_name)
    logger.info("Read %s raw rows from %s", len(df), table_name)
    
    df = normalize_strings(df)
    
    key_cols = BUSINESS_KEYS[table_name]
    
    if table_name in LATEST_BY_UPDATED_AT:
        df = dedupe_latest(df, key_cols)
    elif table_name in APPEND_DEDUP:
        df = dedupe_append(df, key_cols)
        
    truncate_bronze_table(table_name)
    write_bronze_table(df, table_name)
    
    logger.info("Wrote %s bronze rows to %s", len(df), table_name)
    
def main() -> None:
    for table_name in TABLES:
        process_table(table_name)
        
if __name__ == "__main__":
    main()
        
        
    
    