from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.utils.db import get_engine
from src.utils.logger import get_logger

logger = get_logger(__name__)

def load_csv(file_path: Path) -> pd.DataFrame:
    return pd.read_csv(file_path)

def write_to_raw_table(df: pd.DataFrame, table_name: str) -> int:
    engine = get_engine()
    
    df.to_sql(
        name=table_name,
        con=engine,
        schema="raw",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    
    logger.info("Loaded %s rows into raw.%s", len(df), table_name)
    return len(df)

