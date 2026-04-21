from __future__ import annotations

import hashlib
from datetime import datetime
import re

import pandas as pd 

def build_record_hash(row: pd.Seris) -> str:
    raw_string = "||".join("" if pd.isna(v) else str(v) for v in row.values)
    return hashlib.sha256(raw_string.encode("utf-8")).hexdigest()

def add_ingestion_metadata(
    df: pd.DataFrame,
    batch_id: int,
    source_file_name: str,
) -> pd.DataFrame:
    result = df.copy()
    
    result["batch_id"] = batch_id
    result["source_file_name"] = source_file_name
    result["ingested_at"] = datetime.utcnow()
    result["record_hash"] = result.apply(build_record_hash, axis=1)
    
    return result

