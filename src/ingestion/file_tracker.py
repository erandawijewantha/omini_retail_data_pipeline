from __future__ import annotations

import hashlib
from pathlib import Path

from sqlalchemy import text

from src.utils.db import get_engine

def calculate_file_checksum(file_path: Path) -> str:
    hasher = hashlib.sha256()
    
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
            
    return hasher.hexdigest()

def is_file_processed(file_name: str, file_checksum: str) -> bool:
    engine = get_engine()
    
    sql = text("""
            SELECT 1
            FROM control.processed_files
            WHERE file_name = :file_name
            AND file_checksum = :file_checksum
            LIMIT 1
        """)
    
    with engine.begin() as conn:
        result = conn.execute(
            sql,
            {
                "file_name": file_name,
                "file_checksum": file_checksum,
            },
        ).fetchone()
        
    return result is not None

def mark_file_processed(
    file_name: str,
    file_checksum: str,
    source_name: str,
    batch_id: int,
) -> None:
    engine = get_engine()
    
    sql = text("""
            INSERT INTO control.processed_files (
                file_name,
                file_checksum,
                source_name,
                batch_id
            )
            VALUES (
                :file_name,
                :file_checksum,
                :source_name,
                :batch_id
            )
            ON CONFLICT (file_name)
            DO UPDATE SET
                file_checksum = EXCLUDED.file_checksum,
                source_name = EXCLUDED.source_name,
                batch_id = EXCLUDED.batch_id,
                processed_at = CURRENT_TIMESTAMP
            """)
    
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "file_name": file_name,
                "file_checksum": file_checksum,
                "source_name": source_name,
                "batch_id": batch_id,
            },
        )