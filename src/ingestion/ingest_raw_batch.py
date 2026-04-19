from __future__ import annotations

from pathlib import Path

from src.ingestion.batch_control import complete_batch, fail_batch, start_batch
from src.ingestion.file_tracker import (
    calculate_file_checksum,
    is_file_processed,
    mark_file_processed,
)

from src.ingestion.metadata import add_ingestion_metadata
from src.ingestion.raw_loader import load_csv, write_to_raw_table
from src.utils.logger import get_logger

logger = get_logger(__name__)

VALID_TABLES = {
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
}

def infer_table_name(file_path: Path) -> str:
    return file_path.stem.lower()

def ingest_file(file_path: Path, pipeline_name: str, source_name: str) -> None:
    file_checksum = calculate_file_checksum(file_path)
    file_name = file_path.name
    
    if is_file_processed()