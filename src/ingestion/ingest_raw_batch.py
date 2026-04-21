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
    
    if is_file_processed(file_name=file_name, file_checksum=file_checksum):
        logger.info("Skipping already processed file: %s", file_name)
        return
    
    batch_id = start_batch(
        pipeline_name = pipeline_name,
        source_name = source_name,
        batch_reference = file_name,
    )
    
    try:
        table_name = infer_table_name(file_path)
        
        if table_name not in VALID_TABLES:
            raise ValueError(f"Unsupported raw table name inferred from file: {table_name}")
        
        df = load_csv(file_path)
        rows_read = len(df)
        
        df = add_ingestion_metadata(
            df=df,
            batch_id=batch_id,
            source_file_name=file_name,
        )
        
        rows_loaded = write_to_raw_table(df=df, table_name=table_name)
        
        mark_file_processed(
            file_name=file_name,
            file_checksum=file_checksum,
            source_name=source_name,
            batch_id=batch_id,
        )
        
        complete_batch(
            batch_id = batch_id,
            rows_read = rows_read,
            rows_loaded=rows_loaded,
            rows_rejected=0,
        )
        
    except Exception as exc:
        fail_batch(batch_id=batch_id, error_message=str(exc))
        raise
    
def ingest_folder(folder_path: Path, pipeline_name: str, source_name: str) -> None:
    csv_files = sorted(folder_path.glob("*.csv"))
    
    if not csv_files:
        logger.warning("No CSV files found in %s", folder_path)
        return
    
    for file_path in csv_files:
        logger.info("Ingesting file: %s", file_path)
        ingest_file(file_path=file_path, pipeline_name=pipeline_name, source_name = source_name)
            

def main() -> None:
    master_folder = Path("data/source_batches/master")
    txn_batch_001 = Path("data/source_batches/transactions/batch_001")
    txn_batch_002 = Path("data/source_batches/transactions/batch_002")
    
    ingest_folder(
        folder_path=master_folder,
        pipeline_name = "omni_retail_raw_ingestion",
        source_name="master_files",
    )
    
    ingest_folder(
        folder_path=txn_batch_001,
        pipeline_name = "omni_retail_raw_ingestion",
        source_name="transactions_batch_001",
    )
    
    ingest_folder(
        folder_path=txn_batch_002,
        pipeline_name = "omni_retail_raw_ingestion",
        source_name="transactions_batch_002",
    )

if __name__ == "__main__":
    main()