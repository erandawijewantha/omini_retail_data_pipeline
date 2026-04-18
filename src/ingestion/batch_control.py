from __future__ import annotations

from sqlalchemy import text

from src.utils.db import get_engine
from src.utils.logger import get_logger

logger = get_logger(__name__)


def start_batch(
    pipeline_name: str,
    source_name: str,
    batch_reference: str | None = None,
) -> int:
    engine = get_engine()

    sql = text("""
        INSERT INTO control.etl_batch (
            pipeline_name,
            source_name,
            batch_reference,
            status
        )
        VALUES (
            :pipeline_name,
            :source_name,
            :batch_reference,
            'STARTED'
        )
        RETURNING batch_id
    """)

    with engine.begin() as conn:
        batch_id = conn.execute(
            sql,
            {
                "pipeline_name": pipeline_name,
                "source_name": source_name,
                "batch_reference": batch_reference,
            },
        ).scalar_one()

    logger.info("Started batch_id=%s", batch_id)
    return batch_id


def complete_batch(
    batch_id: int,
    rows_read: int = 0,
    rows_loaded: int = 0,
    rows_rejected: int = 0,
) -> None:
    engine = get_engine()

    sql = text("""
        UPDATE control.etl_batch
        SET
            status = 'SUCCESS',
            rows_read = :rows_read,
            rows_loaded = :rows_loaded,
            rows_rejected = :rows_rejected,
            ended_at = CURRENT_TIMESTAMP
        WHERE batch_id = :batch_id
    """)

    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "batch_id": batch_id,
                "rows_read": rows_read,
                "rows_loaded": rows_loaded,
                "rows_rejected": rows_rejected,
            },
        )

    logger.info("Completed batch_id=%s", batch_id)


def fail_batch(batch_id: int, error_message: str) -> None:
    engine = get_engine()

    sql = text("""
        UPDATE control.etl_batch
        SET
            status = 'FAILED',
            error_message = :error_message,
            ended_at = CURRENT_TIMESTAMP
        WHERE batch_id = :batch_id
    """)

    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "batch_id": batch_id,
                "error_message": error_message,
            },
        )

    logger.error("Failed batch_id=%s | %s", batch_id, error_message)