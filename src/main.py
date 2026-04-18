from __future__ import annotations

from src.ingestion.batch_control import complete_batch, fail_batch, start_batch


def main() -> None:
    batch_id = start_batch(
        pipeline_name="omni_retail_pipeline",
        source_name="manual_test",
        batch_reference="test_run_001",
    )

    try:
        complete_batch(
            batch_id=batch_id,
            rows_read=100,
            rows_loaded=95,
            rows_rejected=5,
        )
    except Exception as exc:
        fail_batch(batch_id=batch_id, error_message=str(exc))
        raise


if __name__ == "__main__":
    main()