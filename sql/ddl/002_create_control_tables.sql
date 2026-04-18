CREATE TABLE IF NOT EXISTS control.etl_batch (
    batch_id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    source_name VARCHAR(100) NOT NULL,
    batch_reference VARCHAR(200),
    status VARCHAR(30) NOT NULL,
    rows_read INT DEFAULT 0,
    rows_loaded INT DEFAULT 0,
    rows_rejected INT DEFAULT 0,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS control.etl_watermark (
    table_name VARCHAR(150) PRIMARY KEY,
    watermark_column VARCHAR(100) NOT NULL,
    last_watermark_value TIMESTAMP,
    last_batch_id BIGINT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS control.processed_files (
    file_name VARCHAR(255) PRIMARY KEY,
    file_checksum VARCHAR(128) NOT NULL,
    source_name VARCHAR(100) NOT NULL,
    batch_id BIGINT,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS control.reject_log (
    reject_id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT,
    table_name VARCHAR(150) NOT NULL,
    reject_reason TEXT NOT NULL,
    raw_payload JSONB,
    logged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
