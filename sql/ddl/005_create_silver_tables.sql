CREATE TABLE IF NOT EXISTS silver.dim_customer_current(
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(20),
    date_of_birth DATE,
    email VARCHAR(255),
    phone VARCHAR(50),
    city VARCHAR(100),
    region VARCHAR(100),
    loyalty_tier VARCHAR(50),
    customer_segment VARCHAR(50),
    registered_at TIMESTAMP,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id BIGINT,
    source_file_name VARCHAR(255),
    ingested_at TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS silver.dim_product_current (
    product_id VARCHAR(50) PRIMARY KEY,
    sku VARCHAR(100),
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price NUMERIC(12,2),
    cost_price NUMERIC(12,2),
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id BIGINT,
    source_file_name VARCHAR(255),
    ingested_at TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS silver.orders_clean (
    order_id VARCHAR(50) PRIMARY KEY,
    order_channel VARCHAR(50),
    store_id VARCHAR(50),
    customer_id VARCHAR(50),
    employee_id VARCHAR(50),
    promotion_id VARCHAR(50),
    order_status VARCHAR(50),
    order_ts TIMESTAMP,
    gross_amount NUMERIC(14,2),
    discount_amount NUMERIC(14,2),
    net_amount NUMERIC(14,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id BIGINT,
    source_file_name VARCHAR(255),
    ingested_at TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS silver.payments_clean (
    payment_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    payment_amount NUMERIC(14,2),
    payment_ts TIMESTAMP,
    reference_number VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id BIGINT,
    source_file_name VARCHAR(255),
    ingested_at TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS silver.order_items_clean (
    order_item_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INT,
    unit_price NUMERIC(12,2),
    discount_amount NUMERIC(12,2),
    line_amount NUMERIC(14,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id BIGINT,
    source_file_name VARCHAR(255),
    ingested_at TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS silver.refunds_clean (
    refund_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    payment_id VARCHAR(50),
    refund_reason VARCHAR(255),
    refund_amount NUMERIC(14,2),
    refund_ts TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id BIGINT,
    source_file_name VARCHAR(255),
    ingested_at TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS silver.inventory_movements_clean (
    inventory_movement_id VARCHAR(50) PRIMARY KEY,
    store_id VARCHAR(50),
    product_id VARCHAR(50),
    movement_type VARCHAR(50),
    quantity_change INT,
    movement_ts TIMESTAMP,
    reference_id VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id BIGINT,
    source_file_name VARCHAR(255),
    ingested_at TIMESTAMP,
    record_hash VARCHAR(64)
);