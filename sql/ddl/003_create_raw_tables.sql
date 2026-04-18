CREATE TABLE IF NOT EXISTS raw.stores (
    store_id VARCHAR(50),
    store_name VARCHAR(150),
    store_type VARCHAR(50),
    city VARCHAR(100),
    region VARCHAR(100),
    opened_date DATE,
    manager_employee_id VARCHAR(50),
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id BIGINT,
    source_file_name VARCHAR(255),
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id VARCHAR(50),
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
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id VARCHAR(50),
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
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS raw.employees (
    employee_id VARCHAR(50),
    store_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    role_name VARCHAR(100),
    hire_date DATE,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id BIGINT,
    source_file_name VARCHAR(255),
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS raw.promotions (
    promotion_id VARCHAR(50),
    promotion_name VARCHAR(150),
    discount_type VARCHAR(50),
    discount_value NUMERIC(12,2),
    start_date DATE,
    end_date DATE,
    channel VARCHAR(50),
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id BIGINT,
    source_file_name VARCHAR(255),
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id VARCHAR(50),
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
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id VARCHAR(50),
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
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id VARCHAR(50),
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
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS raw.refunds (
    refund_id VARCHAR(50),
    order_id VARCHAR(50),
    payment_id VARCHAR(50),
    refund_reason VARCHAR(255),
    refund_amount NUMERIC(14,2),
    refund_ts TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id BIGINT,
    source_file_name VARCHAR(255),
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS raw.inventory_movements (
    inventory_movement_id VARCHAR(50),
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
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64)
);