CREATE TABLE IF NOT EXISTS silver.dim_customer_history (
    customer_sk BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(20),
    city VARCHAR(100),
    region VARCHAR(100),
    loyalty_tier VARCHAR(50),
    customer_segment VARCHAR(50),
    is_active BOOLEAN,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    is_current BOOLEAN,
    record_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS silver.dim_product_history (
    product_sk BIGSERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    sku VARCHAR(100),
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price NUMERIC(12,2),
    cost_price NUMERIC(12,2),
    is_active BOOLEAN,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    is_current BOOLEAN,
    record_hash VARCHAR(64)
);