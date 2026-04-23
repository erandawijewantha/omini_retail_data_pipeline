CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(20),
    city VARCHAR(100),
    region VARCHAR(100),
    loyalty_tier VARCHAR(50),
    customer_segment VARCHAR(50),
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_id VARCHAR(50) PRIMARY KEY,
    sku VARCHAR(100),
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price NUMERIC(12,2),
    cost_price NUMERIC(12,2),
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS gold.dim_store (
    store_id VARCHAR(50) PRIMARY KEY,
    store_name VARCHAR(150),
    store_type VARCHAR(50),
    city VARCHAR(100),
    region VARCHAR(100),
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS gold.fact_sales (
    order_id VARCHAR(50) PRIMARY KEY,
    order_date DATE,
    order_ts TIMESTAMP,
    order_channel VARCHAR(50),
    store_id VARCHAR(50),
    customer_id VARCHAR(50),
    promotion_id VARCHAR(50),
    order_status VARCHAR(50),
    gross_amount NUMERIC(14,2),
    discount_amount NUMERIC(14,2),
    net_amount NUMERIC(14,2),
    payment_count INT,
    total_paid_amount NUMERIC(14,2),
    item_count INT
);

CREATE TABLE IF NOT EXISTS gold.fact_refunds (
    refund_id VARCHAR(50) PRIMARY KEY,
    refund_date DATE,
    refund_ts TIMESTAMP,
    order_id VARCHAR(50),
    payment_id VARCHAR(50),
    customer_id VARCHAR(50),
    store_id VARCHAR(50),
    refund_reason VARCHAR(255),
    refund_amount NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS gold.mart_daily_sales (
    order_date DATE,
    store_id VARCHAR(50),
    order_channel VARCHAR(50),
    total_orders INT,
    total_sales_amount NUMERIC(14,2),
    total_discount_amount NUMERIC(14,2),
    total_paid_amount NUMERIC(14,2),
    total_items INT,
    PRIMARY KEY (order_date, store_id, order_channel)
);