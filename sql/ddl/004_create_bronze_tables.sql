CREATE TABLE IF NOT EXISTS bronze.stores AS
SELECT * FROM raw.stores WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS bronze.customers AS
SELECT * FROM raw.customers WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS bronze.products AS
SELECT * FROM raw.products WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS bronze.employees AS
SELECT * FROM raw.employees WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS bronze.promotions AS
SELECT * FROM raw.promotions WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS bronze.orders AS
SELECT * FROM raw.orders WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS bronze.order_items AS
SELECT * FROM raw.order_items WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS bronze.payments AS
SELECT * FROM raw.payments WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS bronze.refunds AS
SELECT * FROM raw.refunds WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS bronze.inventory_movements AS
SELECT * FROM raw.inventory_movements WHERE 1 = 0;