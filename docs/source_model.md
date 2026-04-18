# Source Model - OmniRetail Data Platform

## 1. Purpose

This document defines the **source-system design** for the OmniRetail Data Platform project.

The goal is to simulate a realistic multi-source retail environment with enough complexity to cover key data engineering patterns such as:

- full and incremental loading
- idempotent processing
- late-arriving data
- duplicates
- upserts
- Slowly Changing Dimensions (SCD)
- data quality checks
- raw to bronze to silver to gold modeling

This document focuses on the **source layer only**.

---

## 2. Source System Overview

The project simulates a retail company operating through:

- physical stores
- online/e-commerce channels
- promotions
- customer loyalty behavior
- inventory movements
- payment and refund events

The initial version includes **10 source tables**.

### Master / Reference Tables
- stores
- customers
- products
- employees
- promotions

### Transaction / Event Tables
- orders
- order_items
- payments
- refunds
- inventory_movements

---

## 3. Source Modeling Principles

The source model is designed using the following rules:

1. Every table must have a clear **business key**
2. Every mutable table must include:
   - `created_at`
   - `updated_at`
3. Every event table should include an event timestamp
4. Some source tables are append-only, while others are mutable
5. Some source tables are candidates for SCD Type 2 in downstream layers
6. The generator should intentionally produce realistic bad data and operational edge cases

---

## 4. Table Summary

| Table Name | Category | Business Key | Load Pattern | Mutable | SCD Candidate |
|------------|----------|--------------|-------------|---------|---------------|
| stores | master | store_id | full initial, then upsert | yes | low |
| customers | master | customer_id | upsert incremental | yes | yes |
| products | master | product_id | upsert incremental | yes | yes |
| employees | master | employee_id | upsert incremental | yes | maybe |
| promotions | reference | promotion_id | upsert incremental | yes | low |
| orders | transaction header | order_id | incremental upsert | yes | no |
| order_items | transaction line | order_item_id | incremental append/upsert | low | no |
| payments | event | payment_id | incremental append | low | no |
| refunds | event | refund_id | incremental append | low | no |
| inventory_movements | event | inventory_movement_id | incremental append | low | no |

---

## 5. Detailed Source Table Definitions

---

### 5.1 stores

**Purpose**  
Represents retail branches or physical locations.

**Business Key**  
`store_id`

**Grain**  
One record per store

**Columns**
- `store_id`
- `store_name`
- `store_type`
- `city`
- `region`
- `opened_date`
- `manager_employee_id`
- `is_active`
- `created_at`
- `updated_at`

**Notes**
- `manager_employee_id` may refer to an employee assigned as store manager
- stores may become inactive later
- store details may change over time, but historical tracking is not critical in phase 1

**Load Behavior**
- full load initially
- upsert on reruns and future batches
- incremental based on `updated_at`

---

### 5.2 customers

**Purpose**  
Represents retail customers across all channels.

**Business Key**  
`customer_id`

**Grain**  
One record per customer

**Columns**
- `customer_id`
- `first_name`
- `last_name`
- `gender`
- `date_of_birth`
- `email`
- `phone`
- `city`
- `region`
- `loyalty_tier`
- `customer_segment`
- `registered_at`
- `is_active`
- `created_at`
- `updated_at`

**Notes**
- customer details can change over time
- segment, city, and loyalty tier are strong SCD candidates
- some customers may become inactive

**Load Behavior**
- incremental upsert
- watermark column: `updated_at`

**SCD Potential**
- yes, in silver/gold
- expected SCD Type 2 candidate fields:
  - city
  - region
  - loyalty_tier
  - customer_segment
  - is_active

---

### 5.3 products

**Purpose**  
Represents the retail product catalog.

**Business Key**  
`product_id`

**Grain**  
One record per product

**Columns**
- `product_id`
- `sku`
- `product_name`
- `category`
- `subcategory`
- `brand`
- `unit_price`
- `cost_price`
- `is_active`
- `created_at`
- `updated_at`

**Notes**
- product price may change
- product category may be corrected
- products may become inactive/discontinued

**Load Behavior**
- incremental upsert
- watermark column: `updated_at`

**SCD Potential**
- yes, in silver/gold
- candidate fields:
  - unit_price
  - cost_price
  - category
  - subcategory
  - brand
  - is_active

---

### 5.4 employees

**Purpose**  
Represents store or channel employees involved in transactions.

**Business Key**  
`employee_id`

**Grain**  
One record per employee

**Columns**
- `employee_id`
- `store_id`
- `first_name`
- `last_name`
- `role_name`
- `hire_date`
- `is_active`
- `created_at`
- `updated_at`

**Notes**
- employees are mainly used to support POS/store operations
- store assignments may change later
- optional future SCD candidate

**Load Behavior**
- incremental upsert
- watermark column: `updated_at`

**SCD Potential**
- optional in future phases
- candidate fields:
  - store_id
  - role_name
  - is_active

---

### 5.5 promotions

**Purpose**  
Represents campaigns or discounts applied to transactions.

**Business Key**  
`promotion_id`

**Grain**  
One record per promotion

**Columns**
- `promotion_id`
- `promotion_name`
- `discount_type`
- `discount_value`
- `start_date`
- `end_date`
- `channel`
- `is_active`
- `created_at`
- `updated_at`

**Notes**
- promotions may be active for online only, store only, or both
- some orders may not have promotions

**Load Behavior**
- incremental upsert
- watermark column: `updated_at`

---

### 5.6 orders

**Purpose**  
Represents order headers from store or e-commerce channels.

**Business Key**  
`order_id`

**Grain**  
One record per order

**Columns**
- `order_id`
- `order_channel`
- `store_id`
- `customer_id`
- `employee_id`
- `promotion_id`
- `order_status`
- `order_ts`
- `gross_amount`
- `discount_amount`
- `net_amount`
- `created_at`
- `updated_at`

**Notes**
- this is a mutable transactional table
- order status may change after initial creation
- online orders may not have `employee_id`
- some store orders may have null `promotion_id`

**Typical Status Values**
- CREATED
- PAID
- CANCELLED
- COMPLETED
- REFUNDED
- PARTIALLY_REFUNDED

**Load Behavior**
- incremental upsert
- watermark column: `updated_at`

---

### 5.7 order_items

**Purpose**  
Represents product-level line items within each order.

**Business Key**  
`order_item_id`

**Grain**  
One record per order line item

**Columns**
- `order_item_id`
- `order_id`
- `product_id`
- `quantity`
- `unit_price`
- `discount_amount`
- `line_amount`
- `created_at`
- `updated_at`

**Notes**
- line items typically do not change often
- if they do change, use upsert logic by `order_item_id`
- `line_amount` should usually equal:
  - (`quantity` × `unit_price`) - `discount_amount`

**Load Behavior**
- append with dedupe or upsert
- recommended watermark column: `updated_at`

---

### 5.8 payments

**Purpose**  
Represents payment events against orders.

**Business Key**  
`payment_id`

**Grain**  
One record per payment event

**Columns**
- `payment_id`
- `order_id`
- `payment_method`
- `payment_status`
- `payment_amount`
- `payment_ts`
- `reference_number`
- `created_at`
- `updated_at`

**Notes**
- append-only behavior is preferred
- one order may have multiple payment attempts
- failed payments can exist
- successful payments may occur after order creation

**Typical Payment Methods**
- CASH
- CARD
- WALLET
- BANK_TRANSFER

**Typical Payment Status**
- SUCCESS
- FAILED
- PENDING
- REVERSED

**Load Behavior**
- incremental append
- watermark column: `payment_ts`

---

### 5.9 refunds

**Purpose**  
Represents refund events linked to payments/orders.

**Business Key**  
`refund_id`

**Grain**  
One record per refund event

**Columns**
- `refund_id`
- `order_id`
- `payment_id`
- `refund_reason`
- `refund_amount`
- `refund_ts`
- `created_at`
- `updated_at`

**Notes**
- refunds may happen later than payment/order date
- not all orders have refunds
- partial refunds are allowed

**Load Behavior**
- incremental append
- watermark column: `refund_ts`

---

### 5.10 inventory_movements

**Purpose**  
Represents stock inflow/outflow movements by store and product.

**Business Key**  
`inventory_movement_id`

**Grain**  
One record per stock movement event

**Columns**
- `inventory_movement_id`
- `store_id`
- `product_id`
- `movement_type`
- `quantity_change`
- `movement_ts`
- `reference_id`
- `created_at`
- `updated_at`

**Notes**
- inventory movement can be caused by:
  - sale
  - return
  - restock
  - adjustment
  - transfer
- negative quantity usually means stock outflow
- positive quantity usually means stock inflow

**Typical Movement Types**
- SALE
- REFUND
- RESTOCK
- ADJUSTMENT
- TRANSFER_IN
- TRANSFER_OUT

**Load Behavior**
- incremental append
- watermark column: `movement_ts`

---

## 6. Source Relationships

The main source relationships are:

- `employees.store_id` → `stores.store_id`
- `stores.manager_employee_id` → `employees.employee_id` (logical relationship, optional enforcement later)
- `orders.store_id` → `stores.store_id`
- `orders.customer_id` → `customers.customer_id`
- `orders.employee_id` → `employees.employee_id`
- `orders.promotion_id` → `promotions.promotion_id`
- `order_items.order_id` → `orders.order_id`
- `order_items.product_id` → `products.product_id`
- `payments.order_id` → `orders.order_id`
- `refunds.order_id` → `orders.order_id`
- `refunds.payment_id` → `payments.payment_id`
- `inventory_movements.store_id` → `stores.store_id`
- `inventory_movements.product_id` → `products.product_id`

---

## 7. Incremental Loading Strategy

### 7.1 Append-Only Tables
These tables behave like event streams and should generally be loaded using append logic with deduplication safeguards.

- payments
- refunds
- inventory_movements

**Watermark columns**
- `payment_ts`
- `refund_ts`
- `movement_ts`

**Handling rules**
- append new rows
- prevent duplicate ingestion using business key and/or record hash
- allow late-arriving rows if timestamp is older than current load but record key is new

---

### 7.2 Incremental Upsert Tables
These tables can receive new rows and updates to existing rows.

- stores
- customers
- products
- employees
- promotions
- orders

**Watermark column**
- `updated_at`

**Handling rules**
- insert if business key does not exist
- update if business key exists and source `updated_at` is newer
- preserve current-state record in silver
- for selected dimensions, later store historical versions using SCD Type 2

---

### 7.3 Hybrid Table
`order_items`

**Handling rules**
- append if new
- deduplicate by `order_item_id`
- optionally upsert if an updated version arrives

**Watermark column**
- `updated_at`

---

## 8. SCD Candidates

The following source tables are expected to become Slowly Changing Dimensions in downstream layers:

### customers
Likely SCD Type 2 attributes:
- city
- region
- loyalty_tier
- customer_segment
- is_active

### products
Likely SCD Type 2 attributes:
- unit_price
- cost_price
- category
- subcategory
- brand
- is_active

### employees (optional)
Potential SCD Type 2 attributes:
- store_id
- role_name
- is_active

---

## 9. Data Quality and Bad Data Scenarios

The synthetic generator must intentionally create realistic source issues.

### Required scenarios

#### Duplicates
- duplicate orders
- duplicate payments
- duplicate inventory movement records

#### Mutable updates
- order status changes from CREATED to PAID or CANCELLED
- customer segment updates
- product price changes
- store activation/inactivation

#### Missing or null values
- null promotion_id on many orders
- missing email or phone for some customers
- missing employee_id for e-commerce orders

#### Late arriving data
- payment arrives in a later batch for an older order
- refund arrives 1–3 days later
- inventory adjustment arrives late

#### Business inconsistencies
- payment amount differs from order net amount
- refund amount greater than payment amount
- negative quantity where not expected
- invalid order status
- invalid movement type

#### Reject testing
- malformed records
- null business key
- invalid timestamps
- wrong numeric types
- orphan child records (e.g. order_item for missing order)

These cases should support:
- reject logging
- quarantine logic
- data quality validation
- idempotency testing

---

## 10. Expected Raw Layer Approach

The raw layer should preserve source data with minimal modification.

Recommended additional metadata fields in raw tables:
- `batch_id`
- `source_file_name`
- `ingested_at`
- `record_hash`

Purpose:
- preserve source traceability
- support rerun safety
- support duplicate detection
- support file-level auditability

---

## 11. Modeling Boundaries

### In Scope for Phase 1
- source modeling
- synthetic batch generation
- raw / bronze / silver / gold
- incremental loads
- idempotent reruns
- PostgreSQL
- Power BI

### Out of Scope for Phase 1
- Airflow
- Docker
- Kafka
- cloud storage
- streaming architecture
- external APIs

---

## 12. Next Steps

After this source model is approved, the implementation order will be:

1. create raw-layer DDL
2. create synthetic data generator
3. generate source batches
4. build raw ingestion logic
5. build bronze transformations
6. build silver transformations
7. build gold marts
8. connect Power BI to gold layer