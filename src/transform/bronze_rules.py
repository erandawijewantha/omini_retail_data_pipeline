from __future__ import annotations

from __future__ import annotations

BUSINESS_KEYS = {
    "stores": ["store_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "employees": ["employee_id"],
    "promotions": ["promotion_id"],
    "orders": ["order_id"],
    "order_items": ["order_item_id"],
    "payments": ["payment_id"],
    "refunds": ["refund_id"],
    "inventory_movements": ["inventory_movement_id"],
}

LATEST_BY_UPDATED_AT = {
    "stores",
    "customers",
    "products",
    "employees",
    "promotions",
    "orders",
}

APPEND_DEDUP = {
    "order_items",
    "payments",
    "refunds",
    "inventory_movements",
}