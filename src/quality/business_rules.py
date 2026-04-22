from __future__ import annotations

import pandas as pd

from src.quality.cleaners import (
    clean_code,
    clean_datetime,
    clean_numeric,
    normalize_dataframe_strings,
)

VALID_ORDER_STATUS = {
    "CREATED",
    "PAID",
    "COMPLETED",
    "CANCELLED",
    "REFUNDED",
    "PARTIALLY_REFUNDED",
    "PAYMENT_FAILED",
}

VALID_PAYMENT_STATUS = {
    "SUCCESS",
    "FAILED",
    "PENDING",
    "REVERSED",
}

VALID_MOVEMENT_TYPES = {
    "SALE",
    "REFUND",
    "RESTOCK",
    "ADJUSTMENT",
    "TRANSFER_IN",
    "TRANSFER_OUT",
}


def prepare_orders(orders: pd.DataFrame) -> pd.DataFrame:
    result = normalize_dataframe_strings(orders)

    if "order_status" in result.columns:
        result["order_status"] = clean_code(result["order_status"])

    for col in ["order_ts", "created_at", "updated_at", "ingested_at"]:
        if col in result.columns:
            result[col] = clean_datetime(result[col])

    for col in ["gross_amount", "discount_amount", "net_amount"]:
        if col in result.columns:
            result[col] = clean_numeric(result[col])

    return result


def prepare_payments(payments: pd.DataFrame) -> pd.DataFrame:
    result = normalize_dataframe_strings(payments)

    if "payment_status" in result.columns:
        result["payment_status"] = clean_code(result["payment_status"])

    if "payment_method" in result.columns:
        result["payment_method"] = clean_code(result["payment_method"])

    for col in ["payment_ts", "created_at", "updated_at", "ingested_at"]:
        if col in result.columns:
            result[col] = clean_datetime(result[col])

    if "payment_amount" in result.columns:
        result["payment_amount"] = clean_numeric(result["payment_amount"])

    return result


def prepare_order_items(order_items: pd.DataFrame) -> pd.DataFrame:
    result = normalize_dataframe_strings(order_items)

    for col in ["created_at", "updated_at", "ingested_at"]:
        if col in result.columns:
            result[col] = clean_datetime(result[col])

    for col in ["quantity"]:
        if col in result.columns:
            result[col] = clean_numeric(result[col])

    for col in ["unit_price", "discount_amount", "line_amount"]:
        if col in result.columns:
            result[col] = clean_numeric(result[col])

    return result


def prepare_refunds(refunds: pd.DataFrame) -> pd.DataFrame:
    result = normalize_dataframe_strings(refunds)

    for col in ["refund_ts", "created_at", "updated_at", "ingested_at"]:
        if col in result.columns:
            result[col] = clean_datetime(result[col])

    if "refund_amount" in result.columns:
        result["refund_amount"] = clean_numeric(result["refund_amount"])

    return result


def prepare_inventory_movements(inventory_movements: pd.DataFrame) -> pd.DataFrame:
    result = normalize_dataframe_strings(inventory_movements)

    if "movement_type" in result.columns:
        result["movement_type"] = clean_code(result["movement_type"])

    for col in ["movement_ts", "created_at", "updated_at", "ingested_at"]:
        if col in result.columns:
            result[col] = clean_datetime(result[col])

    if "quantity_change" in result.columns:
        result["quantity_change"] = clean_numeric(result["quantity_change"])

    return result


def filter_valid_orders(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    stores: pd.DataFrame,
) -> pd.DataFrame:
    result = prepare_orders(orders)

    valid_customer_ids = set(customers["customer_id"].dropna())
    valid_store_ids = set(stores["store_id"].dropna())

    result = result[result["order_id"].notna()]
    result = result[result["customer_id"].isin(valid_customer_ids)]
    result = result[result["store_id"].isin(valid_store_ids)]
    result = result[result["order_status"].isin(VALID_ORDER_STATUS)]
    result = result[result["net_amount"].fillna(0) >= 0]

    return result


def filter_valid_payments(
    payments: pd.DataFrame,
    orders: pd.DataFrame,
) -> pd.DataFrame:
    result = prepare_payments(payments)

    valid_order_ids = set(orders["order_id"].dropna())

    result = result[result["payment_id"].notna()]
    result = result[result["order_id"].isin(valid_order_ids)]
    result = result[result["payment_status"].isin(VALID_PAYMENT_STATUS)]
    result = result[result["payment_amount"].fillna(0) >= 0]

    return result


def filter_valid_order_items(
    order_items: pd.DataFrame,
    orders: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    result = prepare_order_items(order_items)

    valid_order_ids = set(orders["order_id"].dropna())
    valid_product_ids = set(products["product_id"].dropna())

    result = result[result["order_item_id"].notna()]
    result = result[result["order_id"].isin(valid_order_ids)]
    result = result[result["product_id"].isin(valid_product_ids)]
    result = result[result["quantity"].fillna(0) > 0]
    result = result[result["unit_price"].fillna(0) >= 0]
    result = result[result["line_amount"].fillna(0) >= 0]

    return result


def filter_valid_refunds(
    refunds: pd.DataFrame,
    payments: pd.DataFrame,
    orders: pd.DataFrame,
) -> pd.DataFrame:
    result = prepare_refunds(refunds)

    valid_payment_ids = set(payments["payment_id"].dropna())
    valid_order_ids = set(orders["order_id"].dropna())

    result = result[result["refund_id"].notna()]
    result = result[result["payment_id"].isin(valid_payment_ids)]
    result = result[result["order_id"].isin(valid_order_ids)]
    result = result[result["refund_amount"].fillna(0) >= 0]

    return result


def filter_valid_inventory_movements(
    inventory_movements: pd.DataFrame,
    stores: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    result = prepare_inventory_movements(inventory_movements)

    valid_store_ids = set(stores["store_id"].dropna())
    valid_product_ids = set(products["product_id"].dropna())

    result = result[result["inventory_movement_id"].notna()]
    result = result[result["store_id"].isin(valid_store_ids)]
    result = result[result["product_id"].isin(valid_product_ids)]
    result = result[result["movement_type"].isin(VALID_MOVEMENT_TYPES)]
    result = result[result["quantity_change"].notna()]

    return result