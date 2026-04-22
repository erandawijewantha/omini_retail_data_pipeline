from __future__ import annotations

import pandas as pd

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
    "SUCESS",
    "FAILED",
    "PENDING",
    "REVERESED",
}

def filter_valid_orders(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    stores: pd.DataFrame,
) -> pd.DataFrame:
    result = orders.copy()
    
    result = result[result["order_id"].notna()]
    result = result[result["customer_id"].isin(customers["customer_id"])]
    result = result[result["store_id"].isin(stores["store_id"])]
    result = result[result["order_status"].isin(VALID_ORDER_STATUS)]
    
    if "net_amount" in result.columns:
        result = result[result["net_amount"].fillna(0) >= 0]
        
    return result

def filter_valid_payments(
    payments: pd.DataFrame,
    orders: pd.DataFrame,
) -> pd.DataFrame:
    result = payments.copy()
    
    result = result[result["payment_id"].notna()]
    result = result[result["order_id"].isin(orders["order_id"])]
    result = result[result["payment_status"].isin(VALID_PAYMENT_STATUS)]
    
    if "payment_amount" in result.columns:
        result = result[result["payment_amount"].fillna(0) >= 0]
        
    return result

