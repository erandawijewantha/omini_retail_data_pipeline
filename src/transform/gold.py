from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from src.utils.db import get_engine
from src.utils.logger import get_logger

logger = get_logger(__name__)

def read_table(schema: str, table_name: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(f"SELECT * FROM {schema}.{table_name}", engine)

def truncate_table(table_name: str) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE gold.{table_name}"))
        
def write_table(df: pd.DataFrame, table_name: str) -> None:
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema="gold",
        if_exists="append",
        index = False,
        method ="multi",
        chunksize=1000,
    )
    
def load_dimensions() -> None:
    customers = read_table("silver", "dim_customer_current")
    products = read_table("silver", "dim_product_current")
    stores = read_table("bronze", "stores")
    
    dim_customer = customers[
        [
            "customer_id",
            "first_name",
            "last_name",
            "gender",
            "city",
            "region",
            "loyalty_tier",
            "customer_segment",
            "is_active",
        ]
    ].copy()
    
    dim_product = products[
        [
            "product_id",
            "sku",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "unit_price",
            "cost_price",
            "is_active",
        ]
    ].copy()
    
    dim_store = stores[
        [
            "store_id",
            "store_name",
            "store_type",
            "city",
            "region",
            "is_active",
        ]
    ].drop_duplicates(subset=["store_id"]).copy()
    
    truncate_table("dim_customer")
    write_table(dim_customer, "dim_customer")

    truncate_table("dim_product")
    write_table(dim_product, "dim_product")

    truncate_table("dim_store")
    write_table(dim_store, "dim_store")

    logger.info("Loaded gold dimensions.")
    
def build_fact_sales() -> pd.DataFrame:
    orders = read_table("silver", "orders_clean")
    payments = read_table("silver", "payments_clean")
    order_items = read_table("silver", "order_items_clean")

    orders["order_ts"] = pd.to_datetime(orders["order_ts"], errors="coerce")
    orders["order_date"] = orders["order_ts"].dt.date

    payment_agg = (
        payments.groupby("order_id", as_index=False)
        .agg(
            payment_count=("payment_id", "count"),
            total_paid_amount=("payment_amount", "sum"),
        )
    )

    items_agg = (
        order_items.groupby("order_id", as_index=False)
        .agg(
            item_count=("order_item_id", "count"),
        )
    )

    fact_sales = (
        orders.merge(payment_agg, on="order_id", how="left")
              .merge(items_agg, on="order_id", how="left")
    )

    fact_sales["payment_count"] = fact_sales["payment_count"].fillna(0).astype(int)
    fact_sales["total_paid_amount"] = fact_sales["total_paid_amount"].fillna(0)
    fact_sales["item_count"] = fact_sales["item_count"].fillna(0).astype(int)

    return fact_sales[
        [
            "order_id",
            "order_date",
            "order_ts",
            "order_channel",
            "store_id",
            "customer_id",
            "promotion_id",
            "order_status",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "payment_count",
            "total_paid_amount",
            "item_count",
        ]
    ].copy()


def build_fact_refunds() -> pd.DataFrame:
    refunds = read_table("silver", "refunds_clean")
    orders = read_table("silver", "orders_clean")

    refunds["refund_ts"] = pd.to_datetime(refunds["refund_ts"], errors="coerce")
    refunds["refund_date"] = refunds["refund_ts"].dt.date

    order_context = orders[
        ["order_id", "customer_id", "store_id"]
    ].drop_duplicates(subset=["order_id"])

    fact_refunds = refunds.merge(order_context, on="order_id", how="left")

    return fact_refunds[
        [
            "refund_id",
            "refund_date",
            "refund_ts",
            "order_id",
            "payment_id",
            "customer_id",
            "store_id",
            "refund_reason",
            "refund_amount",
        ]
    ].copy()
    
    
def build_fact_sales_items() -> pd.DataFrame:
    orders = read_table("silver", "orders_clean")
    order_items = read_table("silver", "order_items_clean")

    orders["order_ts"] = pd.to_datetime(orders["order_ts"], errors="coerce")
    orders["order_date"] = orders["order_ts"].dt.date

    order_context = orders[
        [
            "order_id",
            "order_date",
            "order_ts",
            "store_id",
            "customer_id",
            "order_channel",
        ]
    ].drop_duplicates(subset=["order_id"])

    fact_sales_items = order_items.merge(
        order_context,
        on="order_id",
        how="inner"
    )

    return fact_sales_items[
        [
            "order_item_id",
            "order_id",
            "order_date",
            "order_ts",
            "store_id",
            "customer_id",
            "product_id",
            "order_channel",
            "quantity",
            "unit_price",
            "discount_amount",
            "line_amount",
        ]
    ].copy()


def build_mart_daily_sales(fact_sales: pd.DataFrame) -> pd.DataFrame:
    mart = (
        fact_sales.groupby(
            ["order_date", "store_id", "order_channel"],
            as_index=False
        )
        .agg(
            total_orders=("order_id", "nunique"),
            total_sales_amount=("net_amount", "sum"),
            total_discount_amount=("discount_amount", "sum"),
            total_paid_amount=("total_paid_amount", "sum"),
            total_items=("item_count", "sum"),
        )
    )

    return mart




def load_facts_and_marts() -> None:
    fact_sales = build_fact_sales()
    fact_refunds = build_fact_refunds()
    fact_sales_items = build_fact_sales_items()
    mart_daily_sales = build_mart_daily_sales(fact_sales)

    truncate_table("fact_sales")
    write_table(fact_sales, "fact_sales")

    truncate_table("fact_refunds")
    write_table(fact_refunds, "fact_refunds")

    truncate_table("fact_sales_items")
    write_table(fact_sales_items, "fact_sales_items")

    truncate_table("mart_daily_sales")
    write_table(mart_daily_sales, "mart_daily_sales")



def main() -> None:
    load_dimensions()
    load_facts_and_marts()


if __name__ == "__main__":
    main()