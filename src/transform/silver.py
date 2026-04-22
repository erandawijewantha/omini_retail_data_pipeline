from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from src.quality.business_rules import (
    filter_valid_inventory_movements,
    filter_valid_order_items,
    filter_valid_orders,
    filter_valid_payments,
    filter_valid_refunds,
)
from src.utils.db import get_engine
from src.utils.logger import get_logger

logger = get_logger(__name__)


def read_bronze_table(table_name: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(f"SELECT * FROM bronze.{table_name}", engine)


def truncate_table(table_name: str) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE silver.{table_name}"))


def write_table(df: pd.DataFrame, table_name: str) -> None:
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema="silver",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )


def load_current_dimensions() -> None:
    customers = read_bronze_table("customers")
    products = read_bronze_table("products")

    truncate_table("dim_customer_current")
    write_table(customers, "dim_customer_current")

    truncate_table("dim_product_current")
    write_table(products, "dim_product_current")

    logger.info("Loaded current dimensions.")


def load_clean_transactions() -> None:
    stores = read_bronze_table("stores")
    customers = read_bronze_table("customers")
    products = read_bronze_table("products")
    orders = read_bronze_table("orders")
    order_items = read_bronze_table("order_items")
    payments = read_bronze_table("payments")
    refunds = read_bronze_table("refunds")
    inventory_movements = read_bronze_table("inventory_movements")

    orders_clean = filter_valid_orders(
        orders=orders,
        customers=customers,
        stores=stores,
    )

    payments_clean = filter_valid_payments(
        payments=payments,
        orders=orders_clean,
    )

    order_items_clean = filter_valid_order_items(
        order_items=order_items,
        orders=orders_clean,
        products=products,
    )

    refunds_clean = filter_valid_refunds(
        refunds=refunds,
        payments=payments_clean,
        orders=orders_clean,
    )

    inventory_movements_clean = filter_valid_inventory_movements(
        inventory_movements=inventory_movements,
        stores=stores,
        products=products,
    )

    truncate_table("orders_clean")
    write_table(orders_clean, "orders_clean")

    truncate_table("payments_clean")
    write_table(payments_clean, "payments_clean")

    truncate_table("order_items_clean")
    write_table(order_items_clean, "order_items_clean")

    truncate_table("refunds_clean")
    write_table(refunds_clean, "refunds_clean")

    truncate_table("inventory_movements_clean")
    write_table(inventory_movements_clean, "inventory_movements_clean")

    logger.info("Loaded clean transactional silver tables.")


def main() -> None:
    load_current_dimensions()
    load_clean_transactions()


if __name__ == "__main__":
    main()