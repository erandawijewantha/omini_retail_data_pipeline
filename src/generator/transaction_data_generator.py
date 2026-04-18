from __future__ import annotations

from datetime import timedelta
import random
from pathlib import Path

import pandas as pd
from faker import Faker

from src.generator.base import seed_everything
from src.generator.batch_scenarios import BATCH_001, BATCH_002, BATCH_ROOT, BatchScenario
from src.generator.ids import make_id
from src.generator.writers import write_csv, ensure_dir


fake = Faker()


MASTER_DIR = Path("data/source_batches/master")


def load_master_data() -> dict[str, pd.DataFrame]:
    return {
        "stores": pd.read_csv(MASTER_DIR / "stores.csv"),
        "customers": pd.read_csv(MASTER_DIR / "customers.csv"),
        "products": pd.read_csv(MASTER_DIR / "products.csv"),
        "employees": pd.read_csv(MASTER_DIR / "employees.csv"),
        "promotions": pd.read_csv(MASTER_DIR / "promotions.csv"),
    }


def _pick_order_status(success: bool) -> str:
    if success:
        return random.choice(["PAID", "COMPLETED"])
    return random.choice(["CREATED", "PAYMENT_FAILED", "CANCELLED"])


def generate_orders(
    scenario: BatchScenario,
    stores: pd.DataFrame,
    customers: pd.DataFrame,
    employees: pd.DataFrame,
    promotions: pd.DataFrame,
    start_order_number: int,
) -> pd.DataFrame:
    rows = []

    store_ids = stores["store_id"].dropna().tolist()
    customer_ids = customers["customer_id"].dropna().tolist()
    employee_ids = employees["employee_id"].dropna().tolist()
    promotion_ids = promotions["promotion_id"].dropna().tolist()

    for i in range(scenario.n_orders):
        order_num = start_order_number + i
        order_id = f"ORD{order_num:08d}"

        order_ts = fake.date_time_between(start_date="-30d", end_date="now")
        created_at = order_ts
        updated_at = created_at

        order_channel = random.choice(["STORE", "ONLINE"])
        payment_success = random.random() > scenario.failed_payment_rate

        rows.append(
            {
                "order_id": order_id,
                "order_channel": order_channel,
                "store_id": random.choice(store_ids),
                "customer_id": random.choice(customer_ids),
                "employee_id": random.choice(employee_ids) if order_channel == "STORE" else None,
                "promotion_id": random.choice(promotion_ids) if random.random() > 0.35 else None,
                "order_status": _pick_order_status(payment_success),
                "order_ts": order_ts,
                "gross_amount": 0.0,
                "discount_amount": 0.0,
                "net_amount": 0.0,
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )

    df = pd.DataFrame(rows)

    # duplicate injection
    n_duplicates = int(len(df) * scenario.duplicate_rate)
    if n_duplicates > 0:
        duplicate_rows = df.sample(n=n_duplicates, replace=False, random_state=42)
        df = pd.concat([df, duplicate_rows], ignore_index=True)

    return df


def generate_order_items(
    orders: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    product_rows = products.to_dict(orient="records")
    item_counter = 1

    unique_orders = orders.drop_duplicates(subset=["order_id"])

    for _, order in unique_orders.iterrows():
        n_items = random.randint(1, 5)
        selected_products = random.sample(product_rows, k=n_items)

        gross_total = 0.0
        discount_total = 0.0

        for product in selected_products:
            qty = random.randint(1, 4)
            unit_price = float(product["unit_price"])
            raw_amount = qty * unit_price
            discount_amount = round(raw_amount * random.choice([0, 0, 0.05, 0.10]), 2)
            line_amount = round(raw_amount - discount_amount, 2)

            gross_total += raw_amount
            discount_total += discount_amount

            rows.append(
                {
                    "order_item_id": make_id("OIT", item_counter, 8),
                    "order_id": order["order_id"],
                    "product_id": product["product_id"],
                    "quantity": qty,
                    "unit_price": unit_price,
                    "discount_amount": discount_amount,
                    "line_amount": line_amount,
                    "created_at": order["created_at"],
                    "updated_at": order["updated_at"],
                }
            )
            item_counter += 1

        orders.loc[orders["order_id"] == order["order_id"], "gross_amount"] = round(gross_total, 2)
        orders.loc[orders["order_id"] == order["order_id"], "discount_amount"] = round(discount_total, 2)
        orders.loc[orders["order_id"] == order["order_id"], "net_amount"] = round(gross_total - discount_total, 2)

    return pd.DataFrame(rows)


def generate_payments(
    orders: pd.DataFrame,
    scenario: BatchScenario,
    start_payment_number: int,
) -> pd.DataFrame:
    rows = []
    payment_counter = start_payment_number

    unique_orders = orders.drop_duplicates(subset=["order_id"])

    for _, order in unique_orders.iterrows():
        payment_success = order["order_status"] in {"PAID", "COMPLETED"}
        payment_ts = pd.to_datetime(order["order_ts"]) + timedelta(minutes=random.randint(1, 120))

        if random.random() < scenario.late_payment_rate:
            payment_ts = payment_ts + timedelta(days=random.randint(1, 3))

        payment_status = "SUCCESS" if payment_success else random.choice(["FAILED", "PENDING"])
        payment_amount = float(order["net_amount"]) if payment_success else 0.0

        rows.append(
            {
                "payment_id": make_id("PAY", payment_counter, 8),
                "order_id": order["order_id"],
                "payment_method": random.choice(["CASH", "CARD", "WALLET", "BANK_TRANSFER"]),
                "payment_status": payment_status,
                "payment_amount": round(payment_amount, 2),
                "payment_ts": payment_ts,
                "reference_number": fake.uuid4()[:12],
                "created_at": payment_ts,
                "updated_at": payment_ts,
            }
        )
        payment_counter += 1

    return pd.DataFrame(rows)


def generate_refunds(
    orders: pd.DataFrame,
    payments: pd.DataFrame,
    scenario: BatchScenario,
    start_refund_number: int,
) -> pd.DataFrame:
    rows = []
    refund_counter = start_refund_number

    successful_payments = payments[payments["payment_status"] == "SUCCESS"].copy()

    if successful_payments.empty:
        return pd.DataFrame(columns=[
            "refund_id", "order_id", "payment_id", "refund_reason",
            "refund_amount", "refund_ts", "created_at", "updated_at"
        ])

    n_refunds = int(len(successful_payments) * scenario.refund_rate)
    refund_sample = successful_payments.sample(n=n_refunds, replace=False, random_state=42) if n_refunds > 0 else successful_payments.head(0)

    for _, payment in refund_sample.iterrows():
        refund_ts = pd.to_datetime(payment["payment_ts"]) + timedelta(days=random.randint(1, 5))
        payment_amount = float(payment["payment_amount"])
        refund_amount = round(payment_amount * random.choice([0.25, 0.50, 1.00]), 2)

        rows.append(
            {
                "refund_id": make_id("REF", refund_counter, 8),
                "order_id": payment["order_id"],
                "payment_id": payment["payment_id"],
                "refund_reason": random.choice(
                    ["DAMAGED_ITEM", "CUSTOMER_RETURN", "WRONG_ITEM", "QUALITY_ISSUE"]
                ),
                "refund_amount": refund_amount,
                "refund_ts": refund_ts,
                "created_at": refund_ts,
                "updated_at": refund_ts,
            }
        )
        refund_counter += 1

    return pd.DataFrame(rows)


def generate_inventory_movements(
    order_items: pd.DataFrame,
    refunds: pd.DataFrame,
    orders: pd.DataFrame,
    start_inventory_number: int,
) -> pd.DataFrame:
    rows = []
    movement_counter = start_inventory_number

    order_store_map = orders.drop_duplicates(subset=["order_id"]).set_index("order_id")["store_id"].to_dict()

    for _, item in order_items.iterrows():
        rows.append(
            {
                "inventory_movement_id": make_id("INV", movement_counter, 8),
                "store_id": order_store_map.get(item["order_id"]),
                "product_id": item["product_id"],
                "movement_type": "SALE",
                "quantity_change": -int(item["quantity"]),
                "movement_ts": item["created_at"],
                "reference_id": item["order_id"],
                "created_at": item["created_at"],
                "updated_at": item["updated_at"],
            }
        )
        movement_counter += 1

    for _, refund in refunds.iterrows():
        related_items = order_items[order_items["order_id"] == refund["order_id"]]
        for _, item in related_items.iterrows():
            rows.append(
                {
                    "inventory_movement_id": make_id("INV", movement_counter, 8),
                    "store_id": order_store_map.get(item["order_id"]),
                    "product_id": item["product_id"],
                    "movement_type": "REFUND",
                    "quantity_change": int(item["quantity"]),
                    "movement_ts": refund["refund_ts"],
                    "reference_id": refund["refund_id"],
                    "created_at": refund["created_at"],
                    "updated_at": refund["updated_at"],
                }
            )
            movement_counter += 1

    return pd.DataFrame(rows)


def apply_order_updates(orders: pd.DataFrame, scenario: BatchScenario) -> pd.DataFrame:
    if scenario.order_update_rate <= 0:
        return orders

    updated_orders = orders.drop_duplicates(subset=["order_id"]).copy()
    n_updates = int(len(updated_orders) * scenario.order_update_rate)

    if n_updates <= 0:
        return orders

    sample_idx = updated_orders.sample(n=n_updates, random_state=42).index

    for idx in sample_idx:
        current_status = updated_orders.at[idx, "order_status"]
        if current_status in {"CREATED", "PAYMENT_FAILED"}:
            updated_orders.at[idx, "order_status"] = random.choice(["PAID", "CANCELLED"])
            updated_orders.at[idx, "updated_at"] = pd.to_datetime(updated_orders.at[idx, "updated_at"]) + timedelta(hours=6)

    return pd.concat([orders, updated_orders.loc[sample_idx]], ignore_index=True)


def write_batch_outputs(
    scenario: BatchScenario,
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    payments: pd.DataFrame,
    refunds: pd.DataFrame,
    inventory_movements: pd.DataFrame,
) -> None:
    batch_dir = BATCH_ROOT / scenario.batch_name
    ensure_dir(batch_dir)

    write_csv(orders, batch_dir / "orders.csv")
    write_csv(order_items, batch_dir / "order_items.csv")
    write_csv(payments, batch_dir / "payments.csv")
    write_csv(refunds, batch_dir / "refunds.csv")
    write_csv(inventory_movements, batch_dir / "inventory_movements.csv")


def run_scenario(
    scenario: BatchScenario,
    start_order_number: int,
    start_payment_number: int,
    start_refund_number: int,
    start_inventory_number: int,
) -> dict[str, int]:
    master = load_master_data()

    orders = generate_orders(
        scenario=scenario,
        stores=master["stores"],
        customers=master["customers"],
        employees=master["employees"],
        promotions=master["promotions"],
        start_order_number=start_order_number,
    )

    orders = apply_order_updates(orders, scenario)

    order_items = generate_order_items(orders=orders, products=master["products"])
    payments = generate_payments(
        orders=orders,
        scenario=scenario,
        start_payment_number=start_payment_number,
    )
    refunds = generate_refunds(
        orders=orders,
        payments=payments,
        scenario=scenario,
        start_refund_number=start_refund_number,
    )
    inventory_movements = generate_inventory_movements(
        order_items=order_items,
        refunds=refunds,
        orders=orders,
        start_inventory_number=start_inventory_number,
    )

    write_batch_outputs(
        scenario=scenario,
        orders=orders,
        order_items=order_items,
        payments=payments,
        refunds=refunds,
        inventory_movements=inventory_movements,
    )

    return {
        "orders": len(orders),
        "order_items": len(order_items),
        "payments": len(payments),
        "refunds": len(refunds),
        "inventory_movements": len(inventory_movements),
    }


def main() -> None:
    seed_everything(42)
    Faker.seed(42)

    summary_1 = run_scenario(
        scenario=BATCH_001,
        start_order_number=1,
        start_payment_number=1,
        start_refund_number=1,
        start_inventory_number=1,
    )

    summary_2 = run_scenario(
        scenario=BATCH_002,
        start_order_number=100000,
        start_payment_number=100000,
        start_refund_number=100000,
        start_inventory_number=100000,
    )

    print("Transaction batches generated successfully.")
    print("Batch 001:", summary_1)
    print("Batch 002:", summary_2)


if __name__ == "__main__":
    main()