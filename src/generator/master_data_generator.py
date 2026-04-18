from __future__ import annotations

from datetime import datetime, timedelta
import random

import pandas as pd
from faker import Faker

from src.generator.base import GeneratorConfig, MASTER_DIR, seed_everything
from src.generator.ids import make_id
from src.generator.writers import write_csv


fake = Faker()


def generate_stores(n: int) -> pd.DataFrame:
    rows = []
    regions = ["North", "South", "East", "West", "Central"]
    store_types = ["FLAGSHIP", "MALL", "HIGH_STREET", "EXPRESS"]

    for i in range(1, n + 1):
        created_at = fake.date_time_between(start_date="-5y", end_date="-1y")
        updated_at = created_at + timedelta(days=random.randint(0, 365))

        rows.append(
            {
                "store_id": make_id("STO", i, 4),
                "store_name": f"{fake.city()} Store",
                "store_type": random.choice(store_types),
                "city": fake.city(),
                "region": random.choice(regions),
                "opened_date": fake.date_between(start_date="-5y", end_date="-1y"),
                "manager_employee_id": None,
                "is_active": random.choice([True, True, True, False]),
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )

    return pd.DataFrame(rows)


def generate_customers(n: int) -> pd.DataFrame:
    rows = []
    loyalty_tiers = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
    segments = ["BUDGET", "REGULAR", "PREMIUM", "VIP"]
    regions = ["North", "South", "East", "West", "Central"]

    for i in range(1, n + 1):
        created_at = fake.date_time_between(start_date="-3y", end_date="-30d")
        updated_at = created_at + timedelta(days=random.randint(0, 180))

        rows.append(
            {
                "customer_id": make_id("CUS", i, 6),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "gender": random.choice(["Male", "Female"]),
                "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=75),
                "email": fake.email() if random.random() > 0.05 else None,
                "phone": fake.phone_number() if random.random() > 0.08 else None,
                "city": fake.city(),
                "region": random.choice(regions),
                "loyalty_tier": random.choice(loyalty_tiers),
                "customer_segment": random.choice(segments),
                "registered_at": created_at,
                "is_active": random.choice([True, True, True, False]),
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )

    return pd.DataFrame(rows)


def generate_products(n: int) -> pd.DataFrame:
    rows = []
    categories = {
        "Beverages": ["Tea", "Coffee", "Juice"],
        "Snacks": ["Chips", "Biscuits", "Nuts"],
        "Household": ["Cleaner", "Soap", "Tissue"],
        "Personal Care": ["Shampoo", "Toothpaste", "Cream"],
    }
    brands = ["Nova", "Prime", "FreshCo", "Urban", "DailyMax"]

    for i in range(1, n + 1):
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        created_at = fake.date_time_between(start_date="-4y", end_date="-60d")
        updated_at = created_at + timedelta(days=random.randint(0, 200))

        cost_price = round(random.uniform(100, 3000), 2)
        unit_price = round(cost_price * random.uniform(1.1, 1.8), 2)

        rows.append(
            {
                "product_id": make_id("PRD", i, 6),
                "sku": f"SKU-{i:06d}",
                "product_name": f"{random.choice(brands)} {subcategory} {i}",
                "category": category,
                "subcategory": subcategory,
                "brand": random.choice(brands),
                "unit_price": unit_price,
                "cost_price": cost_price,
                "is_active": random.choice([True, True, True, False]),
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )

    return pd.DataFrame(rows)


def generate_employees(n: int, store_ids: list[str]) -> pd.DataFrame:
    rows = []
    roles = ["Cashier", "Supervisor", "Store Manager", "Sales Associate"]

    for i in range(1, n + 1):
        created_at = fake.date_time_between(start_date="-5y", end_date="-90d")
        updated_at = created_at + timedelta(days=random.randint(0, 180))

        rows.append(
            {
                "employee_id": make_id("EMP", i, 5),
                "store_id": random.choice(store_ids),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "role_name": random.choice(roles),
                "hire_date": fake.date_between(start_date="-5y", end_date="-90d"),
                "is_active": random.choice([True, True, True, False]),
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )

    return pd.DataFrame(rows)


def generate_promotions(n: int) -> pd.DataFrame:
    rows = []
    discount_types = ["PERCENTAGE", "FIXED"]
    channels = ["STORE", "ONLINE", "ALL"]

    for i in range(1, n + 1):
        start_date = fake.date_between(start_date="-1y", end_date="+30d")
        end_date = start_date + timedelta(days=random.randint(7, 30))
        created_at = fake.date_time_between(start_date="-1y", end_date="-10d")
        updated_at = created_at + timedelta(days=random.randint(0, 60))

        rows.append(
            {
                "promotion_id": make_id("PRO", i, 5),
                "promotion_name": f"Promo {i}",
                "discount_type": random.choice(discount_types),
                "discount_value": round(random.uniform(5, 30), 2),
                "start_date": start_date,
                "end_date": end_date,
                "channel": random.choice(channels),
                "is_active": random.choice([True, True, False]),
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )

    return pd.DataFrame(rows)


def generate_master_data() -> None:
    config = GeneratorConfig()
    seed_everything(config.random_seed)
    Faker.seed(config.random_seed)

    stores = generate_stores(config.n_stores)
    customers = generate_customers(config.n_customers)
    products = generate_products(config.n_products)
    employees = generate_employees(config.n_employees, stores["store_id"].tolist())
    promotions = generate_promotions(config.n_promotions)

    write_csv(stores, MASTER_DIR / "stores.csv")
    write_csv(customers, MASTER_DIR / "customers.csv")
    write_csv(products, MASTER_DIR / "products.csv")
    write_csv(employees, MASTER_DIR / "employees.csv")
    write_csv(promotions, MASTER_DIR / "promotions.csv")

    print("Master data generated successfully.")


if __name__ == "__main__":
    generate_master_data()