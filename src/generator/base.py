from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import random


DATA_DIR = Path("data/source_batches")
MASTER_DIR = DATA_DIR / "master"
TRANSACTION_DIR = DATA_DIR / "transactions"


@dataclass(frozen=True)
class GeneratorConfig:
    random_seed: int = 42
    n_stores: int = 25
    n_customers: int = 5000
    n_products: int = 1000
    n_employees: int = 300
    n_promotions: int = 40


def seed_everything(seed: int) -> None:
    random.seed(seed)