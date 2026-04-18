from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


BATCH_ROOT = Path("data/source_batches/transactions")


@dataclass(frozen=True)
class BatchScenario:
    batch_name: str
    n_orders: int
    duplicate_rate: float
    failed_payment_rate: float
    refund_rate: float
    late_payment_rate: float
    order_update_rate: float


BATCH_001 = BatchScenario(
    batch_name="batch_001",
    n_orders=3000,
    duplicate_rate=0.01,
    failed_payment_rate=0.08,
    refund_rate=0.03,
    late_payment_rate=0.00,
    order_update_rate=0.00,
)

BATCH_002 = BatchScenario(
    batch_name="batch_002",
    n_orders=2000,
    duplicate_rate=0.03,
    failed_payment_rate=0.10,
    refund_rate=0.05,
    late_payment_rate=0.08,
    order_update_rate=0.12,
)