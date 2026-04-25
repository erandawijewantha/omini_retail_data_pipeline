from __future__ import annotations

import hashlib

import pandas as pd
from sqlalchemy import text

from src.utils.db import get_engine
from src.utils.logger import get_logger

logger = get_logger(__name__)


def generate_hash(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    safe_df = df[cols].fillna("").astype(str)

    return safe_df.agg("||".join, axis=1).apply(
        lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest()
    )


def apply_scd_type_2(
    source_df: pd.DataFrame,
    history_table: str,
    business_key: str,
    tracked_cols: list[str],
    insert_cols: list[str],
    effective_col: str = "updated_at",
) -> None:
    engine = get_engine()

    source = source_df.copy()

    source["record_hash"] = generate_hash(source, tracked_cols)
    source["effective_from"] = pd.to_datetime(source[effective_col], errors="coerce")
    source["effective_to"] = pd.NaT
    source["is_current"] = True

    # Important: only insert columns that exist in the history table
    source_insert = source[insert_cols].copy()

    existing = pd.read_sql(
        f"""
        SELECT {business_key}, record_hash
        FROM silver.{history_table}
        WHERE is_current = true
        """,
        engine,
    )

    if existing.empty:
        source_insert.to_sql(
            history_table,
            engine,
            schema="silver",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
        )
        logger.info("Initial SCD load completed for %s", history_table)
        return

    merged = source.merge(
        existing,
        on=business_key,
        how="left",
        suffixes=("", "_existing"),
    )

    changed = merged[
        merged["record_hash_existing"].isna()
        | (merged["record_hash"] != merged["record_hash_existing"])
    ].copy()

    if changed.empty:
        logger.info("No SCD changes found for %s", history_table)
        return

    changed_keys = changed[business_key].dropna().unique().tolist()

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                UPDATE silver.{history_table}
                SET is_current = false,
                    effective_to = CURRENT_TIMESTAMP
                WHERE {business_key} = ANY(:keys)
                  AND is_current = true
            """),
            {"keys": changed_keys},
        )

    changed_insert = changed[insert_cols].copy()

    changed_insert.to_sql(
        history_table,
        engine,
        schema="silver",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )

    logger.info("Inserted %s SCD rows into %s", len(changed_insert), history_table)