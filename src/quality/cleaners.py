from __future__ import annotations

import pandas as pd


def clean_text(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.strip()
        .str.strip('"')
        .str.strip("'")
    )


def clean_code(series: pd.Series) -> pd.Series:
    return clean_text(series).str.upper()


def clean_nullable_text(series: pd.Series) -> pd.Series:
    cleaned = clean_text(series)
    return cleaned.replace({"<NA>": pd.NA, "NAN": pd.NA, "NONE": pd.NA, "": pd.NA})


def clean_nullable_code(series: pd.Series) -> pd.Series:
    cleaned = clean_code(series)
    return cleaned.replace({"<NA>": pd.NA, "NAN": pd.NA, "NONE": pd.NA, "": pd.NA})


def clean_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def clean_integer(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def clean_boolean(series: pd.Series) -> pd.Series:
    mapping = {
        "TRUE": True,
        "FALSE": False,
        "1": True,
        "0": False,
        "YES": True,
        "NO": False,
        "Y": True,
        "N": False,
    }

    as_code = clean_code(series)
    return as_code.map(mapping).astype("boolean")


def clean_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def clean_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def normalize_dataframe_strings(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    for col in result.columns:
        if pd.api.types.is_object_dtype(result[col]) or pd.api.types.is_string_dtype(result[col]):
            result[col] = clean_nullable_text(result[col])

    return result