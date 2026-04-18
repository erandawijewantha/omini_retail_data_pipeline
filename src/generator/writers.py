from __future__ import annotations

from pathlib import Path
import pandas as pd


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(df: pd.DataFrame, output_path: Path) -> None:
    ensure_dir(output_path.parent)
    df.to_csv(output_path, index=False)