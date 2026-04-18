from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from dotenv.cli import get

BASE_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = BASE_DIR / ".env"

load_dotenv(ENV_PATH)

def _get_bool(value: str | None, default: False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}

@dataclass(frozen=True)
class Settings:
    app_env: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_echo: bool
    
    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )
        
def get_settings() -> Settings:
    return Settings(
        app_env = os.getenv("APP_ENV", "dev"),
        db_host = os.getenv("DB_HOST", "localhost"),
        db_port = int(os.getenv("DB_PORT", "5432")),
        db_name = os.getenv("DB_NAME", "omini_retail"),
        db_user = os.getenv("DB_USER", "postgres"),
        db_password = os.getenv("DB_PASSWORD", "postgres"),
        db_echo = _get_bool(os.getenv("DB_ECHO"), default=False),
    )
    
settings = get_settings()