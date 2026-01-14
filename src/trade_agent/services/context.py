from __future__ import annotations

from trade_agent.config import (
    AppSettings,
    ConfigValidationException,
    ensure_data_dir,
    load_config,
    resolve_db_path,
    validate_config,
)
from trade_agent.store import SQLiteStore


def load_settings(config_path: str = "config.yaml") -> AppSettings:
    settings = load_config(config_path)
    errors = validate_config(settings)
    if errors:
        raise ConfigValidationException(errors)
    ensure_data_dir(settings)
    return settings


def open_store(settings: AppSettings) -> SQLiteStore:
    return SQLiteStore(resolve_db_path(settings))
