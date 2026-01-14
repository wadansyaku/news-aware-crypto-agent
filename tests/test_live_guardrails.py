from __future__ import annotations

from pathlib import Path

from trade_agent.config import AppSettings, load_config, resolve_db_path
from trade_agent.executor import execute_intent
from trade_agent.intent import TradePlan, from_plan
from trade_agent.store import SQLiteStore


def _make_settings(tmp_path: Path, trading_yaml: str) -> AppSettings:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""
app:
  data_dir: {tmp_path.as_posix()}
  db_path: { (tmp_path / 'test.db').as_posix() }
trading:
{trading_yaml}
""",
        encoding="utf-8",
    )
    return load_config(str(config_path))


def _insert_intent(store: SQLiteStore) -> str:
    plan = TradePlan(
        symbol="BTC/JPY",
        side="buy",
        size=1.0,
        price=1000.0,
        confidence=0.5,
        rationale="test",
        strategy="baseline",
    )
    intent = from_plan(plan, mode="live", expiry_seconds=300)
    store.save_order_intent(intent)
    return intent.intent_id


def test_live_rejects_when_dry_run(monkeypatch, tmp_path: Path) -> None:
    settings = _make_settings(
        tmp_path,
        """
  dry_run: true
  require_approval: false
  i_understand_live_trading: true
""",
    )
    store = SQLiteStore(resolve_db_path(settings))
    intent_id = _insert_intent(store)

    monkeypatch.setenv("I_UNDERSTAND_LIVE_TRADING", "true")
    result = execute_intent(store, intent_id, settings, mode="live")
    assert result.status == "rejected"
    assert result.message == "dry_run enabled"
    store.close()


def test_live_requires_double_consent(monkeypatch, tmp_path: Path) -> None:
    settings = _make_settings(
        tmp_path,
        """
  dry_run: false
  require_approval: false
  i_understand_live_trading: true
""",
    )
    store = SQLiteStore(resolve_db_path(settings))
    intent_id = _insert_intent(store)

    monkeypatch.delenv("I_UNDERSTAND_LIVE_TRADING", raising=False)
    result = execute_intent(store, intent_id, settings, mode="live")
    assert result.status == "rejected"
    assert result.message == "live trading not acknowledged"
    store.close()


def test_live_rejects_missing_credentials(monkeypatch, tmp_path: Path) -> None:
    settings = _make_settings(
        tmp_path,
        """
  dry_run: false
  require_approval: false
  i_understand_live_trading: true
""",
    )
    store = SQLiteStore(resolve_db_path(settings))
    intent_id = _insert_intent(store)

    monkeypatch.setenv("I_UNDERSTAND_LIVE_TRADING", "true")
    monkeypatch.delenv(settings.exchange.api_key_env, raising=False)
    monkeypatch.delenv(settings.exchange.api_secret_env, raising=False)

    result = execute_intent(store, intent_id, settings, mode="live")
    assert result.status == "rejected"
    assert result.message == "missing API credentials"
    store.close()
