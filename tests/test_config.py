from __future__ import annotations

from pathlib import Path

from trade_agent.config import load_config


def test_config_overrides(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
trading:
  approval_phrase: "OK"
  symbol_whitelist:
    - "ETH/JPY"
backtest:
  fee_bps: 12
  slippage_bps: 7
  assume_taker: false
""",
        encoding="utf-8",
    )
    settings = load_config(str(config_path))
    assert settings.trading.approval_phrase == "OK"
    assert settings.trading.symbol_whitelist == ["ETH/JPY"]
    assert settings.backtest.maker_fee_bps == 12
    assert settings.backtest.taker_fee_bps == 12
    assert settings.backtest.slippage_bps == 7
    assert settings.backtest.assume_taker is False

