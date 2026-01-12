from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    import ccxt  # pragma: no cover

from trade_agent.config import ExchangeConfig


@dataclass
class ExchangeClient:
    exchange: Any

    def load_markets(self) -> None:
        self.exchange.load_markets()

    def fetch_candles(self, symbol: str, timeframe: str, limit: int) -> list[list[Any]]:
        return self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)

    def fetch_orderbook(self, symbol: str) -> dict[str, Any]:
        return self.exchange.fetch_order_book(symbol)

    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        return self.exchange.fetch_ticker(symbol)

    def create_limit_order(
        self, symbol: str, side: str, amount: float, price: float, post_only: bool
    ) -> dict[str, Any]:
        params = {}
        if post_only and self.exchange.has.get("postOnly"):
            params["postOnly"] = True
        return self.exchange.create_order(symbol, "limit", side, amount, price, params)

    def fetch_order(self, order_id: str, symbol: str) -> dict[str, Any]:
        return self.exchange.fetch_order(order_id, symbol)

    def cancel_order(self, order_id: str, symbol: str) -> dict[str, Any]:
        return self.exchange.cancel_order(order_id, symbol)


def build_exchange(config: ExchangeConfig) -> ExchangeClient:
    import ccxt  # local import to avoid hard dependency during tests

    if not hasattr(ccxt, config.name):
        raise ValueError(f"Exchange {config.name} not supported by ccxt")
    klass = getattr(ccxt, config.name)
    api_key = os.getenv(config.api_key_env, "")
    api_secret = os.getenv(config.api_secret_env, "")
    password = os.getenv(config.password_env, "")
    exchange = klass(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "password": password,
            "enableRateLimit": True,
            "options": config.options,
        }
    )
    return ExchangeClient(exchange=exchange)


def check_public_connection(client: ExchangeClient) -> tuple[bool, str]:
    try:
        client.load_markets()
        server_time = client.exchange.fetch_time()
        return True, f"ok (server_time={server_time})"
    except Exception as exc:  # noqa: BLE001
        return False, f"error: {exc}"


def has_credentials(config: ExchangeConfig) -> bool:
    return bool(os.getenv(config.api_key_env) and os.getenv(config.api_secret_env))
