from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    import ccxt  # pragma: no cover

from trade_agent.config import ExchangeConfig


def _build_ohlcv_from_trades(
    exchange: Any, symbol: str, timeframe: str, limit: int, since: int | None
) -> list[list[Any]]:
    if not exchange.has.get("fetchTrades"):
        raise RuntimeError(f"{exchange.id} fetchTrades() is not supported")
    try:
        tf_seconds = exchange.parse_timeframe(timeframe)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"failed to parse timeframe {timeframe}: {exc}") from exc
    tf_ms = int(tf_seconds * 1000)
    if since is None:
        since = int(time.time() * 1000) - tf_ms * limit

    trade_limit = min(max(limit * 50, 200), 1000)
    max_batches = 5
    trades: list[dict[str, Any]] = []
    since_cursor = since
    for _ in range(max_batches):
        batch = exchange.fetch_trades(symbol, since=since_cursor, limit=trade_limit)
        if not batch:
            break
        trades.extend(batch)
        timestamps = [t.get("timestamp") for t in batch if t.get("timestamp") is not None]
        if not timestamps:
            break
        last_ts = max(timestamps)
        since_cursor = last_ts + 1
        all_ts = [t.get("timestamp") for t in trades if t.get("timestamp") is not None]
        if all_ts and max(all_ts) - min(all_ts) >= tf_ms * limit:
            break
    buckets: dict[int, list[float]] = {}
    for trade in trades:
        ts = trade.get("timestamp")
        price = trade.get("price")
        amount = trade.get("amount") or 0.0
        if ts is None or price is None:
            continue
        bucket = int(ts // tf_ms) * tf_ms
        if bucket not in buckets:
            buckets[bucket] = [float(price), float(price), float(price), float(price), 0.0]
        o, h, l, c, v = buckets[bucket]
        h = max(h, float(price))
        l = min(l, float(price))
        c = float(price)
        v += float(amount)
        buckets[bucket] = [o, h, l, c, v]

    ohlcv = []
    for bucket in sorted(buckets.keys()):
        o, h, l, c, v = buckets[bucket]
        ohlcv.append([bucket, o, h, l, c, v])
    return ohlcv[-limit:]


@dataclass
class ExchangeClient:
    exchange: Any

    def load_markets(self) -> None:
        self.exchange.load_markets()

    def fetch_candles(
        self, symbol: str, timeframe: str, limit: int, since: int | None = None
    ) -> list[list[Any]]:
        if self.exchange.has.get("fetchOHLCV"):
            return self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit, since=since)
        return _build_ohlcv_from_trades(self.exchange, symbol, timeframe, limit, since)

    def fetch_orderbook(self, symbol: str) -> dict[str, Any]:
        return self.exchange.fetch_order_book(symbol)

    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        return self.exchange.fetch_ticker(symbol)

    def fetch_balance(self) -> dict[str, Any]:
        if not self.exchange.has.get("fetchBalance"):
            raise RuntimeError(f"{self.exchange.id} fetchBalance() is not supported")
        return self.exchange.fetch_balance()

    def fetch_my_trades(
        self, symbol: str, since: int | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        if not self.exchange.has.get("fetchMyTrades"):
            raise RuntimeError(f"{self.exchange.id} fetchMyTrades() is not supported")
        return self.exchange.fetch_my_trades(symbol, since=since, limit=limit)

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
        if client.exchange.has.get("fetchTime"):
            server_time = client.exchange.fetch_time()
            return True, f"server_time={server_time}"
        return True, "fetchTime unsupported; markets loaded"
    except Exception as exc:  # noqa: BLE001
        return False, f"error: {exc}"


def has_credentials(config: ExchangeConfig) -> bool:
    return bool(os.getenv(config.api_key_env) and os.getenv(config.api_secret_env))
