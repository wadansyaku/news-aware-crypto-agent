from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


class ConfigValidationError(Exception):
    def __init__(self, field: str, message: str, suggestion: str = "") -> None:
        self.field = field
        self.message = message
        self.suggestion = suggestion
        super().__init__(f"{field}: {message}")

    def __str__(self) -> str:
        if self.suggestion:
            return f"{self.field}: {self.message} ({self.suggestion})"
        return f"{self.field}: {self.message}"


class ConfigValidationException(Exception):
    def __init__(self, errors: list[ConfigValidationError]) -> None:
        self.errors = errors
        message = "\n".join(str(err) for err in errors)
        super().__init__(message)


@dataclass
class AppConfig:
    name: str
    timezone: str
    data_dir: str
    db_path: str
    log_level: str


@dataclass
class ExchangeConfig:
    name: str
    api_key_env: str
    api_secret_env: str
    password_env: str
    enable_rate_limit: bool
    options: dict[str, Any]


@dataclass
class TradingConfig:
    mode: str
    dry_run: bool
    require_approval: bool
    approval_phrase: str
    kill_switch: bool
    i_understand_live_trading: bool
    long_only: bool
    symbol_whitelist: list[str]
    base_currency: str
    timeframes: list[str]
    candle_limit: int
    order_timeout_seconds: int
    post_only: bool
    intent_expiry_seconds: int
    maker_emulation: "MakerEmulationConfig"


@dataclass
class MakerEmulationConfig:
    buffer_bps: float
    use_tick: bool


@dataclass
class RiskConfig:
    capital_jpy: float
    max_position_pct: float
    max_order_notional_jpy: float
    max_loss_jpy_per_trade: float
    max_loss_jpy_per_day: float
    max_orders_per_day: int
    cooldown_minutes: int
    cooldown_bypass_pct: float


@dataclass
class NewsConfig:
    rss_urls: list[str]
    keyword_flags: list[str]
    source_weights: dict[str, float]
    sentiment_lookback_hours: int
    news_latency_seconds: int


@dataclass
class StrategyBaselineConfig:
    sma_period: int
    momentum_lookback: int
    base_position_pct: float


@dataclass
class StrategyNewsOverlayConfig:
    sentiment_boost_threshold: float
    sentiment_cut_threshold: float
    boost_multiplier: float
    cut_multiplier: float


@dataclass
class StrategiesConfig:
    baseline: StrategyBaselineConfig
    news_overlay: StrategyNewsOverlayConfig


@dataclass
class PaperConfig:
    seed: int
    slippage_bps: float
    fee_bps: float
    fill_probability: float
    spread_bps: float


@dataclass
class BacktestConfig:
    maker_fee_bps: float
    taker_fee_bps: float
    slippage_bps: float
    assume_taker: bool


@dataclass
class AutopilotConfig:
    enabled: bool
    max_order_notional_jpy: float
    max_loss_jpy_per_trade: float
    min_confidence: float
    symbol_whitelist: list[str]


@dataclass
class RunnerConfig:
    enabled: bool
    market_poll_seconds: int
    news_poll_seconds: int
    propose_poll_seconds: int
    propose_cooldown_seconds: int
    orderbook: bool
    jitter_seconds: int
    max_backoff_seconds: int


@dataclass
class AppSettings:
    app: AppConfig
    exchange: ExchangeConfig
    trading: TradingConfig
    risk: RiskConfig
    news: NewsConfig
    strategies: StrategiesConfig
    paper: PaperConfig
    backtest: BacktestConfig
    autopilot: AutopilotConfig
    runner: RunnerConfig


DEFAULTS: dict[str, Any] = {
    "app": {
        "name": "trade-agent",
        "timezone": "UTC",
        "data_dir": "data",
        "db_path": "trade_agent.db",
        "log_level": "INFO",
    },
    "exchange": {
        "name": "bitflyer",
        "api_key_env": "EXCHANGE_API_KEY",
        "api_secret_env": "EXCHANGE_API_SECRET",
        "password_env": "EXCHANGE_API_PASSWORD",
        "enable_rate_limit": True,
        "options": {"defaultType": "spot"},
    },
    "trading": {
        "mode": "paper",
        "dry_run": True,
        "require_approval": True,
        "approval_phrase": "I APPROVE",
        "kill_switch": False,
        "i_understand_live_trading": False,
        "long_only": True,
        "symbol_whitelist": ["BTC/JPY"],
        "base_currency": "JPY",
        "timeframes": ["1m"],
        "candle_limit": 500,
        "order_timeout_seconds": 30,
        "post_only": True,
        "intent_expiry_seconds": 900,
        "maker_emulation": {"buffer_bps": 0.1, "use_tick": True},
    },
    "risk": {
        "capital_jpy": 500000,
        "max_position_pct": 0.2,
        "max_order_notional_jpy": 50000,
        "max_loss_jpy_per_trade": 5000,
        "max_loss_jpy_per_day": 15000,
        "max_orders_per_day": 5,
        "cooldown_minutes": 5,
        "cooldown_bypass_pct": 0.02,
    },
    "news": {
        "rss_urls": [],
        "keyword_flags": [],
        "source_weights": {},
        "sentiment_lookback_hours": 12,
        "news_latency_seconds": 600,
    },
    "strategies": {
        "baseline": {
            "sma_period": 20,
            "momentum_lookback": 10,
            "base_position_pct": 0.1,
        },
        "news_overlay": {
            "sentiment_boost_threshold": 0.2,
            "sentiment_cut_threshold": -0.2,
            "boost_multiplier": 1.3,
            "cut_multiplier": 0.5,
        },
    },
    "paper": {
        "seed": 42,
        "slippage_bps": 5,
        "fee_bps": 10,
        "fill_probability": 0.7,
        "spread_bps": 2,
    },
    "backtest": {
        "fee_bps": 10,
        "maker_fee_bps": 5,
        "taker_fee_bps": 10,
        "slippage_bps": 5,
        "assume_taker": True,
    },
    "autopilot": {
        "enabled": False,
        "max_order_notional_jpy": 10000,
        "max_loss_jpy_per_trade": 2000,
        "min_confidence": 0.6,
        "symbol_whitelist": ["BTC/JPY"],
    },
    "runner": {
        "enabled": True,
        "market_poll_seconds": 30,
        "news_poll_seconds": 120,
        "propose_poll_seconds": 60,
        "propose_cooldown_seconds": 300,
        "orderbook": False,
        "jitter_seconds": 2,
        "max_backoff_seconds": 300,
    },
}


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merged[key] = _merge_dicts(base[key], value)
        else:
            merged[key] = value
    return merged


def _get(config: dict[str, Any], *keys: str, default: Any) -> Any:
    node: Any = config
    for key in keys:
        if not isinstance(node, dict):
            return default
        node = node.get(key)
    return default if node is None else node


def load_raw_config(path: str) -> dict[str, Any]:
    if not Path(path).exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def save_raw_config(path: str, data: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, sort_keys=False, allow_unicode=False)


def load_config(path: str) -> AppSettings:
    load_dotenv(override=False)
    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    merged = _merge_dicts(DEFAULTS, raw)
    raw_backtest = raw.get("backtest", {}) if isinstance(raw, dict) else {}
    has_maker = isinstance(raw_backtest, dict) and "maker_fee_bps" in raw_backtest
    has_taker = isinstance(raw_backtest, dict) and "taker_fee_bps" in raw_backtest

    app = AppConfig(
        name=_get(merged, "app", "name", default=DEFAULTS["app"]["name"]),
        timezone=_get(merged, "app", "timezone", default=DEFAULTS["app"]["timezone"]),
        data_dir=_get(merged, "app", "data_dir", default=DEFAULTS["app"]["data_dir"]),
        db_path=_get(merged, "app", "db_path", default=DEFAULTS["app"]["db_path"]),
        log_level=_get(merged, "app", "log_level", default=DEFAULTS["app"]["log_level"]),
    )

    exchange = ExchangeConfig(
        name=_get(merged, "exchange", "name", default=DEFAULTS["exchange"]["name"]),
        api_key_env=_get(
            merged, "exchange", "api_key_env", default=DEFAULTS["exchange"]["api_key_env"]
        ),
        api_secret_env=_get(
            merged,
            "exchange",
            "api_secret_env",
            default=DEFAULTS["exchange"]["api_secret_env"],
        ),
        password_env=_get(
            merged,
            "exchange",
            "password_env",
            default=DEFAULTS["exchange"]["password_env"],
        ),
        enable_rate_limit=bool(
            _get(
                merged,
                "exchange",
                "enable_rate_limit",
                default=DEFAULTS["exchange"]["enable_rate_limit"],
            )
        ),
        options=dict(
            _get(merged, "exchange", "options", default=DEFAULTS["exchange"]["options"])
        ),
    )

    trading = TradingConfig(
        mode=_get(merged, "trading", "mode", default=DEFAULTS["trading"]["mode"]),
        dry_run=bool(_get(merged, "trading", "dry_run", default=DEFAULTS["trading"]["dry_run"])),
        require_approval=bool(
            _get(
                merged,
                "trading",
                "require_approval",
                default=DEFAULTS["trading"]["require_approval"],
            )
        ),
        approval_phrase=_get(
            merged,
            "trading",
            "approval_phrase",
            default=DEFAULTS["trading"]["approval_phrase"],
        ),
        kill_switch=bool(
            _get(merged, "trading", "kill_switch", default=DEFAULTS["trading"]["kill_switch"])
        ),
        i_understand_live_trading=bool(
            _get(
                merged,
                "trading",
                "i_understand_live_trading",
                default=DEFAULTS["trading"]["i_understand_live_trading"],
            )
        ),
        long_only=bool(
            _get(merged, "trading", "long_only", default=DEFAULTS["trading"]["long_only"])
        ),
        symbol_whitelist=list(
            _get(
                merged,
                "trading",
                "symbol_whitelist",
                default=DEFAULTS["trading"]["symbol_whitelist"],
            )
        ),
        base_currency=_get(
            merged,
            "trading",
            "base_currency",
            default=DEFAULTS["trading"]["base_currency"],
        ),
        timeframes=list(
            _get(
                merged,
                "trading",
                "timeframes",
                default=DEFAULTS["trading"]["timeframes"],
            )
        ),
        candle_limit=int(
            _get(
                merged,
                "trading",
                "candle_limit",
                default=DEFAULTS["trading"]["candle_limit"],
            )
        ),
        order_timeout_seconds=int(
            _get(
                merged,
                "trading",
                "order_timeout_seconds",
                default=DEFAULTS["trading"]["order_timeout_seconds"],
            )
        ),
        post_only=bool(
            _get(
                merged,
                "trading",
                "post_only",
                default=DEFAULTS["trading"]["post_only"],
            )
        ),
        intent_expiry_seconds=int(
            _get(
                merged,
                "trading",
                "intent_expiry_seconds",
                default=DEFAULTS["trading"]["intent_expiry_seconds"],
            )
        ),
        maker_emulation=MakerEmulationConfig(
            buffer_bps=float(
                _get(
                    merged,
                    "trading",
                    "maker_emulation",
                    "buffer_bps",
                    default=DEFAULTS["trading"]["maker_emulation"]["buffer_bps"],
                )
            ),
            use_tick=bool(
                _get(
                    merged,
                    "trading",
                    "maker_emulation",
                    "use_tick",
                    default=DEFAULTS["trading"]["maker_emulation"]["use_tick"],
                )
            ),
        ),
    )

    risk = RiskConfig(
        capital_jpy=float(
            _get(merged, "risk", "capital_jpy", default=DEFAULTS["risk"]["capital_jpy"])
        ),
        max_position_pct=float(
            _get(
                merged,
                "risk",
                "max_position_pct",
                default=DEFAULTS["risk"]["max_position_pct"],
            )
        ),
        max_order_notional_jpy=float(
            _get(
                merged,
                "risk",
                "max_order_notional_jpy",
                default=DEFAULTS["risk"]["max_order_notional_jpy"],
            )
        ),
        max_loss_jpy_per_trade=float(
            _get(
                merged,
                "risk",
                "max_loss_jpy_per_trade",
                default=DEFAULTS["risk"]["max_loss_jpy_per_trade"],
            )
        ),
        max_loss_jpy_per_day=float(
            _get(
                merged,
                "risk",
                "max_loss_jpy_per_day",
                default=DEFAULTS["risk"]["max_loss_jpy_per_day"],
            )
        ),
        max_orders_per_day=int(
            _get(
                merged,
                "risk",
                "max_orders_per_day",
                default=DEFAULTS["risk"]["max_orders_per_day"],
            )
        ),
        cooldown_minutes=int(
            _get(
                merged,
                "risk",
                "cooldown_minutes",
                default=DEFAULTS["risk"]["cooldown_minutes"],
            )
        ),
        cooldown_bypass_pct=float(
            _get(
                merged,
                "risk",
                "cooldown_bypass_pct",
                default=DEFAULTS["risk"]["cooldown_bypass_pct"],
            )
        ),
    )

    news = NewsConfig(
        rss_urls=list(
            _get(merged, "news", "rss_urls", default=DEFAULTS["news"]["rss_urls"])
        ),
        keyword_flags=list(
            _get(merged, "news", "keyword_flags", default=DEFAULTS["news"]["keyword_flags"])
        ),
        source_weights=dict(
            _get(merged, "news", "source_weights", default=DEFAULTS["news"]["source_weights"])
        ),
        sentiment_lookback_hours=int(
            _get(
                merged,
                "news",
                "sentiment_lookback_hours",
                default=DEFAULTS["news"]["sentiment_lookback_hours"],
            )
        ),
        news_latency_seconds=int(
            _get(
                merged,
                "news",
                "news_latency_seconds",
                default=DEFAULTS["news"]["news_latency_seconds"],
            )
        ),
    )

    strategies = StrategiesConfig(
        baseline=StrategyBaselineConfig(
            sma_period=int(
                _get(
                    merged,
                    "strategies",
                    "baseline",
                    "sma_period",
                    default=DEFAULTS["strategies"]["baseline"]["sma_period"],
                )
            ),
            momentum_lookback=int(
                _get(
                    merged,
                    "strategies",
                    "baseline",
                    "momentum_lookback",
                    default=DEFAULTS["strategies"]["baseline"]["momentum_lookback"],
                )
            ),
            base_position_pct=float(
                _get(
                    merged,
                    "strategies",
                    "baseline",
                    "base_position_pct",
                    default=DEFAULTS["strategies"]["baseline"]["base_position_pct"],
                )
            ),
        ),
        news_overlay=StrategyNewsOverlayConfig(
            sentiment_boost_threshold=float(
                _get(
                    merged,
                    "strategies",
                    "news_overlay",
                    "sentiment_boost_threshold",
                    default=DEFAULTS["strategies"]["news_overlay"][
                        "sentiment_boost_threshold"
                    ],
                )
            ),
            sentiment_cut_threshold=float(
                _get(
                    merged,
                    "strategies",
                    "news_overlay",
                    "sentiment_cut_threshold",
                    default=DEFAULTS["strategies"]["news_overlay"]["sentiment_cut_threshold"],
                )
            ),
            boost_multiplier=float(
                _get(
                    merged,
                    "strategies",
                    "news_overlay",
                    "boost_multiplier",
                    default=DEFAULTS["strategies"]["news_overlay"]["boost_multiplier"],
                )
            ),
            cut_multiplier=float(
                _get(
                    merged,
                    "strategies",
                    "news_overlay",
                    "cut_multiplier",
                    default=DEFAULTS["strategies"]["news_overlay"]["cut_multiplier"],
                )
            ),
        ),
    )

    paper = PaperConfig(
        seed=int(_get(merged, "paper", "seed", default=DEFAULTS["paper"]["seed"])),
        slippage_bps=float(
            _get(merged, "paper", "slippage_bps", default=DEFAULTS["paper"]["slippage_bps"])
        ),
        fee_bps=float(
            _get(merged, "paper", "fee_bps", default=DEFAULTS["paper"]["fee_bps"])
        ),
        fill_probability=float(
            _get(
                merged,
                "paper",
                "fill_probability",
                default=DEFAULTS["paper"]["fill_probability"],
            )
        ),
        spread_bps=float(
            _get(merged, "paper", "spread_bps", default=DEFAULTS["paper"]["spread_bps"])
        ),
    )

    fee_bps = float(_get(merged, "backtest", "fee_bps", default=DEFAULTS["backtest"]["fee_bps"]))
    backtest = BacktestConfig(
        maker_fee_bps=float(
            _get(merged, "backtest", "maker_fee_bps", default=fee_bps)
            if has_maker
            else fee_bps
        ),
        taker_fee_bps=float(
            _get(merged, "backtest", "taker_fee_bps", default=fee_bps)
            if has_taker
            else fee_bps
        ),
        slippage_bps=float(
            _get(
                merged,
                "backtest",
                "slippage_bps",
                default=DEFAULTS["backtest"]["slippage_bps"],
            )
        ),
        assume_taker=bool(
            _get(
                merged,
                "backtest",
                "assume_taker",
                default=DEFAULTS["backtest"]["assume_taker"],
            )
        ),
    )

    autopilot = AutopilotConfig(
        enabled=bool(
            _get(merged, "autopilot", "enabled", default=DEFAULTS["autopilot"]["enabled"])
        ),
        max_order_notional_jpy=float(
            _get(
                merged,
                "autopilot",
                "max_order_notional_jpy",
                default=DEFAULTS["autopilot"]["max_order_notional_jpy"],
            )
        ),
        max_loss_jpy_per_trade=float(
            _get(
                merged,
                "autopilot",
                "max_loss_jpy_per_trade",
                default=DEFAULTS["autopilot"]["max_loss_jpy_per_trade"],
            )
        ),
        min_confidence=float(
            _get(
                merged,
                "autopilot",
                "min_confidence",
                default=DEFAULTS["autopilot"]["min_confidence"],
            )
        ),
        symbol_whitelist=list(
            _get(
                merged,
                "autopilot",
                "symbol_whitelist",
                default=DEFAULTS["autopilot"]["symbol_whitelist"],
            )
        ),
    )

    runner = RunnerConfig(
        enabled=bool(_get(merged, "runner", "enabled", default=DEFAULTS["runner"]["enabled"])),
        market_poll_seconds=int(
            _get(
                merged,
                "runner",
                "market_poll_seconds",
                default=DEFAULTS["runner"]["market_poll_seconds"],
            )
        ),
        news_poll_seconds=int(
            _get(
                merged,
                "runner",
                "news_poll_seconds",
                default=DEFAULTS["runner"]["news_poll_seconds"],
            )
        ),
        propose_poll_seconds=int(
            _get(
                merged,
                "runner",
                "propose_poll_seconds",
                default=DEFAULTS["runner"]["propose_poll_seconds"],
            )
        ),
        propose_cooldown_seconds=int(
            _get(
                merged,
                "runner",
                "propose_cooldown_seconds",
                default=DEFAULTS["runner"]["propose_cooldown_seconds"],
            )
        ),
        orderbook=bool(
            _get(
                merged,
                "runner",
                "orderbook",
                default=DEFAULTS["runner"]["orderbook"],
            )
        ),
        jitter_seconds=int(
            _get(
                merged,
                "runner",
                "jitter_seconds",
                default=DEFAULTS["runner"]["jitter_seconds"],
            )
        ),
        max_backoff_seconds=int(
            _get(
                merged,
                "runner",
                "max_backoff_seconds",
                default=DEFAULTS["runner"]["max_backoff_seconds"],
            )
        ),
    )

    return AppSettings(
        app=app,
        exchange=exchange,
        trading=trading,
        risk=risk,
        news=news,
        strategies=strategies,
        paper=paper,
        backtest=backtest,
        autopilot=autopilot,
        runner=runner,
    )


def validate_config(settings: AppSettings) -> list[ConfigValidationError]:
    errors: list[ConfigValidationError] = []

    if not settings.exchange.name:
        errors.append(
            ConfigValidationError(
                field="exchange.name",
                message="取引所名が指定されていません",
                suggestion="config.yaml で exchange.name を設定してください (例: bitflyer)",
            )
        )

    for sym in settings.trading.symbol_whitelist:
        if "/" not in sym:
            errors.append(
                ConfigValidationError(
                    field="trading.symbol_whitelist",
                    message=f"無効なシンボル形式: {sym}",
                    suggestion="BASE/QUOTE 形式で指定してください (例: BTC/JPY)",
                )
            )

    if settings.risk.max_order_notional_jpy > settings.risk.capital_jpy:
        errors.append(
            ConfigValidationError(
                field="risk.max_order_notional_jpy",
                message="最大注文額が資本を超えています",
                suggestion="max_order_notional_jpy <= capital_jpy に設定してください",
            )
        )

    return errors


def ensure_data_dir(settings: AppSettings) -> Path:
    data_dir = Path(settings.app.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def resolve_db_path(settings: AppSettings) -> str:
    path = Path(settings.app.db_path)
    if not path.is_absolute():
        data_dir = Path(settings.app.data_dir)
        if path.parts and path.parts[0] == data_dir.name:
            return str(path)
        path = data_dir / path
    return str(path)
