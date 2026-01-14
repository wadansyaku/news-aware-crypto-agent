from __future__ import annotations

import json
import logging
import os
from typing import Optional

import typer

from trade_agent.config import ConfigValidationException
from trade_agent.runner import Runner
from trade_agent.services import approval as approval_service
from trade_agent.services import context, execution, ingest, propose, reporting, status

app = typer.Typer(help="News-aware crypto spot trading agent")


def _approved_by() -> str:
    return os.getenv("USER") or os.getenv("USERNAME") or "local"


def _load_settings(config_path: str):
    try:
        return context.load_settings(config_path)
    except ConfigValidationException as exc:
        typer.echo("config validation failed:")
        for err in exc.errors:
            typer.echo(f"- {err}")
        raise typer.Exit(code=1) from exc


def _apply_runner_overrides(
    settings,
    market_poll_seconds: Optional[int],
    news_poll_seconds: Optional[int],
    propose_poll_seconds: Optional[int],
    propose_cooldown_seconds: Optional[int],
    jitter_seconds: Optional[int],
    max_backoff_seconds: Optional[int],
    orderbook: Optional[bool],
):
    runner = settings.runner
    if market_poll_seconds is not None:
        runner.market_poll_seconds = market_poll_seconds
    if news_poll_seconds is not None:
        runner.news_poll_seconds = news_poll_seconds
    if propose_poll_seconds is not None:
        runner.propose_poll_seconds = propose_poll_seconds
    if propose_cooldown_seconds is not None:
        runner.propose_cooldown_seconds = propose_cooldown_seconds
    if jitter_seconds is not None:
        runner.jitter_seconds = jitter_seconds
    if max_backoff_seconds is not None:
        runner.max_backoff_seconds = max_backoff_seconds
    if orderbook is not None:
        runner.orderbook = orderbook
    return runner


@app.command("status")
def status_cmd(config: str = typer.Option("config.yaml", help="Path to config.yaml")) -> None:
    settings = _load_settings(config)
    store = context.open_store(settings)
    store.close()
    payload = status.get_status(settings)
    typer.echo(json.dumps(payload, indent=2))


@app.command("ingest")
def ingest_cmd(
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
    symbol: Optional[str] = typer.Option(None, help="Symbol to ingest"),
    orderbook: bool = typer.Option(False, help="Ingest orderbook snapshot"),
    news_only: bool = typer.Option(False, "--news", help="Ingest news only"),
    features_only: bool = typer.Option(False, "--features", help="Generate features only"),
) -> None:
    settings = _load_settings(config)
    store = context.open_store(settings)
    try:
        result = ingest.ingest(
            settings,
            store,
            ingest.IngestParams(
                symbol=symbol,
                orderbook=orderbook,
                news_only=news_only,
                features_only=features_only,
            ),
        )
        typer.echo(json.dumps(result, indent=2))
    except ValueError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    finally:
        store.close()


@app.command("propose")
def propose_cmd(
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
    symbol: Optional[str] = typer.Option(None, help="Symbol to trade"),
    strategy: str = typer.Option("baseline", help="baseline or news_overlay"),
    mode: str = typer.Option("paper", help="paper or live"),
    refresh: bool = typer.Option(False, help="Refresh candles from exchange"),
) -> None:
    settings = _load_settings(config)
    store = context.open_store(settings)
    try:
        result = propose.propose(
            settings,
            store,
            propose.ProposeParams(
                symbol=symbol,
                strategy=strategy,
                mode=mode,
                refresh=refresh,
            ),
        )
        typer.echo(json.dumps(result, indent=2))
    except ValueError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    finally:
        store.close()


@app.command("approve")
def approve_cmd(
    intent_id: str,
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
    phrase: Optional[str] = typer.Option(None, help="Approval phrase"),
) -> None:
    settings = _load_settings(config)
    store = context.open_store(settings)
    try:
        phrase = phrase or typer.prompt("Approval phrase")
        result = approval_service.approve_intent(
            settings,
            store,
            intent_id,
            phrase,
            _approved_by(),
        )
        typer.echo(json.dumps(result.__dict__, indent=2))
    except ValueError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    finally:
        store.close()


@app.command("execute")
def execute_cmd(
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
    intent_id: Optional[str] = typer.Option(None, help="Intent ID"),
    mode: str = typer.Option("paper", help="paper or live"),
) -> None:
    settings = _load_settings(config)
    store = context.open_store(settings)
    try:
        result = execution.execute(settings, store, intent_id=intent_id, mode=mode)
        typer.echo(json.dumps(result.__dict__, indent=2))
    except ValueError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    finally:
        store.close()


@app.command("approve-execute")
def approve_execute_cmd(
    intent_id: str,
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
    phrase: Optional[str] = typer.Option(None, help="Approval phrase"),
    mode: str = typer.Option("paper", help="paper or live"),
) -> None:
    settings = _load_settings(config)
    store = context.open_store(settings)
    try:
        phrase = phrase or typer.prompt("Approval phrase")
        result = execution.approve_and_execute(
            settings,
            store,
            intent_id=intent_id,
            phrase=phrase,
            approved_by=_approved_by(),
            mode=mode,
        )
        typer.echo(
            json.dumps(
                {
                    "approval": result.approval.__dict__,
                    "execution": result.execution.__dict__,
                },
                indent=2,
            )
        )
    except ValueError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    finally:
        store.close()


@app.command("backtest")
def backtest_cmd(
    start: str = typer.Option(..., "--from", help="Start date YYYY-MM-DD"),
    end: str = typer.Option(..., "--to", help="End date YYYY-MM-DD"),
    strategy: str = typer.Option("baseline", help="baseline or news_overlay"),
    symbol: Optional[str] = typer.Option(None, help="Symbol"),
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
) -> None:
    settings = _load_settings(config)
    store = context.open_store(settings)
    try:
        result = reporting.backtest(settings, store, start, end, strategy, symbol)
        typer.echo(json.dumps(result, indent=2))
    except ValueError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    finally:
        store.close()


@app.command("report")
def report_cmd(
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
    mode: Optional[str] = typer.Option(None, help="Filter by mode"),
) -> None:
    settings = _load_settings(config)
    store = context.open_store(settings)
    try:
        result = reporting.report(settings, store, mode)
        typer.echo(json.dumps(result, indent=2))
    except ValueError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    finally:
        store.close()


@app.command("run")
def run_cmd(
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
    symbol: Optional[str] = typer.Option(None, help="Symbol to trade"),
    strategy: str = typer.Option("baseline", help="baseline or news_overlay"),
    mode: str = typer.Option("paper", help="paper or live"),
    orderbook: Optional[bool] = typer.Option(None, help="Ingest orderbook snapshot"),
    market_poll_seconds: Optional[int] = typer.Option(
        None, help="Market ingest interval seconds"
    ),
    news_poll_seconds: Optional[int] = typer.Option(
        None, help="News ingest interval seconds"
    ),
    propose_poll_seconds: Optional[int] = typer.Option(
        None, help="Propose interval seconds"
    ),
    propose_cooldown_seconds: Optional[int] = typer.Option(
        None, help="Cooldown seconds for identical proposals"
    ),
    jitter_seconds: Optional[int] = typer.Option(None, help="Jitter seconds"),
    max_backoff_seconds: Optional[int] = typer.Option(None, help="Max backoff seconds"),
    once: bool = typer.Option(False, help="Run a single cycle and exit"),
    max_cycles: Optional[int] = typer.Option(
        None, help="Max cycles for testing; exits after N loops"
    ),
) -> None:
    settings = _load_settings(config)
    logging.basicConfig(level=getattr(logging, settings.app.log_level, logging.INFO))
    _apply_runner_overrides(
        settings,
        market_poll_seconds,
        news_poll_seconds,
        propose_poll_seconds,
        propose_cooldown_seconds,
        jitter_seconds,
        max_backoff_seconds,
        orderbook,
    )
    store = context.open_store(settings)
    runner = Runner(
        settings,
        store,
        propose_params=propose.ProposeParams(
            symbol=symbol,
            strategy=strategy,
            mode=mode,
            refresh=False,
        ),
    )
    runner.install_signal_handlers()
    try:
        runner.run(once=once, max_cycles=max_cycles)
    except KeyboardInterrupt:
        runner.request_stop()
    finally:
        store.close()


if __name__ == "__main__":
    app()
