from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from trade_agent.config import ConfigValidationException, load_raw_config, save_raw_config
from trade_agent.runner import Runner
from trade_agent.services import (
    approval,
    analysis,
    alerts,
    context,
    execution,
    external,
    ingest,
    portfolio,
    positions,
    propose,
    queries,
    reporting,
    status,
)

BASE_DIR = Path(__file__).resolve().parents[3]
WEB_DIR = BASE_DIR / "web"
STATIC_DIR = WEB_DIR / "static"

app = FastAPI(title="Trade Agent Web", docs_url=None, redoc_url=None)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

_RUNNER_LOCK = threading.Lock()
_RUNNER_THREAD: threading.Thread | None = None
_RUNNER_INSTANCE: Runner | None = None


class IngestRequest(BaseModel):
    symbol: Optional[str] = None
    orderbook: bool = False
    news_only: bool = False
    features_only: bool = False


class ProposeRequest(BaseModel):
    symbol: Optional[str] = None
    strategy: str = "baseline"
    mode: str = "paper"
    refresh: bool = False


class ApproveRequest(BaseModel):
    intent_id: str
    phrase: str


class ApproveExecuteRequest(BaseModel):
    intent_id: str
    phrase: str
    mode: str = "paper"


class ExecuteRequest(BaseModel):
    intent_id: Optional[str] = None
    mode: str = "paper"


class BacktestRequest(BaseModel):
    start: str
    end: str
    strategy: str = "baseline"
    symbol: Optional[str] = None


class ReportRequest(BaseModel):
    mode: Optional[str] = None


class ClosePositionRequest(BaseModel):
    symbol: Optional[str] = None
    mode: str = "paper"


class SafetyUpdateRequest(BaseModel):
    mode: Optional[str] = None
    dry_run: Optional[bool] = None
    require_approval: Optional[bool] = None
    kill_switch: Optional[bool] = None
    autopilot_enabled: Optional[bool] = None
    i_understand_live_trading: Optional[bool] = None
    cooldown_minutes: Optional[int] = None
    cooldown_bypass_pct: Optional[float] = None
    max_loss_jpy_per_day: Optional[float] = None
    max_orders_per_day: Optional[int] = None


class RunnerStartRequest(BaseModel):
    strategy: str = "news_overlay"
    mode: str = "paper"


class AlertCreateRequest(BaseModel):
    symbol: str
    condition: str
    threshold: float


class ExternalIngestRequest(BaseModel):
    symbols: Optional[list[str]] = None
    limit: Optional[int] = None


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


def _approved_by() -> str:
    return "local"


def _load_settings():
    try:
        return context.load_settings()
    except ConfigValidationException as exc:
        message = "config validation failed:\\n" + "\\n".join(str(err) for err in exc.errors)
        raise HTTPException(status_code=400, detail=message) from exc


def _config_path() -> str:
    return "config.yaml"


@app.get("/api/status")
async def status_api() -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    store.close()
    return status.get_status(settings)


@app.post("/api/config/safety")
async def safety_update_api(payload: SafetyUpdateRequest) -> dict:
    config_path = _config_path()
    raw = load_raw_config(config_path)
    if not isinstance(raw, dict):
        raw = {}

    trading = raw.setdefault("trading", {})
    autopilot = raw.setdefault("autopilot", {})
    risk = raw.setdefault("risk", {})
    if payload.mode:
        if payload.mode not in {"paper", "live"}:
            raise HTTPException(status_code=400, detail="invalid mode")
        trading["mode"] = payload.mode
    if payload.dry_run is not None:
        trading["dry_run"] = bool(payload.dry_run)
    if payload.require_approval is not None:
        trading["require_approval"] = bool(payload.require_approval)
    if payload.kill_switch is not None:
        trading["kill_switch"] = bool(payload.kill_switch)
    if payload.i_understand_live_trading is not None:
        trading["i_understand_live_trading"] = bool(payload.i_understand_live_trading)
    if payload.autopilot_enabled is not None:
        autopilot["enabled"] = bool(payload.autopilot_enabled)
    if payload.cooldown_minutes is not None:
        risk["cooldown_minutes"] = int(payload.cooldown_minutes)
    if payload.cooldown_bypass_pct is not None:
        risk["cooldown_bypass_pct"] = float(payload.cooldown_bypass_pct)
    if payload.max_loss_jpy_per_day is not None:
        risk["max_loss_jpy_per_day"] = float(payload.max_loss_jpy_per_day)
    if payload.max_orders_per_day is not None:
        risk["max_orders_per_day"] = int(payload.max_orders_per_day)

    save_raw_config(config_path, raw)
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        store.log_event(
            "config_update",
            {"scope": "safety", "updates": payload.model_dump(exclude_unset=True)},
        )
    finally:
        store.close()
    return status.get_status(settings)


@app.post("/api/ingest")
async def ingest_api(payload: IngestRequest) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        result = ingest.ingest(
            settings,
            store,
            ingest.IngestParams(
                symbol=payload.symbol,
                orderbook=payload.orderbook,
                news_only=payload.news_only,
                features_only=payload.features_only,
            ),
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        store.close()


@app.post("/api/propose")
async def propose_api(payload: ProposeRequest) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        result = propose.propose(
            settings,
            store,
            propose.ProposeParams(
                symbol=payload.symbol,
                strategy=payload.strategy,
                mode=payload.mode,
                refresh=payload.refresh,
            ),
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        store.close()


@app.post("/api/approve")
async def approve_api(payload: ApproveRequest) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        result = approval.approve_intent(
            settings,
            store,
            payload.intent_id,
            payload.phrase,
            _approved_by(),
        )
        return result.__dict__
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        store.close()


@app.post("/api/approve_execute")
async def approve_execute_api(payload: ApproveExecuteRequest) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        result = execution.approve_and_execute(
            settings,
            store,
            intent_id=payload.intent_id,
            phrase=payload.phrase,
            approved_by=_approved_by(),
            mode=payload.mode,
        )
        return {
            "approval": result.approval.__dict__,
            "execution": result.execution.__dict__,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        store.close()


@app.post("/api/execute")
async def execute_api(payload: ExecuteRequest) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        result = execution.execute(
            settings,
            store,
            intent_id=payload.intent_id,
            mode=payload.mode,
        )
        return result.__dict__
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        store.close()


@app.post("/api/backtest")
async def backtest_api(payload: BacktestRequest) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return reporting.backtest(
            settings,
            store,
            payload.start,
            payload.end,
            payload.strategy,
            payload.symbol,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        store.close()


@app.get("/api/backtest/results")
async def backtest_results_api(limit: int = 20) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        reports = []
        for report in queries.list_backtest_reports(store, limit=limit):
            metrics = report.get("metrics") or {}
            if "strategy" not in metrics and ":" not in report.get("period", ""):
                continue
            reports.append(report)
        return {"results": reports}
    finally:
        store.close()


@app.get("/api/runner/state")
async def runner_state_api() -> dict:
    settings = _load_settings()
    state_path = Path(settings.app.data_dir) / "runner_state.json"
    if not state_path.exists():
        return {"exists": False, "running": False}
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"exists": True, "running": False, "error": "invalid state file"}
    updated_at = datetime.fromtimestamp(state_path.stat().st_mtime, timezone.utc).isoformat()
    running = _runner_state_running(settings)
    return {
        "exists": True,
        "running": running,
        "updated_at": updated_at,
        "state": state,
    }


def _runner_running() -> bool:
    return _RUNNER_THREAD is not None and _RUNNER_THREAD.is_alive()


def _runner_state_running(settings) -> bool:
    state_path = Path(settings.app.data_dir) / "runner_state.json"
    if not state_path.exists():
        return False
    updated_at = datetime.fromtimestamp(state_path.stat().st_mtime, timezone.utc)
    max_interval = max(
        settings.runner.market_poll_seconds,
        settings.runner.news_poll_seconds,
        settings.runner.propose_poll_seconds,
    )
    return (datetime.now(timezone.utc) - updated_at).total_seconds() <= max_interval * 3


@app.post("/api/runner/start")
async def runner_start_api(payload: RunnerStartRequest) -> dict:
    global _RUNNER_THREAD, _RUNNER_INSTANCE
    with _RUNNER_LOCK:
        if _runner_running():
            raise HTTPException(status_code=400, detail="runner already running")

        settings = _load_settings()
        if _runner_state_running(settings):
            raise HTTPException(status_code=400, detail="runner already active (state file)")
        if payload.strategy not in {"baseline", "news_overlay"}:
            raise HTTPException(status_code=400, detail="invalid strategy")
        if payload.mode not in {"paper", "live"}:
            raise HTTPException(status_code=400, detail="invalid mode")

        def _run() -> None:
            global _RUNNER_INSTANCE
            store = context.open_store(settings)
            runner = Runner(
                settings,
                store,
                propose_params=propose.ProposeParams(
                    strategy=payload.strategy,
                    mode=payload.mode,
                    refresh=False,
                ),
            )
            with _RUNNER_LOCK:
                _RUNNER_INSTANCE = runner
            try:
                runner.run()
            finally:
                store.close()

        thread = threading.Thread(target=_run, daemon=True)
        _RUNNER_THREAD = thread
        thread.start()

    settings = _load_settings()
    store = context.open_store(settings)
    try:
        store.log_event(
            "runner_start",
            {"strategy": payload.strategy, "mode": payload.mode},
        )
    finally:
        store.close()
    return {"status": "started"}


@app.post("/api/runner/stop")
async def runner_stop_api() -> dict:
    global _RUNNER_THREAD, _RUNNER_INSTANCE
    with _RUNNER_LOCK:
        if not _runner_running():
            return {"status": "not_running"}
        if _RUNNER_INSTANCE:
            _RUNNER_INSTANCE.request_stop()
        thread = _RUNNER_THREAD
    if thread:
        thread.join(timeout=2)
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        store.log_event("runner_stop", {})
    finally:
        store.close()
    return {"status": "stopping"}


@app.get("/api/watchlist")
async def watchlist_api() -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return alerts.watchlist(settings, store)
    finally:
        store.close()


@app.post("/api/alerts")
async def alerts_create_api(payload: AlertCreateRequest) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        if payload.symbol not in settings.trading.symbol_whitelist:
            raise HTTPException(status_code=400, detail="symbol not in whitelist")
        return alerts.create_alert(store, payload.symbol, payload.condition, payload.threshold)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        store.close()


@app.get("/api/alerts")
async def alerts_list_api(check: bool = False) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        _, price_map = alerts.build_price_snapshot(settings, store)
        triggered = alerts.check_alerts(store, price_map) if check else []
        alert_list = alerts.list_alerts(store, current_prices=price_map)
        return {"alerts": alert_list, "triggered": triggered}
    finally:
        store.close()


@app.delete("/api/alerts/{alert_id}")
async def alerts_delete_api(alert_id: int) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        store.delete_alert(alert_id)
        return {"deleted": alert_id}
    finally:
        store.close()


@app.post("/api/report")
async def report_api(payload: ReportRequest) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return reporting.report(settings, store, payload.mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        store.close()


@app.get("/api/analytics")
async def analytics_api(mode: Optional[str] = None) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return reporting.analytics(settings, store, mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        store.close()


@app.get("/api/analysis/performance")
async def analysis_performance_api(
    mode: Optional[str] = None,
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return analysis.internal_performance(settings, store, mode, symbol, start, end)
    finally:
        store.close()


@app.get("/api/analysis/intents")
async def analysis_intents_api(
    mode: Optional[str] = None,
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return analysis.intent_outcomes(settings, store, mode, symbol, start, end)
    finally:
        store.close()


@app.get("/api/external/summary")
async def external_summary_api(
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return analysis.external_summary(settings, store, symbol, start, end)
    finally:
        store.close()


@app.post("/api/external/ingest")
async def external_ingest_api(payload: ExternalIngestRequest) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return external.ingest_external(
            settings, store, symbols=payload.symbols, limit=payload.limit
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        store.close()


@app.get("/api/news")
async def news_api(limit: int = 50) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return {"items": queries.latest_news_with_features(store, limit=limit)}
    finally:
        store.close()


@app.get("/api/news/sentiment_timeline")
async def sentiment_timeline_api(hours: int = 24) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return {"timeline": queries.sentiment_timeline(store, hours=hours)}
    finally:
        store.close()


@app.get("/api/intents")
async def intents_api(limit: int = 20) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return {"intents": queries.list_intents(store, limit)}
    finally:
        store.close()


@app.get("/api/audit")
async def audit_api(
    event: Optional[str] = None,
    events: Optional[str] = None,
    intent_id: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 200,
) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        logs = queries.list_audit_logs(store, event=event, limit=limit)
        if events:
            allow = {e.strip() for e in events.split(",") if e.strip()}
            logs = [log for log in logs if log["event"] in allow]
        if intent_id:
            logs = [
                log
                for log in logs
                if log.get("data", {}).get("intent_id") == intent_id
            ]
        if start:
            logs = [log for log in logs if log["ts"] >= start]
        if end:
            logs = [log for log in logs if log["ts"] <= end]
        return {"logs": logs}
    finally:
        store.close()


@app.get("/api/audit/summary")
async def audit_summary_api(limit: int = 1000) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        logs = queries.list_audit_logs(store, event="risk_check", limit=limit)
        total = len(logs)
        approved = 0
        rejected = 0
        reasons: dict[str, int] = {}
        for log in logs:
            status = log.get("data", {}).get("status")
            reason = log.get("data", {}).get("reason") or "unknown"
            if status == "approved":
                approved += 1
            elif status == "rejected":
                rejected += 1
                reasons[reason] = reasons.get(reason, 0) + 1
        top_reasons = sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:3]
        return {
            "total": total,
            "approved": approved,
            "rejected": rejected,
            "approval_rate": approved / total if total else 0.0,
            "rejection_reasons": reasons,
            "top_reasons": [{"reason": r, "count": c} for r, c in top_reasons],
        }
    finally:
        store.close()


@app.get("/api/position")
async def position_api(symbol: Optional[str] = None) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return queries.position(settings, store, symbol)
    finally:
        store.close()


@app.get("/api/portfolio")
async def portfolio_api() -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return portfolio.get_portfolio(settings, store)
    finally:
        store.close()


@app.get("/api/position/overview")
async def position_overview_api(symbol: Optional[str] = None) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        return queries.position_overview(settings, store, symbol)
    finally:
        store.close()


@app.post("/api/position/close")
async def position_close_api(payload: ClosePositionRequest) -> dict:
    settings = _load_settings()
    store = context.open_store(settings)
    try:
        result = positions.close_position(
            settings,
            store,
            positions.ClosePositionParams(symbol=payload.symbol, mode=payload.mode),
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        store.close()


def main() -> None:
    import uvicorn

    uvicorn.run("trade_agent.apps.web:app", host="127.0.0.1", port=8000, reload=False)


if __name__ == "__main__":
    main()
