from __future__ import annotations

import json
import logging
import random
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from trade_agent.config import AppSettings, RunnerConfig, ensure_data_dir
from trade_agent.schemas import canonical_json, sha256_hex
from trade_agent.services import ingest as ingest_service
from trade_agent.intent import TradePlan
from trade_agent.services import propose as propose_service
from trade_agent.services.ingest import IngestParams
from trade_agent.services.propose import ProposalCandidate, ProposeParams
from trade_agent.store import SQLiteStore


@dataclass
class RunnerState:
    iteration: int = 0
    last_success_ingest_market_at: str | None = None
    last_success_ingest_news_at: str | None = None
    last_success_propose_at: str | None = None
    last_error_at: str | None = None
    last_error_summary: str | None = None
    last_signature: str | None = None
    last_signature_at: str | None = None


class Runner:
    def __init__(
        self,
        settings: AppSettings,
        store: SQLiteStore,
        runner: RunnerConfig | None = None,
        *,
        ingest_fn: Callable[[AppSettings, SQLiteStore, IngestParams], dict[str, Any]] | None = None,
        prepare_proposal_fn: Callable[
            [AppSettings, SQLiteStore, ProposeParams], ProposalCandidate
        ]
        | None = None,
        finalize_proposal_fn: Callable[
            [AppSettings, SQLiteStore, ProposalCandidate, ProposeParams], dict[str, Any]
        ]
        | None = None,
        now_fn: Callable[[], datetime] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
        state_path: Path | None = None,
        logger: logging.Logger | None = None,
        propose_params: ProposeParams | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.config = runner or settings.runner
        self.ingest_fn = ingest_fn or ingest_service.ingest
        self.prepare_proposal_fn = prepare_proposal_fn or propose_service.prepare_proposal
        self.finalize_proposal_fn = finalize_proposal_fn or propose_service.finalize_proposal
        self.now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self.sleep_fn = sleep_fn or time.sleep
        self.logger = logger or logging.getLogger("trade_agent.runner")
        self.stop_requested = False
        self.backoff_seconds = 0
        self.propose_params = propose_params or ProposeParams()

        ensure_data_dir(settings)
        self.state_path = state_path or Path(settings.app.data_dir) / "runner_state.json"
        self.state = self._load_state()

        now_ts = self._now_ts()
        self.next_market_ts = now_ts
        self.next_news_ts = now_ts
        self.next_propose_ts = now_ts

    def request_stop(self) -> None:
        self.stop_requested = True

    def install_signal_handlers(self) -> None:
        def _handler(signum, _frame):
            self.logger.info("runner stopping (signal=%s)", signum)
            self.request_stop()

        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

    def _now_ts(self) -> float:
        return self.now_fn().timestamp()

    def _now_iso(self) -> str:
        return self.now_fn().isoformat()

    def _load_state(self) -> RunnerState:
        if not self.state_path.exists():
            return RunnerState()
        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return RunnerState()
        return RunnerState(
            iteration=int(raw.get("iteration", 0)),
            last_success_ingest_market_at=raw.get("last_success_ingest_market_at"),
            last_success_ingest_news_at=raw.get("last_success_ingest_news_at"),
            last_success_propose_at=raw.get("last_success_propose_at"),
            last_error_at=raw.get("last_error_at"),
            last_error_summary=raw.get("last_error_summary"),
            last_signature=raw.get("last_signature"),
            last_signature_at=raw.get("last_signature_at"),
        )

    def _write_state(self) -> None:
        payload = {
            "iteration": self.state.iteration,
            "last_success_ingest_market_at": self.state.last_success_ingest_market_at,
            "last_success_ingest_news_at": self.state.last_success_ingest_news_at,
            "last_success_propose_at": self.state.last_success_propose_at,
            "last_error_at": self.state.last_error_at,
            "last_error_summary": self.state.last_error_summary,
            "last_signature": self.state.last_signature,
            "last_signature_at": self.state.last_signature_at,
        }
        self.state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _jitter(self) -> float:
        if self.config.jitter_seconds <= 0:
            return 0.0
        return random.uniform(0, float(self.config.jitter_seconds))

    def _schedule_next(self, now_ts: float, interval: int) -> float:
        return now_ts + float(interval) + self._jitter()

    def _plan_signature(self, plan: TradePlan, mode: str) -> str:
        payload = {
            "symbol": plan.symbol,
            "side": plan.side,
            "size": round(float(plan.size), 8),
            "price": round(float(plan.price), 8),
            "strategy": plan.strategy,
            "mode": mode,
            "order_type": "limit",
            "time_in_force": "GTC",
        }
        return sha256_hex(canonical_json(payload))

    def _within_cooldown(self, signature: str, now: datetime) -> bool:
        if not self.state.last_signature or not self.state.last_signature_at:
            return False
        if self.state.last_signature != signature:
            return False
        try:
            last_ts = datetime.fromisoformat(self.state.last_signature_at)
        except ValueError:
            return False
        delta = (now - last_ts).total_seconds()
        return delta < self.config.propose_cooldown_seconds

    def run(self, *, once: bool = False, max_cycles: int | None = None) -> None:
        cycles = 0
        while not self.stop_requested:
            if once and cycles >= 1:
                break
            if max_cycles is not None and cycles >= max_cycles:
                break

            cycle_start = time.perf_counter()
            cycles += 1
            self.state.iteration += 1
            now = self.now_fn()
            now_ts = now.timestamp()
            errors: list[str] = []
            ingest_attempted = False
            ingest_failed = False

            if now_ts >= self.next_market_ts:
                ingest_attempted = True
                start = time.perf_counter()
                try:
                    result = self.ingest_fn(
                        self.settings,
                        self.store,
                        IngestParams(orderbook=self.config.orderbook, market_only=True),
                    )
                    error_items = result.get("errors") or []
                    ok = not error_items
                    if ok:
                        self.state.last_success_ingest_market_at = self._now_iso()
                    else:
                        ingest_failed = True
                        errors.append(f"market ingest errors={len(error_items)}")
                    elapsed = time.perf_counter() - start
                    self.logger.info(
                        "runner.market_ingest ok=%s candles=%s duration=%.2fs",
                        ok,
                        result.get("candles"),
                        elapsed,
                    )
                except Exception as exc:  # noqa: BLE001
                    ingest_failed = True
                    errors.append(f"market ingest exception={exc}")
                    self.logger.warning("runner.market_ingest failed: %s", exc)
                self.next_market_ts = self._schedule_next(now_ts, self.config.market_poll_seconds)

            if now_ts >= self.next_news_ts:
                ingest_attempted = True
                start = time.perf_counter()
                try:
                    news_result = self.ingest_fn(
                        self.settings,
                        self.store,
                        IngestParams(news_only=True),
                    )
                    feature_result = self.ingest_fn(
                        self.settings,
                        self.store,
                        IngestParams(features_only=True),
                    )
                    news_errors = (news_result.get("errors") or []) + (
                        feature_result.get("errors") or []
                    )
                    ok = not news_errors
                    if ok:
                        self.state.last_success_ingest_news_at = self._now_iso()
                    else:
                        ingest_failed = True
                        errors.append(f"news ingest errors={len(news_errors)}")
                    elapsed = time.perf_counter() - start
                    self.logger.info(
                        "runner.news_ingest ok=%s inserted=%s features=%s duration=%.2fs",
                        ok,
                        news_result.get("news", {}).get("inserted", 0),
                        feature_result.get("features_added", 0),
                        elapsed,
                    )
                except Exception as exc:  # noqa: BLE001
                    ingest_failed = True
                    errors.append(f"news ingest exception={exc}")
                    self.logger.warning("runner.news_ingest failed: %s", exc)
                self.next_news_ts = self._schedule_next(now_ts, self.config.news_poll_seconds)

            propose_due = now_ts >= self.next_propose_ts
            should_propose = propose_due or ingest_attempted
            if should_propose:
                start = time.perf_counter()
                if ingest_failed:
                    self.logger.warning("runner.propose skipped (ingest failed)")
                else:
                    try:
                        candidate = self.prepare_proposal_fn(
                            self.settings,
                            self.store,
                            self.propose_params,
                        )
                        if candidate.status != "proposed" or not candidate.plan:
                            self.logger.info(
                                "runner.propose status=%s reason=%s",
                                candidate.status,
                                candidate.reason,
                            )
                        else:
                            signature = self._plan_signature(
                                candidate.plan, mode=self.propose_params.mode
                            )
                            if self._within_cooldown(signature, now):
                                self.logger.info("runner.propose skipped (no change)")
                            else:
                                result = self.finalize_proposal_fn(
                                    self.settings, self.store, candidate, self.propose_params
                                )
                                self.state.last_success_propose_at = self._now_iso()
                                self.state.last_signature = signature
                                self.state.last_signature_at = self._now_iso()
                                self.logger.info(
                                    "runner.propose intent=%s side=%s size=%s price=%s",
                                    result.get("intent_id"),
                                    result.get("side"),
                                    result.get("size"),
                                    result.get("price"),
                                )
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"propose exception={exc}")
                        self.logger.warning("runner.propose failed: %s", exc)
                elapsed = time.perf_counter() - start
                self.logger.info("runner.propose duration=%.2fs", elapsed)
                self.next_propose_ts = self._schedule_next(
                    now_ts, self.config.propose_poll_seconds
                )

            if errors:
                self.state.last_error_at = self._now_iso()
                self.state.last_error_summary = "; ".join(errors)[:500]
                if self.backoff_seconds <= 0:
                    self.backoff_seconds = 1
                else:
                    self.backoff_seconds = min(
                        int(self.backoff_seconds * 2), int(self.config.max_backoff_seconds)
                    )
            else:
                self.backoff_seconds = 0

            self._write_state()

            if once or self.stop_requested:
                break

            next_due = min(self.next_market_ts, self.next_news_ts, self.next_propose_ts)
            sleep_seconds = max(0.0, next_due - self._now_ts())
            if self.backoff_seconds > 0:
                sleep_seconds = max(sleep_seconds, float(self.backoff_seconds))
            elapsed = time.perf_counter() - cycle_start
            self.logger.info("runner.cycle duration=%.2fs sleep=%.2fs", elapsed, sleep_seconds)
            if sleep_seconds > 0:
                self.sleep_fn(sleep_seconds)
