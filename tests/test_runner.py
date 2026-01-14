from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from trade_agent.config import load_config
from trade_agent.intent import TradePlan
from trade_agent.runner import Runner
from trade_agent.services.ingest import IngestParams
from trade_agent.services.propose import ProposalCandidate, ProposeParams


class FakeClock:
    def __init__(self) -> None:
        self.current = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.current

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.current = self.current + timedelta(seconds=seconds)


class FakeStore:
    pass


def make_settings(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
runner:
  market_poll_seconds: 1
  news_poll_seconds: 1
  propose_poll_seconds: 1
  propose_cooldown_seconds: 300
  jitter_seconds: 0
  max_backoff_seconds: 10
""",
        encoding="utf-8",
    )
    return load_config(str(config_path))


def test_runner_once_runs_one_cycle(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    clock = FakeClock()
    calls: dict[str, int] = {"ingest": 0, "prepare": 0, "finalize": 0}

    def ingest_fn(_settings, _store, _params: IngestParams):
        calls["ingest"] += 1
        return {"errors": []}

    def prepare_fn(_settings, _store, _params: ProposeParams):
        calls["prepare"] += 1
        plan = TradePlan(
            symbol="BTC/JPY",
            side="buy",
            size=0.1,
            price=100,
            confidence=0.7,
            rationale="test",
            strategy="baseline",
        )
        return ProposalCandidate(status="proposed", plan=plan, features_ref="x")

    def finalize_fn(_settings, _store, _candidate, _params):
        calls["finalize"] += 1
        return {"intent_id": "intent-1", "side": "buy", "size": 0.1, "price": 100}

    runner = Runner(
        settings,
        FakeStore(),
        ingest_fn=ingest_fn,
        prepare_proposal_fn=prepare_fn,
        finalize_proposal_fn=finalize_fn,
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        state_path=tmp_path / "runner_state.json",
        propose_params=ProposeParams(),
    )
    runner.run(once=True)
    assert calls["prepare"] == 1
    assert calls["finalize"] == 1


def test_runner_max_cycles(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    settings.runner.propose_cooldown_seconds = 0
    clock = FakeClock()
    calls = {"prepare": 0, "finalize": 0}

    def ingest_fn(_settings, _store, _params: IngestParams):
        return {"errors": []}

    def prepare_fn(_settings, _store, _params: ProposeParams):
        calls["prepare"] += 1
        plan = TradePlan(
            symbol="BTC/JPY",
            side="buy",
            size=0.1,
            price=100,
            confidence=0.7,
            rationale="test",
            strategy="baseline",
        )
        return ProposalCandidate(status="proposed", plan=plan, features_ref="x")

    def finalize_fn(_settings, _store, _candidate, _params):
        calls["finalize"] += 1
        return {"intent_id": f"intent-{calls['finalize']}", "side": "buy", "size": 0.1, "price": 100}

    runner = Runner(
        settings,
        FakeStore(),
        ingest_fn=ingest_fn,
        prepare_proposal_fn=prepare_fn,
        finalize_proposal_fn=finalize_fn,
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        state_path=tmp_path / "runner_state.json",
        propose_params=ProposeParams(),
    )
    runner.run(max_cycles=2)
    assert calls["prepare"] == 2
    assert calls["finalize"] == 2


def test_runner_ingest_failure_skips_propose_and_backoff(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    clock = FakeClock()
    calls = {"prepare": 0}

    def ingest_fn(_settings, _store, _params: IngestParams):
        raise RuntimeError("boom")

    def prepare_fn(_settings, _store, _params: ProposeParams):
        calls["prepare"] += 1
        return ProposalCandidate(status="rejected", plan=None, features_ref=None, reason="skip")

    runner = Runner(
        settings,
        FakeStore(),
        ingest_fn=ingest_fn,
        prepare_proposal_fn=prepare_fn,
        finalize_proposal_fn=lambda *_args, **_kwargs: {},
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        state_path=tmp_path / "runner_state.json",
        propose_params=ProposeParams(),
    )
    runner.run(max_cycles=1)
    assert calls["prepare"] == 0
    assert clock.sleeps and clock.sleeps[0] >= 1


def test_runner_dedup_skips_identical(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    clock = FakeClock()
    calls = {"finalize": 0}

    def ingest_fn(_settings, _store, _params: IngestParams):
        return {"errors": []}

    def prepare_fn(_settings, _store, _params: ProposeParams):
        plan = TradePlan(
            symbol="BTC/JPY",
            side="buy",
            size=0.1,
            price=100,
            confidence=0.7,
            rationale="test",
            strategy="baseline",
        )
        return ProposalCandidate(status="proposed", plan=plan, features_ref="x")

    def finalize_fn(_settings, _store, _candidate, _params):
        calls["finalize"] += 1
        return {"intent_id": f"intent-{calls['finalize']}", "side": "buy", "size": 0.1, "price": 100}

    runner = Runner(
        settings,
        FakeStore(),
        ingest_fn=ingest_fn,
        prepare_proposal_fn=prepare_fn,
        finalize_proposal_fn=finalize_fn,
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        state_path=tmp_path / "runner_state.json",
        propose_params=ProposeParams(),
    )
    runner.run(max_cycles=2)
    assert calls["finalize"] == 1
