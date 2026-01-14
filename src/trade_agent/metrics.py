from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean, stdev
from typing import Iterable

import sqlite3

from trade_agent import db


@dataclass
class Metrics:
    total_pnl: float
    total_return: float
    cagr: float
    sharpe: float
    max_drawdown: float
    win_rate: float
    profit_factor: float
    turnover: float
    fees: float
    num_trades: int


def _max_drawdown(equity: list[float]) -> float:
    peak = equity[0] if equity else 0.0
    max_dd = 0.0
    for value in equity:
        peak = max(peak, value)
        drawdown = peak - value
        max_dd = max(max_dd, drawdown)
    return max_dd


def _parse_ts(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).timestamp()
    except Exception:  # noqa: BLE001
        return None


def compute_metrics(
    trades: Iterable[dict],
    capital_jpy: float | None = None,
    start_at: str | None = None,
    end_at: str | None = None,
) -> tuple[Metrics, list[float]]:
    pnl_list = [float(t.get("pnl_jpy", 0.0)) for t in trades]
    equity = []
    running = 0.0
    for pnl in pnl_list:
        running += pnl
        equity.append(running)

    wins = sum(1 for pnl in pnl_list if pnl > 0)
    num_trades = len(pnl_list)
    win_rate = wins / num_trades if num_trades else 0.0
    turnover = sum(float(t.get("notional_jpy", 0.0)) for t in trades)
    fees = sum(float(t.get("fee_jpy", 0.0)) for t in trades)

    total_pnl = sum(pnl_list)
    total_return = total_pnl / capital_jpy if capital_jpy and capital_jpy > 0 else 0.0

    gross_profit = sum(pnl for pnl in pnl_list if pnl > 0)
    gross_loss = abs(sum(pnl for pnl in pnl_list if pnl < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0

    timestamps = []
    for trade in trades:
        ts = _parse_ts(trade.get("created_at"))
        if ts is not None:
            timestamps.append(ts)
    if start_at:
        ts = _parse_ts(start_at)
        if ts is not None:
            timestamps.append(ts)
    if end_at:
        ts = _parse_ts(end_at)
        if ts is not None:
            timestamps.append(ts)

    cagr = 0.0
    if timestamps and capital_jpy and capital_jpy > 0:
        start_ts = min(timestamps)
        end_ts = max(timestamps)
        years = max((end_ts - start_ts) / (365.25 * 24 * 3600), 0.0)
        if years > 0 and total_return > -1.0:
            cagr = (1 + total_return) ** (1 / years) - 1

    sharpe = 0.0
    if capital_jpy and capital_jpy > 0 and len(pnl_list) >= 2:
        returns = [pnl / capital_jpy for pnl in pnl_list]
        if stdev(returns) > 0:
            sharpe = mean(returns) / stdev(returns) * (len(returns) ** 0.5)

    metrics = Metrics(
        total_pnl=total_pnl,
        total_return=total_return,
        cagr=cagr,
        sharpe=sharpe,
        max_drawdown=_max_drawdown(equity),
        win_rate=win_rate,
        profit_factor=profit_factor,
        turnover=turnover,
        fees=fees,
        num_trades=num_trades,
    )
    return metrics, equity


def load_trades_from_db(conn: sqlite3.Connection, mode: str | None = None) -> list[dict]:
    query = "SELECT pnl_jpy, meta_json, created_at FROM trade_results"
    params = []
    if mode:
        query += " WHERE mode = ?"
        params.append(mode)
    query += " ORDER BY created_at ASC"
    cur = conn.execute(query, params)
    trades: list[dict] = []
    for row in cur.fetchall():
        meta = json.loads(row["meta_json"]) if row["meta_json"] else {}
        trades.append(
            {
                "pnl_jpy": float(row["pnl_jpy"]),
                "notional_jpy": float(meta.get("notional", 0.0)),
                "fee_jpy": float(meta.get("fee", 0.0)),
                "created_at": row["created_at"],
            }
        )
    return trades


def load_trade_details_from_db(conn: sqlite3.Connection, mode: str | None = None) -> list[dict]:
    query = (
        "SELECT tr.intent_id, tr.pnl_jpy, tr.created_at, tr.mode, tr.meta_json, "
        "oi.symbol, oi.side, oi.size as intent_size, oi.price as intent_price "
        "FROM trade_results tr JOIN order_intents oi ON tr.intent_id = oi.intent_id"
    )
    params = []
    if mode:
        query += " WHERE tr.mode = ?"
        params.append(mode)
    query += " ORDER BY tr.created_at ASC"
    cur = conn.execute(query, params)
    rows: list[dict] = []
    for row in cur.fetchall():
        meta = json.loads(row["meta_json"]) if row["meta_json"] else {}
        rows.append(
            {
                "intent_id": row["intent_id"],
                "created_at": row["created_at"],
                "mode": row["mode"],
                "symbol": row["symbol"],
                "side": row["side"],
                "size": float(meta.get("size") or row["intent_size"] or 0.0),
                "price": float(meta.get("fill_price") or row["intent_price"] or 0.0),
                "fee_jpy": float(meta.get("fee", 0.0)),
                "pnl_jpy": float(row["pnl_jpy"]),
            }
        )
    return rows


def save_trade_csv(trades: Iterable[dict], output_dir: str, prefix: str) -> str:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    csv_path = Path(output_dir) / f"{prefix}_trades.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["created_at", "intent_id", "mode", "symbol", "side", "size", "price", "fee_jpy", "pnl_jpy"]
        )
        for trade in trades:
            writer.writerow(
                [
                    trade.get("created_at"),
                    trade.get("intent_id"),
                    trade.get("mode"),
                    trade.get("symbol"),
                    trade.get("side"),
                    trade.get("size"),
                    trade.get("price"),
                    trade.get("fee_jpy"),
                    trade.get("pnl_jpy"),
                ]
            )
    return str(csv_path)


def format_summary(metrics: Metrics) -> str:
    return (
        f"Total PnL: {metrics.total_pnl:.2f} JPY\n"
        f"Total Return: {metrics.total_return:.2%}\n"
        f"CAGR: {metrics.cagr:.2%}\n"
        f"Sharpe (trade-based): {metrics.sharpe:.2f}\n"
        f"Max Drawdown: {metrics.max_drawdown:.2f}\n"
        f"Win Rate: {metrics.win_rate:.2%}\n"
        f"Profit Factor: {metrics.profit_factor:.2f}\n"
        f"Turnover: {metrics.turnover:.2f}\n"
        f"Fees: {metrics.fees:.2f}\n"
        f"Trades: {metrics.num_trades}\n"
    )


def save_report(metrics: Metrics, equity: list[float], output_dir: str, prefix: str) -> dict[str, str]:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    json_path = Path(output_dir) / f"{prefix}_report.json"
    csv_path = Path(output_dir) / f"{prefix}_equity.csv"
    summary_path = Path(output_dir) / f"{prefix}_summary.txt"

    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(metrics.__dict__, handle, indent=2)

    with open(csv_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["step", "equity"])
        for idx, value in enumerate(equity, start=1):
            writer.writerow([idx, value])

    summary = format_summary(metrics)
    with open(summary_path, "w", encoding="utf-8") as handle:
        handle.write(summary)

    return {"json": str(json_path), "csv": str(csv_path), "summary": str(summary_path)}
