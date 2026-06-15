"""Persist backtest evidence into MySQL runtime tables."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any


_METRIC_KEYS = (
    "initial_cash",
    "final_value",
    "total_return_pct",
    "annual_return_pct",
    "max_drawdown_pct",
    "win_rate_pct",
    "sharpe_ratio",
    "calmar_ratio",
    "total_trades",
    "buy_trades",
    "sell_trades",
    "winning_trades",
    "losing_trades",
    "positions_open",
    "execution_semantics",
    "execution_funnel",
    "signal_alpha",
    "signal_validation",
)


def save_backtest_result(conn: Any, result: dict[str, Any], *, request: dict[str, Any]) -> dict[str, Any]:
    """Write one backtest run plus its full trade log and equity curve."""
    run_id = str(request.get("run_id") or f"bt_{uuid.uuid4().hex[:16]}")
    created_at = datetime.now(timezone.utc).isoformat()
    codes = _normalize_codes(request.get("codes"))
    start = str(request.get("start") or request.get("start_date") or "")
    end = str(request.get("end") or request.get("end_date") or "")
    trades = list(result.get("trade_log") or result.get("trades") or [])
    equity_curve = list(result.get("equity_curve") or [])
    metrics = {key: result.get(key) for key in _METRIC_KEYS if key in result}

    conn.execute(
        """INSERT INTO backtest_runs
           (run_id, preset, codes_json, start_date, end_date, initial_cash,
            final_value, metrics_json, request_json, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            run_id,
            str(result.get("preset") or request.get("preset") or ""),
            _json(codes),
            start,
            end,
            float(result.get("initial_cash") or request.get("initial_cash") or 0.0),
            _optional_float(result.get("final_value")),
            _json(metrics),
            _json(request),
            created_at,
        ),
    )
    if trades:
        conn.executemany(
            """INSERT INTO backtest_trades
               (run_id, trade_index, trade_date, code, name, side, price, shares,
                pnl, return_pct, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    run_id,
                    index,
                    str(trade.get("date") or trade.get("trade_date") or ""),
                    str(trade.get("code") or ""),
                    str(trade.get("name") or ""),
                    str(trade.get("side") or ""),
                    _optional_float(trade.get("price")),
                    _optional_int(trade.get("shares")),
                    _optional_float(trade.get("pnl")),
                    _optional_float(trade.get("return_pct")),
                    _json(trade),
                )
                for index, trade in enumerate(trades)
            ],
        )
    if equity_curve:
        conn.executemany(
            """INSERT INTO backtest_equity_curve
               (run_id, curve_index, trade_date, equity, cash, positions, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    run_id,
                    index,
                    str(point.get("date") or point.get("trade_date") or ""),
                    float(point.get("equity") or 0.0),
                    _optional_float(point.get("cash")),
                    _optional_int(point.get("positions")),
                    _json(point),
                )
                for index, point in enumerate(equity_curve)
            ],
        )

    return {
        "status": "recorded",
        "run_id": run_id,
        "trade_count": len(trades),
        "equity_curve_points": len(equity_curve),
    }


def _normalize_codes(value: Any) -> list[str]:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
