from __future__ import annotations

import json
from pathlib import Path

from astock_trading.research.continuation_validation import run_continuation_validation


def run_continuation_backtest(
    codes,
    start: str,
    end: str,
    hold_days: int = 2,
    top_n: int = 3,
    data_dir: str | Path | None = None,
) -> dict:
    trades = _simulate_ranked_trades(
        codes=codes,
        start=start,
        end=end,
        hold_days=hold_days,
        top_n=top_n,
        data_dir=data_dir,
    )
    total_return = sum(t["return_pct"] for t in trades)
    win_rate = 0.0 if not trades else sum(1 for t in trades if t["return_pct"] > 0) / len(trades) * 100
    return {
        "codes": list(codes),
        "start": start,
        "end": end,
        "hold_days": hold_days,
        "top_n": top_n,
        "total_return_pct": round(total_return, 2),
        "win_rate_pct": round(win_rate, 2),
        "trades": trades,
    }


def _simulate_ranked_trades(
    codes,
    start: str,
    end: str,
    hold_days: int,
    top_n: int,
    data_dir=None,
) -> list[dict]:
    if data_dir is not None:
        trades_path = Path(data_dir) / "continuation_trades.json"
        if trades_path.exists():
            return json.loads(trades_path.read_text(encoding="utf-8"))

    payload = run_continuation_validation(
        codes=list(codes),
        start=start,
        end=end,
        top_n=top_n,
        data_dir=data_dir,
    )
    ranked_rows = payload.get("ranked_returns") if isinstance(payload, dict) else None
    if not ranked_rows:
        return []

    trades: list[dict] = []
    for row in ranked_rows:
        if int(row["rank"]) > top_n:
            continue
        if hold_days == 1:
            return_pct = float(row["open_t1_return"]) * 100
        elif hold_days == 2 and row.get("t2_return") is not None:
            return_pct = float(row["t2_return"]) * 100
        elif hold_days >= 3 and row.get("t3_return") is not None:
            return_pct = float(row["t3_return"]) * 100
        else:
            return_pct = float(row["t1_return"]) * 100
        trades.append(
            {
                "trade_date": row["trade_date"],
                "code": row["code"],
                "hold_days": hold_days,
                "return_pct": round(return_pct, 2),
                "entry_mode": "open",
                "top_n": top_n,
                "rank": int(row["rank"]),
            }
        )
    return trades
