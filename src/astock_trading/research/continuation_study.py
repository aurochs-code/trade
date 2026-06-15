from __future__ import annotations

import math
from pathlib import Path

from astock_trading.research.continuation_validation import run_continuation_validation


def run_continuation_study(
    codes: list[str],
    start: str,
    end: str,
    top_ns: tuple[int, ...] = (1, 2, 3),
    hold_days_list: tuple[int, ...] = (1, 2, 3),
    data_dir: str | Path | None = None,
) -> dict:
    normalized_top_ns = _normalize_positive_ints(top_ns)
    normalized_hold_days = _normalize_positive_ints(hold_days_list)

    validation = run_continuation_validation(
        codes=codes,
        start=start,
        end=end,
        top_n=max(normalized_top_ns),
        data_dir=data_dir,
    )
    ranked_rows = validation.get("ranked_returns", [])

    comparison_report: list[dict] = []
    for top_n in normalized_top_ns:
        for hold_days in normalized_hold_days:
            trades = _build_trade_rows(ranked_rows, top_n=top_n, hold_days=hold_days)
            total_return = round(sum(t["return_pct"] for t in trades), 2)
            avg_return = 0.0 if not trades else round(total_return / len(trades), 2)
            win_rate = 0.0 if not trades else round(sum(1 for t in trades if t["return_pct"] > 0) / len(trades) * 100, 2)
            comparison_report.append(
                {
                    "top_n": top_n,
                    "hold_days": hold_days,
                    "trade_count": len(trades),
                    "trading_days": len({t["trade_date"] for t in trades}),
                    "total_return_pct": total_return,
                    "avg_trade_return_pct": avg_return,
                    "win_rate_pct": win_rate,
                }
            )

    comparison_report.sort(
        key=lambda row: (
            -row["total_return_pct"],
            -row["win_rate_pct"],
            -row["avg_trade_return_pct"],
            row["top_n"],
            row["hold_days"],
        )
    )

    return {
        "codes": list(codes),
        "start": start,
        "end": end,
        "top_ns": list(normalized_top_ns),
        "hold_days_list": list(normalized_hold_days),
        "comparison_report": comparison_report,
        "best_setup": comparison_report[0] if comparison_report else None,
        "validation_snapshot": {
            "top_n": validation.get("top_n"),
            "top_n_report": validation.get("top_n_report", []),
            "score_bucket_report": validation.get("score_bucket_report", []),
            "execution_report": validation.get("execution_report", []),
            "candidate_report": validation.get("candidate_report", [])[:5],
        },
    }


def _normalize_positive_ints(values: tuple[int, ...]) -> tuple[int, ...]:
    normalized = tuple(dict.fromkeys(int(value) for value in values if int(value) > 0))
    if not normalized:
        raise ValueError("at least one positive integer is required")
    return normalized


def _build_trade_rows(ranked_rows: list[dict], top_n: int, hold_days: int) -> list[dict]:
    trades: list[dict] = []
    for row in ranked_rows:
        if int(row["rank"]) > top_n:
            continue
        return_pct = _resolve_return_pct(row, hold_days)
        if return_pct is None:
            continue
        trades.append(
            {
                "trade_date": row["trade_date"],
                "code": row["code"],
                "return_pct": round(return_pct * 100, 2),
            }
        )
    return trades


def _resolve_return_pct(row: dict, hold_days: int) -> float | None:
    if hold_days == 1:
        return _first_available(
            _coerce_float(row.get("open_t1_return")),
            _coerce_float(row.get("t1_return")),
        )
    if hold_days == 2:
        return _first_available(
            _coerce_float(row.get("t2_return")),
            _coerce_float(row.get("t1_return")),
        )
    if hold_days >= 3:
        return _first_available(
            _coerce_float(row.get("t3_return")),
            _coerce_float(row.get("t2_return")),
            _coerce_float(row.get("t1_return")),
        )
    return _coerce_float(row.get("t1_return"))


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    return numeric


def _first_available(*values: float | None) -> float | None:
    for value in values:
        if value is not None:
            return value
    return None
