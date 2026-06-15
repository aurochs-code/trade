"""P6-1 自适应风控建议。

只读取行情、余额曲线和复盘证据，输出人工可审计建议；不自动修改策略配置。
"""

from __future__ import annotations

import json
from statistics import mean
from typing import Any

from astock_trading.platform.domain_events import RISK_ADAPTIVE_SUGGESTION_PROPOSED
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import utc_now_iso

DEFAULT_MARKET_SYMBOLS = ("000001", "399001", "399006")
HIGH_VOLATILITY_PCT = 0.035
LOW_VOLATILITY_PCT = 0.015
DRAWDOWN_REDUCE_PCT = 0.08
LOSS_STREAK_RAISE_THRESHOLD = 2
LOSS_STREAK_REDUCE_THRESHOLD = 3


def run_adaptive_risk(
    conn: Any,
    *,
    lookback_days: int = 20,
    min_market_bars: int = 10,
    record: bool = False,
    config_version: str = "unknown",
) -> dict:
    """生成自适应风控建议；可选写入风控建议事件。"""
    store = EventStore(conn)
    market = _market_volatility(conn, lookback_days=lookback_days, min_market_bars=min_market_bars)
    equity = _equity_curve(conn, lookback_days=lookback_days)
    loss_streak = _loss_streak(conn, equity)

    suggestions = {
        "stop_loss_adjustment": _stop_loss_adjustment(market),
        "position_limit_adjustment": _position_limit_adjustment(equity, loss_streak),
        "buy_threshold_adjustment": _buy_threshold_adjustment(loss_streak),
    }
    gaps = _evidence_gaps(market, equity, loss_streak, min_market_bars=min_market_bars)
    payload = {
        "analysis": "adaptive_risk",
        "status": "ok" if not gaps else "insufficient_data",
        "generated_at": utc_now_iso(),
        "config_version": config_version,
        "lookback_days": lookback_days,
        "inputs": {
            "market_volatility": market,
            "equity_curve": equity,
            "loss_streak": loss_streak,
        },
        "suggestions": suggestions,
        "evidence_gaps": gaps,
        "guardrails": {
            "auto_apply": False,
            "manual_confirmation_required": True,
            "reason": "P6-1 只输出风控调整建议和证据留痕，不自动修改 strategy.yaml，也不触发真实下单。",
        },
        "recorded_event_id": "",
    }
    payload["report_markdown"] = render_adaptive_risk_report(payload)

    if record:
        event_id = store.append(
            "risk:adaptive",
            "risk",
            RISK_ADAPTIVE_SUGGESTION_PROPOSED,
            payload={key: value for key, value in payload.items() if key != "recorded_event_id"},
            metadata={"source": "adaptive_risk", "config_version": config_version},
        )
        payload["recorded_event_id"] = event_id
        _write_report_artifact(conn, event_id, payload["report_markdown"])

    return payload


def render_adaptive_risk_report(payload: dict) -> str:
    """渲染中文自适应风控建议报告。"""
    status_label = {"ok": "可参考", "insufficient_data": "证据不足"}.get(
        str(payload.get("status") or ""),
        str(payload.get("status") or ""),
    )
    lines = [
        "# P6-1 自适应风控建议",
        "",
        f"- 状态：{status_label}",
        f"- 回看窗口：{payload.get('lookback_days')} 天",
        "- 自动修改配置：否",
        "- 真实下单：否，仍需人工确认",
        "",
        "## 输入证据",
    ]
    market = (payload.get("inputs") or {}).get("market_volatility") or {}
    equity = (payload.get("inputs") or {}).get("equity_curve") or {}
    streak = (payload.get("inputs") or {}).get("loss_streak") or {}
    lines.extend([
        f"- 市场波动：样本 {market.get('sample_count', 0)} 条，平均日内振幅 "
        f"{market.get('average_intraday_range_pct', 0):.2%}",
        f"- 账户回撤：样本 {equity.get('sample_count', 0)} 条，最大回撤 "
        f"{equity.get('max_drawdown_pct', 0):.2%}",
        f"- 连续亏损：{streak.get('consecutive_loss_days', 0)} 天（来源：{streak.get('source', 'none')}）",
        "",
        "## 建议",
    ])
    for key, item in (payload.get("suggestions") or {}).items():
        lines.append(f"- {_suggestion_label(key)}：{item.get('display_action')}。{item.get('reason')}")
    gaps = payload.get("evidence_gaps") or []
    if gaps:
        lines.extend(["", "## 证据缺口"])
        lines.extend(f"- {gap}" for gap in gaps)
    return "\n".join(lines)


def _suggestion_label(key: str) -> str:
    return {
        "stop_loss_adjustment": "止损宽度",
        "position_limit_adjustment": "仓位上限",
        "buy_threshold_adjustment": "买入阈值",
    }.get(key, key)


def _market_volatility(conn: Any, *, lookback_days: int, min_market_bars: int) -> dict:
    rows = _market_bar_rows(conn, DEFAULT_MARKET_SYMBOLS, lookback_days)
    source = "major_indices"
    if len(rows) < min_market_bars:
        rows = _recent_market_bar_rows(conn, lookback_days)
        source = "all_market_bars" if rows else "none"

    ranges = []
    symbols = set()
    latest_date = ""
    for row in rows:
        close_cents = int(row["close_cents"] or 0)
        if close_cents <= 0:
            continue
        symbols.add(str(row["symbol"]))
        latest_date = max(latest_date, str(row["bar_date"] or ""))
        ranges.append((int(row["high_cents"]) - int(row["low_cents"])) / close_cents)

    average_range = round(mean(ranges), 4) if ranges else 0.0
    regime = "normal"
    if not ranges:
        regime = "unknown"
    elif average_range >= HIGH_VOLATILITY_PCT:
        regime = "high"
    elif average_range <= LOW_VOLATILITY_PCT:
        regime = "low"

    return {
        "source": source,
        "sample_count": len(ranges),
        "min_required": min_market_bars,
        "symbols": sorted(symbols),
        "latest_bar_date": latest_date,
        "average_intraday_range_pct": average_range,
        "regime": regime,
        "thresholds": {
            "high_volatility_pct": HIGH_VOLATILITY_PCT,
            "low_volatility_pct": LOW_VOLATILITY_PCT,
        },
    }


def _market_bar_rows(conn: Any, symbols: tuple[str, ...], lookback_days: int) -> list[Any]:
    placeholders = ", ".join("?" for _ in symbols)
    return conn.execute(
        f"""SELECT symbol, bar_date, high_cents, low_cents, close_cents
            FROM market_bars
            WHERE period = 'daily' AND symbol IN ({placeholders})
            ORDER BY bar_date DESC, symbol
            LIMIT ?""",
        (*symbols, max(lookback_days * len(symbols), lookback_days)),
    ).fetchall()


def _recent_market_bar_rows(conn: Any, lookback_days: int) -> list[Any]:
    return conn.execute(
        """SELECT symbol, bar_date, high_cents, low_cents, close_cents
           FROM market_bars
           WHERE period = 'daily'
           ORDER BY bar_date DESC, symbol
           LIMIT ?""",
        (lookback_days,),
    ).fetchall()


def _equity_curve(conn: Any, *, lookback_days: int) -> dict:
    points = _balance_event_points(conn)[-max(lookback_days, 1):]
    if not points:
        latest = _latest_projection_balance(conn)
        if latest:
            points = [{
                "occurred_at": str(latest.get("updated_at") or ""),
                "total_asset_cents": int(latest.get("total_asset_cents") or 0),
                "consecutive_loss_days": int(latest.get("consecutive_loss_days") or 0),
                "source": "projection_balances",
            }]

    assets = [point["total_asset_cents"] for point in points if point["total_asset_cents"] > 0]
    max_drawdown = _max_drawdown(assets)
    latest_point = points[-1] if points else {}
    return {
        "source": latest_point.get("source", "none") if latest_point else "none",
        "sample_count": len(assets),
        "latest_total_asset_cents": int(latest_point.get("total_asset_cents") or 0),
        "max_drawdown_pct": max_drawdown,
        "drawdown_reduce_threshold_pct": DRAWDOWN_REDUCE_PCT,
        "latest_consecutive_loss_days": int(latest_point.get("consecutive_loss_days") or 0),
    }


def _balance_event_points(conn: Any) -> list[dict]:
    rows = conn.execute(
        """SELECT event_id, event_type, payload_json, occurred_at, stream_version
           FROM event_log
           WHERE event_type LIKE 'balance.%'
           ORDER BY occurred_at, stream_version
           LIMIT 500"""
    ).fetchall()
    points = []
    for row in rows:
        payload = _json_dict(row["payload_json"])
        total_asset_cents = _money_cents(payload, "total_asset_cents", "total_asset", "total")
        if total_asset_cents <= 0:
            continue
        points.append({
            "occurred_at": str(row["occurred_at"] or ""),
            "total_asset_cents": total_asset_cents,
            "consecutive_loss_days": int(payload.get("consecutive_loss_days", 0) or 0),
            "source": "balance_events",
        })
    return points


def _latest_projection_balance(conn: Any) -> dict | None:
    row = conn.execute(
        """SELECT cash_cents, total_asset_cents, daily_pnl_cents, consecutive_loss_days, updated_at
           FROM projection_balances
           ORDER BY updated_at DESC
           LIMIT 1"""
    ).fetchone()
    return _row_dict(row) if row else None


def _loss_streak(conn: Any, equity: dict) -> dict:
    balance_loss_days = int(equity.get("latest_consecutive_loss_days") or 0)
    if balance_loss_days > 0:
        return {
            "source": equity.get("source", "balance"),
            "consecutive_loss_days": balance_loss_days,
            "review_loss_streak": _review_loss_streak(conn),
            "raise_threshold_at": LOSS_STREAK_RAISE_THRESHOLD,
            "reduce_position_at": LOSS_STREAK_REDUCE_THRESHOLD,
        }

    review_streak = _review_loss_streak(conn)
    return {
        "source": "trade_reviews" if review_streak > 0 else "none",
        "consecutive_loss_days": review_streak,
        "review_loss_streak": review_streak,
        "raise_threshold_at": LOSS_STREAK_RAISE_THRESHOLD,
        "reduce_position_at": LOSS_STREAK_REDUCE_THRESHOLD,
    }


def _review_loss_streak(conn: Any) -> int:
    rows = conn.execute(
        """SELECT payload_json, occurred_at, stream_version
           FROM event_log
           WHERE event_type = 'trade.review.recorded'
           ORDER BY occurred_at DESC, stream_version DESC
           LIMIT 50"""
    ).fetchall()
    streak = 0
    for row in rows:
        payload = _json_dict(row["payload_json"])
        if _float(payload.get("latest_return_pct")) < 0:
            streak += 1
            continue
        break
    return streak


def _stop_loss_adjustment(market: dict) -> dict:
    sample_count = int(market.get("sample_count") or 0)
    if sample_count < int(market.get("min_required") or 0):
        return {
            "action": "hold",
            "display_action": "暂不调整止损",
            "multiplier": 1.0,
            "reason": "市场 K 线样本不足，不能用局部噪声推导止损宽度。",
        }

    avg_range = float(market.get("average_intraday_range_pct") or 0.0)
    if avg_range >= HIGH_VOLATILITY_PCT:
        return {
            "action": "widen",
            "display_action": "建议适度放宽止损",
            "multiplier": 1.15,
            "reason": f"近期平均日内振幅 {avg_range:.2%}，高于 {HIGH_VOLATILITY_PCT:.2%} 高波动阈值。",
        }
    if avg_range <= LOW_VOLATILITY_PCT:
        return {
            "action": "tighten",
            "display_action": "建议适度收紧止损",
            "multiplier": 0.9,
            "reason": f"近期平均日内振幅 {avg_range:.2%}，低于 {LOW_VOLATILITY_PCT:.2%} 低波动阈值。",
        }
    return {
        "action": "hold",
        "display_action": "维持当前止损",
        "multiplier": 1.0,
        "reason": "近期波动处于常规区间，暂不需要调整止损宽度。",
    }


def _position_limit_adjustment(equity: dict, loss_streak: dict) -> dict:
    drawdown = float(equity.get("max_drawdown_pct") or 0.0)
    loss_days = int(loss_streak.get("consecutive_loss_days") or 0)
    if int(equity.get("sample_count") or 0) < 2 and loss_days == 0:
        return {
            "action": "hold",
            "display_action": "暂不调整仓位上限",
            "multiplier": 1.0,
            "reason": "账户权益曲线或连续亏损证据不足。",
        }
    if drawdown >= DRAWDOWN_REDUCE_PCT or loss_days >= LOSS_STREAK_REDUCE_THRESHOLD:
        reasons = []
        if drawdown >= DRAWDOWN_REDUCE_PCT:
            reasons.append(f"最大回撤 {drawdown:.2%} 已达到 {DRAWDOWN_REDUCE_PCT:.2%} 降仓阈值")
        if loss_days >= LOSS_STREAK_REDUCE_THRESHOLD:
            reasons.append(f"连续亏损 {loss_days} 天")
        return {
            "action": "reduce",
            "display_action": "建议降低仓位上限",
            "multiplier": 0.75,
            "reason": "；".join(reasons) + "，下一轮交易前应先降风险暴露。",
        }
    return {
        "action": "hold",
        "display_action": "维持当前仓位上限",
        "multiplier": 1.0,
        "reason": "回撤和连续亏损状态未触发降仓阈值。",
    }


def _buy_threshold_adjustment(loss_streak: dict) -> dict:
    loss_days = int(loss_streak.get("consecutive_loss_days") or 0)
    if loss_days >= LOSS_STREAK_RAISE_THRESHOLD:
        return {
            "action": "raise",
            "display_action": "建议抬高买入阈值",
            "score_delta": 0.3,
            "reason": f"连续亏损 {loss_days} 天，先提高新开仓质量门槛，避免情绪化补交易次数。",
        }
    if loss_streak.get("source") == "none":
        return {
            "action": "hold",
            "display_action": "暂不调整买入阈值",
            "score_delta": 0.0,
            "reason": "缺少连续盈亏证据，不能判断是否应提高或降低买入门槛。",
        }
    return {
        "action": "hold",
        "display_action": "维持当前买入阈值",
        "score_delta": 0.0,
        "reason": "连续亏损未达到抬高阈值的条件。",
    }


def _evidence_gaps(market: dict, equity: dict, loss_streak: dict, *, min_market_bars: int) -> list[str]:
    gaps = []
    if int(market.get("sample_count") or 0) < min_market_bars:
        gaps.append(
            f"市场 K 线样本不足：需要至少 {min_market_bars} 条，当前 {market.get('sample_count', 0)} 条。"
        )
    if int(equity.get("sample_count") or 0) < 2:
        gaps.append("账户权益曲线样本不足：至少需要 2 个 balance.* 或余额投影点才能估算回撤。")
    if loss_streak.get("source") == "none":
        gaps.append("缺少连续盈亏证据：未找到余额连续亏损天数或近期交易复盘。")
    return gaps


def _write_report_artifact(conn: Any, event_id: str, markdown: str) -> None:
    conn.execute(
        """REPLACE INTO report_artifacts
           (artifact_id, run_id, report_type, format, content, delivered_to, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            f"adaptive_risk_{event_id}",
            event_id,
            "adaptive_risk",
            "markdown",
            markdown,
            "local",
            utc_now_iso(),
        ),
    )


def _max_drawdown(assets: list[int]) -> float:
    if len(assets) < 2:
        return 0.0
    peak = assets[0]
    max_drawdown = 0.0
    for value in assets:
        peak = max(peak, value)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - value) / peak)
    return round(max_drawdown, 4)


def _json_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _row_dict(row: Any) -> dict:
    return {key: row[key] for key in row.keys()}


def _money_cents(payload: dict, cents_key: str, *money_keys: str) -> int:
    if cents_key in payload and payload[cents_key] is not None:
        return int(payload[cents_key])
    for key in money_keys:
        value = payload.get(key)
        if value is not None:
            return int(round(float(value) * 100))
    return 0


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
