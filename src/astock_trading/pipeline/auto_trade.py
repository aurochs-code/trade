"""
pipeline/auto_trade.py — 模拟盘自动交易

基于公共选股池的评分结果，自动在妙想模拟盘执行买卖。
持仓/资金以 MX API 为 source of truth，不污染实盘数据。

流程：
1. 查模拟盘持仓 + 资金（MX API）
2. 大盘择时信号
3. 风控检查（对模拟盘持仓）→ 自动卖出
4. 读公共池评分 → 决策 → 自动买入
5. 事件记录（account=paper）+ Discord 推送
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import date, datetime, time, timedelta, timezone

from astock_trading.pipeline.context import PipelineContext
from astock_trading.pipeline.paper_account import PaperAccount, PaperPosition, PaperBalance
from astock_trading.pipeline.strategy_profiles import latest_strategy_profile_activation_request
from astock_trading.platform.candidate_evidence import enrich_candidate_rows_with_latest_scores
from astock_trading.platform.domain_events import (
    AUTO_TRADE_DIAGNOSTIC,
    AUTO_TRADE_EXECUTED,
    AUTO_TRADE_SUMMARY,
    DECISION_SUGGESTED,
)
from astock_trading.platform.paths import resolve_config_dir
from astock_trading.platform.pipeline_policy import new_trade_guard_decision
from astock_trading.platform.time import MARKET_TZ, is_market_weekday, iso_to_local, local_date_bounds_utc, local_now
from astock_trading.platform.time import local_now_str, local_today, local_today_str
from astock_trading.strategy.models import MarketSignal, Style
from astock_trading.risk.rules import check_exit_signals, get_risk_params

_logger = logging.getLogger(__name__)


def _get_highest_since_entry(code: str, entry_date: date, current_price: float, conn=None) -> float:
    """
    从本地 market_bars 获取持仓期内历史最高价。

    用于移动止盈标杆。若本地 K 线不足则 fallback 到 current_price
    （即"标杆=现价，移动止盈不生效"行为），避免自动交易路径直接依赖外部数据源。
    """
    if conn is not None:
        highest = _get_highest_since_entry_from_market_bars(conn, code, entry_date)
        if highest is not None:
            return max(float(highest), float(current_price))
    return current_price


def _get_highest_since_entry_from_market_bars(conn, code: str, entry_date: date) -> float | None:
    symbols = _market_bar_symbols(code)
    placeholders = ",".join("?" for _ in symbols)
    try:
        row = conn.execute(
            f"""SELECT MAX(high_cents) AS high_cents
                FROM market_bars
                WHERE symbol IN ({placeholders})
                  AND period = ?
                  AND bar_date >= ?""",
            (*symbols, "daily", entry_date.isoformat()),
        ).fetchone()
    except Exception as exc:
        _logger.debug("[auto_trade] 本地 K 线最高价读取失败 %s: %s", code, exc)
        return None
    if not row or not row["high_cents"]:
        return None
    return int(row["high_cents"]) / 100


def _market_bar_symbols(code: str) -> tuple[str, ...]:
    normalized = str(code or "").strip()
    if not normalized:
        return ("",)
    symbols = {normalized}
    if len(normalized) == 6 and normalized.isdigit():
        suffix = "SH" if normalized.startswith(("6", "9")) else "SZ"
        symbols.add(f"{normalized}.{suffix}")
        symbols.add(f"{suffix.lower()}{normalized}")
    return tuple(sorted(symbols))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_auto_trade_cfg(ctx: PipelineContext) -> dict:
    """读取 auto_trade 配置段。"""
    return ctx.cfg.get("auto_trade", {})


def _parse_iso(value: str) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _buy_guard_max_age_hours(ctx: PipelineContext, cfg: dict) -> int:
    guard_cfg = cfg.get("buy_guard", {})
    scoring_cfg = ctx.cfg.get("scoring", {})
    for value in (
        guard_cfg.get("max_age_hours"),
        cfg.get("candidate_pool_max_age_hours"),
        scoring_cfg.get("max_age_hours"),
        scoring_cfg.get("freshness_max_age_hours"),
    ):
        if value:
            return int(value)
    return 24


def _execution_profile_state(ctx: PipelineContext) -> dict:
    current_profile = os.getenv("ASTOCK_CONFIG_PROFILE", "default") or "default"
    available_profiles = _available_strategy_profile_names()
    profiles_available = {"trend_swing", "short_continuation", "defensive_watch"}.issubset(
        set(available_profiles)
    )
    mixed_default_config = bool(ctx.cfg.get("continuation") and ctx.cfg.get("backtest_presets"))
    if current_profile == "default" and mixed_default_config and profiles_available:
        recommended_profile = "trend_swing"
        latest_request = latest_strategy_profile_activation_request(
            ctx.event_store,
            target_profile=recommended_profile,
        )
        return {
            "current_profile": current_profile,
            "status": "review_required",
            "safe_to_auto_apply": False,
            "recommended_profile": recommended_profile,
            "available_profiles": available_profiles,
            "activation_request_status": "recorded" if latest_request else "missing",
            "latest_activation_request": latest_request,
            "message": "当前仍在 default 混合配置；自动模拟前需要人工确认执行 profile。",
        }
    return {
        "current_profile": current_profile,
        "status": "ok",
        "safe_to_auto_apply": True,
        "recommended_profile": current_profile,
        "available_profiles": available_profiles,
        "message": "执行 profile 已明确。",
    }


def _available_strategy_profile_names() -> list[str]:
    profile_dir = resolve_config_dir() / "profiles"
    if not profile_dir.exists():
        return []
    return sorted(path.stem for path in profile_dir.glob("*.yaml"))


def _candidate_pool_state(ctx: PipelineContext, now: datetime, max_age_hours: int) -> dict:
    row = ctx.conn.execute(
        """SELECT
               COUNT(*) AS total_count,
               SUM(CASE WHEN pool_tier = 'core' THEN 1 ELSE 0 END) AS core_count,
               SUM(CASE WHEN pool_tier = 'watch' THEN 1 ELSE 0 END) AS watch_count,
               SUM(CASE WHEN pool_tier = 'radar' THEN 1 ELSE 0 END) AS radar_count,
               MAX(COALESCE(NULLIF(last_scored_at, ''), added_at)) AS latest_scored_at
           FROM projection_candidate_pool"""
    ).fetchone()
    total_count = int(row["total_count"] or 0)
    core_count = int(row["core_count"] or 0)
    watch_count = int(row["watch_count"] or 0)
    radar_count = int(row["radar_count"] or 0)
    latest_scored_at = row["latest_scored_at"]
    latest = _parse_iso(latest_scored_at)
    age_hours = (now - latest).total_seconds() / 3600 if latest else None
    fresh = age_hours is not None and age_hours <= max_age_hours
    return {
        "total_count": total_count,
        "core_count": core_count,
        "watch_count": watch_count,
        "radar_count": radar_count,
        "latest_scored_at": latest_scored_at,
        "age_hours": round(age_hours, 2) if age_hours is not None else None,
        "max_age_hours": max_age_hours,
        "fresh": fresh,
        "freshness_status": "fresh" if fresh else "stale",
        "refresh_required_before_next_window": False,
    }


def _annotate_candidate_pool_freshness_for_window(pool: dict, window_state: dict) -> dict:
    if pool.get("fresh"):
        pool["freshness_status"] = "fresh"
        pool["refresh_required_before_next_window"] = False
        return pool
    if not window_state.get("trading_day", True):
        pool["freshness_status"] = "refresh_required_before_next_window"
        pool["refresh_required_before_next_window"] = True
        return pool
    pool["freshness_status"] = "stale"
    pool["refresh_required_before_next_window"] = False
    return pool


def _candidate_pool_freshness_blocker(pool: dict) -> dict | None:
    if pool.get("fresh"):
        return None
    if pool.get("refresh_required_before_next_window"):
        return {
            "reason": "candidate_refresh_required_before_next_window",
            "label": "下个买入窗口前需要重新刷新候选评分",
        }
    return {"reason": "scoring_inputs_stale", "label": "候选池评分已过期"}


def _current_core_entry_signals(ctx: PipelineContext, *, limit: int = 5) -> list[dict]:
    """读取当前核心候选里已经触发入场信号的只读复核证据。"""
    try:
        rows = ctx.conn.execute(
            """SELECT code, pool_tier, name, score, added_at, last_scored_at, note
               FROM projection_candidate_pool
               WHERE pool_tier = 'core'
               ORDER BY score DESC, COALESCE(NULLIF(last_scored_at, ''), added_at) DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
    except Exception as exc:
        _logger.debug("[auto_trade] 当前核心入场信号读取失败: %s", exc)
        return []

    candidates = [dict(row) for row in rows]
    enrich_candidate_rows_with_latest_scores(ctx.conn, candidates)

    result: list[dict] = []
    for item in candidates:
        if not item.get("entry_signal"):
            continue
        code = str(item.get("code") or "")
        tier = str(item.get("pool_tier") or "")
        result.append({
            "code": code,
            "name": item.get("name") or code,
            "pool_tier": tier,
            "pool_tier_label": _pool_tier_label(tier),
            "score": float(item.get("score") or 0),
            "entry_signal": True,
            "primary_strategy_route": item.get("primary_strategy_route"),
            "primary_strategy_route_label": item.get("primary_strategy_route_label"),
            "technical_detail": item.get("technical_detail") or "",
            "data_quality": item.get("data_quality") or "",
            "review_command": f"atrade stock analyze {code} --json",
        })
    return result


def _pool_tier_label(tier: str) -> str:
    return {"core": "核心", "watch": "观察", "radar": "强势观察"}.get(tier, tier or "未分层")


def _entry_signal_gap(current_entry_signals: list[dict], buy_signal: dict) -> dict | None:
    if int(buy_signal.get("count") or 0) > 0 or not current_entry_signals:
        return None
    first = current_entry_signals[0]
    command = first.get("review_command") or f"atrade stock analyze {first.get('code', '')} --json"
    return {
        "status": "entry_signal_without_fresh_buy_intent",
        "summary": (
            "当前核心候选已有入场信号，但没有同日新鲜买入意向；"
            "先复核单票，再等待下一次评分决策链路生成同日买入意向。"
        ),
        "next_action": {
            "type": "review_current_entry_signal",
            "label": "复核当前核心入场信号",
            "command": command,
            "safe_to_auto_apply": True,
            "writes_state": False,
            "writes_environment": False,
            "writes_order": False,
            "requires_user_approval": False,
            "risk_level": "read_only",
            "command_contract_id": "stock_analyze",
        },
        "guardrails": {
            "entry_signal_is_buy_intent": False,
            "places_order": False,
            "requires_same_day_buy_decision": True,
        },
    }


def _query_recent_events(ctx: PipelineContext, event_type: str, *, since: str, limit: int = 200) -> list[dict]:
    """按新到旧读取事件；避免 EventStore 默认升序 limit 截掉最新信号。"""
    if hasattr(ctx.event_store, "_conn"):
        try:
            from astock_trading.platform.events import EventStore

            rows = ctx.conn.execute(
                """SELECT * FROM event_log
                   WHERE event_type = ? AND occurred_at >= ?
                   ORDER BY occurred_at DESC, stream_version DESC
                   LIMIT ?""",
                (event_type, since, limit),
            ).fetchall()
            return [EventStore._row_to_dict(row) for row in rows]
        except Exception as exc:
            _logger.debug("[auto_trade] 事件倒序查询失败，回退 EventStore.query: %s", exc)
    events = ctx.event_store.query(
        event_type=event_type,
        since=since,
        limit=limit,
    )
    return sorted(events, key=lambda event: event.get("occurred_at", ""), reverse=True)


def _fresh_decision_events(ctx: PipelineContext, now: datetime, max_age_hours: int) -> list[dict]:
    since = (now - timedelta(hours=max_age_hours)).isoformat()
    events = _query_recent_events(ctx, DECISION_SUGGESTED, since=since, limit=500)
    fresh = []
    for event in events:
        occurred = _parse_iso(event.get("occurred_at", ""))
        if occurred and now - timedelta(hours=max_age_hours) <= occurred <= now:
            fresh.append(event)
    return fresh


def _usable_buy_decision_events(
    ctx: PipelineContext,
    cfg: dict,
    now: datetime,
    max_age_hours: int,
) -> list[dict]:
    return [
        event
        for event in _fresh_decision_events(ctx, now, max_age_hours)
        if (event.get("payload") or {}).get("action") == "BUY"
        and _buy_signal_matches_current_window(event, cfg, now)
    ]


def _buy_signal_matches_current_window(event: dict, cfg: dict, now: datetime) -> bool:
    return _buy_signal_unusable_reason(event, cfg, now) is None


def _buy_signal_unusable_reason(event: dict, cfg: dict, now: datetime) -> tuple[str, str] | None:
    occurred = _parse_iso(event.get("occurred_at", ""))
    if occurred is None:
        return ("invalid_time", "买入意向时间不可解析")
    current_local = now.astimezone(MARKET_TZ) if now.tzinfo else now.replace(tzinfo=MARKET_TZ)
    occurred_local = occurred.astimezone(MARKET_TZ) if occurred.tzinfo else occurred.replace(tzinfo=MARKET_TZ)
    if occurred_local.date() != current_local.date():
        return ("not_same_day", "买入意向不是当前交易日产生")
    if not is_market_weekday(current_local) or not is_market_weekday(occurred_local):
        return ("non_trading_day", "买入意向发生日或当前检查日不是交易日")

    buy_window = cfg.get("buy_window") or {}
    end = _parse_hhmm(buy_window.get("end", ""))
    if end is None:
        return None
    occurred_time = occurred_local.replace(second=0, microsecond=0).time()
    if occurred_time > end:
        return ("after_buy_window_end", "买入意向产生时间晚于模拟买入窗口")
    return None


def _record_buy_diagnostic(
    ctx: PipelineContext,
    run_id: str,
    reason: str,
    message: str,
    details: dict,
) -> dict:
    diagnostic = {
        "reason": reason,
        "message": message,
        "details": details,
        "checked_at": _now_iso(),
    }
    ctx.event_store.append(
        stream="paper:diagnostic",
        stream_type="paper_trade",
        event_type=AUTO_TRADE_DIAGNOSTIC,
        payload=diagnostic,
        metadata={"run_id": run_id, "account": "paper"},
    )
    return diagnostic


def _buy_side_diagnostics(
    ctx: PipelineContext,
    run_id: str,
    cfg: dict,
    now: datetime | None = None,
) -> list[dict]:
    current = now or local_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=MARKET_TZ)
    current = current.astimezone(timezone.utc)
    max_age_hours = _buy_guard_max_age_hours(ctx, cfg)
    guard = _new_trade_guard(ctx)
    if not guard["allow_new_trades"]:
        return [
            _record_buy_diagnostic(
                ctx,
                run_id,
                "new_trade_guard_blocked",
                "单日异常保护触发，禁止新增买入",
                guard,
            )
        ]

    execution_profile = _execution_profile_state(ctx)
    if not execution_profile.get("safe_to_auto_apply", False):
        return [
            _record_buy_diagnostic(
                ctx,
                run_id,
                "profile_review_required",
                "执行 profile 未完成人工确认，禁止自动买入",
                execution_profile,
            )
        ]

    pool = _candidate_pool_state(ctx, current, max_age_hours)
    diagnostics: list[dict] = []

    if pool["core_count"] <= 0:
        diagnostics.append(
            _record_buy_diagnostic(
                ctx,
                run_id,
                "core_pool_empty",
                "核心候选池为空，禁止自动买入",
                pool,
            )
        )
        return diagnostics

    if not pool["fresh"]:
        diagnostics.append(
            _record_buy_diagnostic(
                ctx,
                run_id,
                "scoring_inputs_stale",
                "候选池评分已过期，禁止自动买入",
                pool,
            )
        )
        return diagnostics

    decisions = _usable_buy_decision_events(ctx, cfg, current, max_age_hours)
    if not decisions:
        diagnostics.append(
            _record_buy_diagnostic(
                ctx,
                run_id,
                "no_fresh_decision_events",
                "未发现当前买入窗口可用的新鲜买入意向，禁止自动买入",
                {"max_age_hours": max_age_hours},
            )
        )

    return diagnostics


def _new_trade_guard(ctx: PipelineContext) -> dict:
    risk_day_start_utc, _ = local_date_bounds_utc()
    now = local_now()
    now_utc = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
    recovery_cutoff_utc = (now_utc - timedelta(days=1)).isoformat()
    failed_runs = (
        ctx.run_journal.get_failed_runs(days=1)
        if hasattr(ctx, "run_journal") and ctx.run_journal is not None
        else []
    )
    successful_runs = []
    if hasattr(ctx, "run_journal") and ctx.run_journal is not None and hasattr(ctx.run_journal, "list_runs"):
        try:
            successful_runs = ctx.run_journal.list_runs(status="completed", limit=100)
        except Exception:
            successful_runs = []
    if recovery_cutoff_utc:
        successful_runs = [
            run for run in successful_runs
            if str(run.get("started_at") or "") >= recovery_cutoff_utc
        ]
    portfolio_breaches = ctx.event_store.query(
        event_type="risk.portfolio_breach",
        since=risk_day_start_utc,
        limit=20,
    )
    return new_trade_guard_decision(
        failed_runs=failed_runs,
        successful_runs=successful_runs,
        portfolio_breaches=portfolio_breaches,
    )


def _calc_buy_shares(price: float, cash: float, position_pct: float, total_asset: float) -> int:
    """
    计算买入股数（100 的整数倍）。

    position_pct: 目标仓位占比（如 0.10）
    """
    if price <= 0 or total_asset <= 0:
        return 0
    target_amount = total_asset * position_pct
    max_by_cash = cash * 0.95  # 留 5% 余量
    amount = min(target_amount, max_by_cash)
    shares = int(amount / price / 100) * 100
    return max(shares, 0)


def _parse_hhmm(value: str) -> time | None:
    try:
        hour, minute = value.split(":", 1)
        return time(int(hour), int(minute))
    except (AttributeError, TypeError, ValueError):
        return None


def _is_time_in_window(now: datetime, window_cfg: dict | None) -> bool:
    """Return True when no valid window is configured, or now is inside it."""
    if not window_cfg:
        return True
    start = _parse_hhmm(window_cfg.get("start", ""))
    end = _parse_hhmm(window_cfg.get("end", ""))
    if start is None or end is None:
        return True

    current_dt = now.astimezone(MARKET_TZ) if now.tzinfo else now
    current = current_dt.replace(second=0, microsecond=0).time()
    if start <= end:
        return start <= current <= end
    return current >= start or current <= end


def _trade_window_state(cfg: dict, now: datetime | None = None) -> dict:
    current = now or local_now()
    current_local = current.astimezone(MARKET_TZ) if current.tzinfo else current.replace(tzinfo=MARKET_TZ)
    trading_day = is_market_weekday(current_local)
    return {
        "buy_open": trading_day and _is_time_in_window(current, cfg.get("buy_window")),
        "sell_open": trading_day and _is_time_in_window(current, cfg.get("sell_window")),
        "trading_day": trading_day,
        "checked_at": current.isoformat(),
    }


def _fresh_buy_signal_summary(
    ctx: PipelineContext,
    cfg: dict,
    *,
    now: datetime | None = None,
) -> dict:
    """汇总新鲜买入意向，用于解释错过买入窗口的情况。"""
    current = now or local_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=MARKET_TZ)
    current_utc = current.astimezone(timezone.utc)
    max_age_hours = _buy_guard_max_age_hours(ctx, cfg)
    buy_events = []
    for event in _usable_buy_decision_events(ctx, cfg, current_utc, max_age_hours):
        buy_events.append(_buy_signal_event_summary(event, ctx=ctx))
    buy_events.sort(key=lambda item: (item.get("score") or 0, item.get("occurred_at") or ""), reverse=True)
    return {
        "count": len(buy_events),
        "max_age_hours": max_age_hours,
        "top": buy_events[0] if buy_events else {},
    }


def _recent_unusable_buy_signal_summary(
    ctx: PipelineContext,
    cfg: dict,
    *,
    now: datetime | None = None,
) -> dict:
    """解释近期 BUY 为什么不能被当前模拟买入窗口承接。"""
    current = now or local_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=MARKET_TZ)
    current_utc = current.astimezone(timezone.utc)
    max_age_hours = _buy_guard_max_age_hours(ctx, cfg)
    buy_events = []
    for event in _fresh_decision_events(ctx, current_utc, max_age_hours):
        payload = event.get("payload", {}) or {}
        if payload.get("action") != "BUY":
            continue
        reason = _buy_signal_unusable_reason(event, cfg, current_utc)
        if reason is None:
            continue
        reason_code, reason_label = reason
        item = _buy_signal_event_summary(event, ctx=ctx)
        item["unusable_reason"] = reason_code
        item["unusable_reason_label"] = reason_label
        item["carries_to_current_window"] = False
        buy_events.append(item)
    buy_events.sort(key=lambda item: (item.get("score") or 0, item.get("occurred_at") or ""), reverse=True)
    return {
        "count": len(buy_events),
        "max_age_hours": max_age_hours,
        "top": buy_events[0] if buy_events else {},
    }


def _buy_signal_event_summary(event: dict, *, ctx: PipelineContext | None = None) -> dict:
    payload = event.get("payload", {}) or {}
    payload_entry_signal = payload.get("entry_signal")
    summary = {
        "event_id": event.get("event_id", ""),
        "occurred_at": event.get("occurred_at", ""),
        "code": payload.get("code", ""),
        "name": payload.get("name", payload.get("code", "")),
        "score": payload.get("score", 0),
        "position_pct": payload.get("position_pct", 0),
        "market_signal": payload.get("market_signal", ""),
        "source_score_event_id": payload.get("source_score_event_id", ""),
        "entry_signal": bool(payload_entry_signal) if payload_entry_signal is not None else False,
        "primary_strategy_route": payload.get("primary_strategy_route"),
        "primary_strategy_route_label": payload.get("primary_strategy_route_label"),
        "technical_detail": payload.get("technical_detail", ""),
        "data_quality": payload.get("data_quality", ""),
    }
    score_payload = _source_score_payload(ctx, str(summary.get("source_score_event_id") or ""))
    if score_payload:
        if payload_entry_signal is None:
            summary["entry_signal"] = bool(score_payload.get("entry_signal", False))
        routes = score_payload.get("strategy_routes") or []
        primary_route = summary.get("primary_strategy_route") or score_payload.get("primary_strategy_route")
        summary["primary_strategy_route"] = primary_route
        summary["primary_strategy_route_label"] = (
            summary.get("primary_strategy_route_label")
            or score_payload.get("primary_strategy_route_label")
            or _primary_strategy_route_label(routes, primary_route)
        )
        summary["technical_detail"] = summary.get("technical_detail") or score_payload.get("technical_detail", "")
        summary["data_quality"] = summary.get("data_quality") or score_payload.get("data_quality", "")
    return summary


def _source_score_payload(ctx: PipelineContext | None, event_id: str) -> dict:
    if ctx is None or not event_id:
        return {}
    try:
        row = ctx.conn.execute(
            "SELECT payload_json FROM event_log WHERE event_id = ? LIMIT 1",
            (event_id,),
        ).fetchone()
    except Exception:
        return {}
    if not row:
        return {}
    try:
        payload = json.loads(row["payload_json"])
    except (TypeError, json.JSONDecodeError, KeyError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _primary_strategy_route_label(routes: list, primary_route: object) -> str | None:
    for route in routes:
        if not isinstance(route, dict):
            continue
        if primary_route and route.get("route") != primary_route:
            continue
        label = route.get("display_name")
        if label:
            return str(label)
    return None


def _paper_account_summary(paper: PaperAccount) -> dict:
    """只读汇总 MX 模拟盘账户；失败时返回可诊断状态。"""
    try:
        positions = paper.get_positions()
        balance = paper.get_balance()
    except Exception as exc:
        return {
            "status": "error",
            "message": f"MX 模拟盘账户读取失败: {exc}",
            "positions_count": 0,
            "total_asset": 0,
            "available_cash": 0,
            "market_value": 0,
        }
    status = "ok" if balance.total_asset > 0 else "warning"
    message = "MX 模拟盘账户可读" if status == "ok" else "MX 模拟盘资金为 0 或账户不可读"
    return {
        "status": status,
        "message": message,
        "positions_count": len(positions),
        "total_asset": balance.total_asset,
        "available_cash": balance.available_cash,
        "market_value": balance.market_value,
        "frozen": balance.frozen,
    }


def build_auto_trade_readiness(
    ctx: PipelineContext,
    *,
    paper_factory=PaperAccount,
    include_account: bool = True,
) -> dict:
    """生成 auto_trade 模拟盘执行预检；只读，不下单。"""
    cfg = _get_auto_trade_cfg(ctx)
    enabled = bool(cfg.get("enabled", False))
    dry_run = bool(cfg.get("dry_run", True))
    now = local_now()
    now_utc = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
    max_age_hours = _buy_guard_max_age_hours(ctx, cfg)
    window_state = _trade_window_state(cfg, now)
    pool = _annotate_candidate_pool_freshness_for_window(
        _candidate_pool_state(ctx, now_utc, max_age_hours),
        window_state,
    )
    buy_signal = _fresh_buy_signal_summary(ctx, cfg, now=now)
    recent_unusable_buy_signal = _recent_unusable_buy_signal_summary(ctx, cfg, now=now)
    current_entry_signals = _current_core_entry_signals(ctx)
    signal_gap = _entry_signal_gap(current_entry_signals, buy_signal)
    guard = _new_trade_guard(ctx)
    execution_profile = _execution_profile_state(ctx)
    account = (
        _paper_account_summary(paper_factory())
        if include_account
        else {
            "status": "skipped",
            "message": "已跳过 MX 模拟盘账户读取",
            "positions_count": None,
            "total_asset": None,
            "available_cash": None,
            "market_value": None,
        }
    )

    mode = "disabled"
    if enabled:
        mode = "shadow_event" if dry_run else "mx_paper_order"

    blockers: list[dict] = []
    if not enabled:
        blockers.append({"reason": "auto_trade_disabled", "label": "auto_trade 未启用"})
    if enabled and dry_run:
        blockers.append({"reason": "dry_run_enabled", "label": "当前只记录试运行事件，不提交 MX 模拟盘委托"})
    if account.get("status") == "error" or (include_account and float(account.get("total_asset") or 0) <= 0):
        blockers.append({"reason": "paper_account_unavailable", "label": "MX 模拟盘账户不可用或资金为 0"})
    if not guard.get("allow_new_trades", False):
        blockers.append({"reason": "new_trade_guard_blocked", "label": "单日异常保护阻止新增买入"})

    profile_blockers: list[dict] = []
    if not execution_profile.get("safe_to_auto_apply", False):
        profile_blockers.append({
            "reason": "profile_review_required",
            "label": "当前 default 混合配置需要人工确认执行 profile",
        })

    buy_blockers: list[dict] = []
    if not window_state["buy_open"]:
        buy_blockers.append({"reason": "buy_window_closed", "label": "当前不在模拟买入窗口"})
    if pool["core_count"] <= 0:
        buy_blockers.append({"reason": "core_pool_empty", "label": "核心候选池为空"})
    pool_freshness_blocker = _candidate_pool_freshness_blocker(pool)
    if pool_freshness_blocker:
        buy_blockers.append(pool_freshness_blocker)
    if int(buy_signal.get("count") or 0) <= 0:
        buy_blockers.append({"reason": "no_fresh_buy_signal", "label": "没有新鲜买入意向"})

    paper_order_enabled = enabled and not dry_run
    buy_ready = paper_order_enabled and not blockers and not buy_blockers and not profile_blockers
    if buy_ready:
        buy_status = "ready"
    elif not enabled:
        buy_status = "disabled"
    elif dry_run:
        buy_status = "shadow"
    elif any(item["reason"] == "buy_window_closed" for item in buy_blockers) and int(buy_signal.get("count") or 0) > 0:
        buy_status = "waiting_window"
    else:
        buy_status = "blocked"

    all_blockers = [*blockers, *profile_blockers, *buy_blockers]

    if not enabled:
        status = "disabled"
    elif dry_run:
        status = "shadow"
    elif blockers:
        status = "blocked"
    elif buy_status == "waiting_window":
        status = "waiting_window"
    elif profile_blockers:
        status = "profile_review_required"
    elif buy_blockers:
        status = "blocked"
    else:
        status = "ready"

    summary = _auto_trade_readiness_summary(
        status=status,
        buy_signal=buy_signal,
        recent_unusable_buy_signal=recent_unusable_buy_signal,
        blockers=all_blockers,
        execution_profile=execution_profile,
    )

    next_action = {
        "type": "run_auto_trade" if buy_ready else "inspect_blockers",
        "label": "运行模拟盘自动交易" if buy_ready else "查看预检阻断项",
        "command": "atrade run-pipeline auto_trade --json" if buy_ready else "atrade paper auto-readiness --json",
        "safe_to_auto_apply": buy_ready,
        **_auto_readiness_next_action_contract(
            "run_pipeline_auto_trade" if buy_ready else "paper_auto_readiness"
        ),
    }
    if dry_run:
        next_action = {
            "type": "enable_paper_order_mode",
            "label": "启用 MX 模拟盘委托模式",
            "command": "编辑 config/strategy.yaml: auto_trade.dry_run=false",
            "safe_to_auto_apply": False,
        }
    elif profile_blockers:
        recommended_profile = execution_profile.get("recommended_profile") or "trend_swing"
        if execution_profile.get("latest_activation_request"):
            next_action = {
                "type": "review_recorded_profile_activation",
                "label": "复核已记录的 profile 激活计划",
                "command": f"atrade strategy profile-activation --target {recommended_profile} --json",
                "safe_to_auto_apply": False,
                **_auto_readiness_next_action_contract("strategy_profile_activation_review"),
            }
        else:
            next_action = {
                "type": "confirm_strategy_profile",
                "label": "人工确认执行 profile",
                "command": (
                    f"确认后设置 ASTOCK_CONFIG_PROFILE={recommended_profile} "
                    "再运行 atrade paper auto-readiness --json"
                ),
                "safe_to_auto_apply": False,
            }

    return {
        "command": "paper auto-readiness",
        "status": status,
        "summary": summary,
        "mode": mode,
        "enabled": enabled,
        "dry_run": dry_run,
        "paper_order_submission_enabled": paper_order_enabled,
        "manual_confirmation_required_for_real_trade": True,
        "real_broker_integration": "disabled",
        "checked_at": now.isoformat(),
        "window_state": window_state,
        "execution_profile": execution_profile,
        "paper_account": account,
        "candidate_pool": pool,
        "fresh_buy_signal": buy_signal,
        "recent_unusable_buy_signal": recent_unusable_buy_signal,
        "new_trade_guard": guard,
        "blockers": all_blockers,
        "buy_side": {
            "status": buy_status,
            "ready": buy_ready,
            "blockers": buy_blockers,
            "top_signal": buy_signal.get("top", {}) or {},
            "current_entry_signals": current_entry_signals,
            "signal_gap": signal_gap,
        },
        "next_action": next_action,
        "guardrails": {
            "real_order_auto_execution_allowed": False,
            "paper_order_allowed_when_ready": paper_order_enabled,
            "requires_core_candidate": True,
            "requires_buy_decision": True,
            "requires_fresh_scoring": True,
        },
    }


def _auto_readiness_next_action_contract(command_contract_id: str) -> dict:
    contracts = {
        "run_pipeline_auto_trade": {
            "command_contract_id": "run_pipeline_auto_trade",
            "writes_state": True,
            "writes_environment": False,
            "writes_order": True,
            "requires_user_approval": True,
            "risk_level": "paper_order_execution",
        },
        "paper_auto_readiness": {
            "command_contract_id": "paper_auto_readiness",
            "writes_state": False,
            "writes_environment": False,
            "writes_order": False,
            "requires_user_approval": False,
            "risk_level": "read_only",
        },
        "strategy_profile_activation_review": {
            "command_contract_id": "strategy_profile_activation_review",
            "writes_state": False,
            "writes_environment": False,
            "writes_order": False,
            "requires_user_approval": False,
            "risk_level": "read_only",
        },
    }
    return contracts.get(command_contract_id, {})


def _auto_trade_readiness_summary(
    *,
    status: str,
    buy_signal: dict,
    recent_unusable_buy_signal: dict | None = None,
    blockers: list[dict],
    execution_profile: dict | None = None,
) -> str:
    recent_unusable_text = _recent_unusable_buy_signal_text(recent_unusable_buy_signal)
    if status == "ready":
        return "模拟盘自动交易预检通过；当前可以执行 auto_trade 提交 MX 模拟盘委托。"
    if status == "profile_review_required":
        profile = execution_profile or {}
        recommended = profile.get("recommended_profile") or "trend_swing"
        if profile.get("latest_activation_request"):
            summary = f"当前仍在 default 混合配置；已记录待人工确认的 {recommended} profile 激活计划。"
        else:
            summary = f"当前仍在 default 混合配置；自动模拟前需要人工确认执行 profile，建议确认 {recommended}。"
        other_blockers = [
            item for item in blockers
            if str(item.get("reason") or "") != "profile_review_required"
        ]
        if other_blockers:
            labels = "、".join(str(item.get("label") or item.get("reason") or "未知阻断") for item in other_blockers)
            summary = f"{summary} 其他阻断：{labels}。"
        if recent_unusable_text:
            summary = f"{summary} {recent_unusable_text}"
        return summary
    if status == "waiting_window" and int(buy_signal.get("count") or 0) > 0:
        top = buy_signal.get("top", {}) or {}
        code = top.get("code", "")
        name = top.get("name") or code
        score = float(top.get("score") or 0)
        summary = (
            f"已有新鲜买入意向 {buy_signal.get('count')} 条，但当前不在模拟买入窗口；"
            f"最高分为 {name}({code}) {score:.1f} 分，本轮不会提交模拟买入。"
        )
        profile = execution_profile or {}
        if not profile.get("safe_to_auto_apply", True):
            recommended = profile.get("recommended_profile") or "trend_swing"
            if profile.get("latest_activation_request"):
                return f"{summary}同时已记录待人工确认的 {recommended} profile 激活计划。"
            return f"{summary}同时自动模拟前需要人工确认执行 profile，建议确认 {recommended}。"
        return summary
    if status == "shadow":
        return "当前只记录模拟试运行事件，不提交 MX 模拟盘委托。"
    if status == "disabled":
        return "auto_trade 未启用；不会提交模拟盘委托。"
    if blockers:
        labels = "、".join(str(item.get("label") or item.get("reason") or "未知阻断") for item in blockers)
        summary = f"模拟盘自动交易预检未通过：{labels}。"
        if recent_unusable_text:
            summary = f"{summary} {recent_unusable_text}"
        return summary
    return "模拟盘自动交易预检未通过；请查看阻断项。"


def _recent_unusable_buy_signal_text(signal: dict | None) -> str:
    if not signal or int(signal.get("count") or 0) <= 0:
        return ""
    top = signal.get("top", {}) or {}
    code = top.get("code", "")
    name = top.get("name") or code
    score = float(top.get("score") or 0)
    reason = top.get("unusable_reason_label") or top.get("unusable_reason") or "不满足当前承接窗口"
    return f"近期买入意向 {signal.get('count')} 条不可承接；最高分为 {name}({code}) {score:.1f} 分，原因：{reason}。"


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行模拟盘自动交易 pipeline。"""

    cfg = _get_auto_trade_cfg(ctx)
    if not cfg.get("enabled", False):
        _logger.info("[auto_trade] 未启用，跳过")
        return {"enabled": False, "buys": [], "sells": []}

    dry_run = cfg.get("dry_run", True)
    max_daily_trades = cfg.get("max_daily_trades", 4)
    window_state = _trade_window_state(cfg)
    paper = PaperAccount()

    # ------------------------------------------------------------------
    # 1. 查模拟盘状态
    # ------------------------------------------------------------------
    positions = paper.get_positions()
    balance = paper.get_balance()
    exposure_pct, available_cash = paper.get_exposure()

    _logger.info(
        f"[auto_trade] 模拟盘: {len(positions)} 持仓, "
        f"总资产 ¥{balance.total_asset:,.0f}, 可用 ¥{available_cash:,.0f}, "
        f"仓位 {exposure_pct:.1%}"
    )

    # ------------------------------------------------------------------
    # 2. 大盘信号
    # ------------------------------------------------------------------
    market_state, index_data = asyncio.run(ctx.market_svc.collect_market_state(run_id))
    signal = market_state.signal
    _logger.info(f"[auto_trade] 大盘信号: {signal.value}")

    # 同步指数数据到 projection_market_state 表
    if index_data:
        ctx.projector.sync_market_state(index_data)

    sells: list[dict] = []
    buys: list[dict] = []
    diagnostics: list[dict] = []
    trade_count = 0

    # ------------------------------------------------------------------
    # 3. 风控检查 → 自动卖出
    # ------------------------------------------------------------------
    if window_state["sell_open"]:
        sells = _check_and_sell(ctx, paper, positions, market_state, run_id, cfg, dry_run)
        trade_count += len(sells)
    else:
        _logger.info("[auto_trade] 当前不在卖出时间窗口，跳过自动卖出")

    # ------------------------------------------------------------------
    # 4. 评分决策 → 自动买入
    # ------------------------------------------------------------------
    if trade_count < max_daily_trades and window_state["buy_open"]:
        # 刷新资金（卖出后可能变化）
        if sells:
            balance = paper.get_balance()
            exposure_pct, available_cash = paper.get_exposure()

        remaining_trades = max_daily_trades - trade_count
        buys = _score_and_buy(
            ctx, paper, balance, exposure_pct, available_cash,
            market_state, run_id, cfg, dry_run, remaining_trades,
            diagnostics=diagnostics,
        )
    elif trade_count < max_daily_trades:
        _logger.info("[auto_trade] 当前不在买入时间窗口，跳过自动买入")

    # ------------------------------------------------------------------
    # 5. 汇总 + Discord 推送
    # ------------------------------------------------------------------
    no_trade_summary = _build_no_trade_summary(
        buys=buys,
        sells=sells,
        diagnostics=diagnostics,
        market_signal=signal.value,
        window_state=window_state,
        pending_buy_signal=(
            _fresh_buy_signal_summary(
                ctx,
                cfg,
                now=_parse_iso(window_state.get("checked_at", "")),
            )
            if not window_state["buy_open"]
            else {}
        ),
    )
    _record_summary_event(ctx, run_id, buys, sells, dry_run, no_trade_summary=no_trade_summary)

    embed = None
    if buys or sells:
        embed = _format_auto_trade_embed(buys, sells, balance, market_state, dry_run)
        try:
            from astock_trading.reporting.discord_sender import send_embed
            prefix = "🧪 " if dry_run else ""
            ok, err = send_embed(embed, content=f"{prefix}模拟盘自动交易")
            if not ok:
                _logger.warning(f"[auto_trade] Discord 推送失败: {err}")
        except Exception as e:
            _logger.warning(f"[auto_trade] Discord 推送异常: {e}")
    else:
        _logger.info("[auto_trade] 无交易动作，跳过 Discord 单独推送")

    # Obsidian 日志 + 模拟盘日报
    _write_obsidian_log(ctx, run_id, buys, sells, dry_run)

    # 刷新最新持仓/资金（交易后可能变化）
    final_positions = paper.get_positions()
    final_balance = paper.get_balance()

    # 写模拟盘完整日报
    ctx.obsidian.write_paper_report(
        run_id=run_id,
        positions=final_positions,
        balance={
            "total_asset": final_balance.total_asset,
            "available_cash": final_balance.available_cash,
            "market_value": final_balance.market_value,
        },
        buys=buys,
        sells=sells,
        market_signal=signal.value,
        market_indices=market_state.detail.get("indices", {}),
        no_trade_summary=no_trade_summary,
        dry_run=dry_run,
    )

    # 追加交易记录
    trade_rows = []
    now_str = local_now_str()
    for s in sells:
        trade_rows.append({
            "time": now_str, "side": "sell",
            "name": s.get("name", ""), "code": s.get("code", ""),
            "shares": s.get("shares", 0), "price": s.get("price", 0),
            "amount": s.get("shares", 0) * s.get("price", 0),
            "reason": f"[{s.get('reason', '')}] {s.get('risk_description', '')}".strip(),
        })
    for b in buys:
        trade_rows.append({
            "time": now_str, "side": "buy",
            "name": b.get("name", ""), "code": b.get("code", ""),
            "shares": b.get("shares", 0), "price": b.get("price", 0),
            "amount": b.get("shares", 0) * b.get("price", 0),
            "reason": f"[BUY_CORE_POOL] 评分 {b.get('score', 0):.1f}",
        })
    if trade_rows:
        ctx.obsidian.append_paper_trade_log(trade_rows)

    # 刷新每日巡检报告
    ctx.obsidian.write_daily_output_index(run_id)

    result = {
        "enabled": True,
        "dry_run": dry_run,
        "signal": signal.value,
        "paper_positions": len(positions),
        "paper_total_asset": balance.total_asset,
        "buys": buys,
        "sells": sells,
        "diagnostics": diagnostics,
        "no_trade_summary": no_trade_summary,
        "window_state": window_state,
        "discord_embed": embed,
    }
    _logger.info(f"[auto_trade] 完成: {len(buys)} 买入, {len(sells)} 卖出, dry_run={dry_run}")
    return result


# ======================================================================
# 卖出逻辑
# ======================================================================

def _check_and_sell(
    ctx: PipelineContext,
    paper: PaperAccount,
    positions: list[PaperPosition],
    market_state,
    run_id: str,
    cfg: dict,
    dry_run: bool,
) -> list[dict]:
    """对模拟盘持仓做风控检查，触发则自动卖出。"""
    if not positions:
        return []

    risk_cfg = ctx.cfg.get("risk", {})
    sells = []

    # 批量获取 MA 数据。卖出风控只需要持仓价和均线，优先走轻量盘中接口，
    # 避免完整 collect_batch 额外拉财报/资金流/舆情导致自动交易链路变慢。
    stock_list = [{"code": p.code, "name": p.name} for p in positions]
    try:
        snapshots = asyncio.run(_collect_position_risk_snapshots(ctx, stock_list, run_id))
        ma_data = {}
        for snap in snapshots:
            if snap.technical:
                ma_data[snap.code] = {
                    "ma20": snap.technical.ma20,
                    "ma60": snap.technical.ma60,
                }
    except Exception as e:
        _logger.warning(f"[auto_trade] 批量获取 MA 数据失败: {e}")
        ma_data = {}

    # 大盘 CLEAR 信号 → 全部卖出
    if market_state.signal == MarketSignal.CLEAR:
        _logger.info("[auto_trade] 大盘 CLEAR，清仓所有模拟盘持仓")
        for pos in positions:
            if pos.shares <= 0:
                continue
            sell_info = _execute_sell(paper, pos, "market_clear", run_id, ctx, dry_run)
            if sell_info:
                sells.append(sell_info)
        return sells

    for pos in positions:
        if pos.shares <= 0:
            continue

        # 推断风格（默认 momentum，模拟盘偏短线）
        style = Style.MOMENTUM
        ma_info = ma_data.get(pos.code, {})

        params = get_risk_params(style, risk_cfg)

        # 获取实际买入日期（从事件日志）
        entry_date = local_today()
        paper_events = ctx.event_store.query(
            event_type=AUTO_TRADE_EXECUTED,
            stream=f"paper:{pos.code}",
        )
        for ev in reversed(paper_events):
            p = ev.get("payload", {})
            if p.get("side") == "buy":
                try:
                    entry_date = iso_to_local(ev["occurred_at"]).date()
                except (ValueError, KeyError):
                    pass
                break

        # 持仓期内历史最高收盘价（用于移动止盈标杆）
        highest_since_entry = _get_highest_since_entry(
            pos.code,
            entry_date,
            pos.current_price,
            conn=ctx.conn,
        )

        signals = check_exit_signals(
            code=pos.code,
            avg_cost=pos.avg_cost,
            current_price=pos.current_price,
            entry_date=entry_date,
            today=local_today(),
            highest_since_entry=highest_since_entry,
            entry_day_low=pos.avg_cost,
            params=params,
            ma20=ma_info.get("ma20", 0),
            ma60=ma_info.get("ma60", 0),
        )

        # 只对 immediate 级别自动卖出
        immediate = [s for s in signals if s.urgency == "immediate"]
        if immediate:
            reason = immediate[0].signal_type
            desc = immediate[0].description
            _logger.info(f"[auto_trade] 风控触发卖出 {pos.name}({pos.code}): {desc}")
            sell_info = _execute_sell(paper, pos, reason, run_id, ctx, dry_run)
            if sell_info:
                sell_info["risk_description"] = desc
                sells.append(sell_info)

    return sells


async def _collect_position_risk_snapshots(ctx: PipelineContext, stock_list: list[dict], run_id: str):
    collector = getattr(ctx.market_svc, "collect_intraday_batch", None)
    if callable(collector):
        return await collector(stock_list, run_id)
    return await ctx.market_svc.collect_batch(stock_list, run_id)


def _execute_sell(
    paper: PaperAccount,
    pos: PaperPosition,
    reason: str,
    run_id: str,
    ctx: PipelineContext,
    dry_run: bool,
) -> dict | None:
    """执行模拟盘卖出。"""
    info = {
        "side": "sell",
        "code": pos.code,
        "name": pos.name,
        "shares": pos.shares,
        "price": pos.current_price,
        "avg_cost": pos.avg_cost,
        "realized_pnl_cents": int(round((pos.current_price - pos.avg_cost) * pos.shares * 100)),
        "reason": reason,
        "dry_run": dry_run,
    }
    route_info = _latest_paper_buy_route(ctx, pos.code)
    if route_info:
        info.update(route_info)

    if dry_run:
        _logger.info(f"[auto_trade][DRY] 卖出 {pos.name}({pos.code}) {pos.shares}股")
        info["status"] = "dry_run"
    else:
        result = paper.sell(pos.code, pos.shares)
        if result.success:
            info["status"] = "filled"
            info["order_id"] = result.order_id
            _logger.info(f"[auto_trade] 卖出成功 {pos.name}({pos.code}) {pos.shares}股")
        else:
            _logger.warning(f"[auto_trade] 卖出失败 {pos.name}({pos.code}): {result.error}")
            info["status"] = "failed"
            info["error"] = result.error

    # 记录事件
    ctx.event_store.append(
        stream=f"paper:{pos.code}",
        stream_type="paper_trade",
        event_type=AUTO_TRADE_EXECUTED,
        payload=info,
        metadata={"run_id": run_id, "account": "paper"},
    )
    return info


# ======================================================================
# 买入逻辑
# ======================================================================

def _score_and_buy(
    ctx: PipelineContext,
    paper: PaperAccount,
    balance: PaperBalance,
    exposure_pct: float,
    available_cash: float,
    market_state,
    run_id: str,
    cfg: dict,
    dry_run: bool,
    max_trades: int,
    diagnostics: list[dict] | None = None,
) -> list[dict]:
    """从公共池读取评分，决策后自动买入。"""

    # 大盘 RED/CLEAR 禁止买入
    if market_state.signal in (MarketSignal.RED, MarketSignal.CLEAR):
        _logger.info(f"[auto_trade] 大盘 {market_state.signal.value}，禁止买入")
        return []

    # 仓位上限
    pos_cfg = ctx.cfg.get("risk", {}).get("position", {})
    total_max = pos_cfg.get("total_max", 0.60)
    single_max = pos_cfg.get("single_max", 0.20)

    if exposure_pct >= total_max:
        _logger.info(f"[auto_trade] 仓位 {exposure_pct:.1%} >= {total_max:.0%}，禁止买入")
        return []

    # 本周模拟盘买入次数
    from datetime import timedelta
    today = local_now()
    monday = today.date() - timedelta(days=today.weekday())
    since, _ = local_date_bounds_utc(monday)
    weekly_events = ctx.event_store.query(
        event_type=AUTO_TRADE_EXECUTED,
        since=since,
    )
    weekly_buy_count = sum(
        1 for ev in weekly_events
        if ev.get("payload", {}).get("side") == "buy"
        and ev.get("payload", {}).get("status") in ("filled", "dry_run")
        and ev.get("metadata", {}).get("account") == "paper"
    )

    weekly_max = pos_cfg.get("weekly_max", 2)
    if weekly_buy_count >= weekly_max:
        _logger.info(f"[auto_trade] 本周已买 {weekly_buy_count}/{weekly_max}，禁止买入")
        return []

    buy_diagnostics = _buy_side_diagnostics(ctx, run_id, cfg)
    if buy_diagnostics:
        if diagnostics is not None:
            diagnostics.extend(buy_diagnostics)
        _logger.warning(
            "[auto_trade] 买入前置检查未通过: "
            + ", ".join(d["reason"] for d in buy_diagnostics)
        )
        return []

    # 读公共池评分（最近一次 scoring pipeline 的结果）
    candidates = _get_buy_candidates(
        ctx,
        run_id,
        market_state,
        exposure_pct,
        weekly_buy_count,
        cfg,
        max_age_hours=_buy_guard_max_age_hours(ctx, cfg),
        diagnostics=diagnostics,
    )

    if not candidates:
        _logger.info("[auto_trade] 无符合条件的买入候选")
        return []

    # 已持有的模拟盘股票
    paper_positions = paper.get_positions()
    held_codes = {p.code for p in paper_positions}

    buys = []
    remaining = min(max_trades, weekly_max - weekly_buy_count)

    for candidate in candidates:
        if remaining <= 0:
            break

        code = candidate["code"]
        if code in held_codes:
            continue

        # 计算仓位：正式 BUY 可按市场制度 × 路线策略覆盖仓位；无策略时沿用原始仓位。
        position_pct = _buy_candidate_position_pct(
            ctx,
            candidate,
            market_state,
            single_max=single_max,
            remaining_pct=total_max - exposure_pct,
        )
        if position_pct <= 0.01:
            break

        price = candidate.get("price", 0)
        if price <= 0:
            continue

        shares = _calc_buy_shares(price, available_cash, position_pct, balance.total_asset)
        if shares <= 0:
            continue

        buy_info = _execute_buy(
            paper,
            code,
            candidate.get("name", code),
            shares,
            price,
            run_id,
            ctx,
            dry_run,
            source_event_id=candidate.get("source_event_id", ""),
            source_score_event_id=candidate.get("source_score_event_id", ""),
            primary_strategy_route=candidate.get("primary_strategy_route") or "",
            primary_strategy_route_label=(
                candidate.get("primary_strategy_route_label")
                or _route_label_for_key(candidate.get("primary_strategy_route"))
                or ""
            ),
        )
        if buy_info:
            buy_info["score"] = candidate.get("score", 0)
            buy_info["position_pct"] = position_pct
            buys.append(buy_info)
            remaining -= 1
            # 更新可用资金估算
            available_cash -= shares * price
            exposure_pct += position_pct

    return buys


def _get_buy_candidates(
    ctx: PipelineContext,
    run_id: str,
    market_state,
    exposure_pct: float,
    weekly_buy_count: int,
    cfg: dict,
    max_age_hours: int = 24,
    diagnostics: list[dict] | None = None,
) -> list[dict]:
    """
    从公共池获取买入候选。

    只使用新鲜的 scoring/decision 事件，避免核心池静态数据过期时静默买入。
    """
    current = local_now()
    now = current.astimezone(timezone.utc) if current.tzinfo else current.replace(tzinfo=timezone.utc)
    since = (now - timedelta(hours=max_age_hours)).isoformat()
    recent_decisions = _usable_buy_decision_events(ctx, cfg, now, max_age_hours)
    executed = _recent_paper_buy_keys(ctx, since=since)

    candidates = []
    seen = set()

    for ev in recent_decisions:
        occurred = _parse_iso(ev.get("occurred_at", ""))
        if not occurred or not (now - timedelta(hours=max_age_hours) <= occurred <= now):
            continue
        p = ev.get("payload", {})
        if p.get("action") != "BUY":
            continue
        code = p.get("code", "")
        if code in seen:
            continue
        signal_id = str(p.get("source_score_event_id") or ev.get("event_id", ""))
        if signal_id and signal_id in executed["signal_ids"]:
            continue
        if code and code in executed["codes"]:
            continue
        seen.add(code)
        candidates.append({
            "code": code,
            "name": p.get("name", code),
            "score": p.get("score", 0),
            "position_pct": p.get("position_pct", 0),
            "market_signal": p.get("market_signal") or getattr(getattr(market_state, "signal", None), "value", ""),
            "primary_strategy_route": p.get("primary_strategy_route"),
            "primary_strategy_route_label": p.get("primary_strategy_route_label"),
            "source_event_id": ev.get("event_id", ""),
            "source_score_event_id": p.get("source_score_event_id", ""),
            "price": 0,  # 需要实时获取
        })

    if not candidates:
        return []

    # 获取实时价格
    stock_list = [{"code": c["code"], "name": c["name"]} for c in candidates]
    try:
        snapshots = asyncio.run(ctx.market_svc.collect_batch(stock_list, run_id))
        price_map = {}
        for snap in snapshots:
            if snap.quote and snap.quote.close > 0:
                price_map[snap.code] = snap.quote.close
        for c in candidates:
            c["price"] = price_map.get(c["code"], 0)
    except Exception as e:
        _logger.warning(f"[auto_trade] 获取实时价格失败: {e}")

    unpriced_codes = [c["code"] for c in candidates if c["price"] <= 0]
    if unpriced_codes and len(unpriced_codes) == len(candidates):
        diagnostic = _record_buy_diagnostic(
            ctx,
            run_id,
            "buy_candidate_price_unavailable",
            "买入候选缺少有效价格，未提交模拟买入。",
            {
                "codes": unpriced_codes,
                "candidate_count": len(candidates),
            },
        )
        if diagnostics is not None:
            diagnostics.append(diagnostic)

    # 过滤无价格的
    candidates = [c for c in candidates if c["price"] > 0]
    # 按路线策略优先级、评分降序。没有配置路线策略时等价于原始按评分排序。
    candidates.sort(
        key=lambda c: _buy_candidate_sort_key(ctx, c, market_state),
        reverse=True,
    )

    return candidates


def _buy_candidate_sort_key(ctx: PipelineContext, candidate: dict, market_state) -> tuple[float, float]:
    policy = _route_execution_policy_for_candidate(ctx, candidate, market_state)
    return (
        float(policy.get("priority", 0.0) or 0.0),
        float(candidate.get("score", 0.0) or 0.0),
    )


def _buy_candidate_position_pct(
    ctx: PipelineContext,
    candidate: dict,
    market_state,
    *,
    single_max: float,
    remaining_pct: float,
) -> float:
    policy = _route_execution_policy_for_candidate(ctx, candidate, market_state)
    pct = None
    if policy.get("position_pct") is not None:
        pct = float(policy.get("position_pct") or 0.0)
    if pct is None or pct <= 0:
        pct = single_max * float(getattr(market_state, "multiplier", 0.0) or 0.0)
    return min(pct, single_max, remaining_pct)


def _route_execution_policy_for_candidate(ctx: PipelineContext, candidate: dict, market_state) -> dict:
    policy_map = (
        ctx.cfg.get("scoring", {}).get("route_execution_policy")
        or ctx.cfg.get("auto_trade", {}).get("route_execution_policy")
        or {}
    )
    if not isinstance(policy_map, dict):
        return {}
    signal = str(
        candidate.get("market_signal")
        or getattr(getattr(market_state, "signal", None), "value", "")
        or ""
    )
    route = str(candidate.get("primary_strategy_route") or candidate.get("source_route") or "unknown")
    for key in (f"{signal}:{route}", f"*:{route}", route):
        policy = policy_map.get(key)
        if not isinstance(policy, dict):
            continue
        score_min = policy.get("score_min")
        if score_min is not None and float(candidate.get("score", 0.0) or 0.0) < float(score_min or 0.0):
            return {}
        return policy
    return {}


def _recent_paper_buy_keys(ctx: PipelineContext, *, since: str) -> dict[str, set[str]]:
    events = ctx.event_store.query(
        event_type=AUTO_TRADE_EXECUTED,
        since=since,
        limit=200,
    )
    codes: set[str] = set()
    signal_ids: set[str] = set()
    for event in events:
        if event.get("metadata", {}).get("account") != "paper":
            continue
        payload = event.get("payload", {}) or {}
        if payload.get("side") != "buy":
            continue
        if payload.get("status") not in {"filled", "dry_run"}:
            continue
        code = str(payload.get("code") or "")
        if code:
            codes.add(code)
        for key in ("source_score_event_id", "source_event_id"):
            value = str(payload.get(key) or "")
            if value:
                signal_ids.add(value)
    return {"codes": codes, "signal_ids": signal_ids}


def _execute_buy(
    paper: PaperAccount,
    code: str,
    name: str,
    shares: int,
    price: float,
    run_id: str,
    ctx: PipelineContext,
    dry_run: bool,
    source_event_id: str = "",
    source_score_event_id: str = "",
    primary_strategy_route: str = "",
    primary_strategy_route_label: str = "",
) -> dict | None:
    """执行模拟盘买入。"""
    info = {
        "side": "buy",
        "code": code,
        "name": name,
        "shares": shares,
        "price": price,
        "amount": shares * price,
        "dry_run": dry_run,
        "source_event_id": source_event_id,
        "source_score_event_id": source_score_event_id,
    }
    if primary_strategy_route:
        info["primary_strategy_route"] = primary_strategy_route
    if primary_strategy_route_label:
        info["primary_strategy_route_label"] = primary_strategy_route_label

    if dry_run:
        _logger.info(f"[auto_trade][DRY] 买入 {name}({code}) {shares}股 @ ¥{price:.2f}")
        info["status"] = "dry_run"
    else:
        result = paper.buy(code, shares)
        if result.success:
            info["status"] = "filled"
            info["order_id"] = result.order_id
            _logger.info(f"[auto_trade] 买入成功 {name}({code}) {shares}股 @ ¥{price:.2f}")
        else:
            _logger.warning(f"[auto_trade] 买入失败 {name}({code}): {result.error}")
            info["status"] = "failed"
            info["error"] = result.error

    # 记录事件
    ctx.event_store.append(
        stream=f"paper:{code}",
        stream_type="paper_trade",
        event_type=AUTO_TRADE_EXECUTED,
        payload=info,
        metadata={"run_id": run_id, "account": "paper"},
    )
    return info


def _latest_paper_buy_route(ctx: PipelineContext, code: str) -> dict:
    events = ctx.event_store.query(
        stream=f"paper:{code}",
        event_type=AUTO_TRADE_EXECUTED,
        limit=200,
    )
    for event in reversed(events):
        payload = event.get("payload", {}) or {}
        if payload.get("side") != "buy":
            continue
        if payload.get("status") not in {"filled", "dry_run"}:
            continue
        route = str(payload.get("primary_strategy_route") or "")
        label = str(payload.get("primary_strategy_route_label") or "")
        if not label:
            label = _route_label_for_key(route) or ""
        result = {}
        if route:
            result["primary_strategy_route"] = route
        if label:
            result["primary_strategy_route_label"] = label
        return result
    return {}


def _route_label_for_key(route: object) -> str | None:
    return {
        "flow_confirmed_trend": "资金趋势确认",
        "volume_breakout": "放量突破",
        "pullback_to_ma20": "均线回踩转强",
        "short_continuation": "短续接力",
        "ma_golden_cross": "均线金叉",
        "dragon_head": "龙头策略",
        "shrink_pullback": "缩量回踩",
        "relative_strength_overheat": "强势过热观察",
        "trend_cooling_off": "趋势冷却观察",
        "trend_watch": "趋势观察",
    }.get(str(route or ""))


# ======================================================================
# 报告
# ======================================================================

def _build_no_trade_summary(
    *,
    buys: list,
    sells: list,
    diagnostics: list[dict],
    market_signal: str,
    window_state: dict,
    pending_buy_signal: dict | None = None,
) -> dict:
    """生成无交易解释，供运行结果、事件和报告复用。"""
    if buys or sells:
        return {}
    if diagnostics:
        first = diagnostics[0]
        reason = str(first.get("reason", "unknown"))
        details = first.get("details", {}) or {}
        message = (
            _no_trade_reason_label(reason, details)
            if reason == "core_pool_empty"
            else str(first.get("message") or _no_trade_reason_label(reason, details))
        )
        return {
            "reason": reason,
            "message": message,
            "details": details,
        }
    pending_buy_signal = pending_buy_signal or {}
    if not window_state.get("buy_open") and int(pending_buy_signal.get("count") or 0) > 0:
        top = pending_buy_signal.get("top", {}) or {}
        code = top.get("code", "")
        name = top.get("name") or code
        score = top.get("score", 0)
        return {
            "reason": "buy_window_closed_with_signal",
            "message": (
                f"已有新鲜买入意向 {pending_buy_signal.get('count')} 条，但当前不在模拟买入窗口；"
                f"最高分为 {name}({code}) {float(score or 0):.1f} 分，本轮未执行模拟买入。"
            ),
            "details": {
                "window_state": window_state,
                "pending_buy_signal": pending_buy_signal,
            },
        }
    if not window_state.get("buy_open") and not window_state.get("sell_open"):
        return {
            "reason": "outside_trade_window",
            "message": "当前不在模拟买入或卖出时间窗口，未执行交易。",
            "details": window_state,
        }
    return {
        "reason": "no_trade_signal",
        "message": f"大盘信号 {market_signal} 下暂无符合条件的模拟交易信号。",
        "details": {"market_signal": market_signal, "window_state": window_state},
    }


def _no_trade_reason_label(reason: str, details: dict | None = None) -> str:
    if reason == "core_pool_empty":
        details = details or {}
        watch_count = int(details.get("watch_count") or 0)
        radar_count = int(details.get("radar_count") or 0)
        if watch_count or radar_count:
            return (
                f"核心候选池为空；当前观察候选 {watch_count} 只、"
                f"强势观察 {radar_count} 只，只跟踪不自动买入。"
            )
    labels = {
        "core_pool_empty": "核心候选池为空，禁止自动买入。",
        "scoring_inputs_stale": "候选池评分已过期，禁止自动买入。",
        "no_fresh_decision_events": "未发现新鲜的决策事件，禁止自动买入。",
        "new_trade_guard_blocked": "单日异常保护触发，禁止新增买入。",
        "profile_review_required": "执行 profile 未完成人工确认，禁止自动买入。",
        "buy_window_closed_with_signal": "已有买入意向，但当前不在模拟买入窗口。",
    }
    return labels.get(reason, "未形成符合条件的模拟交易信号。")


def _record_summary_event(
    ctx: PipelineContext,
    run_id: str,
    buys: list,
    sells: list,
    dry_run: bool,
    *,
    no_trade_summary: dict | None = None,
):
    """记录自动交易汇总事件。"""
    ctx.event_store.append(
        stream="paper:summary",
        stream_type="paper_trade",
        event_type=AUTO_TRADE_SUMMARY,
        payload={
            "date": local_today_str(),
            "dry_run": dry_run,
            "buy_count": len(buys),
            "sell_count": len(sells),
            "buys": buys,
            "sells": sells,
            "no_trade_summary": no_trade_summary or {},
        },
        metadata={"run_id": run_id, "account": "paper"},
    )


def _format_auto_trade_embed(
    buys: list, sells: list, balance: PaperBalance, market_state, dry_run: bool,
) -> dict:
    """格式化 Discord embed。"""
    from astock_trading.reporting.discord import _embed, _field, SIGNAL_EMOJI, COLORS

    date_str = local_today_str()
    sig = market_state.signal.value
    sig_emoji = SIGNAL_EMOJI.get(sig, "")
    title_prefix = "🧪 " if dry_run else "🤖 "
    mode = "[模拟]" if dry_run else ""

    fields = [
        _field("大盘", f"{sig_emoji} {sig}"),
        _field("总资产", f"¥{balance.total_asset:,.0f}"),
        _field("可用资金", f"¥{balance.available_cash:,.0f}"),
    ]

    if sells:
        sell_lines = []
        for s in sells:
            status = "✅" if s.get("status") == "filled" else ("🧪" if s.get("status") == "dry_run" else "❌")
            reason = s.get("reason", "")
            sell_lines.append(f"{status} {s['name']}({s['code']}) {s['shares']}股 | {reason}")
        fields.append(_field(f"🔴 卖出{mode}（{len(sells)}）", "\n".join(sell_lines), inline=False))

    if buys:
        buy_lines = []
        for b in buys:
            status = "✅" if b.get("status") == "filled" else ("🧪" if b.get("status") == "dry_run" else "❌")
            score = b.get("score", 0)
            buy_lines.append(
                f"{status} {b['name']}({b['code']}) {b['shares']}股 "
                f"@ ¥{b['price']:.2f} | 评分 {score:.1f}"
            )
        fields.append(_field(f"🟢 买入{mode}（{len(buys)}）", "\n".join(buy_lines), inline=False))

    if not buys and not sells:
        fields.append(_field("📋 操作", "无交易信号", inline=False))

    return _embed(
        title=f"{title_prefix}模拟盘自动交易 — {date_str}",
        color=COLORS.get("info", 0x37474F),
        fields=fields,
        footer="A-Stock Trading · auto_trade · paper",
    )


def _write_obsidian_log(ctx: PipelineContext, run_id: str, buys: list, sells: list, dry_run: bool):
    """写 Obsidian 日志。"""
    mode = "[DRY RUN] " if dry_run else ""
    lines = [f"## {mode}模拟盘自动交易", ""]

    if sells:
        lines.append("### 卖出")
        for s in sells:
            status = s.get("status", "")
            lines.append(f"- 🔴 {s['name']}({s['code']}) {s['shares']}股 | {s.get('reason', '')} [{status}]")
        lines.append("")

    if buys:
        lines.append("### 买入")
        for b in buys:
            status = b.get("status", "")
            lines.append(
                f"- 🟢 {b['name']}({b['code']}) {b['shares']}股 "
                f"@ ¥{b['price']:.2f} | 评分 {b.get('score', 0):.1f} [{status}]"
            )
        lines.append("")

    if not buys and not sells:
        lines.append("无交易信号")

    ctx.obsidian.write_daily_log(run_id, "\n".join(lines))
