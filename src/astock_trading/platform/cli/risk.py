"""风控和仓位计算 CLI 命令。"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import typer

from astock_trading.pipeline.adaptive_risk import run_adaptive_risk
from astock_trading.pipeline.context import build_context
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.database import MissingDatabaseUrl
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import local_today
from astock_trading.risk.rules import check_exit_signals, check_portfolio_risk, get_risk_params
from astock_trading.risk.sizing import calc_position_size
from astock_trading.strategy.models import Style


risk_app = typer.Typer(name="risk", help="风控检查和仓位计算")


def _strategy_config() -> dict:
    data, _errors = ConfigRegistry().load_and_validate()
    return data.get("strategy", {})


def _position_limits(strategy: dict) -> dict:
    return strategy.get("risk", {}).get("position", {})


def _risk_limits(strategy: dict) -> dict:
    return strategy.get("risk", {}).get("portfolio", strategy.get("risk", {}))


def _position_style(style: str) -> Style:
    if style == Style.SLOW_BULL.value:
        return Style.SLOW_BULL
    if style == Style.MOMENTUM.value:
        return Style.MOMENTUM
    return Style.UNKNOWN


def _risk_signal_payload(signal) -> dict:
    return {
        "signal_type": signal.signal_type,
        "trigger_price": signal.trigger_price,
        "current_price": signal.current_price,
        "description": signal.description,
        "urgency": signal.urgency,
    }


@risk_app.command("position")
def risk_position(
    code: str = typer.Argument(..., help="股票代码"),
    score: float = typer.Argument(..., help="评分，用于记录本次仓位建议的依据"),
    price: float = typer.Argument(..., help="当前价格"),
    capital: float | None = typer.Option(None, "--capital", help="总资金；默认读取 strategy.capital"),
    current_exposure_pct: float = typer.Option(0.0, "--current-exposure-pct", help="当前总仓位占比"),
    market_multiplier: float = typer.Option(1.0, "--market-multiplier", help="大盘仓位系数"),
    single_max_pct: float | None = typer.Option(None, "--single-max-pct", help="单股仓位上限"),
    total_max_pct: float | None = typer.Option(None, "--total-max-pct", help="总仓位上限"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """计算建议仓位；只读，不写入数据库。"""
    strategy = _strategy_config()
    position_cfg = _position_limits(strategy)
    total_capital = capital if capital is not None else float(strategy.get("capital", 500000))
    single_max = single_max_pct if single_max_pct is not None else float(position_cfg.get("single_max", 0.20))
    total_max = total_max_pct if total_max_pct is not None else float(position_cfg.get("total_max", 0.60))

    size = calc_position_size(
        total_capital=total_capital,
        current_exposure_pct=current_exposure_pct,
        price=price,
        market_multiplier=market_multiplier,
        single_max_pct=single_max,
        total_max_pct=total_max,
    )
    payload = {
        "code": code,
        "score": score,
        "price": price,
        "capital": total_capital,
        "current_exposure_pct": current_exposure_pct,
        "market_multiplier": market_multiplier,
        "single_max_pct": single_max,
        "total_max_pct": total_max,
        "shares": size.shares,
        "amount": size.amount,
        "pct": size.pct,
    }
    json_or_text(payload, as_json)


@risk_app.command("trial-guard")
def risk_trial_guard(
    capital: float | None = typer.Option(None, "--capital", help="总资金；默认读取 strategy.capital"),
    amount: float | None = typer.Option(None, "--amount", help="拟执行单笔金额，用于检查是否超过试运行上限"),
    trial_ratio: float | None = typer.Option(None, "--trial-ratio", help="试运行比例；默认正式单票上限的一半"),
    single_max_pct: float | None = typer.Option(None, "--single-max-pct", help="正式单票仓位上限"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """审计首轮实盘试运行护栏；只读，不执行交易。"""
    strategy = _strategy_config()
    position_cfg = _position_limits(strategy)
    total_capital = capital if capital is not None else float(strategy.get("capital", 500000))
    single_max = single_max_pct if single_max_pct is not None else float(position_cfg.get("single_max", 0.20))
    ratio = trial_ratio if trial_ratio is not None else float(position_cfg.get("trial_single_max_ratio", 0.50))
    cap_pct = round(single_max * ratio, 4)
    cap_amount = round(total_capital * cap_pct, 2)
    checked_order = None
    status = "ok"
    if amount is not None:
        within_cap = amount <= cap_amount
        checked_order = {
            "amount": amount,
            "within_cap": within_cap,
            "excess_amount": round(max(amount - cap_amount, 0), 2),
        }
        if not within_cap:
            status = "breached"

    runtime_context = _trial_guard_runtime_context()
    blockers = runtime_context.get("blockers", []) or []
    if status == "ok" and any(item.get("reason") == "profile_review_required" for item in blockers):
        status = "profile_review_required"
    candidate_flow = runtime_context.get("candidate_flow", {}) or {}
    candidate_summary = candidate_flow.get("candidate_summary", {}) or _empty_candidate_summary()
    current_entry_signals = candidate_flow.get("current_entry_signals", []) or []
    payload = {
        "status": status,
        "summary": _trial_guard_summary(
            status=status,
            checked_order=checked_order,
            cap_amount=cap_amount,
            blockers=blockers,
            candidate_summary=candidate_summary,
        ),
        "manual_confirmation_required": True,
        "real_broker_integration": "disabled",
        "real_order_auto_execution_allowed": False,
        "trial_position_cap": {
            "capital": total_capital,
            "formal_single_max_pct": single_max,
            "trial_ratio": ratio,
            "cap_pct": cap_pct,
            "cap_amount": cap_amount,
        },
        "checked_order": checked_order,
        "candidate_summary": candidate_summary,
        "current_entry_signals": current_entry_signals,
        **runtime_context,
        "instructions": [
            "系统只生成买入意向和记录人工成交，不直连券商实盘下单。",
            "首轮实盘单笔金额应按试运行上限人工确认；超限时先降低股数或放弃执行。",
        ],
    }
    json_or_text(payload, as_json)


def _trial_guard_summary(
    *,
    status: str,
    checked_order: dict[str, Any] | None,
    cap_amount: float,
    blockers: list[dict[str, Any]],
    candidate_summary: dict[str, Any],
) -> str:
    candidate_text = candidate_summary.get("summary") or _empty_candidate_summary()["summary"]
    if status == "breached" and checked_order:
        excess = checked_order.get("excess_amount", 0)
        return (
            f"试运行护栏未通过：拟执行金额 {checked_order.get('amount')} "
            f"超过试运行上限 {cap_amount}，超出 {excess}；{candidate_text}"
        )
    if blockers:
        labels = "、".join(str(item.get("label") or item.get("reason") or "未知阻断") for item in blockers)
        return f"试运行护栏未通过：{labels}；{candidate_text}"
    return f"试运行护栏通过；{candidate_text}"


def _trial_guard_runtime_context() -> dict[str, Any]:
    try:
        init_db()
        conn = connect()
    except (MissingDatabaseUrl, RuntimeError, OSError) as exc:
        return {
            "candidate_flow": {
                "status": "unavailable",
                "summary": f"运行库不可用，无法读取当前候选流：{exc}",
                "candidate_summary": _empty_candidate_summary(),
                "current_entry_signals": [],
            },
            "execution_profile": {"status": "unknown"},
            "blockers": [],
            "next_action": {
                "type": "diagnose_flow",
                "label": "诊断候选流",
                "command": "atrade diagnose flow --json",
                "safe_to_auto_apply": True,
                **_read_only_action_contract("diagnose_flow"),
            },
        }

    try:
        candidate_flow = _trial_guard_candidate_flow(conn)
        execution_profile = _trial_guard_execution_profile(conn)
    finally:
        conn.close()

    blockers = _trial_guard_blockers(execution_profile)
    next_action = _trial_guard_next_action(
        candidate_flow=candidate_flow,
        execution_profile=execution_profile,
    )
    return {
        "candidate_flow": candidate_flow,
        "execution_profile": execution_profile,
        "blockers": blockers,
        "next_action": next_action,
    }


def _trial_guard_candidate_flow(conn: Any) -> dict[str, Any]:
    rows = conn.execute(
        """SELECT code, pool_tier, name, score, added_at, last_scored_at,
                  streak_days, note
           FROM projection_candidate_pool
           ORDER BY CASE pool_tier WHEN 'core' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END,
                    score DESC,
                    last_scored_at DESC,
                    code
           LIMIT 10"""
    ).fetchall()
    candidates = [dict(row) for row in rows]
    try:
        from astock_trading.platform.candidate_evidence import enrich_candidate_rows_with_latest_scores

        enrich_candidate_rows_with_latest_scores(conn, candidates)
    except Exception:
        pass
    summary = _trial_guard_candidate_summary(candidates)
    return {
        "status": "ok",
        "summary": summary["summary"],
        "candidate_summary": summary,
        "current_entry_signals": [
            _trial_guard_entry_signal_summary(item)
            for item in candidates
            if _truthy(item.get("entry_signal"))
        ],
    }


def _trial_guard_execution_profile(conn: Any) -> dict[str, Any]:
    try:
        from astock_trading.platform.agent_diagnostics import diagnose_schedule

        runtime_profile = diagnose_schedule(conn).get("runtime_profile", {}) or {}
    except Exception as exc:
        return {
            "status": "unknown",
            "message": f"运行 profile 读取失败：{exc}",
        }
    return {
        "status": runtime_profile.get("status", "unknown"),
        "effective_profile": runtime_profile.get("effective_profile"),
        "recommended_profile": runtime_profile.get("recommended_profile"),
        "safe_to_auto_apply": runtime_profile.get("safe_to_auto_apply"),
        "activation_request_status": runtime_profile.get("activation_request_status"),
        "message": runtime_profile.get("message", ""),
    }


def _trial_guard_candidate_summary(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    core_count = sum(1 for item in candidates if item.get("pool_tier") == "core")
    watch_count = sum(1 for item in candidates if item.get("pool_tier") == "watch")
    radar_count = sum(1 for item in candidates if item.get("pool_tier") == "radar")
    entry_signal_count = sum(1 for item in candidates if _truthy(item.get("entry_signal")))
    total = len(candidates)
    return {
        "total": total,
        "core_count": core_count,
        "watch_count": watch_count,
        "radar_count": radar_count,
        "entry_signal_count": entry_signal_count,
        "summary": (
            f"候选池 {total} 只：核心 {core_count}、观察 {watch_count}、强势观察 {radar_count}；"
            f"当前入场信号 {entry_signal_count} 只。"
        ),
    }


def _empty_candidate_summary() -> dict[str, Any]:
    return {
        "total": 0,
        "core_count": 0,
        "watch_count": 0,
        "radar_count": 0,
        "entry_signal_count": 0,
        "summary": "候选池 0 只：核心 0、观察 0、强势观察 0；当前入场信号 0 只。",
    }


def _trial_guard_entry_signal_summary(candidate: dict[str, Any]) -> dict[str, Any]:
    code = str(candidate.get("code") or "")
    tier = str(candidate.get("pool_tier") or "")
    route = candidate.get("primary_strategy_route")
    return {
        "code": code,
        "name": candidate.get("name", ""),
        "pool_tier": tier,
        "pool_tier_label": _pool_tier_label(tier),
        "score": candidate.get("score", 0) or 0,
        "entry_signal": True,
        "primary_strategy_route": route,
        "primary_strategy_route_label": (
            candidate.get("primary_strategy_route_label") or _strategy_route_label(route)
        ),
        "data_quality": candidate.get("data_quality", ""),
        "review_command": f"atrade stock analyze {code} --json" if code else "",
    }


def _trial_guard_blockers(execution_profile: dict[str, Any]) -> list[dict[str, str]]:
    if execution_profile.get("status") != "review_required":
        return []
    target = execution_profile.get("recommended_profile") or "trend_swing"
    return [
        {
            "reason": "profile_review_required",
            "label": "运行 profile 仍需人工确认",
            "command": f"atrade strategy profile-activation --target {target} --json",
        }
    ]


def _trial_guard_next_action(
    *,
    candidate_flow: dict[str, Any],
    execution_profile: dict[str, Any],
) -> dict[str, Any]:
    if execution_profile.get("status") == "review_required":
        target = execution_profile.get("recommended_profile") or "trend_swing"
        return {
            "type": "review_runtime_profile_activation",
            "label": "复核运行 profile 激活",
            "command": f"atrade strategy profile-activation --target {target} --json",
            "reason": "试运行前仍需人工确认运行 profile；该动作只读，不写环境。",
            "safe_to_auto_apply": False,
            **_read_only_action_contract("strategy_profile_activation_review"),
        }
    entry_signals = candidate_flow.get("current_entry_signals", []) or []
    if entry_signals:
        command = entry_signals[0].get("review_command") or "atrade diagnose flow --json"
        return {
            "type": "review_current_entry_signal",
            "label": "复核当前核心入场信号",
            "command": command,
            "reason": "试运行护栏只读；先复核当前入场证据，再看买入意向和窗口。",
            "safe_to_auto_apply": True,
            **_read_only_action_contract("stock_analyze"),
        }
    return {
        "type": "diagnose_flow",
        "label": "诊断候选流",
        "command": "atrade diagnose flow --json",
        "reason": "当前没有可直接复核的入场信号，先看候选流诊断。",
        "safe_to_auto_apply": True,
        **_read_only_action_contract("diagnose_flow"),
    }


def _read_only_action_contract(command_contract_id: str) -> dict[str, Any]:
    return {
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": command_contract_id,
    }


def _pool_tier_label(tier: str) -> str:
    return {"core": "核心", "watch": "观察", "radar": "强势观察"}.get(tier, tier or "未分层")


def _strategy_route_label(route: Any) -> str | None:
    labels = {
        "short_continuation": "短续接力",
        "flow_confirmed_trend": "资金趋势确认",
        "volume_breakout": "放量突破",
        "shrink_pullback": "缩量回踩",
        "ma_golden_cross": "均线金叉",
        "trend_watch": "趋势观察",
        "dragon_head": "龙头策略",
    }
    return labels.get(str(route)) if route else None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "是", "有"}
    return bool(value)


@risk_app.command("adaptive")
def risk_adaptive(
    lookback_days: int = typer.Option(20, "--lookback-days", help="自适应风控证据回看天数"),
    min_market_bars: int = typer.Option(10, "--min-market-bars", help="波动率建议所需的最少 K 线样本"),
    record: bool = typer.Option(False, "--record/--no-record", help="是否记录 risk.adaptive_suggestion.proposed 事件"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """P6-1 自适应风控建议；只读，不自动改配置或下单。"""
    if lookback_days < 1:
        raise typer.BadParameter("--lookback-days must be >= 1")
    if min_market_bars < 1:
        raise typer.BadParameter("--min-market-bars must be >= 1")

    ctx = build_context()
    try:
        payload = run_adaptive_risk(
            ctx.conn,
            lookback_days=lookback_days,
            min_market_bars=min_market_bars,
            record=record,
            config_version=ctx.config_version,
        )
        if as_json:
            json_or_text(payload, True)
            return
        typer.echo(payload["report_markdown"])
    finally:
        ctx.conn.close()


@risk_app.command("check")
def risk_check(
    code: str = typer.Argument(..., help="股票代码"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """检查单只本地持仓的离场风控信号。"""
    init_db()
    conn = connect()
    try:
        from astock_trading.execution.service import ExecutionService

        svc = ExecutionService(EventStore(conn), conn)
        pos = svc.get_position(code)
        if not pos:
            json_or_text({"status": "not_held", "code": code, "signals": []}, as_json)
            return

        style = _position_style(pos.style)
        risk_cfg = _strategy_config().get("risk", {})
        params = get_risk_params(style, risk_cfg)
        today = local_today()
        try:
            entry_date = datetime.strptime(pos.entry_date, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            entry_date = today

        signals = check_exit_signals(
            code=code,
            avg_cost=pos.avg_cost,
            current_price=pos.current_price or pos.avg_cost,
            entry_date=entry_date,
            today=today,
            highest_since_entry=(
                pos.highest_since_entry_cents / 100 if pos.highest_since_entry_cents else pos.avg_cost
            ),
            entry_day_low=pos.entry_day_low_cents / 100 if pos.entry_day_low_cents else pos.avg_cost,
            params=params,
        )
        payload = {
            "status": "ok",
            "code": code,
            "position": pos.to_dict(),
            "signals": [_risk_signal_payload(signal) for signal in signals],
        }
        json_or_text(payload, as_json)
    finally:
        conn.close()


@risk_app.command("portfolio")
def risk_portfolio(
    daily_pnl_pct: float = typer.Option(0.0, "--daily-pnl-pct", help="单日收益率，用小数表示"),
    consecutive_loss_days: int = typer.Option(0, "--consecutive-loss-days", help="连续亏损天数"),
    max_sector_exposure_pct: float = typer.Option(0.0, "--max-sector-exposure-pct", help="最大行业仓位占比"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """检查组合级风控限制；只读。"""
    init_db()
    conn = connect()
    try:
        from astock_trading.execution.service import ExecutionService

        portfolio = ExecutionService(EventStore(conn), conn).get_portfolio()
        positions = portfolio.get("positions", [])
        total_market = portfolio.get("total_market_cents", 0) or 0
        if total_market > 0:
            max_single_exposure_pct = max(
                ((item.get("current_price_cents") or item.get("avg_cost_cents") or 0) * item.get("shares", 0))
                / total_market
                for item in positions
            )
        else:
            max_single_exposure_pct = 0.0

        limits = _risk_limits(_strategy_config())
        breaches = check_portfolio_risk(
            daily_pnl_pct=daily_pnl_pct,
            consecutive_loss_days=consecutive_loss_days,
            max_single_exposure_pct=max_single_exposure_pct,
            max_sector_exposure_pct=max_sector_exposure_pct,
            limits=limits,
        )
        holding_count = int(portfolio.get("holding_count", len(positions)) or 0)
        payload = {
            "status": "breached" if breaches else "ok",
            "scope": "local_projection",
            "summary": (
                f"组合风控检查基于本地投影持仓：{holding_count} 只；"
                "MX 模拟盘持仓需另查 atrade paper status --json。"
            ),
            "portfolio": {
                "scope": "local_projection",
                "holding_count": holding_count,
                "total_market_cents": total_market,
                "positions": positions,
            },
            "paper_account": {
                "status": "not_checked",
                "command": "atrade paper status --json",
                "note": "risk portfolio 只检查本地投影持仓，不代表 MX 模拟盘账户为空。",
            },
            "inputs": {
                "daily_pnl_pct": daily_pnl_pct,
                "consecutive_loss_days": consecutive_loss_days,
                "max_single_exposure_pct": max_single_exposure_pct,
                "max_sector_exposure_pct": max_sector_exposure_pct,
            },
            "breaches": [
                {
                    "rule": breach.rule,
                    "current_value": breach.current_value,
                    "limit_value": breach.limit_value,
                    "description": breach.description,
                }
                for breach in breaches
            ],
        }
        json_or_text(payload, as_json)
    finally:
        conn.close()
