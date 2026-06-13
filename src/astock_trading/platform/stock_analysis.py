"""Single-stock analysis orchestration for CLI and MCP entrypoints."""

from __future__ import annotations

import asyncio
import re
import json
from dataclasses import asdict, is_dataclass, replace
from enum import Enum
from typing import Any, Awaitable, Callable

from astock_trading.market.adapters import MXScreenerAdapter
from astock_trading.market.models import StockSnapshot
from astock_trading.platform.events import EventStore
from astock_trading.platform.history_mirror import diagnose_signal_history
from astock_trading.platform.time import utc_now_iso
from astock_trading.strategy.decider import build_decider_from_config
from astock_trading.strategy.models import (
    DecisionIntent,
    MarketSignal,
    MarketState,
    ScoreResult,
    ScoringWeights,
)
from astock_trading.strategy.scorer import Scorer

StockResolver = Callable[[str], Awaitable[list[dict]]]
StockLookup = Callable[[str], dict | None]

_CODE_RE = re.compile(r"^(?:(?:sh|sz)\.?)?(\d{6})$", re.IGNORECASE)
ENTRY_VOLUME_CONFIRM_MIN = 1.2
ENTRY_RSI_MAX = 70.0
ENTRY_DEVIATION_MAX = 10.0
ENTRY_CHASE_CHANGE_PCT = 8.0


class StockAnalysisError(ValueError):
    """Raised when a stock identifier cannot be resolved."""


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {key: _jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    return value


def normalize_stock_code(value: str) -> str | None:
    """Return a 6-digit A-share code when value already looks like a code."""
    match = _CODE_RE.match(str(value or "").strip())
    if match:
        return match.group(1)
    return None


def _row_code(row: dict) -> str | None:
    for key in ("code", "代码", "股票代码", "证券代码"):
        value = row.get(key)
        if value:
            match = re.search(r"(\d{6})", str(value))
            if match:
                return match.group(1)
    return None


def _row_name(row: dict, fallback: str) -> str:
    for key in ("name", "名称", "股票名称", "证券名称"):
        value = row.get(key)
        if value:
            return str(value)
    return fallback


def _decode_payload(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


def _iter_stock_records(payload: Any) -> list[dict]:
    records: list[dict] = []
    if isinstance(payload, list):
        for item in payload:
            records.extend(_iter_stock_records(item))
        return records
    if not isinstance(payload, dict):
        return records

    code = _row_code(payload)
    name = _row_name(payload, "")
    if code and name:
        records.append({"code": code, "name": name})

    for value in payload.values():
        if isinstance(value, (dict, list)):
            records.extend(_iter_stock_records(value))
    return records


def lookup_stock_identifier_from_db(conn: Any, identifier: str) -> dict | None:
    """Resolve a stock identifier from local projections and recent observations."""
    query = str(identifier or "").strip()
    if not query:
        return None

    code = normalize_stock_code(query)
    if code:
        row = conn.execute(
            """SELECT code, name
               FROM projection_candidate_pool
               WHERE code = ? AND COALESCE(name, '') <> ''
               ORDER BY CASE pool_tier WHEN 'core' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END,
                        last_scored_at DESC
               LIMIT 1""",
            (code,),
        ).fetchone()
        if row:
            return {"code": row["code"], "name": row["name"], "source": "local_cache"}

        rows = conn.execute(
            """SELECT payload_json
               FROM market_observations
               WHERE symbol = ?
               ORDER BY observed_at DESC
               LIMIT 20""",
            (code,),
        ).fetchall()
        for row in rows:
            for record in _iter_stock_records(_decode_payload(row["payload_json"])):
                if record["code"] == code:
                    return {**record, "source": "local_cache"}
        return None

    row = conn.execute(
        """SELECT code, name
           FROM projection_candidate_pool
           WHERE name = ?
           ORDER BY CASE pool_tier WHEN 'core' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END,
                    last_scored_at DESC
           LIMIT 1""",
        (query,),
    ).fetchone()
    if row:
        return {"code": row["code"], "name": row["name"], "source": "local_cache"}

    rows = conn.execute(
        """SELECT payload_json
           FROM market_observations
           ORDER BY observed_at DESC
           LIMIT 1000"""
    ).fetchall()
    for row in rows:
        for record in _iter_stock_records(_decode_payload(row["payload_json"])):
            if record["name"] == query:
                return {**record, "source": "local_cache"}
    return None


async def _lookup_stock_name_from_basic_info(code: str) -> str | None:
    try:
        from astock_trading.market.a_stock_adapters import AStockSignalAdapter

        info = await AStockSignalAdapter().get_basic_info(code)
    except Exception:
        return None

    for key in ("股票简称", "证券简称", "股票名称", "名称"):
        value = info.get(key)
        if value:
            name = str(value).strip()
            if name and name != code:
                return name
    return None


async def _lookup_stock_from_spot(identifier: str) -> dict | None:
    query = str(identifier or "").strip()
    if not query:
        return None

    def _sync() -> dict | None:
        try:
            import akshare as ak

            df = ak.stock_zh_a_spot_em()
        except Exception:
            return None
        if df is None or df.empty:
            return None

        code = normalize_stock_code(query)
        for _, row in df.iterrows():
            row_code = _row_code(row)
            row_name = _row_name(row, "")
            if not row_code or not row_name:
                continue
            if (code and row_code == code) or (not code and row_name == query):
                return {"code": row_code, "name": row_name, "source": "spot"}
        return None

    return await asyncio.to_thread(_sync)


async def resolve_stock_identifier(
    identifier: str,
    resolver: StockResolver | None = None,
    name_lookup: StockLookup | None = None,
) -> dict:
    """Resolve stock code or Chinese name to a code/name pair."""
    query = str(identifier or "").strip()
    if not query:
        raise StockAnalysisError("stock identifier is required")

    code = normalize_stock_code(query)
    if code:
        local = name_lookup(query) if name_lookup else None
        if local:
            return local
        search = resolver or MXScreenerAdapter().search_stocks
        rows = await search(query)
        for row in rows:
            if _row_code(row) == code:
                return {"code": code, "name": _row_name(row, query), "source": "screener"}
        if name := await _lookup_stock_name_from_basic_info(code):
            return {"code": code, "name": name, "source": "basic_info"}
        if spot := await _lookup_stock_from_spot(query):
            return spot
        return {"code": code, "name": "", "source": "input_code"}

    search = resolver or MXScreenerAdapter().search_stocks
    rows = await search(query)
    candidates = [
        {
            "code": code_value,
            "name": _row_name(row, query),
            "source": "screener",
        }
        for row in rows
        if (code_value := _row_code(row))
    ]
    if not candidates:
        local = name_lookup(query) if name_lookup else None
        if local:
            return local
        if spot := await _lookup_stock_from_spot(query):
            return spot
        raise StockAnalysisError(f"cannot resolve stock identifier: {identifier}")

    exact = next((item for item in candidates if item["name"] == query), None)
    return exact or candidates[0]


async def analyze_stock(
    identifier: str,
    ctx: Any,
    *,
    history_days: int = 7,
    resolver: StockResolver | None = None,
) -> dict:
    """Build a non-executing single-stock analysis report."""
    cfg = ctx.cfg
    resolved = await resolve_stock_identifier(
        identifier,
        resolver=resolver,
        name_lookup=lambda value: lookup_stock_identifier_from_db(ctx.conn, value),
    )
    code = resolved["code"]
    name = resolved.get("name") or ""

    snapshot, market_result = await _collect_inputs(ctx, code, name)
    if name:
        snapshot = _with_resolved_snapshot_name(snapshot, name)
    market_state = market_result[0]

    scorer = _build_scorer(cfg)
    decider = build_decider_from_config(cfg)
    score = scorer.score(snapshot)
    current_exposure_pct, weekly_buy_count = _portfolio_inputs(ctx)
    decision = decider.decide(
        score,
        market_state,
        current_exposure_pct=current_exposure_pct,
        weekly_buy_count=weekly_buy_count,
    )

    return build_stock_analysis_payload(
        identifier=identifier,
        resolved={
            **resolved,
            "name": resolved.get("name") or snapshot.name or code,
        },
        snapshot=snapshot,
        score=score,
        decision=decision,
        market_state=market_state,
        profile=_profile_name(),
        config_version=ctx.config_version,
        candidate_pool=_candidate_pool_row(ctx.conn, code),
        history=_score_history(ctx.conn, code, history_days),
        history_signal=_recent_history_signal_analysis(ctx.conn, code, history_days),
        execution_readiness=_execution_readiness_for_stock(ctx),
        decision_inputs={
            "current_exposure_pct": round(current_exposure_pct, 4),
            "weekly_buy_count": weekly_buy_count,
        },
    )


async def _collect_inputs(ctx: Any, code: str, name: str) -> tuple[StockSnapshot, tuple[MarketState, dict]]:
    snapshot = await ctx.market_svc.collect_snapshot(code, name=name, run_id=None)
    market_result = await ctx.market_svc.collect_market_state(run_id=None)
    return snapshot, market_result


def _with_resolved_snapshot_name(snapshot: StockSnapshot, name: str) -> StockSnapshot:
    quote = snapshot.quote
    if quote is not None and (not quote.name or quote.name == quote.code):
        quote = replace(quote, name=name)
    if snapshot.name != name or quote is not snapshot.quote:
        return replace(snapshot, name=name, quote=quote)
    return snapshot


def _build_scorer(cfg: dict) -> Scorer:
    weights_cfg = cfg.get("scoring", {}).get("weights", {})
    return Scorer(
        weights=ScoringWeights(
            technical=weights_cfg.get("technical", 3),
            fundamental=weights_cfg.get("fundamental", 2),
            flow=weights_cfg.get("flow", 2),
            sentiment=weights_cfg.get("sentiment", 3),
        ),
        veto_rules=cfg.get("scoring", {}).get("veto", []),
        entry_cfg=cfg.get("entry_signal", {}),
        continuation_cfg=cfg.get("continuation", {}),
    )


def _portfolio_inputs(ctx: Any) -> tuple[float, int]:
    try:
        from astock_trading.pipeline.helpers import get_current_exposure

        return get_current_exposure(ctx)
    except Exception:
        return 0.0, 0


def _execution_readiness_for_stock(ctx: Any) -> dict | None:
    try:
        from astock_trading.pipeline.auto_trade import build_auto_trade_readiness

        return build_auto_trade_readiness(ctx, include_account=False)
    except Exception:
        return None


def _candidate_pool_row(conn: Any, code: str) -> dict | None:
    row = conn.execute(
        """SELECT code, pool_tier, name, score, added_at, last_scored_at,
                  streak_days, note
           FROM projection_candidate_pool
           WHERE code = ?""",
        (code,),
    ).fetchone()
    return dict(row) if row else None


def _score_history(conn: Any, code: str, days: int) -> list[dict]:
    events = EventStore(conn).query(stream=f"strategy:{code}", event_type="score.calculated")
    recent = events[-days:] if len(events) > days else events
    return [
        {
            "date": event.get("occurred_at", "")[:10],
            "total_score": event["payload"].get("total_score", event["payload"].get("total", 0)),
            "style": event["payload"].get("style", ""),
            "veto_triggered": event["payload"].get("veto_triggered", False),
        }
        for event in recent
    ]


def _recent_history_signal_analysis(conn: Any, code: str, days: int) -> dict | None:
    try:
        rows = conn.execute(
            """SELECT snapshot_date, history_group_id, run_id, phase,
                      MAX(created_at) AS created_at
               FROM signal_history_snapshots
               WHERE phase IN ('screener', 'scoring')
               GROUP BY snapshot_date, history_group_id, run_id, phase
               ORDER BY snapshot_date DESC, created_at DESC
               LIMIT ?""",
            (max(int(days or 1), 1),),
        ).fetchall()
    except Exception:
        return None

    for row in rows:
        row_dict = dict(row)
        payload = diagnose_signal_history(
            conn,
            snapshot_date=row_dict["snapshot_date"],
            history_group_id=row_dict["history_group_id"],
            code=code,
        )
        analysis = payload.get("code_analysis") or {}
        if analysis:
            return {
                "source": "history_mirror",
                "snapshot_date": payload.get("snapshot_date", ""),
                "history_group_id": payload.get("history_group_id", ""),
                "run_id": payload.get("run_id", ""),
                "phase": payload.get("phase", ""),
                "decision_action": analysis.get("decision_action", ""),
                "miss_reason": analysis.get("miss_reason", ""),
                "candidate": analysis.get("candidate"),
                "decision": analysis.get("decision"),
                "pool_item": analysis.get("pool_item"),
            }
    return None


def _profile_name() -> str:
    import os

    return os.getenv("ASTOCK_CONFIG_PROFILE", "default")


def build_stock_analysis_payload(
    *,
    identifier: str,
    resolved: dict,
    snapshot: StockSnapshot,
    score: ScoreResult,
    decision: DecisionIntent,
    market_state: MarketState,
    profile: str,
    config_version: str,
    candidate_pool: dict | None = None,
    history: list[dict] | None = None,
    history_signal: dict | None = None,
    execution_readiness: dict | None = None,
    decision_inputs: dict | None = None,
) -> dict:
    """Compose the public, stable stock analysis payload."""
    score_payload = _score_payload_for_report(score)
    decision_payload = {
        "action": decision.action.value,
        "confidence": decision.confidence,
        "score": decision.score,
        "position_pct": decision.position_pct,
        "market_signal": decision.market_signal.value,
        "market_multiplier": decision.market_multiplier,
        "veto_reasons": decision.veto_reasons,
        "notes": decision.notes,
    }
    candidate_pool_consistency = _candidate_pool_consistency(score, decision, candidate_pool)
    execution_signal_gap = _has_execution_signal_gap(execution_readiness, code=score.code)
    findings = _findings(
        snapshot,
        score,
        decision,
        candidate_pool,
        history_signal,
        candidate_pool_consistency=candidate_pool_consistency,
        execution_signal_gap=execution_signal_gap,
    )
    recommendations = _recommendations(decision, candidate_pool_consistency)
    next_action = _stock_analysis_next_action(
        candidate_pool_consistency,
        decision=decision,
        score=score,
        candidate_pool=candidate_pool,
        execution_readiness=execution_readiness,
        execution_signal_gap=execution_signal_gap,
    )
    code = str(score.code or resolved.get("code") or snapshot.code or identifier)
    name = str(score.name or resolved.get("name") or snapshot.name or code)
    score_total = round(float(score.total or 0), 2)
    action = decision.action.value

    return {
        "analysis": "stock",
        "status": "ok",
        "generated_at": utc_now_iso(),
        "code": code,
        "name": name,
        "score_total": score_total,
        "action": action,
        "action_label": _action_label(action),
        "entry_signal": bool(score.entry_signal),
        "summary": _stock_analysis_summary(
            code=code,
            name=name,
            score_total=score_total,
            action=action,
            entry_signal=bool(score.entry_signal),
            candidate_pool=candidate_pool,
            next_action=next_action,
            execution_signal_gap=execution_signal_gap,
        ),
        "identifier": identifier,
        "resolved": resolved,
        "profile": profile,
        "config_version": config_version,
        "execution_allowed": False,
        "decision_scope": _decision_scope(),
        "execution_readiness": execution_readiness,
        "market": {
            "signal": market_state.signal.value,
            "multiplier": market_state.multiplier,
            "detail": market_state.detail,
        },
        "quote": _jsonable(snapshot.quote),
        "technical": _jsonable(snapshot.technical),
        "fundamental": _jsonable(snapshot.financial),
        "flow": _jsonable(snapshot.flow),
        "sentiment": _jsonable(snapshot.sentiment),
        "score": score_payload,
        "decision": decision_payload,
        "decision_inputs": decision_inputs or {},
        "candidate_pool": candidate_pool,
        "candidate_pool_consistency": candidate_pool_consistency,
        "history": history or [],
        "history_signal": history_signal,
        "findings": findings,
        "recommendations": recommendations,
        "next_action": next_action,
    }


def _action_label(action: str) -> str:
    labels = {
        "BUY": "买入意向",
        "TRIAL_BUY": "试买意向",
        "SELL": "卖出意向",
        "WATCH": "观察",
        "NO_TRADE": "不操作",
        "CLEAR": "观望",
        "HOLD": "持有",
    }
    return labels.get(action, action)


def _score_payload_for_report(score: ScoreResult) -> dict:
    payload = score.to_dict()
    payload["primary_strategy_route_label"] = _primary_strategy_route_label(
        payload.get("strategy_routes") or [],
        payload.get("primary_strategy_route"),
    )
    return payload


def _primary_strategy_route_label(routes: list[dict], primary_route: Any) -> str | None:
    primary = str(primary_route or "")
    if not primary:
        return None
    for route in routes:
        if not isinstance(route, dict):
            continue
        if str(route.get("route") or "") == primary:
            label = str(route.get("display_name") or "").strip()
            return label or None
    return None


def _pool_tier_label(pool_tier: str) -> str:
    labels = {
        "core": "核心",
        "watch": "观察",
        "radar": "强势观察",
    }
    return labels.get(pool_tier, pool_tier or "不在候选池")


def _decision_scope() -> dict:
    return {
        "type": "read_only_instant_analysis",
        "summary": (
            "本命令只做即时单股分析，不写入 decision.suggested；"
            "模拟承接以同日新鲜买入意向和 paper auto-readiness 为准。"
        ),
        "writes_state": False,
        "writes_decision_event": False,
        "execution_allowed": False,
    }


def _has_execution_signal_gap(execution_readiness: dict | None, *, code: str) -> bool:
    if not execution_readiness:
        return False
    buy_side = execution_readiness.get("buy_side") or {}
    gap = buy_side.get("signal_gap") or {}
    if gap.get("status") != "entry_signal_without_fresh_buy_intent":
        return False
    entries = buy_side.get("current_entry_signals") or []
    if not entries:
        return True
    return any(str(item.get("code") or "") == str(code or "") for item in entries)


def _stock_analysis_summary(
    *,
    code: str,
    name: str,
    score_total: float,
    action: str,
    entry_signal: bool,
    candidate_pool: dict | None,
    next_action: dict | None,
    execution_signal_gap: bool = False,
) -> str:
    entry_label = "入场信号已触发" if entry_signal else "入场信号未触发"
    pool_tier = str((candidate_pool or {}).get("pool_tier") or "")
    pool_label = _pool_tier_label(pool_tier)
    next_label = str((next_action or {}).get("label") or "继续观察")
    execution_note = (
        "该结论是只读即时判断，尚未形成可承接的同日买入意向。"
        if execution_signal_gap
        else ""
    )
    return (
        f"{name}({code}) 评分 {score_total}，{_action_label(action)}，{entry_label}；"
        f"候选池层级：{pool_label}。{execution_note}下一步：{next_label}。"
    )


def _candidate_pool_consistency(
    score: ScoreResult,
    decision: DecisionIntent,
    candidate_pool: dict | None,
) -> dict:
    current_action = decision.action.value
    current_score = round(float(score.total or 0), 2)
    base = {
        "current_action": current_action,
        "current_score": current_score,
        "requires_pool_refresh": False,
        "diagnostic_commands": {
            "refresh_candidate_pool": "atrade screener refresh --json",
            "diagnose_flow": "atrade diagnose flow --json",
        },
    }
    if candidate_pool is None:
        return {
            "status": "not_in_pool",
            "summary": "当前股票不在候选池；单股分析只作为复核证据，不触发模拟买入。",
            **base,
        }

    pool_tier = str(candidate_pool.get("pool_tier") or "")
    pool_score = round(float(candidate_pool.get("score") or 0), 2)
    score_delta = round(current_score - pool_score, 2)
    detail = {
        "pool_tier": pool_tier,
        "pool_score": pool_score,
        "pool_last_scored_at": candidate_pool.get("last_scored_at") or "",
        **base,
        "score_delta": score_delta,
    }
    if pool_tier == "core" and current_action != "BUY":
        if score.entry_signal and current_score >= pool_score and _decision_execution_gate_blocks_buy(decision):
            return {
                "status": "execution_gate_blocked",
                "summary": (
                    "当前单股评分和核心候选一致，入场信号已触发；但大盘或执行闸门暂不允许"
                    "形成可承接买入意向，先查看 profile、模拟预检和下个买入窗口，不刷新候选池。"
                ),
                **detail,
                "execution_gate": {
                    "market_signal": decision.market_signal.value,
                    "market_multiplier": decision.market_multiplier,
                    "notes": decision.notes,
                },
            }
        return {
            "status": "current_analysis_weaker_than_pool",
            "summary": "当前单股即时判断为观察，但候选池仍显示核心；先刷新候选池证据，不把旧核心状态当作可模拟买入依据。",
            **detail,
            "requires_pool_refresh": True,
        }
    if pool_tier in {"watch", "radar"} and current_action == "BUY":
        return {
            "status": "current_analysis_stronger_than_pool",
            "summary": "当前单股即时判断已形成买入意向，但候选池仍未进入核心；先刷新候选池证据，再看模拟承接预检。",
            **detail,
            "requires_pool_refresh": True,
        }
    if abs(score_delta) >= 1.0:
        return {
            "status": "score_drift",
            "summary": "当前单股即时评分与候选池评分差异较大；先刷新候选池证据，再复核下一步。",
            **detail,
            "requires_pool_refresh": True,
        }
    return {
        "status": "aligned",
        "summary": "当前单股即时判断与候选池状态基本一致。",
        **detail,
    }


def _stock_analysis_next_action(
    candidate_pool_consistency: dict,
    *,
    decision: DecisionIntent,
    score: ScoreResult,
    candidate_pool: dict | None,
    execution_readiness: dict | None = None,
    execution_signal_gap: bool = False,
) -> dict | None:
    if candidate_pool_consistency.get("requires_pool_refresh"):
        return {
            "type": "refresh_candidate_pool_state",
            "label": "刷新候选池证据",
            "command": "atrade screener refresh --json",
            "reason": "当前单股即时判断与候选池层级不一致，先重建候选池证据。",
            "safe_to_auto_apply": False,
            **_stock_analysis_action_contract(
                "screener_refresh",
                writes_state=True,
                risk_level="state_write",
            ),
        }
    if decision.action.value == "TRIAL_BUY":
        return {
            "type": "trial_buy_risk_guard",
            "label": "计算试买仓位上限",
            "command": "atrade risk trial-guard --json",
            "reason": "当前是试买意向；系统只给低置信小仓判断，不写成交、不提交模拟盘订单。",
            "safe_to_auto_apply": True,
            **_stock_analysis_action_contract("risk_trial_guard"),
        }
    pool_tier = str((candidate_pool or {}).get("pool_tier") or "")
    if pool_tier == "core" and score.entry_signal:
        if profile_action := _stock_analysis_profile_review_action(execution_readiness):
            if decision.action.value != "BUY":
                profile_action["reason"] = (
                    "当前单股入场信号仍在，但大盘或执行闸门尚未形成可承接买入意向；"
                    "运行 profile 仍需人工确认，先只读复核 profile 激活计划。"
                )
            return profile_action
        if execution_signal_gap:
            return {
                "type": "wait_for_fresh_buy_signal",
                "label": "等待同日买入意向",
                "command": "atrade diagnose flow --json",
                "reason": "当前单股即时判断已有入场信号，但尚未形成可承接的同日买入意向；查看候选流和下个买入窗口，不进入模拟承接。",
                "safe_to_auto_apply": True,
                **_stock_analysis_action_contract("diagnose_flow"),
            }
        if decision.action.value != "BUY":
            return {
                "type": "inspect_execution_gate",
                "label": "复核执行闸门",
                "command": "atrade diagnose flow --json",
                "reason": "当前仍是核心入场信号，但即时决策被大盘或执行闸门压成观察；先看候选流、profile 和下个窗口，不刷新候选池。",
                "safe_to_auto_apply": True,
                **_stock_analysis_action_contract("diagnose_flow"),
            }
        return {
            "type": "paper_auto_readiness",
            "label": "复核模拟承接预检",
            "command": "atrade paper auto-readiness --json",
            "reason": "当前是核心候选且已形成买入意向；先检查 profile、买入窗口、风控和模拟盘承接状态，不自动下单。",
            "safe_to_auto_apply": True,
            **_stock_analysis_action_contract("paper_auto_readiness"),
        }
    if pool_tier in {"watch", "radar"} and decision.action.value == "WATCH" and not score.entry_signal:
        return {
            "type": "continue_shadow_trial",
            "label": "继续影子观察",
            "command": "atrade paper trial-plan --json",
            "reason": "当前仍是观察候选且入场信号未触发；继续用影子试运行清单跟踪，不进入模拟买入。",
            "safe_to_auto_apply": True,
            **_stock_analysis_action_contract("paper_trial_plan"),
        }
    return None


def _decision_execution_gate_blocks_buy(decision: DecisionIntent) -> bool:
    if decision.market_multiplier <= 0:
        return True
    if decision.market_signal == MarketSignal.RED:
        return True
    return any("禁止新开仓" in str(note) or "大盘" in str(note) for note in decision.notes)


def _stock_analysis_action_contract(
    command_contract_id: str,
    *,
    writes_state: bool = False,
    risk_level: str = "read_only",
) -> dict:
    return {
        "writes_state": writes_state,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": risk_level,
        "command_contract_id": command_contract_id,
    }


def _stock_analysis_profile_review_action(execution_readiness: dict | None) -> dict | None:
    if not execution_readiness:
        return None
    execution_profile = execution_readiness.get("execution_profile") or {}
    status = str(execution_readiness.get("status") or "")
    profile_status = str(execution_profile.get("status") or "")
    if status != "profile_review_required" and profile_status != "review_required":
        return None
    readiness_action = execution_readiness.get("next_action") or {}
    return {
        "type": str(readiness_action.get("type") or "review_runtime_profile_activation"),
        "label": "复核运行 profile 激活",
        "command": str(
            readiness_action.get("command") or "atrade strategy profile-activation --target trend_swing --json"
        ),
        "reason": "当前单股分析是只读买入意向，但运行 profile 仍需人工确认；先只读复核 profile 激活计划，不进入模拟承接。",
        "safe_to_auto_apply": False,
        **_stock_analysis_action_contract("strategy_profile_activation_review"),
    }


def _findings(
    snapshot: StockSnapshot,
    score: ScoreResult,
    decision: DecisionIntent,
    candidate_pool: dict | None,
    history_signal: dict | None = None,
    candidate_pool_consistency: dict | None = None,
    execution_signal_gap: bool = False,
) -> list[str]:
    findings: list[str] = []
    if snapshot.quote is None:
        findings.append("行情报价不可用")
    if snapshot.technical is None:
        findings.append("技术指标不可用")
    if score.veto_triggered:
        findings.append("触发硬否决：" + "，".join(score.hard_veto))
    if score.warning_signals:
        findings.append("预警信号：" + "，".join(score.warning_signals))
    if not score.entry_signal:
        findings.append("入场信号未触发")
        entry_blockers = _entry_signal_blockers(snapshot)
        if entry_blockers:
            findings.append("入场阻断：" + "；".join(entry_blockers))
        route_gap = _route_gap_finding(score)
        if route_gap:
            findings.append(route_gap)
    if score.data_missing_fields:
        findings.append("缺失数据字段：" + "，".join(score.data_missing_fields))
    if candidate_pool is None:
        findings.append("不在候选池")
    if decision.notes:
        for note in decision.notes:
            if note not in findings:
                findings.append(note)
    if history_signal and history_signal.get("miss_reason"):
        findings.append(f"历史镜像：{history_signal['miss_reason']}")
    if candidate_pool_consistency and candidate_pool_consistency.get("requires_pool_refresh"):
        findings.append(f"候选池一致性：{candidate_pool_consistency.get('summary')}")
    if (candidate_pool_consistency or {}).get("status") == "execution_gate_blocked":
        findings.append(f"执行闸门：{candidate_pool_consistency.get('summary')}")
    if execution_signal_gap:
        findings.append("模拟承接：只读即时判断已有买入意向，但尚未形成可承接的同日买入意向")
    return findings


def _entry_signal_blockers(snapshot: StockSnapshot) -> list[str]:
    t = snapshot.technical
    if t is None:
        return []

    blockers = []
    if not t.golden_cross:
        blockers.append("未出现金叉")
    if t.volume_ratio <= 0:
        blockers.append("量比缺失或不可用")
    elif t.volume_ratio < ENTRY_VOLUME_CONFIRM_MIN:
        blockers.append(
            f"量能确认不足（量比 {t.volume_ratio:.2f} < {ENTRY_VOLUME_CONFIRM_MIN:.1f}）"
        )
    if t.rsi >= ENTRY_RSI_MAX:
        blockers.append(f"RSI 过热（{t.rsi:.1f} >= {ENTRY_RSI_MAX:.0f}）")
    if t.deviation_rate > ENTRY_DEVIATION_MAX:
        blockers.append(
            f"乖离率过高（{t.deviation_rate:.1f}% > {ENTRY_DEVIATION_MAX:.0f}%），追高风险"
        )

    change_pct = snapshot.quote.change_pct if snapshot.quote else t.change_pct
    if change_pct >= ENTRY_CHASE_CHANGE_PCT:
        blockers.append(
            f"当日涨幅较大（{change_pct:.1f}% >= {ENTRY_CHASE_CHANGE_PCT:.0f}%），追高风险"
        )
    return blockers


def _route_gap_finding(score: ScoreResult) -> str | None:
    diagnostics = list(score.route_diagnostics or [])
    if not diagnostics:
        diagnostics = list(score.strategy_routes or [])
    if not diagnostics:
        return None

    primary_route = str(score.primary_strategy_route or "")

    def sort_key(item: Any) -> tuple[int, int, float]:
        route = str(_route_field(item, "route", "") or "")
        status = str(_route_field(item, "status", "") or "")
        try:
            route_score = float(_route_field(item, "route_score", 0.0) or 0.0)
        except (TypeError, ValueError):
            route_score = 0.0
        primary_rank = 0 if primary_route and route == primary_route else 1
        status_rank = 0 if status in {"watch", "blocked"} else 1
        return primary_rank, status_rank, -route_score

    for item in sorted(diagnostics, key=sort_key):
        missing = [str(value) for value in (_route_field(item, "missing_conditions", []) or [])]
        if not missing:
            continue
        display_name = str(_route_field(item, "display_name", "") or "策略路线")
        try:
            route_score = float(_route_field(item, "route_score", 0.0) or 0.0)
        except (TypeError, ValueError):
            route_score = 0.0
        missing_labels = "，".join(_route_condition_label(value) for value in missing)
        return f"路线缺口：{display_name} 命中率 {route_score:.0%}，还缺 {missing_labels}"
    return None


def _route_field(route: object, field: str, default: object = None) -> object:
    if isinstance(route, dict):
        return route.get(field, default)
    return getattr(route, field, default)


def _route_condition_label(condition: str) -> str:
    labels = {
        "recent_golden_cross": "近期金叉/均线结构",
        "golden_cross": "金叉",
        "above_ma20": "站上 MA20",
        "relative_volume_pullback": "相对量能回踩",
        "volume_ratio": "量比确认",
        "volume_ratio_min": "量比下限",
        "flow_strength": "资金强度",
        "liquidity": "成交额流动性",
        "rsi_range": "RSI 区间",
        "ma20_slope": "MA20 斜率",
        "momentum_5d": "5 日动量",
        "deviation_risk": "乖离率风险",
        "change_pct_risk": "当日涨幅风险",
        "change_pct": "当日强度",
        "sector_strength": "板块强度确认",
    }
    return labels.get(condition, condition)


def _recommendations(
    decision: DecisionIntent,
    candidate_pool_consistency: dict | None = None,
) -> list[str]:
    base = ["任何下单前都需要人工确认；本报告只读，不执行交易。"]
    if candidate_pool_consistency and candidate_pool_consistency.get("requires_pool_refresh"):
        base.append("先刷新候选池证据，再把旧候选池层级作为模拟承接依据")
    if decision.action.value == "BUY":
        return base + ["买入意向只作为候选信号；继续复核价格、流动性和组合风险。"]
    if decision.action.value == "TRIAL_BUY":
        return base + ["试买意向不会触发自动下单；它表示系统给出低置信小仓判断，并应继续记录影子表现。"]
    if decision.action.value == "WATCH":
        return base + ["继续观察，等待评分、入场信号和市场门控同时对齐。"]
    return base + ["评分、否决项或市场环境改善前，不新增仓位。"]
