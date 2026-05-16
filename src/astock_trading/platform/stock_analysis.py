"""Single-stock analysis orchestration for CLI and MCP entrypoints."""

from __future__ import annotations

import re
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any, Awaitable, Callable

from astock_trading.market.adapters import MXScreenerAdapter
from astock_trading.market.models import StockSnapshot
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import utc_now_iso
from astock_trading.strategy.decider import build_decider_from_config
from astock_trading.strategy.models import (
    DecisionIntent,
    MarketState,
    ScoreResult,
    ScoringWeights,
)
from astock_trading.strategy.scorer import Scorer

StockResolver = Callable[[str], Awaitable[list[dict]]]

_CODE_RE = re.compile(r"^(?:(?:sh|sz)\.?)?(\d{6})$", re.IGNORECASE)


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


async def resolve_stock_identifier(
    identifier: str,
    resolver: StockResolver | None = None,
) -> dict:
    """Resolve stock code or Chinese name to a code/name pair."""
    query = str(identifier or "").strip()
    if not query:
        raise StockAnalysisError("stock identifier is required")

    code = normalize_stock_code(query)
    if code:
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
    resolved = await resolve_stock_identifier(identifier, resolver=resolver)
    code = resolved["code"]
    name = resolved.get("name") or ""

    snapshot, market_result = await _collect_inputs(ctx, code, name)
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
        decision_inputs={
            "current_exposure_pct": round(current_exposure_pct, 4),
            "weekly_buy_count": weekly_buy_count,
        },
    )


async def _collect_inputs(ctx: Any, code: str, name: str) -> tuple[StockSnapshot, tuple[MarketState, dict]]:
    snapshot = await ctx.market_svc.collect_snapshot(code, name=name, run_id=None)
    market_result = await ctx.market_svc.collect_market_state(run_id=None)
    return snapshot, market_result


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
    )


def _portfolio_inputs(ctx: Any) -> tuple[float, int]:
    try:
        from astock_trading.pipeline.helpers import get_current_exposure

        return get_current_exposure(ctx)
    except Exception:
        return 0.0, 0


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
    decision_inputs: dict | None = None,
) -> dict:
    """Compose the public, stable stock analysis payload."""
    score_payload = score.to_dict()
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
    findings = _findings(snapshot, score, decision, candidate_pool)
    recommendations = _recommendations(decision)

    return {
        "analysis": "stock",
        "status": "ok",
        "generated_at": utc_now_iso(),
        "identifier": identifier,
        "resolved": resolved,
        "profile": profile,
        "config_version": config_version,
        "execution_allowed": False,
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
        "history": history or [],
        "findings": findings,
        "recommendations": recommendations,
    }


def _findings(
    snapshot: StockSnapshot,
    score: ScoreResult,
    decision: DecisionIntent,
    candidate_pool: dict | None,
) -> list[str]:
    findings: list[str] = []
    if snapshot.quote is None:
        findings.append("quote data unavailable")
    if snapshot.technical is None:
        findings.append("technical indicators unavailable")
    if score.veto_triggered:
        findings.append("hard veto triggered: " + ",".join(score.hard_veto))
    if score.warning_signals:
        findings.append("warning signals: " + ",".join(score.warning_signals))
    if not score.entry_signal:
        findings.append("entry signal not triggered")
    if score.data_missing_fields:
        findings.append("missing data fields: " + ",".join(score.data_missing_fields))
    if candidate_pool is None:
        findings.append("not in candidate pool")
    if decision.notes:
        findings.extend(decision.notes)
    return findings


def _recommendations(decision: DecisionIntent) -> list[str]:
    base = ["manual confirmation required before any order; this report never executes trades"]
    if decision.action.value == "BUY":
        return base + ["treat BUY as a candidate intent, then verify price, liquidity, and portfolio risk"]
    if decision.action.value == "WATCH":
        return base + ["keep on watchlist until score, entry signal, and market gate align"]
    return base + ["avoid new exposure until score, veto, or market conditions improve"]
