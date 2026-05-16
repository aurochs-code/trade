"""Pipeline MCP tool payload and context helpers."""

from __future__ import annotations

from typing import Any

from astock_trading.pipeline.context import PipelineContext
from astock_trading.platform.pipeline_runner import execute_pipeline
from astock_trading.reporting.obsidian import ObsidianProjector
from astock_trading.reporting.projectors import ProjectionUpdater
from astock_trading.risk.service import RiskService


def build_pipeline_context(
    *,
    conn: Any,
    event_store: Any,
    run_journal: Any,
    config_snapshot: Any,
    market_svc: Any,
    strategy_svc: Any,
    exec_svc: Any,
    reporter: Any,
    vault_path: str | None,
) -> PipelineContext:
    """Build a PipelineContext from already-initialized MCP services."""
    return PipelineContext(
        conn=conn,
        event_store=event_store,
        run_journal=run_journal,
        config_snapshot=config_snapshot,
        market_svc=market_svc,
        strategy_svc=strategy_svc,
        risk_svc=RiskService(event_store),
        exec_svc=exec_svc,
        projector=ProjectionUpdater(event_store, conn),
        reporter=reporter,
        obsidian=ObsidianProjector(event_store, conn, vault_path),
    )


def run_pipeline_payload(ctx: PipelineContext, pipeline_type: str, *, is_trading_day: bool) -> dict:
    """Return the payload for trade_run_pipeline."""
    outcome = execute_pipeline(ctx, pipeline_type, is_trading_day=is_trading_day)
    if outcome.get("status") != "completed":
        return outcome

    result = outcome.pop("result", {})
    outcome.update(result)
    return outcome
