"""Shared pipeline execution for CLI and MCP surfaces."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from astock_trading.platform.pipeline_policy import data_source_gate_decision, should_skip_pipeline
from astock_trading.platform.time import local_now, local_today_str

VALID_PIPELINES = (
    "morning",
    "noon",
    "intraday_monitor",
    "evening",
    "scoring",
    "weekly",
    "monthly",
    "sentiment",
    "auto_trade",
)
PIPELINE_HELP = ",".join(VALID_PIPELINES)

PipelineCallback = Callable[[str, str, dict | None], None]
_logger = logging.getLogger(__name__)


def execute_pipeline(
    ctx: Any,
    pipeline_type: str,
    *,
    is_trading_day: bool,
    ignore_data_source_health: bool = False,
    on_started: PipelineCallback | None = None,
    on_data_source_warning: PipelineCallback | None = None,
) -> dict:
    """Run a pipeline with the shared skip and data-source health policy."""
    skip_reason = should_skip_pipeline(
        pipeline_type,
        is_trading_day=is_trading_day,
        is_completed_today=ctx.run_journal.is_completed_today(pipeline_type),
    )
    if skip_reason == "non_trading_day":
        message = f"今日（{local_today_str()}）非交易日，{pipeline_type} 跳过"
        return {
            "status": "skipped",
            "pipeline": pipeline_type,
            "reason": skip_reason,
            "message": message,
        }
    if skip_reason == "completed_today":
        message = f"{pipeline_type} 今日已完成，跳过"
        return {
            "status": "skipped",
            "pipeline": pipeline_type,
            "reason": skip_reason,
            "message": message,
        }

    run_id = ctx.run_journal.start_run(pipeline_type, _config_version(ctx))
    if on_started:
        on_started(pipeline_type, run_id, None)

    try:
        data_health = None
        data_source_refresh = None
        data_source_warning = None
        if not ignore_data_source_health:
            from astock_trading.market.health import (
                evaluate_data_source_health,
                record_data_source_health_snapshot,
            )

            data_health = evaluate_data_source_health(ctx.conn)
            _record_data_source_health_snapshot(
                record_data_source_health_snapshot,
                ctx.conn,
                data_health,
                run_id=run_id,
            )
            gate = data_source_gate_decision(pipeline_type, data_health)
            if gate == "failed":
                from astock_trading.platform.data_source_refresh import refresh_required_data_sources

                data_source_refresh = refresh_required_data_sources(ctx, run_id=run_id)
                data_health = evaluate_data_source_health(ctx.conn)
                _record_data_source_health_snapshot(
                    record_data_source_health_snapshot,
                    ctx.conn,
                    data_health,
                    run_id=run_id,
                )
                gate = data_source_gate_decision(pipeline_type, data_health)

            if gate == "failed":
                missing = ",".join(data_health.get("required_missing", []))
                prefix = "核心数据源刷新后仍不可用" if data_source_refresh else "核心数据源不可用"
                message = f"{prefix}，{pipeline_type} 跳过: {missing}"
                artifacts = {"data_sources": data_health}
                if data_source_refresh is not None:
                    artifacts["data_source_refresh"] = data_source_refresh
                ctx.run_journal.fail_run(run_id, message, artifacts=artifacts)
                return {
                    "status": "failed",
                    "pipeline": pipeline_type,
                    "run_id": run_id,
                    "reason": "data_source_health_failed",
                    "message": message,
                    "data_sources": data_health,
                    "data_source_refresh": data_source_refresh,
                }
            if gate == "warning":
                missing = ",".join(data_health.get("optional_missing", []))
                message = f"辅助数据源降级，{pipeline_type} 继续运行: {missing}"
                data_source_warning = {
                    "message": message,
                    "optional_missing": data_health.get("optional_missing", []),
                    "data_sources": data_health,
                }
                if on_data_source_warning:
                    on_data_source_warning(pipeline_type, run_id, data_source_warning)

        result = _run_pipeline(ctx, pipeline_type, run_id)
        safe_result = _json_safe_result(result)
        artifacts = {"result": "ok"}
        artifacts.update(_pipeline_audit_artifacts(pipeline_type, safe_result))
        if data_health is not None:
            artifacts["data_sources"] = data_health
        if data_source_refresh is not None:
            artifacts["data_source_refresh"] = data_source_refresh
        ctx.run_journal.complete_run(run_id, artifacts=artifacts)

        payload = {
            "status": "completed",
            "pipeline": pipeline_type,
            "run_id": run_id,
            "result": safe_result,
        }
        if data_health is not None:
            payload["data_sources"] = data_health
        if data_source_refresh is not None:
            payload["data_source_refresh"] = data_source_refresh
        if data_source_warning is not None:
            payload["data_source_warning"] = data_source_warning
        return payload
    except Exception as exc:
        ctx.run_journal.fail_run(run_id, str(exc))
        return {
            "status": "failed",
            "pipeline": pipeline_type,
            "run_id": run_id,
            "error": str(exc),
        }


def _config_version(ctx: Any) -> str:
    config_version = getattr(ctx, "config_version", None)
    if config_version:
        return config_version
    config_snapshot = getattr(ctx, "config_snapshot", None)
    return config_snapshot.version if config_snapshot else "unknown"


def _record_data_source_health_snapshot(record_func, conn: Any, data_health: dict, *, run_id: str) -> None:
    try:
        record_func(conn, data_health, run_id=run_id)
    except Exception as exc:
        _logger.debug("[pipeline_runner] 数据源健康历史记录失败: %s", exc)


def _run_pipeline(ctx: Any, pipeline_type: str, run_id: str) -> dict:
    if pipeline_type == "morning":
        from astock_trading.pipeline.morning import run
    elif pipeline_type == "noon":
        from astock_trading.pipeline.noon import run
    elif pipeline_type == "intraday_monitor":
        from astock_trading.pipeline.intraday_monitor import run
    elif pipeline_type == "scoring":
        from astock_trading.pipeline.scoring import run
    elif pipeline_type == "evening":
        from astock_trading.pipeline.evening import run
    elif pipeline_type == "weekly":
        from astock_trading.pipeline.weekly import run
    elif pipeline_type == "sentiment":
        from astock_trading.pipeline.sentiment import run
    elif pipeline_type == "auto_trade":
        from astock_trading.pipeline.auto_trade import run
    elif pipeline_type == "monthly":
        from astock_trading.pipeline.weekly import _generate_monthly_review

        _generate_monthly_review(ctx, run_id, local_now())
        return {}
    else:
        raise ValueError(f"Unknown pipeline: {pipeline_type}")

    return run(ctx, run_id) or {}


def _json_safe_result(result: dict) -> dict:
    return {key: value for key, value in result.items() if key != "discord_embed"}


def _pipeline_audit_artifacts(pipeline_type: str, result: dict) -> dict:
    if pipeline_type != "auto_trade":
        return {}
    keys = (
        "enabled",
        "dry_run",
        "signal",
        "paper_positions",
        "paper_total_asset",
        "buys",
        "sells",
        "diagnostics",
        "no_trade_summary",
        "window_state",
    )
    return {
        "auto_trade": {
            key: result[key]
            for key in keys
            if key in result
        }
    }
