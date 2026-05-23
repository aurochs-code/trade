"""策略 profile 对比 CLI。"""

from __future__ import annotations

from pathlib import Path

import typer

from astock_trading.pipeline.context import build_context
from astock_trading.pipeline.strategy_health import run_strategy_health_review
from astock_trading.pipeline.strategy_profiles import (
    apply_strategy_profile_activation,
    build_strategy_profile_activation_plan,
    compare_strategy_profiles,
    propose_strategy_allocation,
)
from astock_trading.platform.cli.common import json_or_text


strategy_app = typer.Typer(name="strategy", help="策略 profile 和多策略评估")


@strategy_app.command("profiles")
def strategy_profiles(
    profiles: str = typer.Option(
        "trend_swing,short_continuation,defensive_watch",
        "--profiles",
        help="逗号分隔的配置 profile 名称",
    ),
    record: bool = typer.Option(False, "--record/--no-record", help="是否记录 strategy.profile_comparison.proposed 事件"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """P6-2 多策略 profile 对比；只读，不切换执行 profile。"""
    profile_names = tuple(dict.fromkeys(name.strip() for name in profiles.split(",") if name.strip()))
    if not profile_names:
        raise typer.BadParameter("--profiles 至少需要一个 profile 名称")

    ctx = build_context()
    try:
        payload = compare_strategy_profiles(ctx.conn, profiles=profile_names, record=record)
        if as_json:
            json_or_text(payload, True)
            return
        typer.echo(payload["report_markdown"])
    finally:
        ctx.conn.close()


@strategy_app.command("profile-activation")
def strategy_profile_activation(
    target: str = typer.Option("trend_swing", "--target", help="需要人工确认激活的目标 profile"),
    record: bool = typer.Option(False, "--record/--no-record", help="是否记录 strategy.profile_activation.requested 事件"),
    apply_env: bool = typer.Option(False, "--apply-env", help="人工确认后写入运行 .env"),
    yes: bool = typer.Option(False, "--yes", help="确认写入运行 .env"),
    env_file: Path | None = typer.Option(None, "--env-file", help="运行 .env 路径"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """生成 profile 激活计划；默认不修改环境，可用 --apply-env --yes 人工确认写入 .env。"""
    ctx = build_context()
    try:
        try:
            if apply_env:
                payload = apply_strategy_profile_activation(
                    ctx.conn,
                    target_profile=target,
                    env_file=env_file,
                    confirm=yes,
                )
            else:
                payload = build_strategy_profile_activation_plan(
                    ctx.conn,
                    target_profile=target,
                    record=record,
                )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        if as_json:
            json_or_text(payload, True)
            return
        typer.echo(payload["report_markdown"])
    finally:
        ctx.conn.close()


@strategy_app.command("allocation")
def strategy_allocation(
    profiles: str = typer.Option(
        "trend_swing,short_continuation,defensive_watch",
        "--profiles",
        help="逗号分隔的配置 profile 名称",
    ),
    capital: float = typer.Option(500000.0, "--capital", help="用于生成隔离资金桶建议的总资金"),
    min_samples: int = typer.Option(10, "--min-samples", help="启用或暂停策略所需的最少复盘样本"),
    record: bool = typer.Option(False, "--record/--no-record", help="是否记录 strategy.capital_allocation.proposed 事件"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """P6-2 策略间资金隔离和弱策略处理建议；只读，不自动分配资金。"""
    profile_names = tuple(dict.fromkeys(name.strip() for name in profiles.split(",") if name.strip()))
    if not profile_names:
        raise typer.BadParameter("--profiles 至少需要一个 profile 名称")
    if capital < 0:
        raise typer.BadParameter("--capital must be >= 0")
    if min_samples < 1:
        raise typer.BadParameter("--min-samples must be >= 1")

    ctx = build_context()
    try:
        payload = propose_strategy_allocation(
            ctx.conn,
            profiles=profile_names,
            total_capital=capital,
            min_samples=min_samples,
            record=record,
        )
        if as_json:
            json_or_text(payload, True)
            return
        typer.echo(payload["report_markdown"])
    finally:
        ctx.conn.close()


@strategy_app.command("health")
def strategy_health(
    min_samples: int = typer.Option(10, "--min-samples", help="输出可参考策略体检所需的最少闭合复盘样本"),
    window_days: int = typer.Option(365, "--window-days", help="复盘样本回看天数"),
    record: bool = typer.Option(False, "--record/--no-record", help="是否记录 strategy.health_report.proposed 事件"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """P6-3 策略体检：行业、市值、持仓天数、入场信号和时间归因。"""
    if min_samples < 1:
        raise typer.BadParameter("--min-samples must be >= 1")
    if window_days < 1:
        raise typer.BadParameter("--window-days must be >= 1")

    ctx = build_context()
    try:
        payload = run_strategy_health_review(
            ctx.conn,
            min_samples=min_samples,
            window_days=window_days,
            record=record,
        )
        if as_json:
            json_or_text(payload, True)
            return
        typer.echo(payload["report_markdown"])
    finally:
        ctx.conn.close()
