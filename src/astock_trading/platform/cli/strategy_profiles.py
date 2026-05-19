"""策略 profile 对比 CLI。"""

from __future__ import annotations

import typer

from astock_trading.pipeline.context import build_context
from astock_trading.pipeline.strategy_profiles import compare_strategy_profiles
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
