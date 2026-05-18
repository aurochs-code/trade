"""Data-source health and endpoint validation commands."""

from __future__ import annotations

import asyncio
from typing import Optional

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect
from astock_trading.platform.time import local_today_str


data_sources_app = typer.Typer(name="data-sources", help="数据源健康")


def _format_metric(value: object, fmt: str, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    try:
        return format(value, fmt)
    except (TypeError, ValueError):
        return fallback


@data_sources_app.command("status")
def data_sources_status(
    max_age_hours: Optional[int] = typer.Option(None, "--max-age-hours", help="覆盖所有数据源最大年龄"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查看数据源最近观测健康状态。"""
    from astock_trading.market.health import evaluate_data_source_health

    conn = connect()
    try:
        result = evaluate_data_source_health(conn, max_age_hours=max_age_hours)
        if as_json:
            json_or_text(result, True)
            return

        typer.echo(f"Data sources: {result['status']}")
        for name, item in result["checks"].items():
            required = "required" if item["required"] else "optional"
            typer.echo(
                f"  {name}: {item['status']} ({required}) "
                f"age={_format_metric(item['age_hours'], '.2f')}h "
                f"count={item['payload_count']} source={item['source'] or '-'}"
            )
    finally:
        conn.close()


def register_check_data_sources(app: typer.Typer) -> None:
    @app.command("check-data-sources")
    def check_data_sources(
        code: str = typer.Argument("000858", help="验收股票代码"),
        trade_date: str = typer.Option("", help="交易日期 YYYY-MM-DD，默认今天"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """验收新增 A 股数据源端点，并写入 market_observations。"""
        from astock_trading.pipeline.context import build_context
        from astock_trading.platform.data_source_refresh import refresh_required_data_sources

        ctx = build_context()
        date_value = trade_date or local_today_str()
        run_id = f"check_data_sources_{date_value.replace('-', '')}"
        try:
            core = refresh_required_data_sources(ctx, code=code, trade_date=date_value, run_id=run_id)
            concepts = asyncio.run(ctx.market_svc.collect_concept_blocks(code, run_id=run_id))
            daily_lhb = asyncio.run(ctx.market_svc.collect_daily_dragon_tiger(date_value, run_id=run_id))
            lhb = asyncio.run(ctx.market_svc.collect_dragon_tiger(code, date_value, run_id=run_id))
            lockup = asyncio.run(ctx.market_svc.collect_lockup_expiry(code, date_value, run_id=run_id))
            industry = asyncio.run(ctx.market_svc.collect_industry_comparison(5, run_id=run_id))
            announcements = asyncio.run(ctx.market_svc.collect_announcements(code, 5, run_id=run_id))
            reports = asyncio.run(ctx.market_svc.collect_research_reports(code, 1, run_id=run_id))
            news = asyncio.run(ctx.market_svc.collect_stock_news(code, 5, run_id=run_id))
            basic = asyncio.run(ctx.market_svc.collect_basic_info(code, run_id=run_id))
            from astock_trading.market.health import evaluate_data_source_health

            health = evaluate_data_source_health(ctx.conn)
            flow_health = health["checks"]["baidu_fund_flow"]

            checks = {
                "hot_stocks": core["checks"]["hot_stocks"],
                "northbound_realtime": {
                    "available": core["northbound_points"] > 0,
                    "count": core["northbound_points"],
                    "required": True,
                },
                "baidu_fund_flow": {
                    "available": flow_health["status"] == "healthy",
                    "count": flow_health["payload_count"],
                    "required": True,
                    "source": flow_health["source"],
                    "current_fetch_available": core["flow_available"],
                },
                "industry_comparison": {
                    "available": industry.get("total", 0) > 0,
                    "count": industry.get("total", 0),
                    "required": False,
                },
                "announcements": {
                    "available": len(announcements) > 0,
                    "count": len(announcements),
                    "required": False,
                },
                "research_reports": {
                    "available": len(reports) > 0,
                    "count": len(reports),
                    "required": False,
                },
                "stock_news": {"available": len(news) > 0, "count": len(news), "required": False},
                "basic_info": {"available": len(basic) > 0, "count": len(basic), "required": False},
            }
            result = {
                "status": health["status"],
                "code": code,
                "date": date_value,
                "hot_stocks": core["hot_stocks"],
                "concept_tags": concepts.get("concept_tags", []),
                "northbound_points": core["northbound_points"],
                "daily_dragon_tiger": daily_lhb.get("total_records", 0),
                "dragon_tiger_records": len(lhb.get("records", [])),
                "lockup_upcoming": len(lockup.get("upcoming", [])),
                "industry_total": industry.get("total", 0),
                "announcements": len(announcements),
                "research_reports": len(reports),
                "stock_news": len(news),
                "basic_info_fields": len(basic),
                "flow_available": core["flow_available"],
                "checks": checks,
                "health": health,
                "required_missing": health["required_missing"],
                "optional_missing": health["optional_missing"],
            }
            if as_json:
                json_or_text(result, True)
            else:
                for key, value in result.items():
                    typer.echo(f"{key}: {value}")
        finally:
            ctx.conn.close()
