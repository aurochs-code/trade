"""Market intelligence CLI commands backed by opencli data sources."""

from __future__ import annotations

import asyncio

import typer

from astock_trading.pipeline.context import build_context
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.time import local_now_str
from astock_trading.reporting.market_formatters import format_market_intel_line


market_intel_app = typer.Typer(name="market-intel", help="市场新闻、热点和板块强度")


def _sort_sectors(rows: list[dict], limit: int) -> list[dict]:
    seen = set()
    deduped = []
    for row in rows:
        key = row.get("code") or row.get("name")
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    deduped.sort(
        key=lambda item: (
            float(item.get("change_pct", 0) or 0),
            float(item.get("main_net", 0) or 0),
            -int(item.get("rank", 999) or 999),
        ),
        reverse=True,
    )
    return deduped[:limit]


def _sector_heatmap_rows(rows: list[dict], limit: int) -> list[dict]:
    normalized = []
    for idx, row in enumerate(rows[:limit], start=1):
        normalized.append({
            "rank": idx,
            "code": row.get("code", ""),
            "name": row.get("name", ""),
            "price": 0.0,
            "change_pct": row.get("change_pct", 0) or 0,
            "main_net": row.get("main_net", row.get("amount", 0)) or 0,
            "lead_stock": row.get("lead_stock", ""),
            "lead_change_pct": row.get("lead_change_pct", 0) or 0,
            "up_count": row.get("up_count", 0) or 0,
            "down_count": row.get("down_count", 0) or 0,
            "type": "industry",
            "sort": "change",
            "source": "sector_heatmap",
        })
    return normalized


async def _collect_brief(
    market_svc,
    *,
    query: str,
    limit: int,
    include_global: bool,
    run_id: str,
) -> dict:
    news_method = getattr(market_svc, "collect_news_search", None)
    if query and news_method is not None:
        news_task = news_method(query, limit=limit, run_id=run_id)
    else:
        news_task = market_svc.collect_finance_flash(limit=limit, run_id=run_id)

    tasks = [
        news_task,
        market_svc.collect_cross_platform_hot_stocks(limit=limit, run_id=run_id),
        market_svc.collect_hot_sectors(limit=limit, sector_type="industry", sort="change", run_id=run_id),
        market_svc.collect_hot_sectors(limit=limit, sector_type="concept", sort="change", run_id=run_id),
        market_svc.collect_hot_sectors(limit=limit, sector_type="industry", sort="money-flow", run_id=run_id),
    ]
    if include_global:
        tasks.append(market_svc.collect_global_risk_news(limit=limit, run_id=run_id))

    results = await asyncio.gather(*tasks)
    finance_flash = results[0]
    hot_stocks = results[1]
    industry_sectors = results[2]
    concept_sectors = results[3]
    money_flow_sectors = results[4]
    global_risk_news = results[5] if include_global else []
    strong_sectors = _sort_sectors([*industry_sectors, *concept_sectors], limit)

    if not strong_sectors and hasattr(market_svc, "collect_sector_heatmap"):
        heatmap_rows = await market_svc.collect_sector_heatmap(run_id=run_id)
        strong_sectors = _sector_heatmap_rows(heatmap_rows, limit)

    return {
        "query": query,
        "run_id": run_id,
        "execution_allowed": False,
        "source": "opencli",
        "finance_flash": finance_flash[:limit],
        "global_risk_news": global_risk_news[:limit],
        "hot_stocks": hot_stocks[:limit],
        "strong_sectors": strong_sectors,
        "money_flow_sectors": money_flow_sectors[:limit],
    }


def _format_sector_line(item: dict) -> str:
    lead = item.get("lead_stock") or "-"
    return (
        f"#{item.get('rank', '-')} {item.get('name', '')} "
        f"涨跌 {float(item.get('change_pct', 0) or 0):.2f}% "
        f"主线股 {lead}"
    )


def _format_brief_text(payload: dict) -> str:
    lines = ["市场情报摘要", f"run_id: {payload['run_id']}", ""]

    if payload.get("finance_flash"):
        lines.append("财经快讯")
        for item in payload["finance_flash"][:5]:
            lines.append(f"- {format_market_intel_line(item, 'finance_flash')}")
        lines.append("")

    if payload.get("strong_sectors"):
        lines.append("强势板块")
        for item in payload["strong_sectors"][:5]:
            lines.append(f"- {_format_sector_line(item)}")
        lines.append("")

    if payload.get("money_flow_sectors"):
        lines.append("资金流板块")
        for item in payload["money_flow_sectors"][:5]:
            lines.append(f"- {_format_sector_line(item)}")
        lines.append("")

    if payload.get("hot_stocks"):
        lines.append("跨平台热股")
        for item in payload["hot_stocks"][:5]:
            lines.append(
                f"- #{item.get('rank', '-')} {item.get('name', '')}"
                f"({item.get('code', '')}) 来源数 {item.get('source_count', '-')}"
            )
        lines.append("")

    if payload.get("global_risk_news"):
        lines.append("海外风险")
        for item in payload["global_risk_news"][:5]:
            lines.append(f"- {format_market_intel_line(item, 'global_risk')}")

    return "\n".join(lines).rstrip()


@market_intel_app.command("brief")
def market_intel_brief(
    query: str = typer.Option("", "--query", "-q", help="可选问题/关键词，如：今天热点新闻和强势板块"),
    limit: int = typer.Option(5, "--limit", help="每类最大返回数量"),
    include_global: bool = typer.Option(True, "--include-global/--no-global", help="是否包含海外风险新闻"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """汇总今日热点新闻、强势板块、资金流板块和跨平台热股。"""
    safe_limit = max(1, min(int(limit or 5), 20))
    run_id = f"market_intel_{local_now_str('%H%M%S')}"
    ctx = build_context()
    try:
        payload = asyncio.run(
            _collect_brief(
                ctx.market_svc,
                query=query.strip(),
                limit=safe_limit,
                include_global=include_global,
                run_id=run_id,
            )
        )
        json_or_text(payload if as_json else _format_brief_text(payload), as_json)
    finally:
        ctx.conn.close()


@market_intel_app.command("search")
def market_intel_search(
    query: str = typer.Argument(..., help="新闻关键词或问题"),
    limit: int = typer.Option(10, "--limit", help="最大返回数量"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """按关键词检索财经新闻；中文优先过滤新浪/东财快讯，英文或海外词会补充 Reuters。"""
    safe_limit = max(1, min(int(limit or 10), 40))
    run_id = f"market_news_search_{local_now_str('%H%M%S')}"
    ctx = build_context()
    try:
        rows = asyncio.run(ctx.market_svc.collect_news_search(query, limit=safe_limit, run_id=run_id))
        payload = {
            "query": query,
            "run_id": run_id,
            "execution_allowed": False,
            "source": "opencli",
            "count": len(rows),
            "news": rows,
        }
        if as_json:
            json_or_text(payload, True)
        else:
            lines = [f"新闻检索: {query}", f"run_id: {run_id}", ""]
            lines.extend(f"- {format_market_intel_line(item, 'finance_flash')}" for item in rows[:safe_limit])
            json_or_text("\n".join(lines).rstrip(), False)
    finally:
        ctx.conn.close()
