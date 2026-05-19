"""Market intelligence CLI commands backed by opencli data sources."""

from __future__ import annotations

import asyncio

import typer

from astock_trading.pipeline.context import build_context
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.time import local_now_str, local_today
from astock_trading.reporting.market_formatters import format_market_intel_line


market_intel_app = typer.Typer(name="market-intel", help="市场新闻、热点和板块强度")


def _run_market_call(coro_factory):
    ctx = build_context()
    try:
        return asyncio.run(coro_factory(ctx.market_svc))
    finally:
        ctx.conn.close()


def _bounded_limit(limit: int, default: int = 20, maximum: int = 100) -> int:
    return max(1, min(int(limit or default), maximum))


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


@market_intel_app.command("signal")
def market_signal(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询大盘择时信号和仓位系数。"""
    run_id = f"market_signal_{local_now_str('%H%M%S')}"

    async def collect(market_svc):
        state, _index_data = await market_svc.collect_market_state(run_id=run_id)
        signal = getattr(state.signal, "value", state.signal)
        return {"run_id": run_id, "signal": signal, "multiplier": state.multiplier, "detail": state.detail}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("hot-stocks")
def hot_stocks(
    trade_date: str = typer.Option("", "--trade-date", help="交易日期 YYYY-MM-DD；空值为最近"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询同花顺当日强势股和题材归因。"""
    run_id = f"hot_stocks_{local_now_str('%H%M%S')}"

    async def collect(market_svc):
        rows = await market_svc.collect_hot_stocks(trade_date or None, run_id=run_id)
        return {"run_id": run_id, "trade_date": trade_date, "count": len(rows), "stocks": rows}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("concepts")
def concept_blocks(
    code: str = typer.Argument(..., help="股票代码"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询行业、概念和地域归属。"""
    run_id = f"concepts_{local_now_str('%H%M%S')}"

    async def collect(market_svc):
        data = await market_svc.collect_concept_blocks(code, run_id=run_id)
        return {"run_id": run_id, "code": code, **data}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("fund-flow")
def fund_flow(
    code: str = typer.Argument(..., help="股票代码"),
    days: int = typer.Option(5, "--days", help="历史资金流天数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询百度资金流向并映射为系统资金维度。"""
    from astock_trading.market.adapters import BaiduFundFlowAdapter

    adapter = BaiduFundFlowAdapter()
    flow = asyncio.run(adapter.get_fund_flow(code, days=days))
    realtime = adapter.get_fund_flow_realtime_sync(code, local_today().strftime("%Y%m%d"))
    json_or_text(
        {
            "code": code,
            "days": days,
            "flow": flow.__dict__ if flow else None,
            "realtime_tail": realtime[-5:],
        },
        as_json,
    )


@market_intel_app.command("northbound")
def northbound(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询同花顺北向资金分钟流向。"""
    run_id = f"northbound_{local_now_str('%H%M%S')}"

    async def collect(market_svc):
        rows = await market_svc.collect_northbound_realtime(run_id=run_id)
        return {"run_id": run_id, "count": len(rows), "rows": rows[-20:]}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("daily-dragon-tiger")
def daily_dragon_tiger(
    trade_date: str = typer.Option("", "--trade-date", help="交易日期 YYYY-MM-DD；空值为最近"),
    min_net_buy: float = typer.Option(0.0, "--min-net-buy", help="最低净买入，单位万元；0 表示不过滤"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询全市场龙虎榜。"""
    run_id = f"daily_lhb_{local_now_str('%H%M%S')}"

    async def collect(market_svc):
        return await market_svc.collect_daily_dragon_tiger(
            trade_date or None,
            min_net_buy if min_net_buy > 0 else None,
            run_id=run_id,
        )

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("dragon-tiger")
def dragon_tiger(
    code: str = typer.Argument(..., help="股票代码"),
    trade_date: str = typer.Option("", "--trade-date", help="交易日期 YYYY-MM-DD；空值为今天"),
    look_back: int = typer.Option(30, "--look-back", help="回看天数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询个股龙虎榜记录、席位和机构统计。"""
    run_id = f"lhb_{local_now_str('%H%M%S')}"
    date_value = trade_date or local_today().isoformat()

    async def collect(market_svc):
        data = await market_svc.collect_dragon_tiger(code, date_value, look_back, run_id=run_id)
        return {"run_id": run_id, "code": code, "trade_date": date_value, **data}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("lockup-expiry")
def lockup_expiry(
    code: str = typer.Argument(..., help="股票代码"),
    trade_date: str = typer.Option("", "--trade-date", help="交易日期 YYYY-MM-DD；空值为今天"),
    forward_days: int = typer.Option(90, "--forward-days", help="未来解禁预警天数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询限售解禁历史和未来解禁预警。"""
    run_id = f"lockup_{local_now_str('%H%M%S')}"
    date_value = trade_date or local_today().isoformat()

    async def collect(market_svc):
        data = await market_svc.collect_lockup_expiry(code, date_value, forward_days, run_id=run_id)
        return {"run_id": run_id, "code": code, "trade_date": date_value, **data}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("industry-comparison")
def industry_comparison(
    top_n: int = typer.Option(20, "--top-n", help="返回行业数量"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询同花顺行业横向对比。"""
    run_id = f"industry_{local_now_str('%H%M%S')}"

    async def collect(market_svc):
        return await market_svc.collect_industry_comparison(top_n, run_id=run_id)

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("announcements")
def announcements(
    code: str = typer.Argument(..., help="股票代码"),
    limit: int = typer.Option(20, "--limit", help="最大返回数量"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询巨潮公告列表。"""
    run_id = f"announcements_{local_now_str('%H%M%S')}"
    safe_limit = _bounded_limit(limit)

    async def collect(market_svc):
        rows = await market_svc.collect_announcements(code, safe_limit, run_id=run_id)
        return {"run_id": run_id, "code": code, "count": len(rows), "announcements": rows}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("research-reports")
def research_reports(
    code: str = typer.Argument(..., help="股票代码"),
    max_pages: int = typer.Option(2, "--max-pages", help="最大页数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询东财研报列表和 PDF URL。"""
    run_id = f"research_reports_{local_now_str('%H%M%S')}"

    async def collect(market_svc):
        rows = await market_svc.collect_research_reports(code, max_pages, run_id=run_id)
        return {"run_id": run_id, "code": code, "count": len(rows), "reports": rows}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("stock-news")
def stock_news(
    code: str = typer.Argument(..., help="股票代码"),
    limit: int = typer.Option(20, "--limit", help="最大返回数量"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询个股新闻。"""
    run_id = f"stock_news_{local_now_str('%H%M%S')}"
    safe_limit = _bounded_limit(limit)

    async def collect(market_svc):
        rows = await market_svc.collect_stock_news(code, safe_limit, run_id=run_id)
        return {"run_id": run_id, "code": code, "count": len(rows), "news": rows}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("cls-flash")
def cls_flash(
    limit: int = typer.Option(20, "--limit", help="最大返回数量"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询财联社快讯。"""
    run_id = f"cls_flash_{local_now_str('%H%M%S')}"
    safe_limit = _bounded_limit(limit)

    async def collect(market_svc):
        rows = await market_svc.collect_cls_flash(safe_limit, run_id=run_id)
        return {"run_id": run_id, "count": len(rows), "news": rows}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("global-news")
def global_news(
    limit: int = typer.Option(20, "--limit", help="最大返回数量"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询东财全球财经资讯。"""
    run_id = f"global_news_{local_now_str('%H%M%S')}"
    safe_limit = _bounded_limit(limit)

    async def collect(market_svc):
        rows = await market_svc.collect_global_news(safe_limit, run_id=run_id)
        return {"run_id": run_id, "count": len(rows), "news": rows}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("basic-info")
def basic_info(
    code: str = typer.Argument(..., help="股票代码"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询东财个股基本资料。"""
    run_id = f"basic_info_{local_now_str('%H%M%S')}"

    async def collect(market_svc):
        data = await market_svc.collect_basic_info(code, run_id=run_id)
        return {"run_id": run_id, "code": code, "info": data}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("f10")
def f10(
    code: str = typer.Argument(..., help="股票代码"),
    category: str = typer.Option("最新提示", "--category", help="F10 分类"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询 F10 文本资料。"""
    run_id = f"f10_{local_now_str('%H%M%S')}"

    async def collect(market_svc):
        text = await market_svc.collect_f10(code, category, run_id=run_id)
        return {"run_id": run_id, "code": code, "category": category, "text": text[:12000]}

    json_or_text(_run_market_call(collect), as_json)


@market_intel_app.command("mx-data")
def mx_data(
    query: str = typer.Argument(..., help="妙想金融数据自然语言查询"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """执行妙想金融数据自然语言查询。"""
    from astock_trading.pipeline.paper_account import _mx_call

    result = asyncio.run(_mx_call(lambda client: client.query_data(query)))
    json_or_text(result, as_json)


@market_intel_app.command("watchlist")
def watchlist(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询东方财富自选股列表。"""
    from astock_trading.pipeline.paper_account import _mx_call

    result = asyncio.run(_mx_call(lambda client: client.get_self_select()))
    data = result.get("data", {})
    all_results = data.get("allResults", {})
    result_data = all_results.get("result", {})
    data_list = result_data.get("dataList", [])
    stocks = [
        {
            "code": item.get("SECURITY_CODE", ""),
            "name": item.get("SECURITY_SHORT_NAME", ""),
            "price": item.get("NEWEST_PRICE"),
            "change_pct": item.get("CHG"),
        }
        for item in data_list
    ]
    json_or_text({"count": len(stocks), "stocks": stocks}, as_json)


@market_intel_app.command("watchlist-manage")
def watchlist_manage(
    action: str = typer.Argument(..., help="自选股管理自然语言动作"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """管理东方财富自选股。"""
    from astock_trading.pipeline.paper_account import _mx_call

    result = asyncio.run(_mx_call(lambda client: client.manage_self_select(action)))
    json_or_text(result, as_json)
