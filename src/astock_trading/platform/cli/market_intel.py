"""Market intelligence CLI commands backed by opencli data sources."""

from __future__ import annotations

import asyncio
import time

import typer

from astock_trading.pipeline.context import build_context
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.time import local_now_str, local_today
from astock_trading.platform.watchlist_sync import (
    build_watchlist_sync_plan,
    load_candidate_pool_items,
    load_local_position_items,
    mx_position_items,
    watchlist_items_from_mx_result,
    watchlist_manage_action,
)
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
    stocks = watchlist_items_from_mx_result(result)
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


@market_intel_app.command("watchlist-sync")
def watchlist_sync(
    source: str = typer.Option("candidate-pool", "--source", help="同步来源，目前仅支持 candidate-pool"),
    include_radar: bool = typer.Option(True, "--include-radar/--no-include-radar", help="是否把强势观察加入目标自选"),
    preserve_holdings: bool = typer.Option(True, "--preserve-holdings/--no-preserve-holdings", help="保留 MX 模拟盘和本地投影持仓"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只生成计划，不修改 MX 自选"),
    yes: bool = typer.Option(False, "--yes", "-y", help="确认执行 MX 自选增删"),
    operation_delay: float = typer.Option(1.5, "--operation-delay", help="每次 MX 自选写入后的等待秒数"),
    max_retries: int = typer.Option(3, "--max-retries", help="遇到 MX 限频时每个操作最多重试次数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """按最新候选池重建 MX 自选；默认只预演，执行必须加 --yes。"""
    if source != "candidate-pool":
        raise typer.BadParameter("--source 目前仅支持 candidate-pool")

    include_tiers = ("core", "watch", "radar") if include_radar else ("core", "watch")
    ctx = build_context()
    try:
        candidates = load_candidate_pool_items(ctx.conn, include_tiers=include_tiers)
        local_positions = load_local_position_items(ctx.conn) if preserve_holdings else []
    finally:
        ctx.conn.close()

    from astock_trading.pipeline.paper_account import PaperAccount, _mx_call

    current_result = asyncio.run(_mx_call(lambda client: client.get_self_select()))
    if not _mx_read_ok(current_result):
        payload = _watchlist_sync_failure_payload(
            dry_run=bool(dry_run or not yes),
            include_radar=include_radar,
            error="读取 MX 当前自选失败，已停止同步，避免按不完整状态误删或漏删自选。",
            result=current_result,
        )
        json_or_text(payload, as_json)
        raise typer.Exit(1)
    current_watchlist = watchlist_items_from_mx_result(current_result)
    paper_positions = []
    if preserve_holdings:
        positions_result = asyncio.run(_mx_call(lambda client: client.mock_positions()))
        if not _mx_read_ok(positions_result, require_code=True):
            payload = _watchlist_sync_failure_payload(
                dry_run=bool(dry_run or not yes),
                include_radar=include_radar,
                error="读取 MX 模拟盘持仓失败，已停止同步，避免误删持仓自选。",
                result=positions_result,
            )
            json_or_text(payload, as_json)
            raise typer.Exit(1)
        paper_positions = mx_position_items(PaperAccount._parse_positions(positions_result))
    plan = build_watchlist_sync_plan(
        candidates=candidates,
        current_watchlist=current_watchlist,
        mx_positions=paper_positions,
        local_positions=local_positions,
        preserve_holdings=preserve_holdings,
    )
    plan["dry_run"] = bool(dry_run or not yes)
    plan["execution_allowed"] = bool(yes and not dry_run)
    plan["command"] = "market-intel watchlist-sync"
    plan["include_radar"] = include_radar

    operations = []
    if yes and not dry_run:
        specs = [("remove", item) for item in plan["remove"]] + [("add", item) for item in plan["add"]]
        delay = max(0.0, float(operation_delay or 0.0))
        retries = max(0, int(max_retries or 0))
        for index, (operation_action, item) in enumerate(specs):
            operations.append(_run_watchlist_operation(
                _mx_call,
                operation_action,
                item,
                delay=delay,
                max_retries=retries,
            ))
            if delay and index < len(specs) - 1:
                time.sleep(delay)
        failed = [item for item in operations if not item["ok"]]
        plan["status"] = "failed" if failed else ("synced" if operations else "up_to_date")
        plan["operations"] = operations
        plan["failed_operations"] = failed
    else:
        plan["status"] = "dry_run"
        plan["operations"] = [
            _planned_watchlist_operation("remove", item)
            for item in plan["remove"]
        ] + [
            _planned_watchlist_operation("add", item)
            for item in plan["add"]
        ]
        plan["next_action"] = {
            "label": "确认同步 MX 自选",
            "command": "atrade market-intel watchlist-sync --source candidate-pool --preserve-holdings --yes --json",
            "risk_level": "external_state_write",
            "writes_order": False,
            "requires_user_approval": True,
        }

    json_or_text(plan, as_json)
    if plan["status"] == "failed":
        raise typer.Exit(1)


def _planned_watchlist_operation(action: str, item: dict) -> dict:
    return {
        "action": action,
        "code": item.get("code", ""),
        "name": item.get("name", ""),
        "mx_action": watchlist_manage_action(action, item),
        "ok": None,
    }


def _watchlist_operation_payload(action: str, item: dict, mx_action: str, result: dict) -> dict:
    return {
        "action": action,
        "code": item.get("code", ""),
        "name": item.get("name", ""),
        "mx_action": mx_action,
        "ok": _mx_write_ok(result),
        "result": result,
    }


def _run_watchlist_operation(
    mx_call,
    action: str,
    item: dict,
    *,
    delay: float,
    max_retries: int,
) -> dict:
    mx_action = watchlist_manage_action(action, item)
    attempts = []
    for attempt in range(max_retries + 1):
        result = asyncio.run(mx_call(lambda client, mx_action=mx_action: client.manage_self_select(mx_action)))
        attempts.append(result)
        if _mx_write_ok(result):
            payload = _watchlist_operation_payload(action, item, mx_action, result)
            payload["attempts"] = attempt + 1
            return payload
        if not _mx_rate_limited(result) or attempt >= max_retries:
            payload = _watchlist_operation_payload(action, item, mx_action, result)
            payload["attempts"] = attempt + 1
            payload["attempt_results"] = attempts
            return payload
        time.sleep(max(5.0, delay * 4 * (attempt + 1)))

    payload = _watchlist_operation_payload(action, item, mx_action, attempts[-1] if attempts else {})
    payload["attempts"] = len(attempts)
    payload["attempt_results"] = attempts
    return payload


def _mx_write_ok(result: dict) -> bool:
    if not isinstance(result, dict):
        return False
    if result.get("error"):
        return False
    code = result.get("code")
    if code is None:
        return True
    return str(code) in {"0", "200"}


def _mx_rate_limited(result: dict) -> bool:
    if not isinstance(result, dict):
        return False
    return str(result.get("code") or result.get("status") or "") == "112"


def _mx_read_ok(result: dict, *, require_code: bool = False) -> bool:
    if not isinstance(result, dict):
        return False
    if result.get("error"):
        return False
    code = result.get("code")
    if code is None:
        return not require_code
    return str(code) in {"0", "200"}


def _watchlist_sync_failure_payload(*, dry_run: bool, include_radar: bool, error: str, result: dict) -> dict:
    return {
        "status": "failed",
        "command": "market-intel watchlist-sync",
        "source": "candidate-pool",
        "dry_run": dry_run,
        "execution_allowed": False,
        "include_radar": include_radar,
        "error": error,
        "mx_result": result,
        "guardrails": {
            "writes_order": False,
            "real_trade": False,
            "external_state": "mx_watchlist",
            "stopped_before_external_write": True,
        },
    }
