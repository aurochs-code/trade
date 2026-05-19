"""MCP tool registrations for MX data, watchlists, and paper trading."""

from __future__ import annotations

import asyncio
import json
from typing import Callable

from astock_trading.platform.domain_events import AUTO_TRADE_EXECUTED


async def _mx_call(coro_fn):
    """Run one MX API call and close the async client."""
    from astock_trading.market.mx_async import MXAsyncClient

    client = MXAsyncClient()
    try:
        return await coro_fn(client)
    finally:
        await client.close()


def register_paper_tools(mcp, safe: Callable) -> dict[str, Callable]:
    """Register MX data, watchlist, and paper-trading MCP tools."""

    @mcp.tool()
    @safe
    def trade_mx_data(query: str) -> str:
        """妙想金融数据查询（自然语言，如"双环传动最近3年营收"）。"""
        result = asyncio.run(_mx_call(lambda c: c.query_data(query)))
        return json.dumps(result, ensure_ascii=False, default=str)

    @mcp.tool()
    @safe
    def trade_watchlist() -> str:
        """查询东方财富自选股列表。"""
        result = asyncio.run(_mx_call(lambda c: c.get_self_select()))

        data = result.get("data", {})
        all_results = data.get("allResults", {})
        result_data = all_results.get("result", {})
        data_list = result_data.get("dataList", [])

        stocks = []
        for item in data_list:
            stocks.append({
                "code": item.get("SECURITY_CODE", ""),
                "name": item.get("SECURITY_SHORT_NAME", ""),
                "price": item.get("NEWEST_PRICE"),
                "change_pct": item.get("CHG"),
            })
        return json.dumps({"count": len(stocks), "stocks": stocks}, ensure_ascii=False, default=str)

    @mcp.tool()
    @safe
    def trade_watchlist_manage(action: str) -> str:
        """管理自选股（自然语言，如"把贵州茅台加入自选"、"删除双环传动"）。"""
        result = asyncio.run(_mx_call(lambda c: c.manage_self_select(action)))
        return json.dumps(result, ensure_ascii=False, default=str)

    @mcp.tool()
    @safe
    def trade_auto_trade(dry_run: bool = True) -> str:
        """
        执行模拟盘自动交易 pipeline（选股→评分→风控→买卖）。
        是否启用和 dry_run 均以配置文件为准；MCP 不临时改写运行配置。
        """
        from astock_trading.pipeline.context import build_context

        ctx = build_context()
        try:
            run_id = ctx.run_journal.start_run("auto_trade", ctx.config_version)
            from astock_trading.pipeline.auto_trade import run

            result = run(ctx, run_id)
            ctx.run_journal.complete_run(run_id, artifacts={"result": "ok"})
            return json.dumps(result, ensure_ascii=False, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)
        finally:
            ctx.conn.close()

    @mcp.tool()
    @safe
    def trade_paper_status() -> str:
        """查询模拟盘状态（持仓 + 资金 + 最近交易记录）。"""
        from astock_trading.pipeline.context import build_context
        from astock_trading.pipeline.paper_account import PaperAccount

        paper = PaperAccount()
        positions = paper.get_positions()
        balance = paper.get_balance()

        ctx = build_context()
        try:
            recent = ctx.event_store.query(
                event_type=AUTO_TRADE_EXECUTED,
                limit=10,
            )
            trades = [event.get("payload", {}) for event in recent]
        except Exception:
            trades = []
        finally:
            ctx.conn.close()

        return json.dumps({
            "positions": [
                {
                    "code": position.code,
                    "name": position.name,
                    "shares": position.shares,
                    "avg_cost": position.avg_cost,
                    "current_price": position.current_price,
                    "pnl": position.pnl,
                    "pnl_pct": position.pnl_pct,
                }
                for position in positions
            ],
            "balance": {
                "total_asset": balance.total_asset,
                "available_cash": balance.available_cash,
                "market_value": balance.market_value,
            },
            "recent_trades": trades,
        }, ensure_ascii=False, default=str)

    @mcp.tool()
    @safe
    def trade_mock_portfolio() -> str:
        """查询妙想模拟盘持仓。"""
        result = asyncio.run(_mx_call(lambda c: c.mock_positions()))
        return json.dumps(result, ensure_ascii=False, default=str)

    @mcp.tool()
    @safe
    def trade_mock_balance() -> str:
        """查询妙想模拟盘账户资金。"""
        result = asyncio.run(_mx_call(lambda c: c.mock_balance()))
        return json.dumps(result, ensure_ascii=False, default=str)

    @mcp.tool()
    @safe
    def trade_mock_orders() -> str:
        """查询妙想模拟盘委托记录。"""
        result = asyncio.run(_mx_call(lambda c: c.mock_orders()))
        return json.dumps(result, ensure_ascii=False, default=str)

    @mcp.tool()
    @safe
    def trade_mock_buy(code: str, shares: int, price: float = 0) -> str:
        """模拟盘买入。price=0 为市价委托，shares 须为 100 的整数倍。"""
        if shares % 100 != 0:
            return json.dumps({"error": "shares 必须为 100 的整数倍"}, ensure_ascii=False)
        use_market = price <= 0
        result = asyncio.run(_mx_call(
            lambda c: c.mock_trade("buy", code, shares, price if not use_market else None, use_market)
        ))
        return json.dumps(result, ensure_ascii=False, default=str)

    @mcp.tool()
    @safe
    def trade_mock_sell(code: str, shares: int, price: float = 0) -> str:
        """模拟盘卖出。price=0 为市价委托，shares 须为 100 的整数倍。"""
        if shares % 100 != 0:
            return json.dumps({"error": "shares 必须为 100 的整数倍"}, ensure_ascii=False)
        use_market = price <= 0
        result = asyncio.run(_mx_call(
            lambda c: c.mock_trade("sell", code, shares, price if not use_market else None, use_market)
        ))
        return json.dumps(result, ensure_ascii=False, default=str)

    @mcp.tool()
    @safe
    def trade_mock_cancel(order_id: str = "") -> str:
        """模拟盘撤单。order_id 留空则撤销全部未成交委托。"""
        cancel_all = not order_id.strip()
        result = asyncio.run(_mx_call(
            lambda c: c.mock_cancel(order_id if not cancel_all else None, cancel_all)
        ))
        return json.dumps(result, ensure_ascii=False, default=str)

    return {
        "trade_mx_data": trade_mx_data,
        "trade_watchlist": trade_watchlist,
        "trade_watchlist_manage": trade_watchlist_manage,
        "trade_auto_trade": trade_auto_trade,
        "trade_paper_status": trade_paper_status,
        "trade_mock_portfolio": trade_mock_portfolio,
        "trade_mock_balance": trade_mock_balance,
        "trade_mock_orders": trade_mock_orders,
        "trade_mock_buy": trade_mock_buy,
        "trade_mock_sell": trade_mock_sell,
        "trade_mock_cancel": trade_mock_cancel,
    }
