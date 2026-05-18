"""数据源预热刷新能力，供 CLI 和 pipeline 门禁复用。"""

from __future__ import annotations

import asyncio
from typing import Any

from astock_trading.market.health import evaluate_data_source_health
from astock_trading.platform.time import local_today_str

DEFAULT_REFRESH_CODE = "000858"


def refresh_required_data_sources(
    ctx: Any,
    *,
    code: str = DEFAULT_REFRESH_CODE,
    trade_date: str | None = None,
    run_id: str | None = None,
) -> dict:
    """刷新核心市场数据源，并返回刷新后的健康快照。"""
    date_value = trade_date or local_today_str()
    refresh_run_id = run_id or f"data_source_refresh_{date_value.replace('-', '')}"

    hot = _run(ctx.market_svc.collect_hot_stocks(date_value, run_id=refresh_run_id))
    northbound = _run(ctx.market_svc.collect_northbound_realtime(run_id=refresh_run_id))
    flow = _run(ctx.market_svc._get_flow(code))
    health = evaluate_data_source_health(ctx.conn)
    flow_health = (health.get("checks") or {}).get("baidu_fund_flow", {})

    return {
        "status": health["status"],
        "code": code,
        "date": date_value,
        "run_id": refresh_run_id,
        "hot_stocks": len(hot),
        "northbound_points": len(northbound),
        "flow_available": flow is not None,
        "checks": {
            "hot_stocks": {
                "available": len(hot) > 0,
                "count": len(hot),
                "required": True,
            },
            "northbound_realtime": {
                "available": len(northbound) > 0,
                "count": len(northbound),
                "required": True,
            },
            "baidu_fund_flow": {
                "available": flow_health.get("status") == "healthy",
                "count": flow_health.get("payload_count", 0),
                "required": True,
                "source": flow_health.get("source", ""),
                "current_fetch_available": flow is not None,
            },
        },
        "health": health,
        "required_missing": health["required_missing"],
        "optional_missing": health["optional_missing"],
    }


def _run(coro):
    return asyncio.run(coro)
