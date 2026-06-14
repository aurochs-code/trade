"""Data-source health and endpoint validation commands."""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime
import json
from typing import Any, Optional

import typer

from astock_trading.platform import data_source_diagnostics
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect
from astock_trading.platform.time import local_today_str


data_sources_app = typer.Typer(name="data-sources", help="数据源健康")

SOURCE_QUALITY_DIMENSIONS = (
    ("quote", "行情", "L1", "has_quote"),
    ("technical", "技术指标", "L1", "has_technical"),
    ("financial", "基本面", "L1", "has_financial"),
    ("flow", "资金流", "L1", "has_flow"),
    ("sentiment", "舆情", "L2", "has_sentiment"),
    ("sector", "行业上下文", "L2", "has_sector"),
)


def _format_metric(value: object, fmt: str, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    try:
        return format(value, fmt)
    except (TypeError, ValueError):
        return fallback


def _decode_payload(value: Any) -> Any:
    if value is None:
        return {}
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {}


def _coverage_rate(available: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(available / total, 4)


def _snapshot_has(payload: dict, attr: str, completeness_key: str) -> bool:
    completeness = payload.get("completeness")
    if isinstance(completeness, dict) and completeness_key in completeness:
        return bool(completeness.get(completeness_key))
    return payload.get(attr) is not None


def _source_quality_from_payloads(snapshot_payloads: list[dict], scores: list[dict]) -> dict:
    total = len(snapshot_payloads)
    coverage = {}
    warnings = []

    for attr, label, layer, completeness_key in SOURCE_QUALITY_DIMENSIONS:
        available = sum(
            1
            for snapshot in snapshot_payloads
            if _snapshot_has(snapshot, attr, completeness_key)
        )
        row = {
            "label": label,
            "layer": layer,
            "available": available,
            "missing": max(total - available, 0),
            "total": total,
            "rate": _coverage_rate(available, total),
        }
        coverage[attr] = row
        if total and layer == "L1" and available < total:
            warnings.append(
                f"最近筛选逐票{label}覆盖率 {row['rate']:.1%}，可能影响评分和买入门禁。"
            )

    quality_counter = Counter(str(score.get("data_quality", "ok")) for score in scores)
    missing_counter: Counter = Counter()
    for score in scores:
        fields = score.get("data_missing_fields") or []
        if isinstance(fields, str):
            fields = [fields]
        missing_counter.update(str(item) for item in fields)

    if total == 0:
        status = "warning"
        warnings.append("最近筛选没有可回放的逐票快照，无法评估覆盖率。")
    elif coverage["quote"]["available"] == 0 or coverage["technical"]["available"] == 0:
        status = "failed"
    elif warnings or quality_counter.get("degraded", 0) or quality_counter.get("error", 0):
        status = "warning"
    else:
        status = "ok"

    if quality_counter.get("degraded", 0) or quality_counter.get("error", 0):
        warnings.append(
            f"评分数据质量存在降级 {quality_counter.get('degraded', 0)} 条、错误 {quality_counter.get('error', 0)} 条。"
        )

    return {
        "status": status,
        "sample_size": total,
        "score_count": len(scores),
        "coverage": coverage,
        "score_quality_counts": dict(sorted(quality_counter.items())),
        "missing_fields": [
            {"field": key, "count": count}
            for key, count in sorted(missing_counter.items(), key=lambda item: (-item[1], item[0]))
        ],
        "warnings": warnings,
    }


def _empty_screener_source_quality() -> dict:
    return {
        "status": "empty",
        "run_id": "",
        "history_group_id": "",
        "phase": "",
        "created_at": "",
        "sample_size": 0,
        "score_count": 0,
        "coverage": {},
        "score_quality_counts": {},
        "missing_fields": [],
        "warnings": ["尚无可用于覆盖率回放的筛选镜像。"],
    }


def _latest_screener_source_quality(conn) -> dict:
    return data_source_diagnostics._latest_screener_source_quality(conn)


def build_data_source_diagnosis(
    conn,
    *,
    now: datetime | None = None,
    max_age_hours: Optional[int] = None,
) -> dict:
    """汇总全局门禁、provider 失败和最近筛选逐票覆盖率。"""
    return data_source_diagnostics.build_data_source_diagnosis(
        conn,
        now=now,
        max_age_hours=max_age_hours,
    )


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


@data_sources_app.command("diagnose")
def data_sources_diagnose(
    max_age_hours: Optional[int] = typer.Option(None, "--max-age-hours", help="覆盖所有数据源最大年龄"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """诊断数据源门禁、provider 失败和最近筛选覆盖率。"""
    conn = connect()
    try:
        result = build_data_source_diagnosis(conn, max_age_hours=max_age_hours)
        if as_json:
            json_or_text(result, True)
            return

        typer.echo(f"Data-source diagnosis: {result['status']}")
        for finding in result["findings"]:
            typer.echo(f"  - {finding}")
        if not result["findings"]:
            typer.echo("  - 未发现阻断性数据源问题")
    finally:
        conn.close()


@data_sources_app.command("coverage")
def data_sources_coverage(
    source: Optional[str] = typer.Option(None, "--source", help="限定数据源，如 tushare 或 baostock"),
    adjustflag: Optional[str] = typer.Option(None, "--adjustflag", help="限定复权口径：2=前复权，3=不复权"),
    start: Optional[str] = typer.Option(None, "--start", help="覆盖率统计开始日期 YYYY-MM-DD"),
    end: Optional[str] = typer.Option(None, "--end", help="覆盖率统计结束日期 YYYY-MM-DD"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查看 market_price_bars 市场数据覆盖率。"""
    from astock_trading.market.data_hydration import summarize_price_bar_coverage

    conn = connect()
    try:
        result = summarize_price_bar_coverage(
            conn,
            source=source,
            adjustflag=adjustflag,
            start=start,
            end=end,
        )
        if as_json:
            json_or_text(result, True)
            return

        typer.echo(f"市场数据覆盖率: {result['status']}")
        for item in result["price_bars"]:
            typer.echo(
                f"  {item['source']} adjustflag={item['adjustflag']} "
                f"{item['start_date']}~{item['end_date']} "
                f"rows={item['row_count']} symbols={item['symbol_count']} "
                f"days={item['trading_day_count']}"
            )
        if not result["price_bars"]:
            typer.echo("  - 暂无 market_price_bars 落库数据")
    finally:
        conn.close()


@data_sources_app.command("hydrate-market-bars")
def data_sources_hydrate_market_bars(
    start: str = typer.Argument(..., help="补水开始日期 YYYY-MM-DD"),
    end: str = typer.Argument(..., help="补水结束日期 YYYY-MM-DD"),
    source: str = typer.Option("tushare", "--source", help="数据源；当前仅支持 tushare 全市场普通日线"),
    adjustflag: str = typer.Option("3", "--adjustflag", help="复权口径；Tushare daily 只能使用 3=不复权"),
    limit_dates: Optional[int] = typer.Option(None, "--limit-dates", min=1, help="最多处理几个交易日，用于小范围验收"),
    write: bool = typer.Option(False, "--write", help="实际写入 market_price_bars；默认仅 dry-run"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """按交易日补齐全市场日线 K 线。"""
    from astock_trading.market.data_hydration import hydrate_tushare_daily_market_bars
    from astock_trading.market.tushare_adapters import TushareClient

    if source != "tushare":
        result = {
            "status": "failed",
            "error": f"暂不支持 source={source} 的全市场日线补水；前复权历史请继续用回测 --hydrate-data 按股票池补齐。",
            "source": source,
            "start": start,
            "end": end,
        }
        if as_json:
            json_or_text(result, True)
            raise typer.Exit(1)
        typer.secho(result["error"], fg="red")
        raise typer.Exit(1)

    conn = connect()
    try:
        try:
            result = hydrate_tushare_daily_market_bars(
                conn,
                client=TushareClient.from_env(),
                start=start,
                end=end,
                adjustflag=adjustflag,
                dry_run=not write,
                limit_dates=limit_dates,
            )
        except ValueError as exc:
            result = {
                "status": "failed",
                "error": str(exc),
                "source": source,
                "adjustflag": adjustflag,
                "start": start,
                "end": end,
            }
            if as_json:
                json_or_text(result, True)
                raise typer.Exit(1)
            typer.secho(str(exc), fg="red")
            raise typer.Exit(1)

        if as_json:
            json_or_text(result, True)
            if result.get("status") == "failed":
                raise typer.Exit(1)
            return

        typer.echo(
            f"全市场日线补水: {result['status']} "
            f"{result['start']}~{result['end']} dates={result['trade_date_count']} "
            f"planned={result['planned_rows']} written={result['rows_written']}"
        )
        for note in result.get("notes", []):
            typer.echo(f"  - {note}")
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
