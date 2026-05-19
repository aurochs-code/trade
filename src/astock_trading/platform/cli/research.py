"""Backtest and research CLI commands."""

from __future__ import annotations

import json
from typing import Optional

import typer


def _format_metric(value: object, fmt: str, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    try:
        return format(value, fmt)
    except (TypeError, ValueError):
        return fallback


def _parse_int_csv(value: str) -> tuple[int, ...]:
    parsed = tuple(dict.fromkeys(int(part.strip()) for part in value.split(",") if part.strip()))
    if not parsed:
        raise typer.BadParameter("至少提供一个正整数")
    if any(item <= 0 for item in parsed):
        raise typer.BadParameter("只支持正整数")
    return parsed


def register_research_commands(app: typer.Typer) -> None:
    @app.command("backtest")
    def run_backtest_cmd(
        codes: str = typer.Argument(..., help="逗号分隔股票代码，如 600036,000001,000002"),
        start: str = typer.Argument(..., help="回测开始日期 YYYY-MM-DD"),
        end: str = typer.Argument(..., help="回测结束日期 YYYY-MM-DD"),
        preset: str = typer.Option("保守验证C", help="策略 preset（对应 strategy.yaml）"),
        initial_cash: float = typer.Option(100000.0, help="初始资金（元）"),
        adjustflag: str = typer.Option("2", help="复权: 2=前复权 1=后复权 3=不复权"),
        use_history_mirror: bool = typer.Option(True, "--history-mirror/--no-history-mirror", help="优先读取历史信号镜像，缺失时回退代理回放"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """运行历史回测（生产级四维评分引擎 + baostock 数据）。"""
        from astock_trading.backtest.engine import run_backtest

        result = run_backtest(
            codes=codes,
            start=start,
            end=end,
            preset=preset,
            initial_cash=initial_cash,
            adjustflag=adjustflag,
            use_history_mirror=use_history_mirror,
        )

        if "error" in result:
            typer.echo(f"\u274c {result['error']}", err=True)
            raise typer.Exit(1)

        if as_json:
            typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"回测报告 [{result['preset']}] {start} ~ {end}")
            typer.echo(f"  初始资金: {result['initial_cash']:.0f}  最终: {result['final_value']:.2f}")
            typer.echo(f"  总收益率: {result['total_return_pct']:.2f}%  年化: {result['annual_return_pct']:.2f}%")
            typer.echo(f"  最大回撤: {result['max_drawdown_pct']:.2f}%  胜率: {result['win_rate_pct']:.1f}%")
            typer.echo(f"  夏普比率: {result.get('sharpe_ratio', 0):.2f}")
            typer.echo(
                f"  交易: {result['total_trades']}笔 买/{result['buy_trades']} "
                f"卖/{result['sell_trades']} 胜/{result.get('winning_trades', 0)} "
                f"负/{result.get('losing_trades', 0)}"
            )
            typer.echo(f"  持仓中: {result['positions_open']} 只")

    @app.command("continuation-validate")
    def continuation_validate_cmd(
        codes: str = typer.Argument(..., help="逗号分隔股票代码"),
        start: str = typer.Option(..., help="验证开始日期 YYYY-MM-DD"),
        end: str = typer.Option(..., help="验证结束日期 YYYY-MM-DD"),
        top_n: Optional[int] = typer.Option(None, help="每日保留 Top N（默认读 continuation.scoring.top_n）"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """运行短线续涨评分验证并输出分层和 Top N 报告。"""
        from astock_trading.research.continuation_validation import run_continuation_validation

        result = run_continuation_validation(
            codes=[c.strip() for c in codes.split(",") if c.strip()],
            start=start,
            end=end,
            top_n=top_n,
        )

        if as_json:
            typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
            return

        typer.echo(f"短线续涨验证 {start} ~ {end}")
        typer.echo(f"  Top N: {result['top_n']}")
        typer.echo(f"  Buckets: {len(result['score_bucket_report'])}")
        typer.echo(f"  Execution modes: {len(result['execution_report'])}")
        if result["top_candidates"]:
            typer.echo("  Top candidates:")
            report_rows = result.get("candidate_report", result["top_candidates"])
            for row in report_rows[: min(5, len(report_rows))]:
                scores = row.get("scores", {})
                metrics = row.get("metrics", {})
                forward = row.get("forward_returns", {})
                score_text = _format_metric(row.get("score"), ".1f")
                t1_text = _format_metric(forward.get("t1", row.get("t1_return")), ".2%")
                typer.echo(
                    f"    {row['trade_date']} #{row['rank']} {row['code']} "
                    f"score={score_text} t1={t1_text}"
                )
                typer.echo(
                    "      "
                    f"S={_format_metric(scores.get('strength', row.get('strength_score')), '.2f', '0.00')} "
                    f"C={_format_metric(scores.get('continuity', row.get('continuity_score')), '.2f', '0.00')} "
                    f"Q={_format_metric(scores.get('quality', row.get('quality_score')), '.2f', '0.00')} "
                    f"F={_format_metric(scores.get('flow', row.get('flow_score')), '.2f', '0.00')} "
                    f"St={_format_metric(scores.get('stability', row.get('stability_score')), '.2f', '0.00')} "
                    f"P={_format_metric(scores.get('penalty', row.get('overheat_penalty')), '.2f', '0.00')}"
                )
                typer.echo(
                    "      "
                    f"chg={_format_metric(metrics.get('change_pct'), '.2f')}% "
                    f"cnh={_format_metric(metrics.get('close_near_high'), '.2f')} "
                    f"mom5={_format_metric(metrics.get('momentum_5d'), '.2f')} "
                    f"ret={_format_metric(metrics.get('intraday_retrace'), '.2%')} "
                    f"body={_format_metric(metrics.get('body_ratio'), '.2f')} "
                    f"rsi={_format_metric(metrics.get('rsi'), '.1f')} "
                    f"vr={_format_metric(metrics.get('volume_ratio'), '.2f')}"
                )
                flags = row.get("flags", row.get("notes", []))
                if flags:
                    typer.echo(f"      flags={','.join(flags)}")

    @app.command("continuation-backtest")
    def continuation_backtest_cmd(
        codes: str = typer.Argument(..., help="逗号分隔股票代码"),
        start: str = typer.Argument(..., help="回测开始日期 YYYY-MM-DD"),
        end: str = typer.Argument(..., help="回测结束日期 YYYY-MM-DD"),
        hold_days: int = typer.Option(2, help="持有天数"),
        top_n: int = typer.Option(3, help="每日保留 Top N"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """运行短线续涨 Top N 回测。"""
        from astock_trading.backtest.continuation_backtest import run_continuation_backtest

        result = run_continuation_backtest(
            codes=[c.strip() for c in codes.split(",") if c.strip()],
            start=start,
            end=end,
            hold_days=hold_days,
            top_n=top_n,
        )

        if as_json:
            typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
            return

        typer.echo(f"短线续涨回测 {start} ~ {end}")
        typer.echo(f"  Hold days: {result['hold_days']}  Top N: {result['top_n']}")
        typer.echo(
            f"  Total return: {result['total_return_pct']:.2f}%  Win rate: {result['win_rate_pct']:.2f}%"
        )
        typer.echo(f"  Trades: {len(result['trades'])}")

    @app.command("continuation-study")
    def continuation_study_cmd(
        codes: str = typer.Argument(..., help="逗号分隔股票代码"),
        start: str = typer.Option(..., help="研究开始日期 YYYY-MM-DD"),
        end: str = typer.Option(..., help="研究结束日期 YYYY-MM-DD"),
        top_ns: str = typer.Option("1,2,3", help="需要比较的 Top N 组合，如 1,2,3"),
        hold_days: str = typer.Option("1,2,3", help="需要比较的持有天数，如 1,2,3"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """运行短线续涨收益研究，比较 Top N 与持有天数组合。"""
        from astock_trading.research.continuation_study import run_continuation_study

        result = run_continuation_study(
            codes=[c.strip() for c in codes.split(",") if c.strip()],
            start=start,
            end=end,
            top_ns=_parse_int_csv(top_ns),
            hold_days_list=_parse_int_csv(hold_days),
        )

        if as_json:
            typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
            return

        typer.echo(f"短线续涨收益研究 {start} ~ {end}")
        typer.echo(f"  Top Ns: {','.join(str(v) for v in result['top_ns'])}")
        typer.echo(f"  Hold days: {','.join(str(v) for v in result['hold_days_list'])}")
        best = result.get("best_setup")
        if best:
            typer.echo(
                f"  Best: Top{best['top_n']} / 持有{best['hold_days']}天 "
                f"total={best['total_return_pct']:.2f}% "
                f"win={best['win_rate_pct']:.2f}% "
                f"avg={best['avg_trade_return_pct']:.2f}%"
            )
        typer.echo("  Comparison:")
        for row in result["comparison_report"]:
            typer.echo(
                f"    Top{row['top_n']} / 持有{row['hold_days']}天 "
                f"trades={row['trade_count']} days={row['trading_days']} "
                f"total={row['total_return_pct']:.2f}% "
                f"win={row['win_rate_pct']:.2f}% "
                f"avg={row['avg_trade_return_pct']:.2f}%"
            )
