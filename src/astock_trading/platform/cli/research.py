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


def _parse_str_csv(value: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(part.strip() for part in value.split(",") if part.strip()))


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
        load_financials: bool = typer.Option(True, "--financials/--skip-financials", help="是否加载财务维度；正式回测应保持开启"),
        use_stored_data: bool = typer.Option(False, "--use-stored-data", help="优先使用 MySQL 已落库的 K 线和财务快照"),
        hydrate_data: bool = typer.Option(False, "--hydrate-data", help="远端拉取缺口并写入 MySQL，后续回测只补增量"),
        use_market_bars: bool = typer.Option(False, "--use-market-bars", help="细粒度选项：优先从 market_price_bars 读取 K 线"),
        hydrate_market_bars: bool = typer.Option(False, "--hydrate-market-bars", help="细粒度选项：远端 K 线拉取成功后写入 market_price_bars"),
        red_multiplier: Optional[float] = typer.Option(None, "--red-multiplier", help="回测情景：覆盖 RED 市场仓位乘数；默认不覆盖"),
        execute_red_trial_buy: bool = typer.Option(False, "--red-trial", "--execute-red-trial-buy", help="回测情景：把 RED 下的试买意向按 RED 仓位乘数模拟执行"),
        trial_routes: str = typer.Option("", "--trial-routes", help="回测情景：只执行指定路线的试买意向，逗号分隔；留空表示不过滤"),
        watch_trial_markets: str = typer.Option("", "--watch-trial-markets", help="回测情景：把指定市场制度下的观察路线按试买模拟执行，逗号分隔，如 GREEN,RED"),
        watch_trial_routes: str = typer.Option("", "--watch-trial-routes", help="回测情景：仅升级指定观察路线，逗号分隔；留空表示不过滤"),
        watch_trial_pairs: str = typer.Option("", "--watch-trial-pairs", help="回测情景：仅升级指定市场制度和路线组合，格式 GREEN:relative_strength_overheat,YELLOW:pullback_to_ma20；提供后优先于 markets/routes"),
        watch_trial_score_min: float = typer.Option(6.0, "--watch-trial-score-min", help="观察路线升级试买的最低评分"),
        trailing_stop: Optional[float] = typer.Option(None, "--trailing-stop", help="回测情景：覆盖移动止盈/追踪止损比例，如 0.20；默认使用 preset"),
        score_dimension_mode: str = typer.Option("full", "--score-dimensions", help="评分维度模式：full 或 tech_fundamental，用于验证资金流维度增量"),
        include_signal_alpha: bool = typer.Option(True, "--signal-alpha/--skip-signal-alpha", help="是否计算路线前瞻收益统计；组合 what-if 可跳过以加速"),
        progress_log: bool = typer.Option(False, "--progress-log/--quiet-progress", help="向 stderr 输出回测阶段进度，JSON 仍只写 stdout"),
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
            load_financials=load_financials,
            red_multiplier=red_multiplier,
            execute_red_trial_buy=execute_red_trial_buy,
            execute_trial_buy_routes=_parse_str_csv(trial_routes),
            execute_watch_trial_markets=_parse_str_csv(watch_trial_markets),
            execute_watch_trial_routes=_parse_str_csv(watch_trial_routes),
            execute_watch_trial_pairs=_parse_str_csv(watch_trial_pairs),
            execute_watch_trial_score_min=watch_trial_score_min,
            trailing_stop=trailing_stop,
            score_dimension_mode=score_dimension_mode,
            include_signal_alpha=include_signal_alpha,
            progress_log=progress_log,
            use_stored_data=use_stored_data,
            hydrate_data=hydrate_data,
            use_market_bars=use_market_bars,
            hydrate_market_bars=hydrate_market_bars,
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

    @app.command("backtest-batch")
    def run_backtest_batch_cmd(
        codes: str = typer.Argument(..., help="逗号分隔股票代码，支持较大股票池"),
        start: str = typer.Argument(..., help="回测开始日期 YYYY-MM-DD"),
        end: str = typer.Argument(..., help="回测结束日期 YYYY-MM-DD"),
        preset: str = typer.Option("保守验证C", help="策略 preset（对应 strategy.yaml）"),
        initial_cash: float = typer.Option(100000.0, help="每个批次的初始资金（元）"),
        adjustflag: str = typer.Option("2", help="复权: 2=前复权 1=后复权 3=不复权"),
        use_history_mirror: bool = typer.Option(True, "--history-mirror/--no-history-mirror", help="优先读取历史信号镜像，缺失时回退代理回放"),
        load_financials: bool = typer.Option(True, "--financials/--skip-financials", help="是否加载财务维度；正式回测应保持开启"),
        use_stored_data: bool = typer.Option(False, "--use-stored-data", help="优先使用 MySQL 已落库的 K 线和财务快照"),
        hydrate_data: bool = typer.Option(False, "--hydrate-data", help="远端拉取缺口并写入 MySQL，后续回测只补增量"),
        use_market_bars: bool = typer.Option(False, "--use-market-bars", help="细粒度选项：优先从 market_price_bars 读取 K 线"),
        hydrate_market_bars: bool = typer.Option(False, "--hydrate-market-bars", help="细粒度选项：远端 K 线拉取成功后写入 market_price_bars"),
        batch_size: int = typer.Option(8, "--batch-size", min=1, help="每个子回测批次包含的股票数"),
        batch_timeout_seconds: float = typer.Option(240.0, "--batch-timeout", "--batch-timeout-seconds", min=1.0, help="单个批次最大运行秒数，超时后会拆成单票重试"),
        red_multiplier: Optional[float] = typer.Option(None, "--red-multiplier", help="回测情景：覆盖 RED 市场仓位乘数；默认不覆盖"),
        execute_red_trial_buy: bool = typer.Option(False, "--red-trial", "--execute-red-trial-buy", help="回测情景：把 RED 下的试买意向按 RED 仓位乘数模拟执行"),
        trial_routes: str = typer.Option("", "--trial-routes", help="回测情景：只执行指定路线的试买意向，逗号分隔；留空表示不过滤"),
        watch_trial_markets: str = typer.Option("", "--watch-trial-markets", help="回测情景：把指定市场制度下的观察路线按试买模拟执行，逗号分隔，如 GREEN,RED"),
        watch_trial_routes: str = typer.Option("", "--watch-trial-routes", help="回测情景：仅升级指定观察路线，逗号分隔；留空表示不过滤"),
        watch_trial_pairs: str = typer.Option("", "--watch-trial-pairs", help="回测情景：仅升级指定市场制度和路线组合，格式 GREEN:relative_strength_overheat,YELLOW:pullback_to_ma20；提供后优先于 markets/routes"),
        watch_trial_score_min: float = typer.Option(6.0, "--watch-trial-score-min", help="观察路线升级试买的最低评分"),
        score_dimension_mode: str = typer.Option("full", "--score-dimensions", help="评分维度模式：full 或 tech_fundamental，用于验证资金流维度增量"),
        signal_output_limit: int = typer.Option(200, "--signal-output-limit", min=0, help="JSON 中保留的原始信号行数；0 表示不输出原始行，完整拆分可调大"),
        progress_log: bool = typer.Option(False, "--progress-log/--quiet-progress", help="向 stderr 输出批次和数据阶段进度，JSON 仍只写 stdout"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """大样本批量回测：批次隔离、超时拆分，并汇总信号 Alpha。"""
        from astock_trading.backtest.batch_runner import (
            BacktestBatchConfig,
            run_batched_backtest,
        )

        code_list = [item.strip() for item in codes.split(",") if item.strip()]
        result = run_batched_backtest(
            code_list,
            start,
            end,
            BacktestBatchConfig(
                preset=preset,
                initial_cash=initial_cash,
                adjustflag=adjustflag,
                use_history_mirror=use_history_mirror,
                load_financials=load_financials,
                use_stored_data=use_stored_data,
                hydrate_data=hydrate_data,
                use_market_bars=use_market_bars,
                hydrate_market_bars=hydrate_market_bars,
                red_multiplier=red_multiplier,
                execute_red_trial_buy=execute_red_trial_buy,
                execute_trial_buy_routes=_parse_str_csv(trial_routes),
                execute_watch_trial_markets=_parse_str_csv(watch_trial_markets),
                execute_watch_trial_routes=_parse_str_csv(watch_trial_routes),
                execute_watch_trial_pairs=_parse_str_csv(watch_trial_pairs),
                execute_watch_trial_score_min=watch_trial_score_min,
                score_dimension_mode=score_dimension_mode,
                progress_log=progress_log,
                batch_size=batch_size,
                batch_timeout_seconds=batch_timeout_seconds,
                signal_output_limit=signal_output_limit,
            ),
        )

        if as_json:
            typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
            return

        coverage = result.get("coverage", {})
        portfolio = result.get("portfolio_summary", {})
        typer.echo(f"批量回测 {start} ~ {end} status={result.get('status')}")
        typer.echo(
            f"  股票覆盖: {coverage.get('completed_codes', 0)}/"
            f"{coverage.get('requested_codes', 0)}  失败: {len(coverage.get('failed_codes', []))}"
        )
        typer.echo(
            f"  信号样本: {coverage.get('signal_sample_size', 0)}  "
            f"批次: {coverage.get('completed_batches', 0)} 成功 / "
            f"{coverage.get('failed_batches', 0)} 失败"
        )
        typer.echo(
            f"  批次平均收益: {portfolio.get('avg_total_return_pct', 0):.2f}%  "
            f"最差回撤: {portfolio.get('worst_max_drawdown_pct', 0):.2f}%"
        )

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
