"""Backtest and research CLI commands."""

from __future__ import annotations

import json
from contextlib import nullcontext, redirect_stdout
import sys
from typing import Optional

import typer

DEFAULT_BACKTEST_PRESET = "攻_C_recovery_ma120_green_scale04"


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


def _parse_optional_str_csv(value: str) -> tuple[str, ...] | None:
    parsed = _parse_str_csv(value)
    return parsed or None


def _decode_json_field(value: object, fallback: object):
    if value is None:
        return fallback
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def _discover_replay_codes(start: str, end: str, *, limit: int) -> tuple[str, ...]:
    from astock_trading.platform.db import connect

    conn = connect()
    try:
        rows = conn.execute(
            """SELECT code, MAX(snapshot_date) AS last_seen
               FROM signal_history_discoveries
               WHERE snapshot_date >= ?
                 AND snapshot_date <= ?
               GROUP BY code
               ORDER BY last_seen DESC, code
               LIMIT ?""",
            (start, end, int(limit)),
        ).fetchall()
    finally:
        conn.close()
    return tuple(str(dict(getattr(row, "_mapping", row)).get("code") or "") for row in rows if str(dict(getattr(row, "_mapping", row)).get("code") or ""))


def register_research_commands(app: typer.Typer) -> None:
    @app.command("backtest")
    def run_backtest_cmd(
        codes: str = typer.Argument(..., help="逗号分隔股票代码，如 600036,000001,000002"),
        start: str = typer.Argument(..., help="回测开始日期 YYYY-MM-DD"),
        end: str = typer.Argument(..., help="回测结束日期 YYYY-MM-DD"),
        preset: str = typer.Option(DEFAULT_BACKTEST_PRESET, help="策略 preset（对应 strategy.yaml）"),
        initial_cash: float = typer.Option(100000.0, help="初始资金（元）"),
        adjustflag: str = typer.Option("2", help="复权: 2=前复权 1=后复权 3=不复权"),
        use_history_mirror: bool = typer.Option(True, "--history-mirror/--no-history-mirror", help="优先读取历史信号镜像，缺失时回退代理回放"),
        load_financials: bool = typer.Option(True, "--financials/--skip-financials", help="是否加载财务维度；正式回测应保持开启"),
        use_stored_data: bool = typer.Option(False, "--use-stored-data", help="优先使用 MySQL 已落库的 K 线和财务快照"),
        hydrate_data: bool = typer.Option(False, "--hydrate-data", help="远端拉取缺口并写入 MySQL，后续回测只补增量"),
        use_market_bars: bool = typer.Option(False, "--use-market-bars", help="细粒度选项：优先从 market_price_bars 读取 K 线"),
        hydrate_market_bars: bool = typer.Option(False, "--hydrate-market-bars", help="细粒度选项：远端 K 线拉取成功后写入 market_price_bars"),
        red_multiplier: Optional[float] = typer.Option(None, "--red-multiplier", help="回测情景：覆盖 RED 市场仓位乘数；默认不覆盖"),
        disable_market_reduce_sell: bool = typer.Option(False, "--disable-market-reduce-sell", help="回测情景：禁用大盘 RED/CLEAR 触发的持仓减半卖出；仅用于研究"),
        execute_red_trial_buy: bool = typer.Option(False, "--red-trial", "--execute-red-trial-buy", help="回测情景：把 RED 下的试买意向按 RED 仓位乘数模拟执行"),
        trial_routes: str = typer.Option("", "--trial-routes", help="回测情景：只执行指定路线的试买意向，逗号分隔；留空表示不过滤"),
        buy_phases: str = typer.Option("", "--buy-phases", help="回测情景：正式买入只允许指定市场相位，逗号分隔；留空表示不过滤"),
        watch_trial_markets: str = typer.Option("", "--watch-trial-markets", help="回测情景：把指定市场制度下的观察路线按试买模拟执行，逗号分隔，如 GREEN,RED"),
        watch_trial_routes: str = typer.Option("", "--watch-trial-routes", help="回测情景：仅升级指定观察路线，逗号分隔；留空表示不过滤"),
        watch_trial_pairs: str = typer.Option("", "--watch-trial-pairs", help="回测情景：仅升级指定市场制度和路线组合，格式 GREEN:relative_strength_overheat,YELLOW:pullback_to_ma20；提供后优先于 markets/routes"),
        watch_trial_score_min: Optional[float] = typer.Option(None, "--watch-trial-score-min", help="观察路线升级试买的最低评分；留空使用 preset"),
        watch_trial_score_max: Optional[float] = typer.Option(None, "--watch-trial-score-max", help="观察路线升级试买的最高评分，按左闭右开区间过滤；留空表示不设上限"),
        watch_trial_position_pct: Optional[float] = typer.Option(None, "--watch-trial-position-pct", help="观察路线升级试买的单票模拟仓位；留空则按市场乘数和单票上限计算"),
        watch_trial_phases: str = typer.Option("", "--watch-trial-phases", help="观察路线升级试买的市场相位过滤，逗号分隔，如 below_ma20_slope_up,near_ma20_slope_up"),
        watch_trial_min_above_ma20_days: Optional[int] = typer.Option(None, "--watch-trial-min-above-ma20-days", min=0, help="观察路线升级试买要求指数连续站上 MA20 的最低天数；仅用于研究"),
        watch_trial_min_above_ma20_days_phases: str = typer.Option("", "--watch-trial-min-above-ma20-days-phases", help="站上 MA20 天数下限只作用于指定市场相位，逗号分隔；留空表示所有相位"),
        watch_trial_require_above_ma60_phases: str = typer.Option("", "--watch-trial-require-above-ma60-phases", help="观察路线升级试买要求指数站上 MA60 的市场相位，逗号分隔；仅用于研究"),
        watch_trial_require_above_ma120_phases: str = typer.Option("", "--watch-trial-require-above-ma120-phases", help="观察路线升级试买要求指数站上 MA120 的市场相位，逗号分隔；仅用于研究"),
        holding_max: Optional[int] = typer.Option(None, "--holding-max", min=1, help="回测情景：覆盖最大持仓数；默认使用 preset"),
        trailing_stop: Optional[float] = typer.Option(None, "--trailing-stop", help="回测情景：覆盖移动止盈/追踪止损比例，如 0.20；默认使用 preset"),
        time_stop_days: Optional[int] = typer.Option(None, "--time-stop-days", min=1, help="回测情景：覆盖最长持仓天数；默认使用 preset"),
        stop_loss: Optional[float] = typer.Option(None, "--stop-loss", help="回测情景：覆盖固定止损比例，如 0.06；默认使用 preset"),
        watch_loss_cooldown_days: Optional[int] = typer.Option(None, "--watch-loss-cooldown-days", min=0, help="回测情景：亏损卖出后暂停观察/试买模拟成交的交易日数；仅用于研究"),
        watch_loss_cooldown_phases: str = typer.Option("", "--watch-loss-cooldown-phases", help="回测情景：观察层亏损冷却只在指定市场相位生效，逗号分隔；留空表示所有相位"),
        scale_in_enabled: Optional[bool] = typer.Option(None, "--scale-in/--no-scale-in", help="回测情景：盈利持仓在趋势路线重新确认时补仓；仅用于研究"),
        scale_in_profit_threshold: Optional[float] = typer.Option(None, "--scale-in-profit-threshold", help="趋势加仓最低浮盈比例，如 0.10；留空使用 preset"),
        scale_in_step_position_pct: Optional[float] = typer.Option(None, "--scale-in-step-position-pct", help="每次趋势加仓提高的目标仓位比例，如 0.075"),
        scale_in_max_position_pct: Optional[float] = typer.Option(None, "--scale-in-max-position-pct", help="趋势加仓后的单票最高目标仓位，可高于首笔 single_max"),
        scale_in_max_adds: Optional[int] = typer.Option(None, "--scale-in-max-adds", min=0, help="单票最多趋势加仓次数"),
        scale_in_min_days_between: Optional[int] = typer.Option(None, "--scale-in-min-days-between", min=0, help="同一持仓两次趋势加仓之间的最少交易日"),
        scale_in_routes: str = typer.Option("", "--scale-in-routes", help="允许触发趋势加仓的路线，逗号分隔，如 short_continuation,volume_breakout"),
        scale_in_markets: str = typer.Option("", "--scale-in-markets", help="允许趋势加仓的市场制度，逗号分隔，如 GREEN,YELLOW"),
        scale_in_actions: str = typer.Option("", "--scale-in-actions", help="允许触发持仓加仓的当前动作，逗号分隔；默认 BUY,WATCH"),
        scale_in_require_entry_signal: Optional[bool] = typer.Option(None, "--scale-in-require-entry-signal/--scale-in-no-require-entry-signal", help="趋势加仓是否要求当天仍有入场信号；仅用于研究"),
        scale_in_score_min: Optional[float] = typer.Option(None, "--scale-in-score-min", help="趋势加仓最低评分；留空使用 preset"),
        scale_in_reset_time_stop: Optional[bool] = typer.Option(None, "--scale-in-reset-time-stop/--no-scale-in-reset-time-stop", help="趋势加仓后是否重置时间止损计时；仅用于研究"),
        scale_in_aggressive_max_position_pct: Optional[float] = typer.Option(None, "--scale-in-aggressive-max-position-pct", help="强市场/强路线下趋势加仓的进攻目标仓位上限，如 0.30"),
        scale_in_aggressive_step_position_pct: Optional[float] = typer.Option(None, "--scale-in-aggressive-step-position-pct", help="强市场/强路线下每次趋势加仓提高的目标仓位比例，如 0.08"),
        scale_in_aggressive_markets: str = typer.Option("", "--scale-in-aggressive-markets", help="允许切换进攻加仓的市场制度，逗号分隔，如 GREEN,YELLOW"),
        scale_in_aggressive_routes: str = typer.Option("", "--scale-in-aggressive-routes", help="允许切换进攻加仓的路线，逗号分隔，如 short_continuation,volume_breakout"),
        scale_in_aggressive_phases: str = typer.Option("", "--scale-in-aggressive-phases", help="允许切换进攻加仓的市场相位，逗号分隔；留空表示不按相位过滤"),
        commission_bps: Optional[float] = typer.Option(None, "--commission-bps", min=0.0, help="回测成本：佣金 bps，默认 2.5"),
        min_commission: Optional[float] = typer.Option(None, "--min-commission", min=0.0, help="回测成本：单笔最低佣金，默认 5 元"),
        stamp_tax_bps: Optional[float] = typer.Option(None, "--stamp-tax-bps", min=0.0, help="回测成本：卖出印花税 bps，默认 5"),
        transfer_fee_bps: Optional[float] = typer.Option(None, "--transfer-fee-bps", min=0.0, help="回测成本：过户费 bps，默认 0.1"),
        slippage_bps: Optional[float] = typer.Option(None, "--slippage-bps", min=0.0, help="回测成本：买卖滑点 bps，默认 5"),
        score_dimension_mode: str = typer.Option("full", "--score-dimensions", help="评分维度模式：full 或 tech_fundamental，用于验证资金流维度增量"),
        signal_slices: str = typer.Option("", "--signal-slices", help="信号 Alpha 诊断切片白名单，逗号分隔，如 volume_ratio_bucket,rsi_bucket"),
        include_signal_alpha: bool = typer.Option(True, "--signal-alpha/--skip-signal-alpha", help="是否计算路线前瞻收益统计；组合 what-if 可跳过以加速"),
        reachable_only: bool = typer.Option(False, "--reachable-only/--no-reachable-only", help="只允许历史候选池/评分候选/决策镜像中可发现的买入候选成交"),
        reachable_lookback_days: int = typer.Option(5, "--reachable-lookback-days", min=0, help="可发现性闸门允许的最近发现窗口（自然日）"),
        trade_output_limit: int = typer.Option(50, "--trade-output-limit", help="JSON 中 trades 保留的最近交易数；0 不输出，负数输出全量；trade_log 始终为全量"),
        progress_log: bool = typer.Option(False, "--progress-log/--quiet-progress", help="向 stderr 输出回测阶段进度，JSON 仍只写 stdout"),
        record_run: bool = typer.Option(False, "--record-run", help="将本次回测运行、完整交易日志和权益曲线写入 MySQL"),
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
            disable_market_reduce_sell=disable_market_reduce_sell,
            execute_red_trial_buy=execute_red_trial_buy,
            execute_trial_buy_routes=_parse_optional_str_csv(trial_routes),
            execute_buy_phases=_parse_optional_str_csv(buy_phases),
            execute_watch_trial_markets=_parse_optional_str_csv(watch_trial_markets),
            execute_watch_trial_routes=_parse_optional_str_csv(watch_trial_routes),
            execute_watch_trial_pairs=_parse_optional_str_csv(watch_trial_pairs),
            execute_watch_trial_score_min=watch_trial_score_min,
            execute_watch_trial_score_max=watch_trial_score_max,
            execute_watch_trial_position_pct=watch_trial_position_pct,
            execute_watch_trial_phases=_parse_optional_str_csv(watch_trial_phases),
            execute_watch_trial_min_above_ma20_days=watch_trial_min_above_ma20_days,
            execute_watch_trial_min_above_ma20_days_phases=_parse_optional_str_csv(
                watch_trial_min_above_ma20_days_phases
            ),
            execute_watch_trial_require_above_ma60_phases=_parse_optional_str_csv(
                watch_trial_require_above_ma60_phases
            ),
            execute_watch_trial_require_above_ma120_phases=_parse_optional_str_csv(
                watch_trial_require_above_ma120_phases
            ),
            holding_max=holding_max,
            trailing_stop=trailing_stop,
            time_stop_days=time_stop_days,
            stop_loss=stop_loss,
            watch_loss_cooldown_days=watch_loss_cooldown_days,
            watch_loss_cooldown_phases=_parse_optional_str_csv(watch_loss_cooldown_phases),
            scale_in_enabled=scale_in_enabled,
            scale_in_profit_threshold=scale_in_profit_threshold,
            scale_in_step_position_pct=scale_in_step_position_pct,
            scale_in_max_position_pct=scale_in_max_position_pct,
            scale_in_max_adds=scale_in_max_adds,
            scale_in_min_days_between=scale_in_min_days_between,
            scale_in_routes=_parse_optional_str_csv(scale_in_routes),
            scale_in_market_signals=_parse_optional_str_csv(scale_in_markets),
            scale_in_actions=_parse_optional_str_csv(scale_in_actions),
            scale_in_require_entry_signal=scale_in_require_entry_signal,
            scale_in_score_min=scale_in_score_min,
            scale_in_reset_time_stop=scale_in_reset_time_stop,
            scale_in_aggressive_max_position_pct=scale_in_aggressive_max_position_pct,
            scale_in_aggressive_step_position_pct=scale_in_aggressive_step_position_pct,
            scale_in_aggressive_market_signals=_parse_optional_str_csv(scale_in_aggressive_markets),
            scale_in_aggressive_routes=_parse_optional_str_csv(scale_in_aggressive_routes),
            scale_in_aggressive_phase_buckets=_parse_optional_str_csv(scale_in_aggressive_phases),
            commission_bps=commission_bps,
            min_commission=min_commission,
            stamp_tax_bps=stamp_tax_bps,
            transfer_fee_bps=transfer_fee_bps,
            slippage_bps=slippage_bps,
            score_dimension_mode=score_dimension_mode,
            signal_slices=_parse_optional_str_csv(signal_slices),
            trade_record_limit=None if trade_output_limit < 0 else trade_output_limit,
            include_signal_alpha=include_signal_alpha,
            reachable_only=reachable_only,
            reachable_lookback_days=reachable_lookback_days,
            progress_log=progress_log,
            use_stored_data=use_stored_data,
            hydrate_data=hydrate_data,
            use_market_bars=use_market_bars,
            hydrate_market_bars=hydrate_market_bars,
        )

        if "error" in result:
            typer.echo(f"\u274c {result['error']}", err=True)
            raise typer.Exit(1)

        if record_run:
            from astock_trading.backtest.persistence import save_backtest_result
            from astock_trading.platform.db import connect

            conn = connect()
            try:
                result["recorded_run"] = save_backtest_result(
                    conn,
                    result,
                    request={
                        "codes": list(_parse_str_csv(codes)),
                        "start": start,
                        "end": end,
                        "preset": preset,
                        "initial_cash": initial_cash,
                        "adjustflag": adjustflag,
                        "use_history_mirror": use_history_mirror,
                        "load_financials": load_financials,
                        "use_stored_data": use_stored_data,
                        "hydrate_data": hydrate_data,
                        "use_market_bars": use_market_bars,
                        "hydrate_market_bars": hydrate_market_bars,
                        "red_multiplier": red_multiplier,
                        "disable_market_reduce_sell": disable_market_reduce_sell,
                        "execute_red_trial_buy": execute_red_trial_buy,
                        "trial_routes": _parse_optional_str_csv(trial_routes),
                        "buy_phases": _parse_optional_str_csv(buy_phases),
                        "watch_trial_markets": _parse_optional_str_csv(watch_trial_markets),
                        "watch_trial_routes": _parse_optional_str_csv(watch_trial_routes),
                        "watch_trial_pairs": _parse_optional_str_csv(watch_trial_pairs),
                        "watch_trial_score_min": watch_trial_score_min,
                        "watch_trial_score_max": watch_trial_score_max,
                        "watch_trial_position_pct": watch_trial_position_pct,
                        "watch_trial_phases": _parse_optional_str_csv(watch_trial_phases),
                        "watch_trial_min_above_ma20_days": watch_trial_min_above_ma20_days,
                        "watch_trial_min_above_ma20_days_phases": _parse_optional_str_csv(
                            watch_trial_min_above_ma20_days_phases
                        ),
                        "watch_trial_require_above_ma60_phases": _parse_optional_str_csv(
                            watch_trial_require_above_ma60_phases
                        ),
                        "watch_trial_require_above_ma120_phases": _parse_optional_str_csv(
                            watch_trial_require_above_ma120_phases
                        ),
                        "holding_max": holding_max,
                        "trailing_stop": trailing_stop,
                        "time_stop_days": time_stop_days,
                        "stop_loss": stop_loss,
                        "watch_loss_cooldown_days": watch_loss_cooldown_days,
                        "watch_loss_cooldown_phases": _parse_optional_str_csv(watch_loss_cooldown_phases),
                        "scale_in_enabled": scale_in_enabled,
                        "scale_in_profit_threshold": scale_in_profit_threshold,
                        "scale_in_step_position_pct": scale_in_step_position_pct,
                        "scale_in_max_position_pct": scale_in_max_position_pct,
                        "scale_in_max_adds": scale_in_max_adds,
                        "scale_in_min_days_between": scale_in_min_days_between,
                        "scale_in_routes": _parse_optional_str_csv(scale_in_routes),
                        "scale_in_market_signals": _parse_optional_str_csv(scale_in_markets),
                        "scale_in_actions": _parse_optional_str_csv(scale_in_actions),
                        "scale_in_require_entry_signal": scale_in_require_entry_signal,
                        "scale_in_score_min": scale_in_score_min,
                        "scale_in_reset_time_stop": scale_in_reset_time_stop,
                        "scale_in_aggressive_max_position_pct": scale_in_aggressive_max_position_pct,
                        "scale_in_aggressive_step_position_pct": scale_in_aggressive_step_position_pct,
                        "scale_in_aggressive_market_signals": _parse_optional_str_csv(scale_in_aggressive_markets),
                        "scale_in_aggressive_routes": _parse_optional_str_csv(scale_in_aggressive_routes),
                        "scale_in_aggressive_phase_buckets": _parse_optional_str_csv(scale_in_aggressive_phases),
                        "commission_bps": commission_bps,
                        "min_commission": min_commission,
                        "stamp_tax_bps": stamp_tax_bps,
                        "transfer_fee_bps": transfer_fee_bps,
                        "slippage_bps": slippage_bps,
                        "score_dimension_mode": score_dimension_mode,
                        "signal_slices": _parse_optional_str_csv(signal_slices),
                        "reachable_only": reachable_only,
                        "reachable_lookback_days": reachable_lookback_days,
                        "trade_output_limit": trade_output_limit,
                    },
                )
            finally:
                conn.close()

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
            if record_run and result.get("recorded_run"):
                recorded = result["recorded_run"]
                typer.echo(
                    f"  已记录回测: {recorded.get('run_id')} "
                    f"交易 {recorded.get('trade_count', 0)} 笔 "
                    f"权益点 {recorded.get('equity_curve_points', 0)} 个"
                )

    @app.command("backtest-runs")
    def backtest_runs_cmd(
        limit: int = typer.Option(20, "--limit", min=1, max=200, help="返回最近 N 次回测"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """列出已记录的历史回测运行。"""
        from astock_trading.platform.db import connect

        conn = connect()
        try:
            rows = conn.execute(
                """SELECT run_id, preset, codes_json, start_date, end_date,
                          initial_cash, final_value, metrics_json, created_at
                   FROM backtest_runs
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        finally:
            conn.close()

        runs = [
            {
                "run_id": row["run_id"],
                "preset": row["preset"],
                "codes": _decode_json_field(row["codes_json"], []),
                "start_date": row["start_date"],
                "end_date": row["end_date"],
                "initial_cash": row["initial_cash"],
                "final_value": row["final_value"],
                "metrics": _decode_json_field(row["metrics_json"], {}),
                "created_at": row["created_at"],
            }
            for row in rows
        ]
        payload = {"status": "ok", "count": len(runs), "runs": runs}
        if as_json:
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        typer.echo(f"已记录回测: {len(runs)} 条")
        for item in runs:
            metrics = item.get("metrics") or {}
            typer.echo(
                f"  {item['run_id']} {item['start_date']}~{item['end_date']} "
                f"{','.join(item.get('codes') or [])} "
                f"年化 {_format_metric(metrics.get('annual_return_pct'), '.2f')}%"
            )

    @app.command("replay-production")
    def replay_production_cmd(
        start: str = typer.Argument(..., help="重放开始日期 YYYY-MM-DD"),
        end: str = typer.Argument(..., help="重放结束日期 YYYY-MM-DD"),
        codes: str = typer.Option("", "--codes", help="逗号分隔股票代码；留空则从历史发现索引读取"),
        code_limit: int = typer.Option(500, "--code-limit", min=1, help="未显式给 codes 时最多读取的历史发现股票数"),
        preset: str = typer.Option(DEFAULT_BACKTEST_PRESET, help="策略 preset（对应 strategy.yaml）"),
        initial_cash: float = typer.Option(100000.0, help="初始资金（元）"),
        reachable_lookback_days: int = typer.Option(5, "--reachable-lookback-days", min=0, help="可发现性闸门允许的最近发现窗口（自然日）"),
        load_financials: bool = typer.Option(True, "--financials/--skip-financials", help="是否加载财务维度；正式重放应保持开启"),
        signal_alpha: bool = typer.Option(True, "--signal-alpha/--no-signal-alpha", help="是否汇总信号前瞻收益；全量性能诊断可关闭"),
        trade_output_limit: int = typer.Option(50, "--trade-output-limit", help="JSON 中 trades 保留的最近交易数；0 不输出，负数输出全量"),
        progress_log: bool = typer.Option(False, "--progress-log/--quiet-progress", help="向 stderr 输出重放阶段进度，JSON 仍只写 stdout"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """生产级历史重放：历史镜像 + reachable-only + 已落库数据 + A 股撮合约束。"""
        from astock_trading.backtest import engine as engine_module

        selected_codes = _parse_str_csv(codes) if codes else _discover_replay_codes(start, end, limit=code_limit)
        if not selected_codes:
            payload = {
                "status": "failed",
                "error": "未找到可重放股票代码；请先运行 data-sources replay-discovery/index-discoveries，或显式传 --codes",
                "start": start,
                "end": end,
            }
            if as_json:
                typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
                raise typer.Exit(1)
            typer.echo(f"\u274c {payload['error']}", err=True)
            raise typer.Exit(1)

        output_guard = redirect_stdout(sys.stderr) if as_json else nullcontext()
        with output_guard:
            result = engine_module.run_backtest(
                codes=",".join(selected_codes),
                start=start,
                end=end,
                preset=preset,
                initial_cash=initial_cash,
                adjustflag="3",
                pnl_adjustflag="3",
                use_history_mirror=True,
                load_financials=load_financials,
                reachable_only=True,
                reachable_lookback_days=reachable_lookback_days,
                trade_record_limit=None if trade_output_limit < 0 else trade_output_limit,
                include_signal_alpha=signal_alpha,
                use_stored_data=True,
                use_market_bars=True,
                progress_log=progress_log,
            )
        if "error" in result:
            if as_json:
                typer.echo(json.dumps({"status": "failed", "error": result["error"], "backtest": result}, ensure_ascii=False, indent=2))
                raise typer.Exit(1)
            typer.echo(f"\u274c {result['error']}", err=True)
            raise typer.Exit(1)

        payload = {
            "status": "ok",
            "mode": "production_replay",
            "start": start,
            "end": end,
            "codes": list(selected_codes),
            "reachable_definition": {
                "scope": "最近 K 个自然日内进入过历史发现池、评分候选或决策镜像",
                "lookback_days": reachable_lookback_days,
                "source": "signal_history_discoveries + signal_history_snapshots",
            },
            "point_in_time_contract": {
                "history_mirror": True,
                "reachable_only": True,
                "adjustflag": "3",
                "pnl_adjustflag": "3",
                "use_stored_data": True,
                "signal_alpha": bool(signal_alpha),
                "notes": [
                    "日线发现输入使用不复权口径，避免前复权引入未来价格信息。",
                    "生产重放收益撮合也使用已落库不复权日线，不远程补后复权数据。",
                    "财务维度由回测引擎按 available_date 选择已披露快照。",
                    "交易撮合启用 T+1、锁板不可成交和真实成本模型。",
                ],
            },
            "backtest": result,
        }
        if as_json:
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
            return
        typer.echo(f"生产重放 {start} ~ {end} 股票 {len(selected_codes)} 只")
        typer.echo(
            f"  净收益: {result.get('total_return_pct', 0):.2f}%  "
            f"年化: {result.get('annual_return_pct', 0):.2f}%  "
            f"回撤: {result.get('max_drawdown_pct', 0):.2f}%"
        )

    @app.command("backtest-batch")
    def run_backtest_batch_cmd(
        codes: str = typer.Argument(..., help="逗号分隔股票代码，支持较大股票池"),
        start: str = typer.Argument(..., help="回测开始日期 YYYY-MM-DD"),
        end: str = typer.Argument(..., help="回测结束日期 YYYY-MM-DD"),
        preset: str = typer.Option(DEFAULT_BACKTEST_PRESET, help="策略 preset（对应 strategy.yaml）"),
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
        disable_market_reduce_sell: bool = typer.Option(False, "--disable-market-reduce-sell", help="回测情景：禁用大盘 RED/CLEAR 触发的持仓减半卖出；仅用于研究"),
        execute_red_trial_buy: bool = typer.Option(False, "--red-trial", "--execute-red-trial-buy", help="回测情景：把 RED 下的试买意向按 RED 仓位乘数模拟执行"),
        trial_routes: str = typer.Option("", "--trial-routes", help="回测情景：只执行指定路线的试买意向，逗号分隔；留空表示不过滤"),
        buy_phases: str = typer.Option("", "--buy-phases", help="回测情景：正式买入只允许指定市场相位，逗号分隔；留空表示不过滤"),
        watch_trial_markets: str = typer.Option("", "--watch-trial-markets", help="回测情景：把指定市场制度下的观察路线按试买模拟执行，逗号分隔，如 GREEN,RED"),
        watch_trial_routes: str = typer.Option("", "--watch-trial-routes", help="回测情景：仅升级指定观察路线，逗号分隔；留空表示不过滤"),
        watch_trial_pairs: str = typer.Option("", "--watch-trial-pairs", help="回测情景：仅升级指定市场制度和路线组合，格式 GREEN:relative_strength_overheat,YELLOW:pullback_to_ma20；提供后优先于 markets/routes"),
        watch_trial_score_min: Optional[float] = typer.Option(None, "--watch-trial-score-min", help="观察路线升级试买的最低评分；留空使用 preset"),
        watch_trial_score_max: Optional[float] = typer.Option(None, "--watch-trial-score-max", help="观察路线升级试买的最高评分，按左闭右开区间过滤；留空表示不设上限"),
        watch_trial_position_pct: Optional[float] = typer.Option(None, "--watch-trial-position-pct", help="观察路线升级试买的单票模拟仓位；留空则按市场乘数和单票上限计算"),
        watch_trial_phases: str = typer.Option("", "--watch-trial-phases", help="观察路线升级试买的市场相位过滤，逗号分隔，如 below_ma20_slope_up,near_ma20_slope_up"),
        watch_trial_min_above_ma20_days: Optional[int] = typer.Option(None, "--watch-trial-min-above-ma20-days", min=0, help="观察路线升级试买要求指数连续站上 MA20 的最低天数；仅用于研究"),
        watch_trial_min_above_ma20_days_phases: str = typer.Option("", "--watch-trial-min-above-ma20-days-phases", help="站上 MA20 天数下限只作用于指定市场相位，逗号分隔；留空表示所有相位"),
        watch_trial_require_above_ma60_phases: str = typer.Option("", "--watch-trial-require-above-ma60-phases", help="观察路线升级试买要求指数站上 MA60 的市场相位，逗号分隔；仅用于研究"),
        watch_trial_require_above_ma120_phases: str = typer.Option("", "--watch-trial-require-above-ma120-phases", help="观察路线升级试买要求指数站上 MA120 的市场相位，逗号分隔；仅用于研究"),
        watch_loss_cooldown_days: Optional[int] = typer.Option(None, "--watch-loss-cooldown-days", min=0, help="回测情景：亏损卖出后暂停观察/试买模拟成交的交易日数；仅用于研究"),
        watch_loss_cooldown_phases: str = typer.Option("", "--watch-loss-cooldown-phases", help="回测情景：观察层亏损冷却只在指定市场相位生效，逗号分隔；留空表示所有相位"),
        commission_bps: Optional[float] = typer.Option(None, "--commission-bps", min=0.0, help="回测成本：佣金 bps，默认 2.5"),
        min_commission: Optional[float] = typer.Option(None, "--min-commission", min=0.0, help="回测成本：单笔最低佣金，默认 5 元"),
        stamp_tax_bps: Optional[float] = typer.Option(None, "--stamp-tax-bps", min=0.0, help="回测成本：卖出印花税 bps，默认 5"),
        transfer_fee_bps: Optional[float] = typer.Option(None, "--transfer-fee-bps", min=0.0, help="回测成本：过户费 bps，默认 0.1"),
        slippage_bps: Optional[float] = typer.Option(None, "--slippage-bps", min=0.0, help="回测成本：买卖滑点 bps，默认 5"),
        score_dimension_mode: str = typer.Option("full", "--score-dimensions", help="评分维度模式：full 或 tech_fundamental，用于验证资金流维度增量"),
        signal_slices: str = typer.Option("", "--signal-slices", help="信号 Alpha 诊断切片白名单，逗号分隔，如 volume_ratio_bucket,rsi_bucket"),
        reachable_only: bool = typer.Option(False, "--reachable-only/--no-reachable-only", help="只允许历史候选池/评分候选/决策镜像中可发现的买入候选成交"),
        reachable_lookback_days: int = typer.Option(5, "--reachable-lookback-days", min=0, help="可发现性闸门允许的最近发现窗口（自然日）"),
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
                disable_market_reduce_sell=disable_market_reduce_sell,
                execute_red_trial_buy=execute_red_trial_buy,
                execute_trial_buy_routes=_parse_optional_str_csv(trial_routes),
                execute_buy_phases=_parse_optional_str_csv(buy_phases),
                execute_watch_trial_markets=_parse_optional_str_csv(watch_trial_markets),
                execute_watch_trial_routes=_parse_optional_str_csv(watch_trial_routes),
                execute_watch_trial_pairs=_parse_optional_str_csv(watch_trial_pairs),
                execute_watch_trial_score_min=watch_trial_score_min,
                execute_watch_trial_score_max=watch_trial_score_max,
                execute_watch_trial_position_pct=watch_trial_position_pct,
                execute_watch_trial_phases=_parse_optional_str_csv(watch_trial_phases),
                execute_watch_trial_min_above_ma20_days=watch_trial_min_above_ma20_days,
                execute_watch_trial_min_above_ma20_days_phases=_parse_optional_str_csv(
                    watch_trial_min_above_ma20_days_phases
                ),
                execute_watch_trial_require_above_ma60_phases=_parse_optional_str_csv(
                    watch_trial_require_above_ma60_phases
                ),
                execute_watch_trial_require_above_ma120_phases=_parse_optional_str_csv(
                    watch_trial_require_above_ma120_phases
                ),
                watch_loss_cooldown_days=watch_loss_cooldown_days,
                watch_loss_cooldown_phases=_parse_optional_str_csv(watch_loss_cooldown_phases),
                commission_bps=2.5 if commission_bps is None else commission_bps,
                min_commission=5.0 if min_commission is None else min_commission,
                stamp_tax_bps=5.0 if stamp_tax_bps is None else stamp_tax_bps,
                transfer_fee_bps=0.1 if transfer_fee_bps is None else transfer_fee_bps,
                slippage_bps=5.0 if slippage_bps is None else slippage_bps,
                score_dimension_mode=score_dimension_mode,
                signal_slices=_parse_optional_str_csv(signal_slices),
                reachable_only=reachable_only,
                reachable_lookback_days=reachable_lookback_days,
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
