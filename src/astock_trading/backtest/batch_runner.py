"""Robust batched backtest runner for larger sample validation."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
import json
import io
import multiprocessing as mp
import os
from pathlib import Path
from contextlib import nullcontext
import sys
import tempfile
import time
from statistics import mean, median
import traceback
from typing import Any, Callable

from astock_trading.backtest.signal_analysis import signal_alpha_summary


BatchRunner = Callable[[list[str]], dict[str, Any]]


@dataclass
class BacktestBatchConfig:
    preset: str = "保守验证C"
    initial_cash: float = 100000.0
    adjustflag: str = "2"
    use_history_mirror: bool = True
    red_multiplier: float | None = None
    disable_market_reduce_sell: bool = False
    watch_loss_cooldown_days: int | None = None
    watch_loss_cooldown_phases: tuple[str, ...] | None = None
    execute_red_trial_buy: bool = False
    execute_trial_buy_routes: tuple[str, ...] | None = None
    execute_buy_phases: tuple[str, ...] | None = None
    execute_watch_trial_markets: tuple[str, ...] | None = None
    execute_watch_trial_routes: tuple[str, ...] | None = None
    execute_watch_trial_pairs: tuple[str, ...] | None = None
    execute_watch_trial_score_min: float | None = None
    execute_watch_trial_score_max: float | None = None
    execute_watch_trial_position_pct: float | None = None
    execute_watch_trial_phases: tuple[str, ...] | None = None
    execute_watch_trial_min_above_ma20_days: int | None = None
    execute_watch_trial_min_above_ma20_days_phases: tuple[str, ...] | None = None
    execute_watch_trial_require_above_ma60_phases: tuple[str, ...] | None = None
    execute_watch_trial_require_above_ma120_phases: tuple[str, ...] | None = None
    score_dimension_mode: str = "full"
    reachable_only: bool = False
    reachable_lookback_days: int = 5
    load_financials: bool = True
    use_stored_data: bool = False
    hydrate_data: bool = False
    use_market_bars: bool = False
    hydrate_market_bars: bool = False
    commission_bps: float = 2.5
    min_commission: float = 5.0
    stamp_tax_bps: float = 5.0
    transfer_fee_bps: float = 0.1
    slippage_bps: float = 5.0
    batch_size: int = 8
    batch_timeout_seconds: float = 240.0
    split_on_timeout: bool = True
    signal_output_limit: int = 200
    progress_log: bool = False


def run_batched_backtest(
    codes: list[str],
    start: str,
    end: str,
    config: BacktestBatchConfig | None = None,
    *,
    batch_runner: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run backtests in isolated batches and aggregate signal-quality evidence."""
    cfg = config or BacktestBatchConfig()
    normalized_codes = [str(code).strip() for code in codes if str(code).strip()]
    if not normalized_codes:
        return {"status": "failed", "error": "股票代码列表为空"}

    runner = batch_runner or _run_batch_in_process
    successful_batches: list[dict[str, Any]] = []
    failed_batches: list[dict[str, Any]] = []

    for batch in _chunks(normalized_codes, max(int(cfg.batch_size), 1)):
        _run_batch_with_recovery(
            batch,
            start,
            end,
            cfg,
            runner=runner,
            successful_batches=successful_batches,
            failed_batches=failed_batches,
        )

    all_signals: list[dict[str, Any]] = []
    loaded_codes: set[str] = set()
    for item in successful_batches:
        result = item["result"]
        all_signals.extend((result.get("signal_validation") or {}).get("signals") or [])
        data_coverage = result.get("data_coverage") or {}
        loaded = data_coverage.get("loaded_code_list") or item["codes"]
        loaded_codes.update(str(code) for code in loaded)

    failed_codes = sorted({code for item in failed_batches for code in item["codes"]})
    unknown_signals = [
        signal for signal in all_signals
        if signal.get("primary_strategy_route") == "unknown"
    ]
    signal_limit = max(int(cfg.signal_output_limit), 0)
    visible_signals = all_signals[-signal_limit:] if signal_limit else []

    return {
        "status": "partial" if failed_batches else "completed",
        "start": start,
        "end": end,
        "config": {
            "preset": cfg.preset,
            "batch_size": cfg.batch_size,
            "batch_timeout_seconds": cfg.batch_timeout_seconds,
            "red_multiplier": cfg.red_multiplier,
            "disable_market_reduce_sell": cfg.disable_market_reduce_sell,
            "watch_loss_cooldown_days": cfg.watch_loss_cooldown_days,
            "watch_loss_cooldown_phases": list(cfg.watch_loss_cooldown_phases or ()),
            "execute_red_trial_buy": cfg.execute_red_trial_buy,
            "execute_trial_buy_routes": list(cfg.execute_trial_buy_routes or ()),
            "execute_buy_phases": list(cfg.execute_buy_phases or ()),
            "execute_watch_trial_markets": list(cfg.execute_watch_trial_markets or ()),
            "execute_watch_trial_routes": list(cfg.execute_watch_trial_routes or ()),
            "execute_watch_trial_pairs": list(cfg.execute_watch_trial_pairs or ()),
            "execute_watch_trial_score_min": cfg.execute_watch_trial_score_min,
            "execute_watch_trial_score_max": cfg.execute_watch_trial_score_max,
            "execute_watch_trial_position_pct": cfg.execute_watch_trial_position_pct,
            "execute_watch_trial_phases": list(cfg.execute_watch_trial_phases or ()),
            "execute_watch_trial_min_above_ma20_days": cfg.execute_watch_trial_min_above_ma20_days,
            "execute_watch_trial_min_above_ma20_days_phases": list(
                cfg.execute_watch_trial_min_above_ma20_days_phases or ()
            ),
            "execute_watch_trial_require_above_ma60_phases": list(
                cfg.execute_watch_trial_require_above_ma60_phases or ()
            ),
            "execute_watch_trial_require_above_ma120_phases": list(
                cfg.execute_watch_trial_require_above_ma120_phases or ()
            ),
            "score_dimension_mode": cfg.score_dimension_mode,
            "reachable_only": cfg.reachable_only,
            "reachable_lookback_days": cfg.reachable_lookback_days,
            "load_financials": cfg.load_financials,
            "use_stored_data": cfg.use_stored_data,
            "hydrate_data": cfg.hydrate_data,
            "use_market_bars": cfg.use_market_bars,
            "hydrate_market_bars": cfg.hydrate_market_bars,
            "commission_bps": cfg.commission_bps,
            "min_commission": cfg.min_commission,
            "stamp_tax_bps": cfg.stamp_tax_bps,
            "transfer_fee_bps": cfg.transfer_fee_bps,
            "slippage_bps": cfg.slippage_bps,
        },
        "coverage": {
            "requested_codes": len(normalized_codes),
            "completed_codes": len(loaded_codes),
            "failed_codes": failed_codes,
            "completed_batches": len(successful_batches),
            "failed_batches": len(failed_batches),
            "signal_sample_size": len(all_signals),
        },
        "execution_semantics": _execution_semantics(cfg),
        "discovery_reachability": _aggregate_reachability(
            [item["result"] for item in successful_batches],
            cfg,
        ),
        "portfolio_summary": _portfolio_summary([item["result"] for item in successful_batches]),
        "batch_reports": [
            _compact_batch_report(item["codes"], item["result"])
            for item in successful_batches
        ],
        "failed_batches": failed_batches,
        "signal_alpha": signal_alpha_summary(all_signals),
        "signal_validation": {
            "sample_size": len(all_signals),
            "signals": visible_signals,
            "unknown_route_count": len(unknown_signals),
            "unknown_route_samples": unknown_signals[-20:],
        },
    }


def _execution_semantics(cfg: BacktestBatchConfig) -> dict[str, Any]:
    watch_trial_enabled = bool(
        cfg.execute_watch_trial_pairs
        or cfg.execute_watch_trial_markets
        or cfg.execute_watch_trial_routes
    )
    trial_buy_enabled = bool(
        cfg.execute_red_trial_buy
        or cfg.execute_trial_buy_routes
    )
    loss_cooldown_enabled = bool(cfg.watch_loss_cooldown_days and cfg.watch_loss_cooldown_days > 0)
    research_enabled = bool(watch_trial_enabled or trial_buy_enabled or loss_cooldown_enabled)
    notes = [
        "默认只执行正式 BUY；route_execution_policy 可用于 BUY 排序、仓位覆盖和显式路线正式化。",
    ]
    if research_enabled:
        notes.append("本次批量回测包含显式研究 what-if：允许部分 WATCH/TRIAL_BUY 按规则模拟成交。")
    if loss_cooldown_enabled:
        notes.append("本次批量回测包含观察层亏损冷却：亏损卖出后暂停观察/试买模拟成交。")
    if cfg.reachable_only:
        notes.append("本次批量回测启用可发现性闸门：买入候选必须在历史候选池、评分候选或决策镜像中出现过。")
    return {
        "mode": "research_what_if" if research_enabled else "production_buy_only",
        "buy_only": not research_enabled,
        "t_plus_one": True,
        "signal_execution_lag": "next_trading_day_open",
        "limit_price_model": "execution_price_near_limit_blocked",
        "cost_model": "commission_stamp_transfer_slippage",
        "reachable_only": bool(cfg.reachable_only),
        "reachable_lookback_days": int(cfg.reachable_lookback_days or 0),
        "watch_trial_enabled": watch_trial_enabled,
        "trial_buy_enabled": trial_buy_enabled,
        "watch_loss_cooldown_days": int(cfg.watch_loss_cooldown_days or 0),
        "watch_loss_cooldown_phases": list(cfg.watch_loss_cooldown_phases or ()),
        "watch_trial_min_above_ma20_days": int(cfg.execute_watch_trial_min_above_ma20_days or 0),
        "watch_trial_min_above_ma20_days_phases": list(
            cfg.execute_watch_trial_min_above_ma20_days_phases or ()
        ),
        "watch_trial_require_above_ma60_phases": list(
            cfg.execute_watch_trial_require_above_ma60_phases or ()
        ),
        "watch_trial_require_above_ma120_phases": list(
            cfg.execute_watch_trial_require_above_ma120_phases or ()
        ),
        "route_policy_default_actions": ["BUY"],
        "notes": notes,
    }


def _aggregate_reachability(results: list[dict[str, Any]], cfg: BacktestBatchConfig) -> dict[str, Any]:
    totals: dict[str, Any] = {
        "candidate_checks": 0,
        "reachable_candidates": 0,
        "blocked_candidates": 0,
        "discovery_sources": {},
        "blocked_reasons": {},
        "blocked_codes": {},
    }
    for result in results:
        item = result.get("discovery_reachability") or {}
        totals["candidate_checks"] += int(item.get("candidate_checks") or 0)
        totals["reachable_candidates"] += int(item.get("reachable_candidates") or 0)
        totals["blocked_candidates"] += int(item.get("blocked_candidates") or 0)
        for key in ("discovery_sources", "blocked_reasons", "blocked_codes"):
            for name, count in (item.get(key) or {}).items():
                totals[key][str(name)] = int(totals[key].get(str(name), 0)) + int(count or 0)
    checks = int(totals["candidate_checks"] or 0)
    reachable = int(totals["reachable_candidates"] or 0)
    return {
        "enabled": bool(cfg.reachable_only),
        "lookback_days": int(cfg.reachable_lookback_days or 0),
        **totals,
        "reachable_buy_rate_pct": round(reachable / checks * 100, 2) if checks else 0.0,
    }


def _run_batch_with_recovery(
    codes: list[str],
    start: str,
    end: str,
    cfg: BacktestBatchConfig,
    *,
    runner: Callable[..., dict[str, Any]],
    successful_batches: list[dict[str, Any]],
    failed_batches: list[dict[str, Any]],
) -> None:
    try:
        _log_progress(cfg, "batch_start", codes=",".join(codes), size=len(codes))
        started_at = time.monotonic()
        result = runner(codes, start=start, end=end, config=cfg)
    except TimeoutError as exc:
        _log_progress(cfg, "batch_timeout", codes=",".join(codes), message=str(exc))
        if cfg.split_on_timeout and len(codes) > 1:
            _log_progress(cfg, "batch_split", codes=",".join(codes), parts=len(codes))
            for code in codes:
                _run_batch_with_recovery(
                    [code],
                    start,
                    end,
                    cfg,
                    runner=runner,
                    successful_batches=successful_batches,
                    failed_batches=failed_batches,
                )
            return
        failed_batches.append({
            "codes": list(codes),
            "error": "batch_timeout",
            "message": str(exc),
        })
        return
    except Exception as exc:
        _log_progress(
            cfg,
            "batch_error",
            codes=",".join(codes),
            error=exc.__class__.__name__,
            message=str(exc),
        )
        failed_batches.append({
            "codes": list(codes),
            "error": exc.__class__.__name__,
            "message": str(exc),
        })
        return

    if "error" in result:
        _log_progress(
            cfg,
            "batch_failed",
            codes=",".join(codes),
            error=result.get("error"),
        )
        failed_batches.append({
            "codes": list(codes),
            "error": str(result.get("error") or "backtest_failed"),
            "message": str(result.get("message") or result.get("error") or ""),
        })
        return

    successful_batches.append({"codes": list(codes), "result": result})
    _log_progress(
        cfg,
        "batch_done",
        codes=",".join(codes),
        seconds=round(time.monotonic() - started_at, 2),
        signals=(result.get("signal_validation") or {}).get("sample_size", 0),
    )


def _run_batch_in_process(
    codes: list[str],
    *,
    start: str,
    end: str,
    config: BacktestBatchConfig,
) -> dict[str, Any]:
    ctx = mp.get_context()
    queue = ctx.Queue(maxsize=1)
    fd, result_path = tempfile.mkstemp(prefix="atrade_backtest_batch_", suffix=".json")
    os.close(fd)
    process = ctx.Process(
        target=_run_backtest_child,
        args=(queue, {
            "codes": list(codes),
            "start": start,
            "end": end,
            "preset": config.preset,
            "initial_cash": config.initial_cash,
            "adjustflag": config.adjustflag,
            "use_history_mirror": config.use_history_mirror,
            "red_multiplier": config.red_multiplier,
            "disable_market_reduce_sell": config.disable_market_reduce_sell,
            "watch_loss_cooldown_days": config.watch_loss_cooldown_days,
            "watch_loss_cooldown_phases": None if config.watch_loss_cooldown_phases is None else list(config.watch_loss_cooldown_phases),
            "execute_red_trial_buy": config.execute_red_trial_buy,
            "execute_trial_buy_routes": None if config.execute_trial_buy_routes is None else list(config.execute_trial_buy_routes),
            "execute_buy_phases": None if config.execute_buy_phases is None else list(config.execute_buy_phases),
            "execute_watch_trial_markets": None if config.execute_watch_trial_markets is None else list(config.execute_watch_trial_markets),
            "execute_watch_trial_routes": None if config.execute_watch_trial_routes is None else list(config.execute_watch_trial_routes),
            "execute_watch_trial_pairs": None if config.execute_watch_trial_pairs is None else list(config.execute_watch_trial_pairs),
            "execute_watch_trial_score_min": config.execute_watch_trial_score_min,
            "execute_watch_trial_score_max": config.execute_watch_trial_score_max,
            "execute_watch_trial_position_pct": config.execute_watch_trial_position_pct,
            "execute_watch_trial_phases": None if config.execute_watch_trial_phases is None else list(config.execute_watch_trial_phases),
            "execute_watch_trial_min_above_ma20_days": config.execute_watch_trial_min_above_ma20_days,
            "execute_watch_trial_min_above_ma20_days_phases": (
                None
                if config.execute_watch_trial_min_above_ma20_days_phases is None
                else list(config.execute_watch_trial_min_above_ma20_days_phases)
            ),
            "execute_watch_trial_require_above_ma60_phases": (
                None
                if config.execute_watch_trial_require_above_ma60_phases is None
                else list(config.execute_watch_trial_require_above_ma60_phases)
            ),
            "execute_watch_trial_require_above_ma120_phases": (
                None
                if config.execute_watch_trial_require_above_ma120_phases is None
                else list(config.execute_watch_trial_require_above_ma120_phases)
            ),
            "score_dimension_mode": config.score_dimension_mode,
            "reachable_only": config.reachable_only,
            "reachable_lookback_days": config.reachable_lookback_days,
            "load_financials": config.load_financials,
            "progress_log": config.progress_log,
            "use_stored_data": config.use_stored_data,
            "hydrate_data": config.hydrate_data,
            "use_market_bars": config.use_market_bars,
            "hydrate_market_bars": config.hydrate_market_bars,
            "commission_bps": config.commission_bps,
            "min_commission": config.min_commission,
            "stamp_tax_bps": config.stamp_tax_bps,
            "transfer_fee_bps": config.transfer_fee_bps,
            "slippage_bps": config.slippage_bps,
            "result_path": result_path,
        }),
    )
    process.start()
    process.join(float(config.batch_timeout_seconds))
    if process.is_alive():
        process.terminate()
        process.join(5)
        raise TimeoutError(
            f"批次 {','.join(codes)} 超过 {config.batch_timeout_seconds:.0f}s"
        )

    if queue.empty():
        return {"error": "batch_process_no_result", "message": f"exitcode={process.exitcode}"}

    try:
        payload = queue.get()
        if not payload.get("ok"):
            return {
                "error": payload.get("error") or "batch_process_error",
                "message": payload.get("traceback") or "",
            }
        path = Path(payload.get("result_path") or result_path)
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    finally:
        try:
            Path(result_path).unlink(missing_ok=True)
        except Exception:
            pass


def _run_backtest_child(queue: Any, payload: dict[str, Any]) -> None:
    try:
        from astock_trading.backtest.engine import run_backtest

        stderr_context = nullcontext() if payload["progress_log"] else redirect_stderr(io.StringIO())
        with redirect_stdout(io.StringIO()), stderr_context:
            result = run_backtest(
                codes=",".join(payload["codes"]),
                start=payload["start"],
                end=payload["end"],
                preset=payload["preset"],
                initial_cash=payload["initial_cash"],
                adjustflag=payload["adjustflag"],
                use_history_mirror=payload["use_history_mirror"],
                red_multiplier=payload["red_multiplier"],
                disable_market_reduce_sell=bool(payload.get("disable_market_reduce_sell", False)),
                watch_loss_cooldown_days=payload.get("watch_loss_cooldown_days"),
                watch_loss_cooldown_phases=None if payload.get("watch_loss_cooldown_phases") is None else tuple(payload.get("watch_loss_cooldown_phases") or ()),
                execute_red_trial_buy=payload["execute_red_trial_buy"],
                execute_trial_buy_routes=None if payload.get("execute_trial_buy_routes") is None else tuple(payload["execute_trial_buy_routes"]),
                execute_buy_phases=None if payload.get("execute_buy_phases") is None else tuple(payload.get("execute_buy_phases") or ()),
                execute_watch_trial_markets=None if payload.get("execute_watch_trial_markets") is None else tuple(payload["execute_watch_trial_markets"]),
                execute_watch_trial_routes=None if payload.get("execute_watch_trial_routes") is None else tuple(payload["execute_watch_trial_routes"]),
                execute_watch_trial_pairs=None if payload.get("execute_watch_trial_pairs") is None else tuple(payload["execute_watch_trial_pairs"]),
                execute_watch_trial_score_min=payload["execute_watch_trial_score_min"],
                execute_watch_trial_score_max=payload.get("execute_watch_trial_score_max"),
                execute_watch_trial_position_pct=payload.get("execute_watch_trial_position_pct"),
                execute_watch_trial_phases=None if payload.get("execute_watch_trial_phases") is None else tuple(payload.get("execute_watch_trial_phases") or ()),
                execute_watch_trial_min_above_ma20_days=payload.get("execute_watch_trial_min_above_ma20_days"),
                execute_watch_trial_min_above_ma20_days_phases=(
                    None
                    if payload.get("execute_watch_trial_min_above_ma20_days_phases") is None
                    else tuple(payload.get("execute_watch_trial_min_above_ma20_days_phases") or ())
                ),
                execute_watch_trial_require_above_ma60_phases=(
                    None
                    if payload.get("execute_watch_trial_require_above_ma60_phases") is None
                    else tuple(payload.get("execute_watch_trial_require_above_ma60_phases") or ())
                ),
                execute_watch_trial_require_above_ma120_phases=(
                    None
                    if payload.get("execute_watch_trial_require_above_ma120_phases") is None
                    else tuple(payload.get("execute_watch_trial_require_above_ma120_phases") or ())
                ),
                score_dimension_mode=payload["score_dimension_mode"],
                reachable_only=bool(payload.get("reachable_only", False)),
                reachable_lookback_days=int(payload.get("reachable_lookback_days", 5) or 0),
                signal_record_limit=None,
                load_financials=payload["load_financials"],
                progress_log=payload["progress_log"],
                use_stored_data=payload["use_stored_data"],
                hydrate_data=payload["hydrate_data"],
                use_market_bars=payload["use_market_bars"],
                hydrate_market_bars=payload["hydrate_market_bars"],
                commission_bps=payload.get("commission_bps"),
                min_commission=payload.get("min_commission"),
                stamp_tax_bps=payload.get("stamp_tax_bps"),
                transfer_fee_bps=payload.get("transfer_fee_bps"),
                slippage_bps=payload.get("slippage_bps"),
            )
        with open(payload["result_path"], "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)
        queue.put({"ok": True, "result_path": payload["result_path"]})
    except BaseException as exc:  # noqa: BLE001 - child must report all failures.
        queue.put({
            "ok": False,
            "error": exc.__class__.__name__,
            "traceback": traceback.format_exc(),
        })


def _chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[idx:idx + size] for idx in range(0, len(values), size)]


def _log_progress(cfg: BacktestBatchConfig, event: str, **fields: Any) -> None:
    if not cfg.progress_log:
        return
    chunks = [f"event={event}"]
    for key, value in fields.items():
        chunks.append(f"{key}={value}")
    print("[backtest_batch_progress] " + " ".join(chunks), file=sys.stderr, flush=True)


def _portfolio_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [float(item.get("total_return_pct", 0) or 0) for item in results]
    drawdowns = [float(item.get("max_drawdown_pct", 0) or 0) for item in results]
    win_rates = [float(item.get("win_rate_pct", 0) or 0) for item in results]
    sharpe_ratios = [float(item.get("sharpe_ratio", 0) or 0) for item in results]
    calmar_ratios = [float(item.get("calmar_ratio", 0) or 0) for item in results]
    return {
        "batch_count": len(results),
        "avg_total_return_pct": round(mean(returns), 2) if returns else 0.0,
        "median_total_return_pct": round(median(returns), 2) if returns else 0.0,
        "min_total_return_pct": round(min(returns), 2) if returns else 0.0,
        "max_total_return_pct": round(max(returns), 2) if returns else 0.0,
        "avg_max_drawdown_pct": round(mean(drawdowns), 2) if drawdowns else 0.0,
        "worst_max_drawdown_pct": round(max(drawdowns), 2) if drawdowns else 0.0,
        "avg_win_rate_pct": round(mean(win_rates), 2) if win_rates else 0.0,
        "avg_sharpe_ratio": round(mean(sharpe_ratios), 2) if sharpe_ratios else 0.0,
        "avg_calmar_ratio": round(mean(calmar_ratios), 2) if calmar_ratios else 0.0,
        "buy_trades": sum(int(item.get("buy_trades", 0) or 0) for item in results),
        "sell_trades": sum(int(item.get("sell_trades", 0) or 0) for item in results),
    }


def _compact_batch_report(codes: list[str], result: dict[str, Any]) -> dict[str, Any]:
    return {
        "codes": list(codes),
        "total_return_pct": result.get("total_return_pct", 0),
        "max_drawdown_pct": result.get("max_drawdown_pct", 0),
        "sharpe_ratio": result.get("sharpe_ratio", 0),
        "calmar_ratio": result.get("calmar_ratio", 0),
        "win_rate_pct": result.get("win_rate_pct", 0),
        "buy_trades": result.get("buy_trades", 0),
        "sell_trades": result.get("sell_trades", 0),
        "buy_route_counts": _buy_route_counts(result.get("trades") or []),
        "signal_sample_size": (result.get("signal_alpha") or {}).get("overall", {}).get("sample_size", 0),
    }


def _buy_route_counts(trades: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for trade in trades:
        if trade.get("side") != "buy":
            continue
        action = str(trade.get("source_action") or "")
        route = str(trade.get("source_route") or "unknown")
        key = f"{action}|{route}"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))
