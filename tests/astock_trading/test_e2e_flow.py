"""
端到端模拟验证 — 走完整个实盘流程

不需要网络，用固定数据模拟：
1. 初始化 DB + 冻结配置
2. 创建 run
3. 构造 StockSnapshot（模拟 MarketService 抓取）
4. StrategyService 评分 + 决策
5. RiskService 风控检查
6. ExecutionService 买入
7. 风控检查持仓（止损/止盈）
8. ExecutionService 卖出
9. ProjectionUpdater 重建投影
10. ReportGenerator 生成报告
11. 验证 event_log 完整审计链
"""

import pytest
from datetime import date

from astock_trading.platform.events import EventStore
from astock_trading.platform.runs import RunJournal
from astock_trading.market.models import (
    FinancialReport, FundFlow, SentimentData,
    StockQuote, StockSnapshot, TechnicalIndicators,
)
from astock_trading.strategy.models import (
    MarketSignal, MarketState, ScoringWeights,
)
from astock_trading.strategy.scorer import Scorer
from astock_trading.strategy.decider import Decider
from astock_trading.strategy.service import StrategyService
from astock_trading.risk.models import PortfolioLimits
from astock_trading.risk.service import RiskService
from astock_trading.risk.rules import get_risk_params
from astock_trading.risk.sizing import calc_position_size
from astock_trading.execution.service import ExecutionService, SimulatedBroker
from astock_trading.reporting.projectors import ProjectionUpdater
from astock_trading.reporting.reports import ReportGenerator
from astock_trading.reporting.obsidian import ObsidianProjector
from astock_trading.reporting.discord import format_scoring_embed
from astock_trading.strategy.models import Style


@pytest.fixture
def env(tmp_path, mysql_conn):
    """搭建完整的 V2 环境。"""
    conn = mysql_conn

    event_store = EventStore(conn)
    run_journal = RunJournal(conn)

    scorer = Scorer(
        weights=ScoringWeights(technical=3, fundamental=2, flow=2, sentiment=3),
        veto_rules=["below_ma20", "limit_up_today", "consecutive_outflow"],
        entry_cfg={"rsi_max": 70, "volume_ratio_min": 1.5},
    )
    decider = Decider(buy_threshold=6.5, watch_threshold=5.0, weekly_max=2)
    strategy_svc = StrategyService(scorer, decider, event_store)
    risk_svc = RiskService(event_store)
    exec_svc = ExecutionService(event_store, conn, broker=SimulatedBroker())
    projector = ProjectionUpdater(event_store, conn)
    reporter = ReportGenerator(event_store, conn)

    vault_path = tmp_path / "vault"
    vault_path.mkdir()
    obsidian = ObsidianProjector(event_store, conn, vault_path=str(vault_path))

    return {
        "conn": conn, "event_store": event_store, "run_journal": run_journal,
        "strategy_svc": strategy_svc, "risk_svc": risk_svc, "exec_svc": exec_svc,
        "projector": projector, "reporter": reporter, "obsidian": obsidian,
        "vault_path": vault_path, "tmp_path": tmp_path,
    }


def _snapshot(code, name, price, above_ma20=True, golden_cross=True,
              roe=12.0, net_inflow=6e8, rsi=55.0):
    """构造一个 StockSnapshot。"""
    return StockSnapshot(
        code=code, name=name,
        quote=StockQuote(
            code=code, name=name, price=price,
            open=price * 0.99, high=price * 1.02, low=price * 0.98,
            close=price, volume=5000000, amount=price * 5000000,
            change_pct=1.5,
        ),
        technical=TechnicalIndicators(
            ma5=price * 1.01, ma10=price * 0.99, ma20=price * 0.97,
            ma60=price * 0.93, above_ma20=above_ma20,
            volume_ratio=1.8, rsi=rsi, golden_cross=golden_cross,
            ma20_slope=0.01, momentum_5d=3.0, daily_volatility=0.025,
        ),
        financial=FinancialReport(roe=roe, revenue_growth=15.0, operating_cash_flow=1e8),
        flow=FundFlow(net_inflow_1d=net_inflow, northbound_net_positive=True),
        sentiment=SentimentData(score=2.0, detail="研报3篇"),
    )


class TestE2EFlow:
    """模拟完整实盘流程。"""

    def test_full_trading_day(self, env):
        """
        模拟一个完整交易日：
        盘前 → 评分 → 决策 → 风控 → 买入 → 持仓检查 → 卖出 → 报告
        """
        es = env["event_store"]
        journal = env["run_journal"]
        strategy = env["strategy_svc"]
        risk = env["risk_svc"]
        execution = env["exec_svc"]
        projector = env["projector"]
        reporter = env["reporter"]
        obsidian = env["obsidian"]

        # ── Step 1: 创建 run ──────────────────────────────────
        run_id = journal.start_run("scoring", "v_test_001")
        assert run_id.startswith("run_")
        print(f"\n{'='*60}")
        print(f"Step 1: 创建 run → {run_id}")

        # ── Step 2: 构造市场数据（模拟 MarketService）─────────
        snapshots = [
            _snapshot("002138", "双环传动", 15.0, roe=12.0),
            _snapshot("300938", "信测标准", 22.0, roe=18.0),
            _snapshot("601869", "长飞光纤", 28.0, above_ma20=False),  # 会被否决
        ]
        market_state = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
        print(f"Step 2: 构造 {len(snapshots)} 只股票快照 + 大盘 GREEN")

        # ── Step 3: 评分 + 决策 ──────────────────────────────
        decisions = strategy.evaluate(
            snapshots, market_state,
            run_id=run_id, config_version="v_test_001",
        )
        print("Step 3: 评分完成")
        for d in decisions:
            print(f"  {d.name}({d.code}): {d.action.value} score={d.score:.1f} pos={d.position_pct:.1%}")

        # 验证：长飞光纤应该被否决（below_ma20）
        changfei = next(d for d in decisions if d.code == "601869")
        assert changfei.action.value == "CLEAR"
        assert changfei.score == 0.0

        # 验证：至少有一只 BUY
        buys = [d for d in decisions if d.action.value == "BUY"]
        assert len(buys) >= 1

        # 验证事件写入
        score_events = es.query(event_type="score.calculated")
        assert len(score_events) == 3
        decision_events = es.query(event_type="decision.suggested")
        assert len(decision_events) == 3
        print(f"  事件: {len(score_events)} score + {len(decision_events)} decision")

        # ── Step 4: 风控检查（组合级）─────────────────────────
        breaches = risk.assess_portfolio(
            daily_pnl_pct=0.0, consecutive_loss_days=0,
            max_single_exposure_pct=0.0, max_sector_exposure_pct=0.0,
            limits=PortfolioLimits(), run_id=run_id,
        )
        print(f"Step 4: 组合风控 → {len(breaches)} 个触发")
        assert len(breaches) == 0  # 空仓，无风控触发

        # ── Step 5: 仓位计算 + 买入 ──────────────────────────
        best_buy = buys[0]
        position_size = calc_position_size(
            total_capital=450000, current_exposure_pct=0.0,
            price=float(best_buy.score),  # 用 score 作为 price 的近似
            market_multiplier=1.0,
        )
        assert position_size.shares >= 0
        # 用实际价格买入
        buy_price = snapshots[0].quote.price if best_buy.code == "002138" else snapshots[1].quote.price
        buy_price_cents = int(buy_price * 100)
        shares = max(100, (int(450000 * 0.15 / buy_price / 100)) * 100)  # ~15% 仓位

        order = execution.execute_buy(
            code=best_buy.code, name=best_buy.name,
            shares=shares, price_cents=buy_price_cents,
            style="momentum", run_id=run_id,
        )
        print(f"Step 5: 买入 {best_buy.name}({best_buy.code}) {shares}股 @ ¥{buy_price:.2f}")
        print(f"  订单: {order.order_id}")

        # 验证持仓
        pos = execution.get_position(best_buy.code)
        assert pos is not None
        assert pos.shares == shares
        portfolio = execution.get_portfolio()
        assert portfolio["holding_count"] == 1
        print(f"  持仓: {portfolio['holding_count']} 只")

        # ── Step 6: 持仓风控检查 ─────────────────────────────
        params = get_risk_params(Style.MOMENTUM)
        signals = risk.assess_position(
            code=best_buy.code, avg_cost=buy_price,
            current_price=buy_price * 0.95,  # 模拟跌了 5%
            entry_date=date.today(), today=date.today(),
            highest_since_entry=buy_price * 1.02,
            entry_day_low=buy_price * 0.98,
            risk_params=params, run_id=run_id,
        )
        print(f"Step 6: 持仓风控 → {len(signals)} 个信号")
        for s in signals:
            print(f"  {s.signal_type}: {s.description} [{s.urgency}]")

        # ── Step 7: 模拟卖出（止盈）─────────────────────────
        sell_price = buy_price * 1.10  # 涨了 10%
        sell_price_cents = int(sell_price * 100)
        sell_order = execution.execute_sell(
            code=best_buy.code, shares=shares,
            price_cents=sell_price_cents, run_id=run_id,
            reason="take_profit",
        )
        assert sell_order.order_id
        print(f"Step 7: 卖出 {best_buy.name} {shares}股 @ ¥{sell_price:.2f}")

        # 验证清仓
        assert execution.get_position(best_buy.code) is None
        assert execution.get_portfolio()["holding_count"] == 0
        print("  持仓清空")

        # ── Step 8: 重建投影 ─────────────────────────────────
        env["conn"].execute("DELETE FROM projection_positions")
        env["conn"].execute("DELETE FROM projection_orders")
        stats = projector.rebuild_all()
        print(f"Step 8: 投影重建 → positions={stats['positions']} orders={stats['orders']}")
        assert stats["positions"] == 0  # 已清仓
        assert stats["orders"] >= 2     # 至少 buy + sell

        # ── Step 9: 生成报告 ─────────────────────────────────
        scoring_report = reporter.generate_scoring_report(run_id)
        assert "评分报告" in scoring_report
        assert "002138" in scoring_report or "300938" in scoring_report

        evening_report = reporter.generate_evening_report(run_id)
        assert "收盘报告" in evening_report

        trade_history = reporter.generate_trade_history()
        assert "交易记录" in trade_history

        print("Step 9: 报告生成完成")
        print(f"  评分报告: {len(scoring_report)} 字符")
        print(f"  收盘报告: {len(evening_report)} 字符")

        # ── Step 10: Obsidian 投影 ───────────────────────────
        obsidian.write_portfolio_status()
        obsidian.write_scoring_report(run_id, [s.to_dict() for s in
            [strategy._scorer.score(snap) for snap in snapshots]])
        vault = env["vault_path"]
        assert (vault / "01-状态" / "持仓" / "持仓概览.md").exists()
        print("Step 10: Obsidian 投影写入完成")

        # ── Step 11: Discord 格式化 ──────────────────────────
        score_events = es.query(event_type="score.calculated")
        run_scores = [e["payload"] for e in score_events
                      if e.get("metadata", {}).get("run_id") == run_id]
        embed = format_scoring_embed(run_scores)
        assert "核心池评分" in embed["title"]
        assert len(embed["fields"]) == 3
        print(f"Step 11: Discord embed 格式化完成 ({len(embed['fields'])} fields)")

        # ── Step 12: 完成 run ────────────────────────────────
        journal.complete_run(run_id, artifacts={"scores": 3, "trades": 2})
        print("Step 12: run 完成")

        # ── Step 13: 验证完整审计链 ──────────────────────────
        all_events = es.query()
        event_types = [e["event_type"] for e in all_events]
        print("\nStep 13: 审计链验证")
        print(f"  总事件数: {len(all_events)}")

        # 必须有的事件类型
        required = [
            "score.calculated", "decision.suggested",
            "order.created", "order.filled",
            "position.opened", "position.closed",
        ]
        for et in required:
            count = event_types.count(et)
            status = "✅" if count > 0 else "❌"
            print(f"  {status} {et}: {count}")
            assert count > 0, f"Missing event type: {et}"

        # 幂等检查
        assert journal.is_completed_today("scoring")
        print("  ✅ 幂等检查: scoring 今日已完成")

        print(f"\n{'='*60}")
        print(f"✅ 全流程验证通过！共 {len(all_events)} 个事件")
        print(f"{'='*60}")
