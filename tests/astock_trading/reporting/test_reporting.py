"""Tests for reporting context — projectors, reports, discord, obsidian."""

import pytest

from astock_trading.execution.models import OrderSide
from astock_trading.execution.orders import OrderManager
from astock_trading.execution.positions import PositionManager
from astock_trading.platform.db import init_db, connect
from astock_trading.platform.events import EventStore
from astock_trading.reporting.discord import (
    format_evening_embed, format_morning_embed,
    format_scoring_embed, format_stop_alert_embed,
    format_propose_plan_embed, format_daily_inspection_embed,
    format_manual_confirmation_embed,
)
from astock_trading.reporting.obsidian import ObsidianProjector
from astock_trading.reporting.projectors import ProjectionUpdater
from astock_trading.reporting.reports import ReportGenerator
from astock_trading.reporting.screening_result import render_screening_result


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    yield conn
    conn.close()


@pytest.fixture
def event_store(db):
    return EventStore(db)


def _seed(event_store, db):
    orders = OrderManager(event_store, db)
    positions = PositionManager(event_store, db)
    o1 = orders.create_order("001", "A", OrderSide.BUY, 100, 1000, "run_1")
    orders.fill_order(o1.order_id, 1000, 5, "run_1")
    positions.open_position("001", "A", 100, 1000, "slow_bull", "run_1")
    o2 = orders.create_order("002", "B", OrderSide.BUY, 200, 2000, "run_1")
    orders.fill_order(o2.order_id, 2000, 10, "run_1")
    positions.open_position("002", "B", 200, 2000, "momentum", "run_1")
    o3 = orders.create_order("001", "A", OrderSide.SELL, 100, 1200, "run_1")
    orders.fill_order(o3.order_id, 1200, 5, "run_1")
    positions.close_position("001", 100, 1200, "run_1")

    event_store.append(
        stream="strategy:001", stream_type="strategy", event_type="score.calculated",
        payload={"code": "001", "name": "A", "total_score": 7.5, "technical_score": 2.0,
                 "fundamental_score": 2.0, "flow_score": 1.5, "sentiment_score": 2.0,
                 "style": "slow_bull", "veto_triggered": False},
        metadata={"run_id": "run_1"},
    )
    event_store.append(
        stream="strategy:002", stream_type="strategy", event_type="score.calculated",
        payload={"code": "002", "name": "B", "total_score": 6.0, "technical_score": 1.5,
                 "fundamental_score": 1.5, "flow_score": 1.0, "sentiment_score": 2.0,
                 "style": "momentum", "veto_triggered": False},
        metadata={"run_id": "run_1"},
    )


def test_format_propose_plan_embed_summarizes_blocking_plan():
    embed = format_propose_plan_embed({
        "execution_allowed": False,
        "diagnostics": {
            "status": "warning",
            "findings": ["candidate core pool is empty"],
            "inputs": {
                "candidate_pool": {
                    "total": 1,
                    "core_count": 0,
                    "watch_count": 1,
                    "latest_scored_at": "2026-05-16T00:00:00+00:00",
                },
                "data_sources": {
                    "status": "warning",
                    "required_missing": [],
                    "optional_missing": ["core_pool"],
                },
            },
        },
        "actions": [
            {
                "type": "review_core_pool",
                "priority": "high",
                "reason": "auto_trade buy-side requires fresh core candidates",
            }
        ],
    })

    assert "交易计划" in embed["title"]
    values = "\n".join(field["value"] for field in embed["fields"])
    assert "禁止自动执行" in values
    assert "core=0" in values
    assert "review_core_pool" in values
    assert "candidate core pool is empty" in values


def test_format_daily_inspection_embed_summarizes_health_and_report_path():
    embed = format_daily_inspection_embed({
        "date": "2026-05-16",
        "report_path": "/Users/hxh/Documents/a-stock-trading/trade-vault/02-巡检/2026-05-16.md",
        "failed_commands": [{"name": "health", "returncode": 1}],
        "doctor_status": "ok",
        "health_status": "warning",
        "diagnose_health_status": "warning",
        "data_source_status": "warning",
        "required_missing": [],
        "optional_missing": ["core_pool"],
        "candidate_pool": {"total": 1, "core_count": 0, "watch_count": 1},
        "failed_runs_count": 0,
        "running_runs_count": 0,
        "pending_manual_trades": 2,
        "paper_positions": 4,
        "paper_total_asset": 205212.46,
        "plan_execution_allowed": False,
        "plan_actions": [{"type": "review_core_pool", "priority": "high"}],
    })

    assert "每日巡检" in embed["title"]
    values = "\n".join(field["value"] for field in embed["fields"])
    assert "doctor=ok" in values
    assert "health=warning" in values
    assert "health" in values
    assert "core_pool" in values
    assert "待确认 2" in values
    assert "trade-vault/02-巡检/2026-05-16.md" in values


def test_format_daily_inspection_embed_expands_manual_trades_and_route_blockers():
    embed = format_daily_inspection_embed({
        "date": "2026-05-16",
        "doctor_status": "ok",
        "health_status": "ok",
        "diagnose_health_status": "ok",
        "candidate_pool": {"total": 2, "core_count": 0, "watch_count": 2},
        "pending_manual_trades": 1,
        "pending_manual_trade_items": [
            {
                "code": "600703",
                "name": "三安光电",
                "side": "BUY",
                "score": 6.3,
                "position_pct": 0.16,
            }
        ],
        "route_blocked_watch_candidates": [
            {
                "code": "300558",
                "name": "贝达药业",
                "score": 6.2,
                "note": "screener_refresh:requires_entry_strategy_route",
            }
        ],
    })

    field_names = {field["name"] for field in embed["fields"]}
    assert "人工确认明细" in field_names
    assert "观察池阻断" in field_names
    values = "\n".join(field["value"] for field in embed["fields"])
    assert "三安光电(600703)" in values
    assert "BUY" in values
    assert "6.3" in values
    assert "仓位 16%" in values
    assert "贝达药业(300558)" in values
    assert "缺少有效策略路线" in values


class TestProjectionUpdater:
    def test_rebuild_all(self, event_store, db):
        _seed(event_store, db)
        db.execute("DELETE FROM projection_positions")
        db.execute("DELETE FROM projection_orders")
        stats = ProjectionUpdater(event_store, db).rebuild_all()
        assert stats["positions"] == 1
        assert stats["orders"] == 3

    def test_rebuild_empty(self, event_store, db):
        stats = ProjectionUpdater(event_store, db).rebuild_all()
        assert stats["positions"] == 0

    def test_rebuild_idempotent(self, event_store, db):
        _seed(event_store, db)
        u = ProjectionUpdater(event_store, db)
        assert u.rebuild_all() == u.rebuild_all()

    def test_sync_market_state(self, event_store, db):
        count = ProjectionUpdater(event_store, db).sync_market_state({
            "上证指数": {"symbol": "sh000001", "close": 3200.5, "change_pct": 0.5, "signal": "GREEN"},
            "深证成指": {"symbol": "sz399001", "close": 10500.0, "change_pct": -0.3, "signal": "YELLOW"},
        })
        assert count == 2

    def test_sync_candidate_pool(self, event_store, db):
        count = ProjectionUpdater(event_store, db).sync_candidate_pool([
            {"code": "001", "name": "A", "pool_tier": "core", "score": 7.5},
            {"code": "002", "name": "B", "pool_tier": "watch", "score": 5.5},
        ])
        assert count == 2


class TestReportGenerator:
    def test_scoring_report(self, event_store, db):
        _seed(event_store, db)
        report = ReportGenerator(event_store, db).generate_scoring_report("run_1")
        assert "评分报告" in report and "7.5" in report

    def test_scoring_report_empty(self, event_store, db):
        assert "无评分数据" in ReportGenerator(event_store, db).generate_scoring_report("x")

    def test_portfolio_report(self, event_store, db):
        _seed(event_store, db)
        assert "002" in ReportGenerator(event_store, db).generate_portfolio_report()

    def test_portfolio_report_empty(self, event_store, db):
        assert "无持仓" in ReportGenerator(event_store, db).generate_portfolio_report()

    def test_trade_history(self, event_store, db):
        _seed(event_store, db)
        assert "交易记录" in ReportGenerator(event_store, db).generate_trade_history()

    def test_morning_report(self, event_store, db):
        _seed(event_store, db)
        assert "盘前摘要" in ReportGenerator(event_store, db).generate_morning_report("run_1")

    def test_evening_report(self, event_store, db):
        _seed(event_store, db)
        assert "收盘报告" in ReportGenerator(event_store, db).generate_evening_report("run_1")

    def test_weekly_report(self, event_store, db):
        assert "周报" in ReportGenerator(event_store, db).generate_weekly_report()


class TestDiscordFormat:
    def test_morning_embed(self):
        embed = format_morning_embed({
            "date": "2026-04-14", "market_signal": "GREEN",
            "market": {"上证指数": {"price": 3200.0, "chg_pct": 0.5}},
            "positions": [{"name": "双环传动", "shares": 100, "price": 15.0}],
            "core_pool": [{"name": "大金重工", "score": 7.5}],
            "xueqiu_hot_stocks": [{"rank": 1, "name": "阳光电源", "code": "300274", "change_pct": 13.27, "heat": 2785}],
            "cross_platform_hot_stocks": [{
                "name": "阳光电源", "code": "300274", "change_pct": 13.27,
                "source_count": 3, "sources": ["xueqiu", "eastmoney", "sinafinance"],
            }],
            "finance_flash": [{
                "time": "09:10",
                "title": "商务部回应关税安排",
                "summary": "中美经贸磋商形成积极共识，双方讨论有关产品降税安排。",
                "source": "sinafinance",
            }],
            "global_risk_news": [{
                "title": "Fed rate cut expectations fade",
                "summary": "Treasury yields rise as inflation remains sticky.",
                "source": "bloomberg",
            }],
            "market_announcements": [{"code": "603311", "name": "金海高科", "title": "复牌公告", "category": "复牌公告"}],
        })
        assert "偏强" in embed["description"]
        field_names = {field["name"] for field in embed["fields"]}
        assert {"雪球热搜", "跨平台热度", "财经快讯", "海外风险", "公告提示"} <= field_names
        field_values = "\n".join(field["value"] for field in embed["fields"])
        assert "上次评分" in field_values
        flash_field = next(field for field in embed["fields"] if field["name"] == "财经快讯")
        assert "影响: 宏观/出口链/人民币风险" in flash_field["value"]
        assert "动作:" in flash_field["value"]

    def test_evening_embed(self):
        embed = format_evening_embed({
            "date": "2026-04-14", "market": {"上证指数": {"price": 3210.0, "chg_pct": 0.3}},
            "positions": [{"name": "双环传动", "shares": 100, "pnl_pct": 2.5}],
            "cross_platform_hot_stocks": [{
                "name": "阳光电源", "code": "300274", "change_pct": 13.27,
                "source_count": 3, "sources": ["xueqiu", "eastmoney", "sinafinance"],
            }],
            "finance_flash": [{
                "time": "15:10",
                "title": "商务部回应关税安排",
                "summary": "中美经贸磋商形成积极共识，双方讨论有关产品降税安排。",
                "source": "sinafinance",
            }],
            "global_risk_news": [{
                "title": "Fed rate cut expectations fade",
                "summary": "Treasury yields rise as inflation remains sticky.",
                "source": "reuters",
            }],
            "market_announcements": [{"code": "603311", "name": "金海高科", "title": "复牌公告", "category": "复牌公告"}],
        })
        assert "收盘报告" in embed["title"]
        field_names = {field["name"] for field in embed["fields"]}
        assert {"跨平台热度", "财经快讯", "海外风险", "公告提示"} <= field_names
        risk_field = next(field for field in embed["fields"] if field["name"] == "海外风险")
        assert "影响: 利率/成长股估值" in risk_field["value"]

    def test_scoring_embed(self):
        embed = format_scoring_embed([
            {"name": "A", "code": "001", "total_score": 7.5, "technical_score": 2,
             "fundamental_score": 2, "flow_score": 1.5, "sentiment_score": 2},
        ])
        assert len(embed["fields"]) == 1

    def test_scoring_embed_shows_strategy_route_and_entry_blocker(self):
        embed = format_scoring_embed([
            {
                "name": "Breakout",
                "code": "001",
                "total_score": 6.2,
                "technical_score": 2,
                "fundamental_score": 1,
                "flow_score": 1,
                "sentiment_score": 2,
                "entry_signal": False,
                "strategy_routes": [
                    {"display_name": "放量突破", "confidence": 0.92, "entry_signal": False}
                ],
                "promotion_blockers": ["requires_entry_strategy_route"],
            },
        ])

        value = embed["fields"][0]["value"]
        assert "放量突破" in value
        assert "观察" in value
        assert "缺少有效策略路线" in value

    def test_manual_confirmation_embed_summarizes_required_review_blocks(self):
        embed = format_manual_confirmation_embed({
            "resolved": {"code": "600703", "name": "三安光电"},
            "execution_allowed": False,
            "quote": {"price": 12.3, "change_pct": 1.2},
            "technical": {
                "ma5": 12.0,
                "ma20": 11.5,
                "ma60": 10.8,
                "above_ma20": True,
                "golden_cross": True,
                "rsi": 58,
                "volume_ratio": 1.8,
                "momentum_5d": 3.0,
            },
            "score": {
                "total_score": 6.3,
                "data_quality": "ok",
                "entry_signal": True,
                "strategy_routes": [
                    {
                        "route": "volume_breakout",
                        "display_name": "放量突破",
                        "confidence": 0.92,
                        "entry_signal": True,
                    }
                ],
                "dimensions": [
                    {"name": "technical", "score": 2.4},
                    {"name": "fundamental", "score": 1.4},
                    {"name": "flow", "score": 1.0},
                    {"name": "sentiment", "score": 1.5},
                ],
                "warning_signals": ["turnover_spike"],
            },
            "decision": {
                "action": "BUY",
                "confidence": 6.3,
                "position_pct": 0.16,
                "market_signal": "GREEN",
                "notes": ["market gate ok"],
            },
            "sentiment": {
                "news": [
                    {"title": "MiniLED 订单改善", "level": "event"},
                    {"summary": "机构调研关注产能利用率"},
                ]
            },
            "findings": ["warning signals: turnover_spike"],
            "recommendations": [
                "manual confirmation required before any order; this report never executes trades",
                "treat BUY as a candidate intent, then verify price, liquidity, and portfolio risk",
            ],
        })

        assert "人工确认" in embed["title"]
        assert "三安光电(600703)" in embed["description"]
        names = {field["name"] for field in embed["fields"]}
        assert {
            "核心结论",
            "评分",
            "趋势/路线",
            "买卖点",
            "风险警报",
            "催化因素",
            "操作检查清单",
        } <= names
        values = "\n".join(field["value"] for field in embed["fields"])
        assert "不自动下单" in values
        assert "BUY" in values
        assert "6.3" in values
        assert "放量突破" in values
        assert "现价 12.30" in values
        assert "建议仓位 16%" in values
        assert "turnover_spike" in values
        assert "MiniLED 订单改善" in values
        assert "确认价格/流动性/仓位/止损" in values

    def test_stop_alert_embed(self):
        assert "止损" in format_stop_alert_embed({
            "code": "002138", "signal_type": "stop_loss", "description": "跌破止损线", "urgency": "immediate",
        })["title"]


class TestObsidianProjector:
    def test_portfolio_status(self, event_store, db):
        _seed(event_store, db)
        assert "002" in ObsidianProjector(event_store, db).write_portfolio_status()

    def test_portfolio_status_empty(self, event_store, db):
        assert "无持仓" in ObsidianProjector(event_store, db).write_portfolio_status()

    def test_pool_status(self, event_store, db):
        assert "观察池" in ObsidianProjector(event_store, db).write_pool_status()

    def test_watch_pool_explains_route_blocker_note(self, event_store, db):
        ProjectionUpdater(event_store, db).sync_candidate_pool([
            {
                "code": "001",
                "name": "A",
                "pool_tier": "watch",
                "score": 6.2,
                "note": "screener_refresh:requires_entry_strategy_route",
            }
        ])

        content = ObsidianProjector(event_store, db).write_watch_pool()

        assert "缺少有效策略路线，暂留观察" in content
        assert "requires_entry_strategy_route" not in content

    def test_signal_snapshot_lists_route_blocked_watch_candidates(self, event_store, db):
        ProjectionUpdater(event_store, db).sync_candidate_pool([
            {
                "code": "001",
                "name": "A",
                "pool_tier": "watch",
                "score": 6.2,
                "note": "screener_refresh:requires_entry_strategy_route",
            }
        ])

        content = ObsidianProjector(event_store, db).write_signal_snapshot(
            run_id="run_1",
            market_state_detail={"indices": {}},
            market_signal="YELLOW",
        )

        assert "观察池阻断" in content
        assert "| A | 001 | 6.2 | 缺少有效策略路线，暂留观察 |" in content

    def test_write_to_vault(self, event_store, db, tmp_path):
        vault = tmp_path / "vault"
        vault.mkdir()
        _seed(event_store, db)
        ObsidianProjector(event_store, db, vault_path=str(vault)).write_portfolio_status()
        assert (vault / "01-状态" / "持仓" / "持仓概览.md").exists()

    def test_scoring_report(self, event_store, db):
        content = ObsidianProjector(event_store, db).write_scoring_report(
            "run_1", [{"name": "A", "code": "001", "total_score": 7.5, "style": "momentum"}])
        assert "7.5" in content

    def test_screening_result_uses_configured_buy_threshold(self, event_store, db):
        content = ObsidianProjector(event_store, db).write_screening_result(
            "run_1",
            "test query",
            [{"name": "A", "code": "001", "total_score": 5.6, "veto_triggered": False}],
            buy_threshold=5.5,
        )

        assert "✅可买" in content

    def test_write_screening_result_writes_main_and_candidate_files(
        self, event_store, db, tmp_path
    ):
        vault = tmp_path / "vault"
        projector = ObsidianProjector(event_store, db, vault_path=str(vault))

        content = projector.write_screening_result(
            "run_1",
            "test query",
            [{"name": "A", "code": "001", "total_score": 5.6, "veto_triggered": False}],
            buy_threshold=5.5,
            watch_threshold=5.0,
        )

        assert (vault / "04-决策" / "候选池" / "最新筛选.md").read_text(
            encoding="utf-8"
        ) == content
        candidate_path = vault / "04-决策" / "候选池" / "市场扫描候选.md"
        assert "可买入" in candidate_path.read_text(encoding="utf-8")


class TestScreeningResultRendering:
    def test_render_screening_result_threshold_statuses(self):
        content, candidate_content = render_screening_result(
            today="2026-05-16",
            now="2026-05-16 09:30:00",
            run_id="run_1",
            query="test query",
            scores=[
                {"name": "Buy", "code": "001", "total_score": 6.0},
                {"name": "Watch", "code": "002", "total_score": 5.0},
                {"name": "Avoid", "code": "003", "total_score": 4.9},
                {"name": "Veto", "code": "004", "total_score": 9.0, "veto_triggered": True},
            ],
            buy_threshold=5.5,
            watch_threshold=5.0,
        )

        assert "✅可买" in content
        assert "🟡观察" in content
        assert "❌规避" in content
        assert "🚫否决" in content
        assert candidate_content is not None
        assert "| Buy | 001 | 6.0 |  | 可买入 |" in candidate_content
        assert "| Watch | 002 | 5.0 |  | 观察 |" in candidate_content
        assert "Veto" not in candidate_content

    def test_render_screening_result_shows_strategy_routes(self):
        content, candidate_content = render_screening_result(
            today="2026-05-16",
            now="2026-05-16 09:30:00",
            run_id="run_1",
            query="test query",
            scores=[
                {
                    "name": "Breakout",
                    "code": "001",
                    "total_score": 6.0,
                    "strategy_routes": [
                        {"route": "volume_breakout", "display_name": "放量突破"},
                    ],
                    "primary_strategy_route": "volume_breakout",
                },
            ],
            buy_threshold=5.5,
            watch_threshold=5.0,
        )

        assert "路线" in content
        assert "放量突破" in content
        assert candidate_content is not None
        assert "| Breakout | 001 | 6.0 | 放量突破 |" in candidate_content
