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
        })
        assert "偏强" in embed["description"]

    def test_evening_embed(self):
        assert "收盘报告" in format_evening_embed({
            "date": "2026-04-14", "market": {"上证指数": {"price": 3210.0, "chg_pct": 0.3}},
            "positions": [{"name": "双环传动", "shares": 100, "pnl_pct": 2.5}],
        })["title"]

    def test_scoring_embed(self):
        embed = format_scoring_embed([
            {"name": "A", "code": "001", "total_score": 7.5, "technical_score": 2,
             "fundamental_score": 2, "flow_score": 1.5, "sentiment_score": 2},
        ])
        assert len(embed["fields"]) == 1

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
