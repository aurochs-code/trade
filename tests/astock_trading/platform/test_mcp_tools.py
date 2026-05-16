"""Tests for MCP Server tools — unit tests calling tool logic directly."""

import json
import pytest

from astock_trading.platform.db import init_db, connect
from astock_trading.platform.events import EventStore
from astock_trading.platform.runs import RunJournal
from astock_trading.execution.service import ExecutionService
from astock_trading.reporting.reports import ReportGenerator


@pytest.fixture
def setup_mcp(tmp_path):
    """Set up the MCP server globals for testing."""
    import astock_trading.platform.mcp_server as srv
    from astock_trading.market.service import MarketService
    from astock_trading.market.store import MarketStore
    from astock_trading.strategy.models import ScoringWeights
    from astock_trading.strategy.scorer import Scorer
    from astock_trading.strategy.decider import Decider
    from astock_trading.strategy.service import StrategyService

    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)

    srv._conn = conn
    srv._event_store = EventStore(conn)
    srv._run_journal = RunJournal(conn)
    srv._exec_svc = ExecutionService(srv._event_store, conn)
    srv._report_gen = ReportGenerator(srv._event_store, conn)
    srv._market_svc = MarketService(store=MarketStore(conn))
    srv._config_snapshot = None

    scorer = Scorer(weights=ScoringWeights(), veto_rules=[])
    decider = Decider()
    srv._strategy_svc = StrategyService(scorer, decider, srv._event_store)

    yield srv

    conn.close()
    srv._conn = None
    srv._event_store = None
    srv._market_svc = None
    srv._strategy_svc = None


class TestMCPTools:
    def test_trade_portfolio_empty(self, setup_mcp):
        srv = setup_mcp
        result = json.loads(srv.trade_portfolio())
        assert result["holding_count"] == 0

    def test_trade_portfolio_with_position(self, setup_mcp):
        srv = setup_mcp
        srv._exec_svc.execute_buy("002138", "双环传动", 100, 1500, "momentum", "run_1")

        result = json.loads(srv.trade_portfolio())
        assert result["holding_count"] == 1
        assert result["positions"][0]["code"] == "002138"

    def test_trade_score_history_empty(self, setup_mcp):
        srv = setup_mcp
        result = json.loads(srv.trade_score_history("002138"))
        assert result["code"] == "002138"
        assert result["history"] == []

    def test_trade_score_history_with_data(self, setup_mcp):
        srv = setup_mcp
        srv._event_store.append(
            stream="strategy:002138", stream_type="strategy",
            event_type="score.calculated",
            payload={"code": "002138", "total_score": 7.5, "style": "momentum", "veto_triggered": False},
            metadata={"run_id": "run_1"},
        )

        result = json.loads(srv.trade_score_history("002138"))
        assert len(result["history"]) == 1
        assert result["history"][0]["total_score"] == 7.5

    def test_trade_trade_events_empty(self, setup_mcp):
        result = json.loads(setup_mcp.trade_trade_events())
        assert result["count"] == 0
        assert result["trades"] == []

    def test_trade_calc_position(self, setup_mcp):
        result = json.loads(setup_mcp.trade_calc_position("002138", 7.5, 15.0))
        assert result["code"] == "002138"
        assert result["shares"] > 0
        assert result["shares"] % 100 == 0

    def test_trade_market_signal_fallback(self, setup_mcp):
        """When V1 market_timer is unavailable, should return fallback."""
        result = json.loads(setup_mcp.trade_market_signal())
        assert "signal" in result

    def test_trade_diagnose_health_returns_read_only_diagnostics(self, setup_mcp):
        result = json.loads(setup_mcp.trade_diagnose_health())

        assert result["diagnostic"] == "health"
        assert result["status"] in {"ok", "warning", "failed"}
        assert "findings" in result
        assert "data_sources" in result["inputs"]

    def test_trade_diagnose_strategy_returns_read_only_diagnostics(self, setup_mcp):
        result = json.loads(setup_mcp.trade_diagnose_strategy())

        assert result["diagnostic"] == "strategy"
        assert result["status"] in {"ok", "warning"}
        assert "decision_gates" in result["inputs"]
        assert result["parameter_profiles"]["need_multiple_profiles"] is True

    def test_trade_analyze_stock_returns_non_executing_report(self, setup_mcp):
        result = json.loads(setup_mcp.trade_analyze_stock("002138"))

        assert result["analysis"] == "stock"
        assert result["resolved"]["code"] == "002138"
        assert result["execution_allowed"] is False
        assert "score" in result
        assert "decision" in result

    def test_trade_explain_run_reports_missing_run(self, setup_mcp):
        result = json.loads(setup_mcp.trade_explain_run("missing-run-id"))

        assert result == {
            "status": "not_found",
            "run_id": "missing-run-id",
            "findings": ["run_id not found"],
        }

    def test_trade_propose_plan_never_executes(self, setup_mcp):
        result = json.loads(setup_mcp.trade_propose_plan())

        assert result["status"] == "proposed"
        assert result["plan_type"] == "agent_trade_plan"
        assert result["execution_allowed"] is False
        assert "actions" in result

    def test_trade_run_pipeline_blocks_failed_data_sources(self, setup_mcp, monkeypatch):
        srv = setup_mcp
        import astock_trading.market.health as market_health
        import astock_trading.pipeline.morning as morning_pipeline

        monkeypatch.setattr(srv, "is_trading_day", lambda: True)
        monkeypatch.setattr(
            market_health,
            "evaluate_data_source_health",
            lambda conn: {
                "status": "failed",
                "required_missing": ["baidu_fund_flow"],
                "optional_missing": [],
            },
        )
        monkeypatch.setattr(
            morning_pipeline,
            "run",
            lambda ctx, run_id: {"signal": "GREEN", "positions": 0, "risk_alerts": []},
        )

        result = json.loads(srv.trade_run_pipeline("morning"))

        assert result["status"] == "failed"
        assert result["pipeline"] == "morning"
        assert result["reason"] == "data_source_health_failed"
        assert result["data_sources"]["required_missing"] == ["baidu_fund_flow"]
        last_run = srv._run_journal.get_last_run("morning")
        assert last_run["status"] == "failed"

    def test_trade_run_pipeline_supports_auto_trade(self, setup_mcp, monkeypatch):
        srv = setup_mcp
        import astock_trading.market.health as market_health
        import astock_trading.pipeline.auto_trade as auto_trade_pipeline

        monkeypatch.setattr(srv, "is_trading_day", lambda: True)
        monkeypatch.setattr(
            market_health,
            "evaluate_data_source_health",
            lambda conn: {"status": "ok", "required_missing": [], "optional_missing": []},
        )
        monkeypatch.setattr(
            auto_trade_pipeline,
            "run",
            lambda ctx, run_id: {"enabled": False, "dry_run": True, "buys": [], "sells": []},
        )

        result = json.loads(srv.trade_run_pipeline("auto_trade"))

        assert result["status"] == "completed"
        assert result["pipeline"] == "auto_trade"
        assert result["enabled"] is False
        last_run = srv._run_journal.get_last_run("auto_trade")
        assert last_run["status"] == "completed"

    def test_trade_run_pipeline_continues_with_data_source_warning(self, setup_mcp, monkeypatch):
        srv = setup_mcp
        import astock_trading.market.health as market_health
        import astock_trading.pipeline.evening as evening_pipeline

        monkeypatch.setattr(srv, "is_trading_day", lambda: True)
        monkeypatch.setattr(
            market_health,
            "evaluate_data_source_health",
            lambda conn: {
                "status": "warning",
                "required_missing": [],
                "optional_missing": ["industry_comparison"],
            },
        )
        monkeypatch.setattr(
            evening_pipeline,
            "run",
            lambda ctx, run_id: {"signal": "YELLOW", "positions": 0, "risk_alerts": []},
        )

        result = json.loads(srv.trade_run_pipeline("evening"))

        assert result["status"] == "completed"
        assert result["pipeline"] == "evening"
        assert result["data_source_warning"]["optional_missing"] == ["industry_comparison"]
        last_run = srv._run_journal.get_last_run("evening")
        assert last_run["status"] == "completed"
