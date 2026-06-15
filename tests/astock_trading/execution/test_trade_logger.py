"""Test that trade hooks auto-update Obsidian and write artifacts."""

import pytest

from astock_trading.platform.events import EventStore
from astock_trading.execution.service import ExecutionService, SimulatedBroker
from astock_trading.execution.trade_logger import TradeLogger


@pytest.fixture
def env(tmp_path, mysql_conn):
    conn = mysql_conn
    event_store = EventStore(conn)
    vault = tmp_path / "vault"
    vault.mkdir()

    logger = TradeLogger(event_store, conn, vault_path=str(vault))
    svc = ExecutionService(event_store, conn, broker=SimulatedBroker(), on_trade=[logger])

    yield {"svc": svc, "conn": conn, "vault": vault}


class TestTradeLogger:
    def test_buy_updates_obsidian(self, env):
        env["svc"].execute_buy("002138", "双环传动", 100, 1500, "momentum", "run_1")

        # Obsidian 持仓页应该被创建
        portfolio_md = env["vault"] / "01-状态" / "持仓" / "持仓概览.md"
        assert portfolio_md.exists()
        content = portfolio_md.read_text()
        assert "002138" in content
        assert "双环传动" in content

    def test_buy_writes_daily_log(self, env):
        env["svc"].execute_buy("002138", "双环传动", 100, 1500, "momentum", "run_1")

        inspection = env["vault"] / "02-巡检"
        assert inspection.exists()
        reports = list(inspection.glob("*.md"))
        assert len(reports) >= 1
        content = reports[0].read_text()
        assert "买入" in content

    def test_buy_writes_artifact(self, env):
        env["svc"].execute_buy("002138", "双环传动", 100, 1500, "momentum", "run_1")

        row = env["conn"].execute(
            "SELECT * FROM report_artifacts WHERE report_type = 'trade_log'"
        ).fetchone()
        assert row is not None
        assert "buy" in row["content"]
        assert "002138" in row["content"]

    def test_sell_updates_obsidian(self, env):
        env["svc"].execute_buy("002138", "双环传动", 100, 1500, "momentum", "run_1")
        env["svc"].execute_sell("002138", 100, 1650, "run_1", reason="take_profit")

        content = (env["vault"] / "01-状态" / "持仓" / "持仓概览.md").read_text()
        assert "无持仓" in content  # 已清仓

    def test_hook_failure_doesnt_break_trade(self, env):
        """Hook 失败不影响交易本身。"""
        def bad_hook(info):
            raise RuntimeError("hook exploded")

        svc = ExecutionService(
            EventStore(env["conn"]), env["conn"],
            broker=SimulatedBroker(), on_trade=[bad_hook],
        )
        order = svc.execute_buy("002138", "双环传动", 100, 1500, "momentum", "run_1")
        assert order.order_id.startswith("ord_")
        assert svc.get_position("002138") is not None
