"""Test all four pipelines with mock data (no network)."""

import pytest
import pandas as pd
from pathlib import Path

from astock_trading.market.models import StockQuote
from astock_trading.platform.db import init_db, connect
from astock_trading.platform.events import EventStore
from astock_trading.platform.runs import RunJournal
from astock_trading.market.service import MarketService
from astock_trading.market.store import MarketStore
from astock_trading.strategy.models import ScoringWeights
from astock_trading.strategy.scorer import Scorer
from astock_trading.strategy.decider import Decider
from astock_trading.strategy.service import StrategyService
from astock_trading.risk.service import RiskService
from astock_trading.execution.service import ExecutionService, SimulatedBroker
from astock_trading.reporting.projectors import ProjectionUpdater
from astock_trading.reporting.reports import ReportGenerator
from astock_trading.reporting.obsidian import ObsidianProjector
from astock_trading.pipeline.context import PipelineContext


class MockIntradayProvider:
    def __init__(self, quotes=None, klines=None):
        self._quotes = quotes or {}
        self._klines = klines or {}
        self.realtime_calls = []

    async def get_realtime(self, codes):
        self.realtime_calls.append(list(codes))
        return {c: self._quotes[c] for c in codes if c in self._quotes}

    async def get_kline(self, code, period="daily", count=120):
        return self._klines.get(code)

    async def get_index(self, symbols):
        return {}


class CountingFinancialProvider:
    def __init__(self):
        self.calls = []

    async def get_financial(self, code):
        self.calls.append(code)
        return None


class CountingFlowProvider:
    def __init__(self):
        self.calls = []

    async def get_fund_flow(self, code, days=5):
        self.calls.append(code)
        return None


class CountingSentimentProvider:
    def __init__(self):
        self.calls = []

    async def search_news(self, query):
        self.calls.append(query)
        return None


@pytest.fixture
def ctx(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    es = EventStore(conn)
    vault = tmp_path / "vault"
    vault.mkdir()

    scorer = Scorer(weights=ScoringWeights(), veto_rules=["below_ma20"])
    decider = Decider(buy_threshold=6.5, watch_threshold=5.0)

    c = PipelineContext(
        conn=conn, event_store=es, run_journal=RunJournal(conn),
        config_snapshot=None,
        market_svc=MarketService(store=MarketStore(conn)),
        strategy_svc=StrategyService(scorer, decider, es),
        risk_svc=RiskService(es),
        exec_svc=ExecutionService(es, conn, broker=SimulatedBroker()),
        projector=ProjectionUpdater(es, conn),
        reporter=ReportGenerator(es, conn),
        obsidian=ObsidianProjector(es, conn, vault_path=str(vault)),
        vault_path=str(vault),
    )

    # Seed some pool data
    conn.execute(
        "INSERT INTO projection_candidate_pool (code, pool_tier, name, score, added_at) VALUES (?, ?, ?, ?, ?)",
        ("002138", "core", "双环传动", 7.5, "2026-04-10"),
    )

    yield c
    conn.close()


class TestMorningPipeline:
    def test_runs_without_error(self, ctx):
        from astock_trading.pipeline.morning import run
        result = run(ctx, "run_morning_test")
        assert "signal" in result
        assert isinstance(result["risk_alerts"], list)

    def test_writes_obsidian(self, ctx):
        from astock_trading.pipeline.morning import run
        run(ctx, "run_morning_test")
        assert (Path(ctx.vault_path) / "04-决策" / "今日决策.md").exists()
        assert (Path(ctx.vault_path) / "01-状态" / "持仓" / "持仓概览.md").exists()


class TestScoringPipeline:
    def test_runs_without_error(self, ctx):
        from astock_trading.pipeline.scoring import run
        result = run(ctx, "run_scoring_test")
        # No network, so scored=0 (MarketService returns empty snapshots)
        # But pipeline should not crash
        assert "scored" in result

    def test_writes_obsidian(self, ctx):
        from astock_trading.pipeline.scoring import run
        run(ctx, "run_scoring_test")
        vault = Path(ctx.vault_path)
        assert (vault / "01-状态" / "池子" / "核心池.md").exists()
        assert (vault / "01-状态" / "池子" / "观察池.md").exists()


class TestEveningPipeline:
    def test_runs_without_error(self, ctx):
        from astock_trading.pipeline.evening import run
        result = run(ctx, "run_evening_test")
        assert "signal" in result

    def test_writes_obsidian(self, ctx):
        from astock_trading.pipeline.evening import run
        run(ctx, "run_evening_test")
        assert (Path(ctx.vault_path) / "01-状态" / "持仓" / "持仓概览.md").exists()


class TestIntradayMonitorPipeline:
    def test_alerts_on_daily_loss_and_dedupes_same_day(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))
        ctx.exec_svc.execute_buy("002261", "拓维信息", 100, 10000, "momentum", "seed")
        quote = StockQuote(
            code="002261", name="拓维信息", price=94.0,
            open=100.0, high=100.0, low=93.5, close=94.0,
            volume=1000000, amount=94000000, change_pct=-6.0,
        )
        ctx.market_svc = MarketService(
            market_providers=[MockIntradayProvider(quotes={"002261": quote})],
            store=MarketStore(ctx.conn),
        )

        from astock_trading.pipeline.intraday_monitor import run

        first = run(ctx, "run_intraday_1")
        second = run(ctx, "run_intraday_2")

        assert first["positions"] == 1
        assert [a["signal_type"] for a in first["alerts"]] == ["daily_loss"]
        assert second["alerts"] == []
        assert second["deduped"] == 1

    def test_alerts_on_ma_exit(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))
        ctx.exec_svc.execute_buy("002261", "拓维信息", 100, 1050, "momentum", "seed")
        quote = StockQuote(
            code="002261", name="拓维信息", price=10.0,
            open=10.1, high=10.2, low=9.9, close=10.0,
            volume=1000000, amount=10000000, change_pct=-1.0,
        )
        kline = pd.DataFrame({
            "close": [10.3] * 19 + [10.0],
            "open": [10.3] * 20,
            "high": [10.3] * 20,
            "low": [10.0] * 20,
            "volume": [1000000] * 20,
            "amount": [10000000] * 20,
        })
        ctx.market_svc = MarketService(
            market_providers=[MockIntradayProvider(quotes={"002261": quote}, klines={"002261": kline})],
            store=MarketStore(ctx.conn),
        )

        from astock_trading.pipeline.intraday_monitor import run

        result = run(ctx, "run_intraday_ma")

        assert result["positions"] == 1
        assert [a["signal_type"] for a in result["alerts"]] == ["ma_exit"]

    def test_does_not_collect_scoring_dimensions(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))
        ctx.exec_svc.execute_buy("002261", "拓维信息", 100, 10000, "momentum", "seed")
        quote = StockQuote(
            code="002261", name="拓维信息", price=100.0,
            open=100.0, high=100.0, low=100.0, close=100.0,
            volume=1000000, amount=100000000, change_pct=0.0,
        )
        financial = CountingFinancialProvider()
        flow = CountingFlowProvider()
        sentiment = CountingSentimentProvider()
        ctx.market_svc = MarketService(
            market_providers=[MockIntradayProvider(quotes={"002261": quote})],
            financial_providers=[financial],
            flow_providers=[flow],
            sentiment_providers=[sentiment],
            store=MarketStore(ctx.conn),
        )

        from astock_trading.pipeline.intraday_monitor import run

        run(ctx, "run_intraday_lightweight")

        assert financial.calls == []
        assert flow.calls == []
        assert sentiment.calls == []

    def test_batches_realtime_quotes_for_positions_only(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))
        codes = ["002261", "002138", "000001", "600000"]
        for code in codes:
            ctx.exec_svc.execute_buy(code, code, 100, 10000, "momentum", f"seed_{code}")
        quotes = {
            code: StockQuote(
                code=code, name=code, price=100.0,
                open=100.0, high=100.0, low=100.0, close=100.0,
                volume=1000000, amount=100000000, change_pct=0.0,
            )
            for code in codes
        }
        provider = MockIntradayProvider(quotes=quotes)
        ctx.market_svc = MarketService(
            market_providers=[provider],
            store=MarketStore(ctx.conn),
        )

        from astock_trading.pipeline.intraday_monitor import run

        run(ctx, "run_intraday_positions_only")

        assert provider.realtime_calls == [codes]


class TestWeeklyPipeline:
    def test_runs_without_error(self, ctx):
        from astock_trading.pipeline.weekly import run
        result = run(ctx, "run_weekly_test")
        assert "week" in result
        assert result["buy_count"] == 0

    def test_writes_obsidian(self, ctx):
        from astock_trading.pipeline.weekly import run
        run(ctx, "run_weekly_test")
        vault = Path(ctx.vault_path)
        week_files = list((vault / "03-分析" / "周复盘").glob("*.md"))
        assert len(week_files) >= 1

    def test_counts_multiple_closes_for_same_code_this_week(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))

        ctx.exec_svc.execute_buy("002261", "拓维信息", 100, 1000, "momentum", "seed_buy_1")
        ctx.exec_svc.execute_sell("002261", 100, 1100, "seed_sell_1", "take_profit")
        ctx.exec_svc.execute_buy("002261", "拓维信息", 100, 1000, "momentum", "seed_buy_2")
        ctx.exec_svc.execute_sell("002261", 100, 900, "seed_sell_2", "stop_loss")

        from astock_trading.pipeline.weekly import run

        result = run(ctx, "run_weekly_multi_close")

        assert result["sell_count"] == 2
        assert result["win_rate"] == 0.5
        assert result["net_pnl_cents"] == 0
