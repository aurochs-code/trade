"""Test all four pipelines with mock data (no network)."""

import json
from types import SimpleNamespace

import pytest
import pandas as pd
from pathlib import Path

from astock_trading.market.models import StockQuote, StockSnapshot
from astock_trading.platform.db import init_db, connect
from astock_trading.platform.domain_events import AUTO_TRADE_EXECUTED
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


def _assert_market_history_snapshot(ctx, run_id: str, phase: str) -> None:
    rows = ctx.conn.execute(
        """SELECT phase, snapshot_type, payload_json
           FROM signal_history_snapshots
           WHERE run_id = ?
           ORDER BY snapshot_type""",
        (run_id,),
    ).fetchall()
    assert {row["snapshot_type"] for row in rows} == {"market", "pool", "candidates", "decision"}
    assert {row["phase"] for row in rows} == {phase}
    market_payload = json.loads(next(row["payload_json"] for row in rows if row["snapshot_type"] == "market"))
    assert market_payload["signal"] in {"GREEN", "YELLOW", "RED", "CLEAR"}


class TestPositionPriceRefresh:
    def test_update_position_price_uses_portable_timestamp_sql(self):
        class FakeResult:
            def __init__(self, row):
                self._row = row

            def fetchone(self):
                return self._row

        class FakeConn:
            def __init__(self):
                self.calls = []

            def execute(self, sql, params=()):
                self.calls.append((sql, params))
                if "highest_since_entry_cents" in sql:
                    return FakeResult({
                        "highest_since_entry_cents": 7208,
                        "current_price_cents": 7208,
                        "avg_cost_cents": 7208,
                        "cost_basis_cents": 2162910,
                        "shares": 300,
                    })
                if "avg_cost_cents" in sql:
                    return FakeResult({
                        "avg_cost_cents": 7208,
                        "cost_basis_cents": 2162910,
                        "shares": 300,
                    })
                return FakeResult(None)

        from astock_trading.pipeline.helpers import _update_position_price

        conn = FakeConn()
        _update_position_price(SimpleNamespace(conn=conn), "002156", 71.52)

        update_sql, update_params = conn.calls[-1]
        assert "datetime(" not in update_sql
        assert "updated_at = ?" in update_sql
        assert update_params[:3] == (7152, 7208, -17310)
        assert update_params[-1] == "002156"

    def test_refresh_uses_intraday_quote_for_position_pnl(self, ctx, monkeypatch):
        ctx.exec_svc.record_buy(
            code="002156",
            name="通富微电",
            shares=300,
            price_cents=7208,
            fee_cents=510,
            reason="manual",
            run_id="manual_buy",
        )

        async def fake_collect_intraday_batch(codes, run_id=None):
            assert codes == [{"code": "002156", "name": "通富微电"}]
            return [StockSnapshot(
                code="002156",
                name="通富微电",
                quote=StockQuote(
                    code="002156",
                    name="通富微电",
                    price=71.52,
                    open=72.97,
                    high=78.56,
                    low=71.43,
                    close=71.52,
                    volume=283496432,
                    amount=21114037308.55,
                    change_pct=-5.13,
                ),
            )]

        async def fail_full_snapshot(*args, **kwargs):
            raise AssertionError("持仓刷新应优先走轻量行情接口")

        monkeypatch.setattr(ctx.market_svc, "collect_intraday_batch", fake_collect_intraday_batch)
        monkeypatch.setattr(ctx.market_svc, "collect_batch", fail_full_snapshot)

        from astock_trading.pipeline.helpers import refresh_position_prices

        refreshed = refresh_position_prices(ctx, run_id="run_close")
        row = ctx.conn.execute(
            """SELECT current_price_cents, unrealized_pnl_cents
               FROM projection_positions
               WHERE code = ?""",
            ("002156",),
        ).fetchone()

        assert refreshed == {"002156": 71.52}
        assert row["current_price_cents"] == 7152
        assert row["unrealized_pnl_cents"] == -17310


class TestMorningPipeline:
    def test_runs_without_error(self, ctx):
        from astock_trading.pipeline.morning import run
        result = run(ctx, "run_morning_test")
        assert "signal" in result
        assert isinstance(result["risk_alerts"], list)
        assert result["xueqiu_hot_stocks"] == 0

    def test_includes_xueqiu_hot_stocks_in_summary(self, ctx, monkeypatch):
        async def fake_collect_xueqiu_hot_stocks(limit=10, list_type="10", run_id=None):
            return [{
                "rank": 1,
                "code": "300274",
                "name": "阳光电源",
                "change_pct": 13.27,
                "heat": 2785,
            }]

        monkeypatch.setattr(
            ctx.market_svc,
            "collect_xueqiu_hot_stocks",
            fake_collect_xueqiu_hot_stocks,
        )

        from astock_trading.pipeline.morning import run

        result = run(ctx, "run_morning_xueqiu")

        assert result["xueqiu_hot_stocks"] == 1
        assert any(field["name"] == "雪球热搜" for field in result["discord_embed"]["fields"])

    def test_includes_opencli_finance_context_in_summary(self, ctx, monkeypatch):
        async def fake_collect_cross_platform_hot_stocks(limit=10, run_id=None):
            return [{
                "rank": 1,
                "code": "300274",
                "name": "阳光电源",
                "change_pct": 13.27,
                "source_count": 3,
                "sources": ["xueqiu", "eastmoney", "sinafinance"],
            }]

        async def fake_collect_finance_flash(limit=20, run_id=None):
            return [{"time": "09:10", "title": "商务部回应关税安排", "source": "sinafinance"}]

        async def fake_collect_global_risk_news(limit=12, run_id=None):
            return [{"title": "US-China trade talks continue", "source": "bloomberg"}]

        async def fake_collect_market_announcements(limit=20, run_id=None):
            return [{"code": "603311", "name": "金海高科", "title": "复牌公告", "category": "复牌公告"}]

        monkeypatch.setattr(ctx.market_svc, "collect_cross_platform_hot_stocks", fake_collect_cross_platform_hot_stocks)
        monkeypatch.setattr(ctx.market_svc, "collect_finance_flash", fake_collect_finance_flash)
        monkeypatch.setattr(ctx.market_svc, "collect_global_risk_news", fake_collect_global_risk_news)
        monkeypatch.setattr(ctx.market_svc, "collect_market_announcements", fake_collect_market_announcements)

        from astock_trading.pipeline.morning import run

        result = run(ctx, "run_morning_opencli")

        field_names = {field["name"] for field in result["discord_embed"]["fields"]}
        assert result["cross_platform_hot_stocks"] == 1
        assert result["finance_flash"] == 1
        assert result["global_risk_news"] == 1
        assert result["market_announcements"] == 1
        assert {"跨平台热度", "财经快讯", "海外风险", "公告提示"} <= field_names

    def test_writes_obsidian(self, ctx):
        from astock_trading.pipeline.morning import run
        run(ctx, "run_morning_test")
        assert (Path(ctx.vault_path) / "04-决策" / "今日决策.md").exists()
        assert (Path(ctx.vault_path) / "01-状态" / "持仓" / "持仓概览.md").exists()

    def test_archives_market_history_snapshot(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))

        from astock_trading.pipeline.morning import run

        result = run(ctx, "run_morning_history")

        assert result["history_group_id"].endswith("run_morning_history")
        _assert_market_history_snapshot(ctx, "run_morning_history", "morning")

    def test_skips_standalone_heatmap_when_sector_moves_are_small(self, ctx, monkeypatch):
        calls = []

        async def fake_collect_sector_heatmap(run_id=None):
            return [{"name": "银行", "change_pct": 1.2, "amount": 2_000_000_000}]

        monkeypatch.setattr(ctx.market_svc, "collect_sector_heatmap", fake_collect_sector_heatmap)
        monkeypatch.setattr(
            "astock_trading.reporting.discord_sender.send_embed",
            lambda embed, *args, **kwargs: calls.append(embed) or (True, None),
        )

        from astock_trading.pipeline.morning import run

        result = run(ctx, "run_morning_heatmap_quiet")

        assert result["sector_heatmap_pushed"] is False
        assert len(calls) == 1

    def test_pushes_standalone_heatmap_when_sector_move_is_notable(self, ctx, monkeypatch):
        calls = []

        async def fake_collect_sector_heatmap(run_id=None):
            return [{"name": "机器人", "change_pct": 3.6, "amount": 5_000_000_000}]

        monkeypatch.setattr(ctx.market_svc, "collect_sector_heatmap", fake_collect_sector_heatmap)
        monkeypatch.setattr(
            "astock_trading.reporting.discord_sender.send_embed",
            lambda embed, *args, **kwargs: calls.append(embed) or (True, None),
        )

        from astock_trading.pipeline.morning import run

        result = run(ctx, "run_morning_heatmap_notable")

        assert result["sector_heatmap_pushed"] is True
        assert len(calls) == 2


class TestNoonPipeline:
    def test_archives_market_history_snapshot(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))

        from astock_trading.pipeline.noon import run

        result = run(ctx, "run_noon_history")

        assert result["history_group_id"].endswith("run_noon_history")
        _assert_market_history_snapshot(ctx, "run_noon_history", "noon")

    def test_noon_hot_stock_displays_realtime_quote_before_hot_list_change(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))

        async def fake_collect_cross_platform_hot_stocks(limit=10, run_id=None):
            return [{
                "code": "002031",
                "name": "巨轮智能",
                "change_pct": 9.95,
                "source_count": 3,
                "sources": ["xueqiu", "eastmoney", "sinafinance"],
            }]

        async def fake_collect_intraday_batch(codes, run_id=None):
            assert codes == [{"code": "002031", "name": "巨轮智能"}]
            return [StockSnapshot(
                code="002031",
                name="巨轮智能",
                quote=StockQuote(
                    code="002031",
                    name="巨轮智能",
                    price=8.22,
                    open=8.10,
                    high=8.53,
                    low=8.05,
                    close=8.22,
                    volume=1000000,
                    amount=2_801_000_000,
                    change_pct=0.49,
                ),
            )]

        monkeypatch.setattr(ctx.market_svc, "collect_cross_platform_hot_stocks", fake_collect_cross_platform_hot_stocks)
        monkeypatch.setattr(ctx.market_svc, "collect_intraday_batch", fake_collect_intraday_batch)

        from astock_trading.pipeline.noon import run

        result = run(ctx, "run_noon_hot_quote")

        noon_info = next(field for field in result["discord_embed"]["fields"] if field["name"] == "午间信息")
        assert "巨轮智能(002031)" in noon_info["value"]
        assert "现价 `8.22`" in noon_info["value"]
        assert "现涨 `+0.49%`" in noon_info["value"]
        assert "热榜口径 `+9.95%`" in noon_info["value"]

    def test_includes_opencli_finance_context(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))

        async def fake_collect_cross_platform_hot_stocks(limit=10, run_id=None):
            return [{
                "code": "300274",
                "name": "阳光电源",
                "change_pct": 13.27,
                "source_count": 3,
                "sources": ["xueqiu", "eastmoney", "sinafinance"],
            }]

        async def fake_collect_finance_flash(limit=20, run_id=None):
            return [{"time": "11:20", "title": "北向资金继续流入", "source": "eastmoney"}]

        monkeypatch.setattr(ctx.market_svc, "collect_cross_platform_hot_stocks", fake_collect_cross_platform_hot_stocks)
        monkeypatch.setattr(ctx.market_svc, "collect_finance_flash", fake_collect_finance_flash)

        from astock_trading.pipeline.noon import run

        result = run(ctx, "run_noon_opencli")

        assert result["cross_platform_hot_stocks"] == 1
        assert result["finance_flash"] == 1
        assert any(field["name"] == "午间信息" for field in result["discord_embed"]["fields"])

    def test_skips_standalone_heatmap_when_sector_moves_are_small(self, ctx, monkeypatch):
        calls = []

        async def fake_collect_sector_heatmap(run_id=None):
            return [{"name": "煤炭", "change_pct": -1.1, "amount": 1_500_000_000}]

        monkeypatch.setattr(ctx.market_svc, "collect_sector_heatmap", fake_collect_sector_heatmap)
        monkeypatch.setattr(
            "astock_trading.reporting.discord_sender.send_embed",
            lambda embed, *args, **kwargs: calls.append(embed) or (True, None),
        )

        from astock_trading.pipeline.noon import run

        result = run(ctx, "run_noon_heatmap_quiet")

        assert result["sector_heatmap_pushed"] is False
        assert len(calls) == 1


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

    def test_archives_signal_history_snapshot(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))

        from astock_trading.pipeline.scoring import run

        result = run(ctx, "run_scoring_history")
        rows = ctx.conn.execute(
            "SELECT history_group_id, phase, snapshot_type FROM signal_history_snapshots WHERE run_id = ?",
            ("run_scoring_history",),
        ).fetchall()

        assert result["history_group_id"].startswith("2026-")
        assert {row["snapshot_type"] for row in rows} == {"market", "pool", "candidates", "decision"}
        assert {row["phase"] for row in rows} == {"scoring"}


class TestEveningPipeline:
    def test_runs_without_error(self, ctx):
        from astock_trading.pipeline.evening import run
        result = run(ctx, "run_evening_test")
        assert "signal" in result

    def test_archives_market_history_snapshot(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))

        from astock_trading.pipeline.evening import run

        result = run(ctx, "run_evening_history")

        assert result["history_group_id"].endswith("run_evening_history")
        _assert_market_history_snapshot(ctx, "run_evening_history", "evening")

    def test_includes_opencli_finance_context(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))

        async def fake_collect_cross_platform_hot_stocks(limit=10, run_id=None):
            return [{
                "code": "300274",
                "name": "阳光电源",
                "change_pct": 13.27,
                "source_count": 3,
                "sources": ["xueqiu", "eastmoney", "sinafinance"],
            }]

        async def fake_collect_finance_flash(limit=20, run_id=None):
            return [{"time": "15:10", "title": "商务部回应关税安排", "source": "sinafinance"}]

        async def fake_collect_global_risk_news(limit=12, run_id=None):
            return [{"title": "US-China trade talks continue", "source": "reuters"}]

        async def fake_collect_market_announcements(limit=20, run_id=None):
            return [{"code": "603311", "name": "金海高科", "title": "复牌公告", "category": "复牌公告"}]

        monkeypatch.setattr(ctx.market_svc, "collect_cross_platform_hot_stocks", fake_collect_cross_platform_hot_stocks)
        monkeypatch.setattr(ctx.market_svc, "collect_finance_flash", fake_collect_finance_flash)
        monkeypatch.setattr(ctx.market_svc, "collect_global_risk_news", fake_collect_global_risk_news)
        monkeypatch.setattr(ctx.market_svc, "collect_market_announcements", fake_collect_market_announcements)

        from astock_trading.pipeline.evening import run

        result = run(ctx, "run_evening_opencli")

        field_names = {field["name"] for field in result["discord_embed"]["fields"]}
        assert result["cross_platform_hot_stocks"] == 1
        assert result["finance_flash"] == 1
        assert result["global_risk_news"] == 1
        assert result["market_announcements"] == 1
        assert {"跨平台热度", "财经快讯", "海外风险", "公告提示"} <= field_names

    def test_close_still_pushes_heatmap_summary(self, ctx, monkeypatch):
        calls = []

        async def fake_collect_sector_heatmap(run_id=None):
            return [{"name": "家电", "change_pct": 0.8, "amount": 1_000_000_000}]

        monkeypatch.setattr(ctx.market_svc, "collect_sector_heatmap", fake_collect_sector_heatmap)
        monkeypatch.setattr(
            "astock_trading.reporting.discord_sender.send_embed",
            lambda embed, *args, **kwargs: calls.append(embed) or (True, None),
        )

        from astock_trading.pipeline.evening import run

        result = run(ctx, "run_evening_heatmap_close")

        assert result["sector_heatmap_pushed"] is True
        assert len(calls) == 2

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

    def test_weekly_report_uses_paper_realized_pnl(self, ctx, monkeypatch):
        monkeypatch.setattr("astock_trading.reporting.discord_sender.send_embed", lambda *args, **kwargs: (True, None))
        ctx.event_store.append(
            stream="paper:002138",
            stream_type="paper_trade",
            event_type=AUTO_TRADE_EXECUTED,
            payload={
                "side": "buy",
                "code": "002138",
                "name": "双环传动",
                "shares": 100,
                "price": 10.0,
                "status": "filled",
            },
            metadata={"run_id": "paper_weekly", "account": "paper"},
        )
        ctx.event_store.append(
            stream="paper:002138",
            stream_type="paper_trade",
            event_type=AUTO_TRADE_EXECUTED,
            payload={
                "side": "sell",
                "code": "002138",
                "name": "双环传动",
                "shares": 100,
                "price": 11.2,
                "status": "filled",
                "realized_pnl_cents": 12000,
            },
            metadata={"run_id": "paper_weekly", "account": "paper"},
        )

        from astock_trading.pipeline.weekly import run

        result = run(ctx, "run_weekly_paper_pnl")
        week_file = next((Path(ctx.vault_path) / "03-分析" / "周复盘").glob("*.md"))
        content = week_file.read_text(encoding="utf-8")

        assert result["paper_stats"]["buy_count"] == 1
        assert result["paper_stats"]["sell_count"] == 1
        assert result["paper_stats"]["net_pnl_cents"] == 12000
        assert "| 净盈亏 | ¥+120 |" in content
