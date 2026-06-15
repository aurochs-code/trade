"""技术指标观测持久化测试。"""

import asyncio

import pandas as pd

from astock_trading.market.models import StockQuote
from astock_trading.market.service import MarketService


class RecordingStore:
    def __init__(self):
        self.records = []

    def get_cached(self, symbol, kind):
        return None

    def save_observation(self, source, kind, symbol, payload, run_id=None):
        observation_id = f"obs_{len(self.records) + 1}"
        self.records.append({
            "observation_id": observation_id,
            "source": source,
            "kind": kind,
            "symbol": symbol,
            "payload": payload,
            "run_id": run_id,
        })
        return observation_id


class QuoteAndKlineProvider:
    async def get_realtime(self, codes):
        return {
            code: StockQuote(
                code=code,
                name="双环传动",
                price=12.9,
                open=12.5,
                high=13.0,
                low=12.4,
                close=12.9,
                volume=2_000_000,
                amount=25_800_000,
                change_pct=1.2,
            )
            for code in codes
        }

    async def get_kline(self, code, period="daily", count=120):
        dates = pd.date_range("2026-04-01", periods=60, freq="B").strftime("%Y-%m-%d")
        closes = [10.0 + index * 0.05 for index in range(60)]
        return pd.DataFrame({
            "date": dates,
            "open": [price - 0.1 for price in closes],
            "high": [price + 0.2 for price in closes],
            "low": [price - 0.2 for price in closes],
            "close": closes,
            "volume": [1_000_000 + index * 10_000 for index in range(60)],
            "amount": [10_000_000 + index * 100_000 for index in range(60)],
        })


def _run(coro):
    return asyncio.run(coro)


def _technical_records(store):
    return [record for record in store.records if record["kind"] == "technical"]


def test_collect_snapshot_persists_technical_observation():
    store = RecordingStore()
    svc = MarketService(market_providers=[QuoteAndKlineProvider()], store=store)

    snapshot = _run(svc.collect_snapshot("002138", "双环传动", run_id="run_technical"))

    assert snapshot.technical is not None
    records = _technical_records(store)
    assert len(records) == 1
    assert records[0]["symbol"] == "002138"
    assert records[0]["run_id"] == "run_technical"
    assert records[0]["payload"]["ma20"] == snapshot.technical.ma20
    assert records[0]["payload"]["above_ma20"] is True


def test_collect_intraday_batch_persists_technical_observation():
    store = RecordingStore()
    svc = MarketService(market_providers=[QuoteAndKlineProvider()], store=store)

    snapshots = _run(
        svc.collect_intraday_batch(
            [{"code": "002138", "name": "双环传动"}],
            run_id="run_intraday_technical",
        )
    )

    assert snapshots[0].technical is not None
    records = _technical_records(store)
    assert len(records) == 1
    assert records[0]["symbol"] == "002138"
    assert records[0]["run_id"] == "run_intraday_technical"
    assert records[0]["payload"]["ma20"] == snapshots[0].technical.ma20
