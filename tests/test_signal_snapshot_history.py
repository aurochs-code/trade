import os
import tempfile
import unittest
from unittest import mock


class SignalSnapshotHistoryTests(unittest.TestCase):
    def setUp(self):
        self._old_db_path = os.environ.get("TRADE_STATE_DB_PATH")
        self._tmpdir = tempfile.TemporaryDirectory()
        os.environ["TRADE_STATE_DB_PATH"] = os.path.join(self._tmpdir.name, "trade_state.sqlite3")

    def tearDown(self):
        if self._old_db_path is None:
            os.environ.pop("TRADE_STATE_DB_PATH", None)
        else:
            os.environ["TRADE_STATE_DB_PATH"] = self._old_db_path
        self._tmpdir.cleanup()

    def test_market_snapshot_history_round_trip(self):
        from scripts.state import load_market_snapshot_history, save_market_snapshot_history

        save_market_snapshot_history(
            {
                "as_of_date": "2026-04-11",
                "updated_at": "2026-04-11T15:00:00",
                "signal": "GREEN",
                "source": "unit_test",
                "source_chain": ["unit_test"],
                "indices": {
                    "上证指数": {"close": 3200, "ma20": 3180, "signal": "GREEN"},
                },
            },
            pipeline="stock_screener",
            history_group_id="group-001",
            metadata={"pool": "all", "universe": "tracked"},
        )

        result = load_market_snapshot_history("2026-04-11", history_group_id="group-001", limit=1)
        latest = result["latest"]
        self.assertEqual(result["count"], 1)
        self.assertEqual(latest["signal"], "GREEN")
        self.assertEqual(latest["source"], "unit_test")
        self.assertEqual(latest["history_group_id"], "group-001")
        self.assertEqual(latest["metadata"]["pool"], "all")

    def test_signal_snapshot_bundle_loads_exact_run(self):
        from scripts.state import (
            load_candidate_snapshot_history,
            load_daily_signal_snapshot_bundle,
            load_pool_snapshot_history,
            save_candidate_snapshot_history,
            save_decision_snapshot_history,
            save_market_snapshot_history,
            save_pool_snapshot,
        )

        history_group_id = "screener:2026-04-11:150001"
        with mock.patch("scripts.state.service._project_stocks_yaml", return_value="/tmp/stocks.yaml"), mock.patch(
            "scripts.state.service.ObsidianVault",
            return_value=mock.Mock(sync_pool_projection=mock.Mock(return_value={})),
        ):
            save_pool_snapshot(
                [
                    {
                        "code": "601869",
                        "name": "长飞光纤",
                        "bucket": "core",
                        "total_score": 7.6,
                        "technical_score": 2.8,
                        "fundamental_score": 1.5,
                        "flow_score": 1.2,
                        "sentiment_score": 2.1,
                        "veto_triggered": False,
                        "veto_signals": [],
                        "data_quality": "ok",
                    },
                    {
                        "code": "603108",
                        "name": "润达医疗",
                        "bucket": "watch",
                        "total_score": 5.4,
                        "technical_score": 2.0,
                        "fundamental_score": 1.3,
                        "flow_score": 0.9,
                        "sentiment_score": 1.2,
                        "veto_triggered": False,
                        "veto_signals": [],
                        "data_quality": "ok",
                    },
                ],
                metadata={
                    "snapshot_date": "2026-04-11",
                    "updated_at": "2026-04-11T15:00:01",
                    "pipeline": "stock_screener",
                    "history_group_id": history_group_id,
                    "source": "unit_test",
                    "pool": "all",
                    "universe": "tracked",
                },
            )

        save_market_snapshot_history(
            {
                "as_of_date": "2026-04-11",
                "updated_at": "2026-04-11T15:00:01",
                "signal": "GREEN",
                "source": "unit_test_market",
                "source_chain": ["unit_test_market"],
                "indices": {"上证指数": {"signal": "GREEN"}},
            },
            pipeline="stock_screener",
            history_group_id=history_group_id,
            metadata={"snapshot_date": "2026-04-11"},
        )
        save_decision_snapshot_history(
            {
                "decision": "BUY",
                "action": "BUY",
                "market_signal": "GREEN",
                "portfolio_risk": {"state": "ok"},
            },
            snapshot_date="2026-04-11",
            pipeline="stock_screener",
            history_group_id=history_group_id,
            metadata={"snapshot_date": "2026-04-11"},
        )
        save_candidate_snapshot_history(
            [
                {
                    "code": "601869",
                    "name": "长飞光纤",
                    "total_score": 7.6,
                    "technical_score": 2.8,
                    "fundamental_score": 1.5,
                    "flow_score": 1.2,
                    "sentiment_score": 2.1,
                    "veto_triggered": False,
                    "veto_signals": [],
                    "passed_text": "✅",
                    "recommendation": "买入",
                    "data_quality": "ok",
                },
                {
                    "code": "603108",
                    "name": "润达医疗",
                    "total_score": 5.4,
                    "technical_score": 2.0,
                    "fundamental_score": 1.3,
                    "flow_score": 0.9,
                    "sentiment_score": 1.2,
                    "veto_triggered": False,
                    "veto_signals": [],
                    "passed_text": "🟡",
                    "recommendation": "观察",
                    "data_quality": "ok",
                },
            ],
            snapshot_date="2026-04-11",
            pipeline="stock_screener",
            history_group_id=history_group_id,
            pool="all",
            universe="tracked",
            source="unit_test",
            actionable_count=2,
            metadata={"snapshot_date": "2026-04-11"},
        )

        candidate_history = load_candidate_snapshot_history("2026-04-11", history_group_id=history_group_id, limit=1)
        self.assertEqual(candidate_history["latest"]["candidate_count"], 2)

        pool_history = load_pool_snapshot_history("2026-04-11", history_group_id=history_group_id, limit=1)
        self.assertEqual(pool_history["latest"]["summary"]["core_count"], 1)
        self.assertEqual(pool_history["latest"]["summary"]["watch_count"], 1)

        bundle = load_daily_signal_snapshot_bundle("2026-04-11", history_group_id=history_group_id)
        self.assertEqual(bundle["status"], "ok")
        self.assertEqual(bundle["history_group_id"], history_group_id)
        self.assertEqual(bundle["market_snapshot"]["signal"], "GREEN")
        self.assertEqual(bundle["today_decision"]["decision"], "BUY")
        self.assertEqual(len(bundle["scored_candidates"]), 2)
        self.assertEqual(bundle["pool_snapshot"]["summary"]["core_count"], 1)


if __name__ == "__main__":
    unittest.main()
