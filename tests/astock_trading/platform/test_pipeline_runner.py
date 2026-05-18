from __future__ import annotations

from types import SimpleNamespace

from astock_trading.platform.pipeline_runner import execute_pipeline


class FakeRunJournal:
    def __init__(self) -> None:
        self.completed: dict | None = None
        self.failed: dict | None = None

    def is_completed_today(self, pipeline_type: str) -> bool:
        return False

    def start_run(self, pipeline_type: str, config_version: str) -> str:
        return f"run_{pipeline_type}_{config_version}"

    def complete_run(self, run_id: str, artifacts: dict | None = None) -> None:
        self.completed = {"run_id": run_id, "artifacts": artifacts or {}}

    def fail_run(self, run_id: str, error_message: str, artifacts: dict | None = None) -> None:
        self.failed = {
            "run_id": run_id,
            "error_message": error_message,
            "artifacts": artifacts or {},
        }


def test_execute_pipeline_refreshes_required_data_sources_before_failing(monkeypatch):
    import astock_trading.market.health as market_health
    import astock_trading.pipeline.morning as morning_pipeline
    import astock_trading.platform.data_source_refresh as data_source_refresh

    health_checks = [
        {"status": "failed", "required_missing": ["hot_stocks"], "optional_missing": []},
        {"status": "warning", "required_missing": [], "optional_missing": ["core_pool"]},
    ]
    refresh_calls: list[str] = []

    def fake_evaluate_data_source_health(conn):
        return health_checks.pop(0)

    def fake_refresh_required_data_sources(ctx, *, run_id=None, **kwargs):
        refresh_calls.append(run_id)
        return {"status": "ok", "required_missing": [], "optional_missing": []}

    monkeypatch.setattr(market_health, "evaluate_data_source_health", fake_evaluate_data_source_health)
    monkeypatch.setattr(
        data_source_refresh,
        "refresh_required_data_sources",
        fake_refresh_required_data_sources,
    )
    monkeypatch.setattr(
        morning_pipeline,
        "run",
        lambda ctx, run_id: {"signal": "GREEN", "positions": 0, "risk_alerts": []},
    )

    run_journal = FakeRunJournal()
    ctx = SimpleNamespace(conn=object(), run_journal=run_journal, config_version="test")

    result = execute_pipeline(ctx, "morning", is_trading_day=True)

    assert result["status"] == "completed"
    assert result["pipeline"] == "morning"
    assert refresh_calls == ["run_morning_test"]
    assert result["data_sources"]["status"] == "warning"
    assert result["data_source_refresh"]["status"] == "ok"
    assert result["data_source_warning"]["optional_missing"] == ["core_pool"]
    assert run_journal.completed is not None
    assert run_journal.failed is None
