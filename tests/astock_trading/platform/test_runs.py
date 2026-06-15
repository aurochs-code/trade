"""Tests for platform/runs.py — RunJournal"""

import json
import pytest

from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.runs import RunJournal


@pytest.fixture
def journal(mysql_conn):
    conn = mysql_conn
    # Need a config version for foreign-key-like references
    registry = ConfigRegistry()
    snapshot = registry.freeze(conn)

    j = RunJournal(conn)
    yield j, conn, snapshot.version


def test_start_and_complete(journal):
    j, conn, cv = journal
    run_id = j.start_run("morning", cv)
    assert run_id.startswith("run_morning_")

    last = j.get_last_run("morning")
    assert last is not None
    assert last["status"] == "running"

    j.complete_run(run_id, artifacts={"scores": 10})

    last = j.get_last_run("morning")
    assert last["status"] == "completed"
    assert last["finished_at"] is not None


def test_fail_run(journal):
    j, conn, cv = journal
    run_id = j.start_run("evening", cv)
    j.fail_run(run_id, "network timeout")

    last = j.get_last_run("evening")
    assert last["status"] == "failed"
    assert "network timeout" in last["error_message"]

    failed = j.get_failed_runs(days=1)
    assert len(failed) == 1
    assert failed[0]["run_id"] == run_id


def test_fail_run_can_store_artifacts(journal):
    j, conn, cv = journal
    run_id = j.start_run("morning", cv)
    j.fail_run(run_id, "data source gate failed", artifacts={"data_sources": {"status": "failed"}})

    last = j.get_last_run("morning")
    artifacts = json.loads(last["artifacts_json"])
    assert last["status"] == "failed"
    assert artifacts["data_sources"]["status"] == "failed"


def test_is_completed_today(journal):
    j, conn, cv = journal

    assert j.is_completed_today("scoring") is False

    run_id = j.start_run("scoring", cv)
    assert j.is_completed_today("scoring") is False  # still running

    j.complete_run(run_id)
    assert j.is_completed_today("scoring") is True


def test_idempotency_check(journal):
    """A completed run should prevent re-execution on the same day."""
    j, conn, cv = journal

    run_id = j.start_run("morning", cv)
    j.complete_run(run_id)

    # Simulating idempotency check before starting a new run
    assert j.is_completed_today("morning") is True
    # Different run_type should not be affected
    assert j.is_completed_today("evening") is False


def test_list_runs(journal):
    j, conn, cv = journal
    j.start_run("morning", cv)
    j.start_run("evening", cv)

    runs = j.list_runs()
    assert len(runs) == 2

    runs = j.list_runs(run_type="morning")
    assert len(runs) == 1
    assert runs[0]["run_type"] == "morning"


def test_multiple_runs_same_type(journal):
    j, conn, cv = journal
    r1 = j.start_run("scoring", cv)
    j.fail_run(r1, "error 1")

    r2 = j.start_run("scoring", cv)
    j.complete_run(r2)

    # is_completed_today should be True because r2 succeeded
    assert j.is_completed_today("scoring") is True

    # get_last_run should return the most recent
    last = j.get_last_run("scoring")
    assert last["run_id"] == r2
    assert last["status"] == "completed"


def test_get_last_run_uses_shanghai_business_day(journal):
    j, conn, cv = journal
    conn.execute(
        """INSERT INTO run_log
           (run_id, run_type, scope, config_version, status, started_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "run_manual_1",
            "morning",
            "cn_a",
            cv,
            "completed",
            "2026-04-17T16:30:00+00:00",
        ),
    )

    assert j.get_last_run("morning", date="2026-04-17") is None
    last = j.get_last_run("morning", date="2026-04-18")
    assert last is not None
    assert last["run_id"] == "run_manual_1"
