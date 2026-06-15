"""舆情分类证据持久化测试。"""

import json
from types import SimpleNamespace

from astock_trading.pipeline.sentiment import run


class FakeResult:
    def __init__(self, rows=None, row=None):
        self._rows = rows or []
        self._row = row

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._row


class RecordingConnection:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=()):
        normalized = " ".join(sql.split())
        self.calls.append((normalized, params))
        if "FROM projection_candidate_pool" in normalized:
            return FakeResult([])
        if "kind = 'news_alert'" in normalized:
            return FakeResult([])
        if "kind = 'news_cache'" in normalized and normalized.startswith("SELECT"):
            return FakeResult(row=None)
        return FakeResult([])


class FakeMXSearch:
    def search(self, query):
        return {"query": query}

    def extract_items(self, result):
        return [{
            "informationType": "REPORT",
            "rating": "买入",
            "title": "双环传动深度报告",
            "content": "预计2026年净利润增长30%",
            "insName": "中信证券",
            "entityFullName": "双环传动",
            "date": "2026-06-15",
        }]


def test_sentiment_pipeline_records_classification_evidence(monkeypatch):
    import astock_trading.market.mx.search as mx_search

    monkeypatch.setattr(mx_search, "MXSearch", FakeMXSearch)
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, None),
    )
    conn = RecordingConnection()
    ctx = SimpleNamespace(
        conn=conn,
        exec_svc=SimpleNamespace(
            get_positions=lambda: [SimpleNamespace(code="002138", name="双环传动")]
        ),
        cfg={"sentiment": {"candidate_pool_limit": 10}},
        market_svc=SimpleNamespace(
            collect_finance_flash=lambda limit=30, run_id=None: [],
            collect_xueqiu_comments=lambda code, limit=5, run_id=None: [],
        ),
        obsidian=SimpleNamespace(write_daily_log=lambda run_id, content: None),
    )

    result = run(ctx, "run_sentiment_evidence")

    assert len(result["alerts"]) == 1
    evidence_inserts = [
        params
        for sql, params in conn.calls
        if "sentiment_classification" in sql or (
            len(params) > 2 and params[2] == "sentiment_classification"
        )
    ]
    assert len(evidence_inserts) == 1
    payload = json.loads(evidence_inserts[0][6])
    assert payload["code"] == "002138"
    assert payload["classified"] is True
    assert payload["source_kind"] == "mx_search"
    assert payload["matched_target"] == {"code": "002138", "name": "双环传动"}
    assert payload["classification"]["level"] == "positive"
    assert payload["raw_item"]["title"] == "双环传动深度报告"
