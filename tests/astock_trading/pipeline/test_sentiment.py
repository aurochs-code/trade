"""Tests for pipeline/sentiment.py — hash, classification, and caching"""


from astock_trading.pipeline.sentiment import (
    _classify_item,
    _classify_opencli_watch_item,
    _extract_brief,
    _item_hash,
)


class TestItemHash:
    def test_includes_date(self):
        """Hash should include date to avoid same-title different-date dedup."""
        item1 = {"code": "002138", "title": "双环传动研报", "date": "2026-04-10"}
        item2 = {"code": "002138", "title": "双环传动研报", "date": "2026-04-11"}
        assert _item_hash(item1) != _item_hash(item2)

    def test_same_item_same_hash(self):
        item = {"code": "002138", "title": "双环传动研报", "date": "2026-04-10"}
        assert _item_hash(item) == _item_hash(item)

    def test_different_code_different_hash(self):
        item1 = {"code": "002138", "title": "研报", "date": "2026-04-10"}
        item2 = {"code": "600066", "title": "研报", "date": "2026-04-10"}
        assert _item_hash(item1) != _item_hash(item2)

    def test_hash_length(self):
        item = {"code": "002138", "title": "test", "date": "2026-04-10"}
        assert len(_item_hash(item)) == 16

    def test_uses_sha256(self):
        """Verify it's sha256, not md5."""
        import hashlib
        item = {"code": "002138", "title": "test", "date": "2026-04-10"}
        key = "002138test2026-04-10"
        expected = hashlib.sha256(key.encode()).hexdigest()[:16]
        assert _item_hash(item) == expected


class TestClassifyItem:
    def test_positive_report(self):
        item = {
            "informationType": "REPORT",
            "rating": "买入",
            "title": "双环传动深度报告",
            "content": "预计2026年净利润增长30%",
            "insName": "中信证券",
            "entityFullName": "双环传动",
            "date": "2026-04-10",
        }
        result = _classify_item(item)
        assert result is not None
        assert result["level"] == "positive"
        assert result["emoji"] == "🟢"
        assert "中信证券" in result["summary"]

    def test_negative_report(self):
        item = {
            "informationType": "REPORT",
            "rating": "减持",
            "title": "评级下调",
            "content": "业绩不及预期",
            "insName": "国泰君安",
            "entityFullName": "某股票",
            "date": "2026-04-10",
        }
        result = _classify_item(item)
        assert result is not None
        assert result["level"] == "negative"

    def test_important_announcement(self):
        item = {
            "informationType": "ANNOUNCEMENT",
            "title": "关于重大合同的公告",
            "content": "公司签署重大合同，金额10亿元",
            "rating": "",
        }
        result = _classify_item(item)
        assert result is not None
        assert result["level"] == "event"
        assert result["emoji"] == "📢"

    def test_negative_news(self):
        item = {
            "informationType": "NEWS",
            "title": "某公司爆雷，股价跌停",
            "content": "公司财务造假被查",
            "rating": "",
        }
        result = _classify_item(item)
        assert result is not None
        assert result["level"] == "negative"

    def test_unimportant_item_returns_none(self):
        item = {
            "informationType": "NEWS",
            "title": "市场综述：今日大盘震荡",
            "content": "沪指小幅收涨",
            "rating": "",
        }
        result = _classify_item(item)
        assert result is None

    def test_report_without_rating_returns_none(self):
        item = {
            "informationType": "REPORT",
            "rating": "",
            "title": "行业周报",
            "content": "本周行业动态",
            "insName": "某券商",
            "entityFullName": "某行业",
        }
        result = _classify_item(item)
        assert result is None


class TestOpenCliWatchClassification:
    def test_finance_flash_matches_watch_stock_event(self):
        item = {
            "time": "2026-05-16 09:10:00",
            "title": "双环传动签署重大合同",
            "summary": "双环传动公告称公司签署重大合同，金额10亿元。",
            "source": "eastmoney",
        }

        result = _classify_opencli_watch_item(item, "002138", "双环传动", "finance_flash")

        assert result is not None
        assert result["level"] == "event"
        assert "东财" in result["summary"]

    def test_finance_flash_ignores_unrelated_item(self):
        item = {
            "time": "2026-05-16 09:10:00",
            "title": "市场综述：今日大盘震荡",
            "summary": "沪指小幅收涨。",
            "source": "sinafinance",
        }

        result = _classify_opencli_watch_item(item, "002138", "双环传动", "finance_flash")

        assert result is None

    def test_xueqiu_comment_flags_negative_watch_signal(self):
        item = {
            "created_at": "2026-05-16T09:30:05.000Z",
            "text": "$双环传动(SZ002138)$ 被立案调查传闻需要核实",
            "source": "xueqiu",
        }

        result = _classify_opencli_watch_item(item, "002138", "双环传动", "xueqiu_comments")

        assert result is not None
        assert result["level"] == "negative"
        assert "雪球评论" in result["summary"]


class TestExtractBrief:
    def test_priority_keywords(self):
        text = "公司概况。预计2026年营收增长25%，净利润CAGR达30%。风险提示。"
        brief = _extract_brief(text, 120)
        assert "预计" in brief or "营收" in brief

    def test_fallback_to_truncation(self):
        text = "这是一段没有关键词的普通文本" * 20
        brief = _extract_brief(text, 50)
        assert len(brief) <= 51  # 50 + "…"
        assert brief.endswith("…")

    def test_empty_text(self):
        assert _extract_brief("") == ""
        assert _extract_brief(None) == ""
