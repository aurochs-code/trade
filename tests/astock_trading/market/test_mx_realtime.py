"""MX 实时行情解析测试。"""

from astock_trading.market.mx import realtime


def _mixed_seres_table() -> dict:
    return {
        "searchDataResultDTO": {
            "dataTableDTOList": [
                {
                    "entityCodes": ["09927.HK", "601127.SH"],
                    "nameMap": {
                        "f2": "最新价",
                        "f3": "涨跌幅",
                        "324785": "成交量",
                        "327483": "成交额",
                        "326339": "最高价",
                        "326386": "最低价",
                    },
                    "rawTable": {
                        "headName": ["赛力斯(09927.HK)", "赛力斯(601127.SH)"],
                        "f2": ["70.250", "83.32"],
                        "f3": ["5.160%", "6.05%"],
                        "324785": [2649700, 62806508],
                        "327483": [188836268.0, 5300572919.0],
                        "326339": ["73.20", "86.42"],
                        "326386": ["66.80", "78.56"],
                    },
                }
            ]
        }
    }


def test_parse_mx_table_selects_requested_a_share_from_mixed_hk_a_table():
    parsed = realtime._parse_mx_table(_mixed_seres_table(), target_code="601127")

    assert parsed["最新价"] == 83.32
    assert parsed["涨跌幅"] == 6.05
    assert parsed["成交量"] == 62806508
    assert parsed["_ts"] == "赛力斯(601127.SH)"


def test_parse_mx_table_keeps_legacy_first_row_without_target_code():
    parsed = realtime._parse_mx_table(_mixed_seres_table())

    assert parsed["最新价"] == 70.25
    assert parsed["涨跌幅"] == 5.16
    assert parsed["_ts"] == "赛力斯(09927.HK)"


def test_get_realtime_mx_uses_requested_code_when_mx_returns_mixed_table(monkeypatch):
    realtime._CACHE.clear()

    def fake_post(query: str):
        assert query.startswith("601127 ")
        return _mixed_seres_table()

    monkeypatch.setattr(realtime, "_http_post", fake_post)

    result = realtime.get_realtime_mx(["601127"])

    assert result["601127"]["price"] == 83.32
    assert result["601127"]["change_pct"] == 6.05
    assert result["601127"]["volume"] == 62806508
