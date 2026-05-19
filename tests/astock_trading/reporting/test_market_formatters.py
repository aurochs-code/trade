from astock_trading.reporting.discord import format_combined_stop_alert_embed, format_sector_heatmap_embed
from astock_trading.reporting.market_formatters import (
    format_market_signals_markdown,
    format_sector_heatmap_markdown,
    top_sector_movers,
)


def test_top_sector_movers_returns_largest_losers():
    sectors = [
        {"name": "A", "change_pct": 3.0, "amount": 1e8},
        {"name": "B", "change_pct": 1.0, "amount": 1e8},
        {"name": "C", "change_pct": -0.5, "amount": 1e8},
        {"name": "D", "change_pct": -1.2, "amount": 1e8},
        {"name": "E", "change_pct": -3.8, "amount": 1e8},
        {"name": "F", "change_pct": -2.1, "amount": 1e8},
    ]

    gainers, losers = top_sector_movers(sectors, limit=3)

    assert [sector["name"] for sector in gainers] == ["A", "B"]
    assert [sector["name"] for sector in losers] == ["E", "F", "D"]


def test_cross_platform_hot_stock_change_is_labeled_as_hot_list_context():
    lines = format_market_signals_markdown(
        cross_platform_hot_stocks=[{
            "name": "巨轮智能",
            "code": "002031",
            "change_pct": 9.95,
            "source_count": 3,
            "sources": ["xueqiu", "eastmoney", "sinafinance"],
        }]
    )

    text = "\n".join(lines)

    assert "热榜口径 `+9.95%`(非实时)" in text


def test_sector_heatmap_embed_uses_strongest_losers():
    sectors = [
        {"name": "A", "change_pct": 2.0, "amount": 1e8, "up_count": 3, "down_count": 1},
        {"name": "B", "change_pct": -0.3, "amount": 1e8, "up_count": 2, "down_count": 2},
        {"name": "C", "change_pct": -4.2, "amount": 1e8, "up_count": 1, "down_count": 3},
        {"name": "D", "change_pct": -1.1, "amount": 1e8, "up_count": 1, "down_count": 3},
    ]

    embed = format_sector_heatmap_embed(sectors, title="收盘")

    loser_header_index = next(i for i, field in enumerate(embed["fields"]) if field["value"] == "**❄️ 跌幅前 5**")
    loser_fields = embed["fields"][loser_header_index + 1:loser_header_index + 4]

    assert loser_fields[0]["value"].startswith("🔻 C")
    assert loser_fields[1]["value"].startswith("🔻 D")
    assert loser_fields[2]["value"].startswith("🔻 B")


def test_combined_stop_alert_embed_caps_fields_to_discord_limit():
    signals = [
        {
            "code": f"{i:06d}",
            "signal_type": "stop_loss",
            "description": f"desc-{i}",
            "urgency": "immediate",
        }
        for i in range(30)
    ]

    embed = format_combined_stop_alert_embed(signals)

    assert len(embed["fields"]) == 25
    assert embed["fields"][-1]["name"] == "其余告警"
    assert "6" in embed["fields"][-1]["value"]


def test_sector_heatmap_markdown_uses_strongest_losers():
    sectors = [
        {"name": "A", "change_pct": 2.0, "amount": 1e8, "up_count": 3, "down_count": 1},
        {"name": "B", "change_pct": -0.3, "amount": 1e8, "up_count": 2, "down_count": 2},
        {"name": "C", "change_pct": -4.2, "amount": 1e8, "up_count": 1, "down_count": 3},
        {"name": "D", "change_pct": -1.1, "amount": 1e8, "up_count": 1, "down_count": 3},
    ]

    lines = format_sector_heatmap_markdown(sectors)
    loser_rows = [line for line in lines if line.startswith("| 🔻 ")]

    assert loser_rows == [
        "| 🔻 C | `-4.20%` | 1.0亿 |",
        "| 🔻 D | `-1.10%` | 1.0亿 |",
        "| 🔻 B | `-0.30%` | 1.0亿 |",
    ]
