from __future__ import annotations

import asyncio

from astock_trading.market.source_router import SourceRouteOptions, SourceRouter


class ValueProvider:
    def __init__(self, value):
        self.value = value
        self.calls = 0

    async def fetch(self):
        self.calls += 1
        return self.value


class SlowProvider:
    def __init__(self):
        self.calls = 0

    async def fetch(self):
        self.calls += 1
        await asyncio.sleep(1)
        return "late"


class FlakyProvider:
    def __init__(self):
        self.calls = 0

    async def fetch(self):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary outage")
        return "ok"


class FailingProvider:
    def __init__(self):
        self.calls = 0

    async def fetch(self):
        self.calls += 1
        raise RuntimeError("source down")


def test_source_router_times_out_slow_provider_and_falls_back():
    router = SourceRouter()
    slow = SlowProvider()
    fast = ValueProvider("ok")

    result = asyncio.get_event_loop().run_until_complete(
        router.route(
            kind="fund_flow",
            providers=[slow, fast],
            call=lambda provider: provider.fetch(),
            is_success=lambda value: value is not None,
            options=SourceRouteOptions(timeout_seconds=0.01),
        )
    )

    assert result.status == "ok"
    assert result.value == "ok"
    assert result.provider is fast
    assert [attempt.status for attempt in result.attempts] == ["timeout", "ok"]
    assert slow.calls == 1
    assert fast.calls == 1


def test_source_router_retries_transient_provider_error():
    router = SourceRouter()
    flaky = FlakyProvider()

    result = asyncio.get_event_loop().run_until_complete(
        router.route(
            kind="fund_flow",
            providers=[flaky],
            call=lambda provider: provider.fetch(),
            is_success=lambda value: value is not None,
            options=SourceRouteOptions(timeout_seconds=0.1, retries=1),
        )
    )

    assert result.status == "ok"
    assert result.value == "ok"
    assert flaky.calls == 2
    assert [attempt.status for attempt in result.attempts] == ["provider_error", "ok"]


def test_source_router_opens_circuit_after_consecutive_failures():
    router = SourceRouter()
    bad = FailingProvider()
    good = ValueProvider("fallback")
    options = SourceRouteOptions(timeout_seconds=0.1, max_failures=2, cooldown_seconds=60)

    for _ in range(2):
        result = asyncio.get_event_loop().run_until_complete(
            router.route(
                kind="fund_flow",
                providers=[bad],
                call=lambda provider: provider.fetch(),
                is_success=lambda value: value is not None,
                options=options,
            )
        )
        assert result.status == "provider_error"

    result = asyncio.get_event_loop().run_until_complete(
        router.route(
            kind="fund_flow",
            providers=[bad, good],
            call=lambda provider: provider.fetch(),
            is_success=lambda value: value is not None,
            options=options,
        )
    )

    assert result.status == "ok"
    assert result.value == "fallback"
    assert bad.calls == 2
    assert good.calls == 1
    assert [attempt.status for attempt in result.attempts] == ["circuit_open", "ok"]


def test_source_router_treats_empty_result_as_failure_and_falls_back():
    router = SourceRouter()
    empty = ValueProvider(None)
    good = ValueProvider({"items": [1]})

    result = asyncio.get_event_loop().run_until_complete(
        router.route(
            kind="industry_comparison",
            providers=[empty, good],
            call=lambda provider: provider.fetch(),
            is_success=lambda value: bool(value),
            options=SourceRouteOptions(timeout_seconds=0.1),
        )
    )

    assert result.status == "ok"
    assert result.value == {"items": [1]}
    assert [attempt.status for attempt in result.attempts] == ["empty", "ok"]
