"""路线执行策略的轻量工具。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any


def iter_route_policy_entries(
    policy_map: Mapping[str, Any] | None,
    signal: str,
    route: str | None,
) -> Iterator[tuple[str, dict[str, Any]]]:
    """按精确到通配的顺序展开 route policy。

    兼容两种配置形态：
    - ``KEY: {policy}``
    - ``KEY: [{policy_band_1}, {policy_band_2}]``
    """

    if not isinstance(policy_map, Mapping):
        return
    route_name = str(route or "unknown")
    for key in (f"{signal}:{route_name}", f"*:{route_name}", route_name):
        value = policy_map.get(key)
        if isinstance(value, Mapping):
            yield key, dict(value)
            continue
        if isinstance(value, list | tuple):
            for item in value:
                if isinstance(item, Mapping):
                    yield key, dict(item)


def route_policy_values(policy_map: Mapping[str, Any] | None) -> Iterator[dict[str, Any]]:
    """展开配置里所有 policy 条目，供诊断和语义摘要使用。"""

    if not isinstance(policy_map, Mapping):
        return
    for value in policy_map.values():
        if isinstance(value, Mapping):
            yield dict(value)
            continue
        if isinstance(value, list | tuple):
            for item in value:
                if isinstance(item, Mapping):
                    yield dict(item)
