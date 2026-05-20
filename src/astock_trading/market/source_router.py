"""数据源 provider 路由、超时和熔断控制。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import time
from typing import Any, Awaitable, Callable, Optional


@dataclass(frozen=True)
class SourceRouteOptions:
    timeout_seconds: float = 8.0
    retries: int = 0
    max_failures: int = 3
    cooldown_seconds: float = 300.0


@dataclass(frozen=True)
class SourceAttempt:
    provider: Any
    provider_name: str
    status: str
    attempt: int = 1
    latency_ms: int = 0
    error_type: str = ""
    error_message: str = ""


@dataclass(frozen=True)
class SourceRouteResult:
    status: str
    value: Any = None
    provider: Any | None = None
    attempts: list[SourceAttempt] = field(default_factory=list)


@dataclass
class _ProviderState:
    failures: int = 0
    opened_at: Optional[float] = None


class SourceRouter:
    """按 provider 顺序路由，控制单源超时、重试和连续失败熔断。"""

    def __init__(self, default_options: SourceRouteOptions | None = None):
        self._default_options = default_options or SourceRouteOptions()
        self._states: dict[tuple[str, str], _ProviderState] = {}

    async def route(
        self,
        *,
        kind: str,
        providers: list[Any],
        call: Callable[[Any], Awaitable[Any]],
        is_success: Callable[[Any], bool] | None = None,
        options: SourceRouteOptions | None = None,
    ) -> SourceRouteResult:
        route_options = options or self._default_options
        success_check = is_success or (lambda value: value is not None)
        attempts: list[SourceAttempt] = []
        last_status = "empty"

        for provider in providers:
            provider_name = provider.__class__.__name__
            state = self._states.setdefault((kind, provider_name), _ProviderState())
            if self._circuit_open(state, route_options):
                attempts.append(SourceAttempt(
                    provider=provider,
                    provider_name=provider_name,
                    status="circuit_open",
                    error_type="CircuitOpen",
                    error_message="provider 熔断中，跳过本次调用",
                ))
                last_status = "circuit_open"
                continue

            provider_failed = False
            max_attempts = max(route_options.retries, 0) + 1
            for attempt_no in range(1, max_attempts + 1):
                started = time.monotonic()
                try:
                    value = await asyncio.wait_for(
                        call(provider),
                        timeout=route_options.timeout_seconds,
                    )
                except TimeoutError as exc:
                    status = "timeout"
                    attempts.append(self._attempt_from_error(
                        provider,
                        provider_name,
                        status=status,
                        attempt_no=attempt_no,
                        started=started,
                        error=exc,
                        fallback_message=f"provider 超过 {route_options.timeout_seconds:.2f}s 未返回",
                    ))
                    last_status = status
                    provider_failed = True
                    if attempt_no < max_attempts:
                        continue
                    break
                except Exception as exc:
                    status = "provider_error"
                    attempts.append(self._attempt_from_error(
                        provider,
                        provider_name,
                        status=status,
                        attempt_no=attempt_no,
                        started=started,
                        error=exc,
                    ))
                    last_status = status
                    provider_failed = True
                    if attempt_no < max_attempts:
                        continue
                    break

                latency_ms = int((time.monotonic() - started) * 1000)
                if success_check(value):
                    attempts.append(SourceAttempt(
                        provider=provider,
                        provider_name=provider_name,
                        status="ok",
                        attempt=attempt_no,
                        latency_ms=latency_ms,
                    ))
                    self._reset(state)
                    return SourceRouteResult(
                        status="ok",
                        value=value,
                        provider=provider,
                        attempts=attempts,
                    )

                attempts.append(SourceAttempt(
                    provider=provider,
                    provider_name=provider_name,
                    status="empty",
                    attempt=attempt_no,
                    latency_ms=latency_ms,
                    error_type="EmptyResult",
                    error_message="provider 返回空结果",
                ))
                last_status = "empty"
                provider_failed = True
                break

            if provider_failed:
                self._record_failure(state, route_options)

        return SourceRouteResult(status=last_status, attempts=attempts)

    def _circuit_open(self, state: _ProviderState, options: SourceRouteOptions) -> bool:
        if state.opened_at is None:
            return False
        if time.monotonic() - state.opened_at >= options.cooldown_seconds:
            state.opened_at = None
            return False
        return True

    def _record_failure(self, state: _ProviderState, options: SourceRouteOptions) -> None:
        state.failures += 1
        if state.failures >= options.max_failures:
            state.opened_at = time.monotonic()

    def _reset(self, state: _ProviderState) -> None:
        state.failures = 0
        state.opened_at = None

    def _attempt_from_error(
        self,
        provider: Any,
        provider_name: str,
        *,
        status: str,
        attempt_no: int,
        started: float,
        error: BaseException,
        fallback_message: str = "",
    ) -> SourceAttempt:
        return SourceAttempt(
            provider=provider,
            provider_name=provider_name,
            status=status,
            attempt=attempt_no,
            latency_ms=int((time.monotonic() - started) * 1000),
            error_type=error.__class__.__name__,
            error_message=str(error) or fallback_message,
        )
