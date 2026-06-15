"""Shared pytest bootstrap for src-layout imports."""

from __future__ import annotations

import asyncio
import sys
import warnings
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
_SESSION_LOOP: asyncio.AbstractEventLoop | None = None
pytest_plugins = ["tests.astock_trading.helpers.mysql"]

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _ensure_event_loop() -> None:
    global _SESSION_LOOP
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            loop = asyncio.get_event_loop()
        if not loop.is_closed():
            _SESSION_LOOP = loop
            return
    except RuntimeError:
        pass

    if _SESSION_LOOP is None or _SESSION_LOOP.is_closed():
        _SESSION_LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(_SESSION_LOOP)


def pytest_sessionstart(session):
    del session
    _ensure_event_loop()


def pytest_runtest_setup(item):
    del item
    _ensure_event_loop()


def pytest_sessionfinish(session, exitstatus):
    del session, exitstatus
    global _SESSION_LOOP
    loop = _SESSION_LOOP
    if loop is not None and not loop.is_closed():
        loop.close()
    _SESSION_LOOP = None
