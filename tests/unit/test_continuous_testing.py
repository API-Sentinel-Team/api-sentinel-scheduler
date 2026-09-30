"""Tests for the continuous-testing processor (Gap A: Discovery -> Testing)."""
from __future__ import annotations

import uuid

import pytest

from sentinel_scheduler.modules.scheduler.continuous_testing import ContinuousTestingProcessor


@pytest.mark.asyncio
async def test_sweep_is_noop_when_disabled(monkeypatch):
    from sentinel_core.config import settings

    monkeypatch.setattr(settings, "CONTINUOUS_TESTING_ENABLED", False)
    proc = ContinuousTestingProcessor()
    result = await proc.sweep()
    assert result["status"] == "disabled"
    assert result["accounts_triggered"] == 0


@pytest.mark.asyncio
async def test_sweep_triggers_per_account_with_untested_endpoints(monkeypatch):
    from sentinel_core.config import settings

    monkeypatch.setattr(settings, "CONTINUOUS_TESTING_ENABLED", True)

    # Avoid touching the DB: stub the account discovery + the scheduler trigger.
    proc = ContinuousTestingProcessor()

    async def fake_accounts():
        return [1000000, 2000000]

    triggered_accounts = []

    async def fake_trigger(account_id, **kwargs):
        triggered_accounts.append(account_id)
        return {"status": "started", "run_id": str(uuid.uuid4())}

    monkeypatch.setattr(proc, "_accounts_with_untested_endpoints", fake_accounts)
    monkeypatch.setattr(proc._scheduler, "trigger_continuous_discovery_scan", fake_trigger)

    result = await proc.sweep()
    assert result["status"] == "ok"
    assert result["accounts_triggered"] == 2
    assert triggered_accounts == [1000000, 2000000]


@pytest.mark.asyncio
async def test_sweep_continues_when_one_account_errors(monkeypatch):
    from sentinel_core.config import settings

    monkeypatch.setattr(settings, "CONTINUOUS_TESTING_ENABLED", True)
    proc = ContinuousTestingProcessor()

    async def fake_accounts():
        return [1, 2]

    async def flaky_trigger(account_id, **kwargs):
        if account_id == 1:
            raise RuntimeError("boom")
        return {"status": "started"}

    monkeypatch.setattr(proc, "_accounts_with_untested_endpoints", fake_accounts)
    monkeypatch.setattr(proc._scheduler, "trigger_continuous_discovery_scan", flaky_trigger)

    result = await proc.sweep()
    # Account 1 errored but account 2 still triggered -> sweep survived.
    assert result["accounts_triggered"] == 1


@pytest.mark.asyncio
async def test_trigger_continuous_discovery_scan_noop_without_untested(db, monkeypatch):
    from sentinel_core.config import settings
    from sentinel_scheduler.modules.scheduler.test_scheduler import TestScheduler

    monkeypatch.setattr(settings, "CONTINUOUS_TESTING_ENABLED", True)
    # No endpoints in the DB -> nothing to scan.
    scheduler = TestScheduler()
    result = await scheduler.trigger_continuous_discovery_scan(account_id=999999111)
    assert result["status"] == "noop"
    assert result["reason"] == "no_untested_endpoints"
