"""Tests for the OpenAPI drift processor (auto-doc + drift detection)."""
from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_sweep_is_noop_when_disabled(monkeypatch):
    from sentinel_core.config import settings
    from sentinel_scheduler.modules.scheduler.openapi_drift import OpenAPIDriftProcessor

    monkeypatch.setattr(settings, "OPENAPI_DRIFT_ENABLED", False)
    proc = OpenAPIDriftProcessor()
    result = await proc.sweep()
    assert result == {"status": "disabled"}


@pytest.mark.asyncio
async def test_check_account_persists_baseline_when_no_prior_spec(db, monkeypatch):
    from sentinel_core.config import settings
    from sentinel_scheduler.modules.scheduler.openapi_drift import OpenAPIDriftProcessor
    from sentinel_core.models.core import OpenAPISpec
    from sqlalchemy import select

    monkeypatch.setattr(settings, "OPENAPI_DRIFT_ENABLED", True)
    proc = OpenAPIDriftProcessor()

    async def fake_generate_spec(collection_name="Discovered API", account_id=None):
        return {"openapi": "3.0.0", "paths": {"/users": {"get": {}}}}

    monkeypatch.setattr(proc._gen, "generate_spec", fake_generate_spec)

    result = await proc._check_account_with_session(db, account_id=1000000)
    assert result == {"status": "baseline"}

    stored = (await db.execute(
        select(OpenAPISpec).where(OpenAPISpec.account_id == 1000000)
    )).scalars().all()
    assert len(stored) == 1
    assert stored[0].spec_json["paths"] == {"/users": {"get": {}}}


@pytest.mark.asyncio
async def test_check_account_skips_identical_spec(db, monkeypatch):
    from sentinel_core.config import settings
    from sentinel_scheduler.modules.scheduler.openapi_drift import OpenAPIDriftProcessor
    from sentinel_core.models.core import OpenAPISpec
    from sqlalchemy import select

    monkeypatch.setattr(settings, "OPENAPI_DRIFT_ENABLED", True)
    proc = OpenAPIDriftProcessor()

    spec = {"openapi": "3.0.0", "paths": {"/users": {"get": {}}}}
    db.add(OpenAPISpec(account_id=1000000, spec_json=spec))
    await db.commit()

    async def fake_generate_spec(collection_name="Discovered API", account_id=None):
        return dict(spec)

    monkeypatch.setattr(proc._gen, "generate_spec", fake_generate_spec)

    result = await proc._check_account_with_session(db, account_id=1000000)
    assert result == {"status": "unchanged"}

    stored = (await db.execute(
        select(OpenAPISpec).where(OpenAPISpec.account_id == 1000000)
    )).scalars().all()
    assert len(stored) == 1  # no new row was added


@pytest.mark.asyncio
async def test_check_account_persists_new_version_on_additive_change(db, monkeypatch):
    from sentinel_core.config import settings
    from sentinel_scheduler.modules.scheduler.openapi_drift import OpenAPIDriftProcessor
    from sentinel_core.models.core import OpenAPISpec, PolicyViolation
    from sqlalchemy import select

    monkeypatch.setattr(settings, "OPENAPI_DRIFT_ENABLED", True)
    proc = OpenAPIDriftProcessor()

    baseline_spec = {"openapi": "3.0.0", "paths": {"/users": {"get": {}}}}
    db.add(OpenAPISpec(account_id=1000000, spec_json=baseline_spec))
    await db.commit()

    async def fake_generate_spec(collection_name="Discovered API", account_id=None):
        # Purely additive: /users kept as-is, /orders is a brand new path.
        return {
            "openapi": "3.0.0",
            "paths": {"/users": {"get": {}}, "/orders": {"get": {}}},
        }

    monkeypatch.setattr(proc._gen, "generate_spec", fake_generate_spec)

    result = await proc._check_account_with_session(db, account_id=1000000)
    await db.commit()
    assert result["status"] == "unchanged"

    stored = (await db.execute(
        select(OpenAPISpec).where(OpenAPISpec.account_id == 1000000)
    )).scalars().all()
    assert len(stored) == 2  # baseline + persisted additive version

    violations = (await db.execute(
        select(PolicyViolation).where(PolicyViolation.account_id == 1000000)
    )).scalars().all()
    assert len(violations) == 0  # nothing alert-worthy in a purely additive change


@pytest.mark.asyncio
async def test_check_account_detects_drift(db, monkeypatch):
    from sentinel_core.config import settings
    from sentinel_scheduler.modules.scheduler.openapi_drift import OpenAPIDriftProcessor
    from sentinel_core.models.core import OpenAPISpec
    from sqlalchemy import select

    monkeypatch.setattr(settings, "OPENAPI_DRIFT_ENABLED", True)
    proc = OpenAPIDriftProcessor()

    old_spec = {
        "openapi": "3.0.0",
        "paths": {"/users": {"get": {}}, "/orders": {"get": {}}},
    }
    db.add(OpenAPISpec(account_id=1000000, spec_json=old_spec))
    await db.commit()

    async def fake_generate_spec(collection_name="Discovered API", account_id=None):
        # /users removed, /orders untouched - not a total wipeout, so this is
        # legitimate drift rather than a degenerate/partial-generation spec.
        return {"openapi": "3.0.0", "paths": {"/orders": {"get": {}}}}

    monkeypatch.setattr(proc._gen, "generate_spec", fake_generate_spec)

    result = await proc._check_account_with_session(db, account_id=1000000)
    assert result["status"] == "drifted"
    assert result["change_count"] == 1

    stored = (await db.execute(
        select(OpenAPISpec)
        .where(OpenAPISpec.account_id == 1000000)
        .order_by(OpenAPISpec.created_at.desc())
    )).scalars().all()
    assert len(stored) == 2  # baseline + new drifted version


@pytest.mark.asyncio
async def test_drift_creates_violation_alert_and_evidence(db, monkeypatch):
    from sentinel_core.config import settings
    from sentinel_scheduler.modules.scheduler.openapi_drift import OpenAPIDriftProcessor
    from sentinel_core.models.core import OpenAPISpec, APIEndpoint, PolicyViolation, Alert, EvidenceRecord
    from sqlalchemy import select

    monkeypatch.setattr(settings, "OPENAPI_DRIFT_ENABLED", True)
    proc = OpenAPIDriftProcessor()

    db.add(APIEndpoint(id="ep-1", account_id=1000000, method="GET", path="/users"))
    old_spec = {
        "openapi": "3.0.0",
        "paths": {"/users": {"get": {}}, "/orders": {"get": {}}},
    }
    db.add(OpenAPISpec(account_id=1000000, spec_json=old_spec))
    await db.commit()

    async def fake_generate_spec(collection_name="Discovered API", account_id=None):
        # /users removed -> path_removed, HIGH; /orders untouched so this isn't
        # a total wipeout (degenerate-spec guard shouldn't kick in here).
        return {"openapi": "3.0.0", "paths": {"/orders": {"get": {}}}}

    monkeypatch.setattr(proc._gen, "generate_spec", fake_generate_spec)

    result = await proc._check_account_with_session(db, account_id=1000000)
    await db.commit()
    assert result["status"] == "drifted"

    violations = (await db.execute(
        select(PolicyViolation).where(PolicyViolation.account_id == 1000000)
    )).scalars().all()
    assert len(violations) == 1
    assert violations[0].rule_type == "DRIFT"
    assert violations[0].severity == "HIGH"
    assert violations[0].endpoint_id == "ep-1"

    alerts = (await db.execute(
        select(Alert).where(Alert.account_id == 1000000)
    )).scalars().all()
    assert len(alerts) == 1
    assert alerts[0].category == "API_DRIFT"
    assert alerts[0].severity == "HIGH"

    evidence = (await db.execute(
        select(EvidenceRecord).where(EvidenceRecord.account_id == 1000000)
    )).scalars().all()
    assert len(evidence) == 1
    assert evidence[0].evidence_type == "drift"


@pytest.mark.asyncio
async def test_check_account_skips_degenerate_spec(db, monkeypatch):
    from sentinel_core.config import settings
    from sentinel_scheduler.modules.scheduler.openapi_drift import OpenAPIDriftProcessor
    from sentinel_core.models.core import OpenAPISpec, PolicyViolation
    from sqlalchemy import select

    monkeypatch.setattr(settings, "OPENAPI_DRIFT_ENABLED", True)
    proc = OpenAPIDriftProcessor()

    old_spec = {
        "openapi": "3.0.0",
        "paths": {"/users": {"get": {}}, "/orders": {"get": {}}},
    }
    db.add(OpenAPISpec(account_id=1000000, spec_json=old_spec))
    await db.commit()

    async def fake_generate_spec(collection_name="Discovered API", account_id=None):
        # Total wipeout - looks like a discovery outage or partial read, not
        # legitimate drift. Should be skipped rather than becoming the new
        # baseline or raising thousands of findings.
        return {"openapi": "3.0.0", "paths": {}}

    monkeypatch.setattr(proc._gen, "generate_spec", fake_generate_spec)

    result = await proc._check_account_with_session(db, account_id=1000000)
    await db.commit()
    assert result == {"status": "skipped_degenerate"}

    stored = (await db.execute(
        select(OpenAPISpec).where(OpenAPISpec.account_id == 1000000)
    )).scalars().all()
    assert len(stored) == 1  # the good baseline was NOT overwritten

    violations = (await db.execute(
        select(PolicyViolation).where(PolicyViolation.account_id == 1000000)
    )).scalars().all()
    assert len(violations) == 0  # no findings raised for a degenerate spec


@pytest.mark.asyncio
async def test_sweep_continues_when_one_account_errors(monkeypatch):
    from sentinel_core.config import settings
    from sentinel_scheduler.modules.scheduler.openapi_drift import OpenAPIDriftProcessor

    monkeypatch.setattr(settings, "OPENAPI_DRIFT_ENABLED", True)
    proc = OpenAPIDriftProcessor()

    async def fake_accounts():
        return [1, 2]

    async def flaky_check(account_id):
        if account_id == 1:
            raise RuntimeError("boom")
        return {"status": "unchanged"}

    monkeypatch.setattr(proc, "_accounts_with_endpoints", fake_accounts)
    monkeypatch.setattr(proc, "_check_account", flaky_check)

    result = await proc.sweep()
    assert result["status"] == "ok"
    assert result["accounts_checked"] == 2
    assert result["accounts_drifted"] == 0
