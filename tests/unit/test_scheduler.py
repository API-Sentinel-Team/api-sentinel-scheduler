import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker



@pytest.fixture(autouse=True)
def _allow_unauthenticated_active_scans_for_scheduler_unit_tests(monkeypatch):
    monkeypatch.setattr(
        "sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_REQUIRE_AUTH_PROFILE_FOR_ACTIVE_SCANS",
        False,
    )


@pytest.mark.asyncio
async def test_scheduler_trigger_queues_run_even_with_legacy_background_mode(test_engine, monkeypatch):
    from sentinel_core.models.core import APIEndpoint, TestRun
    from sentinel_scheduler.modules.scheduler.test_scheduler import TestScheduler

    session_factory = async_sessionmaker(bind=test_engine, expire_on_commit=False)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.AsyncSessionLocal", session_factory)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_SCAN_EXECUTION_MODE", "background")

    async with session_factory() as db:
        db.add(
            APIEndpoint(
                id="endpoint-1",
                account_id=1000000,
                method="GET",
                protocol="https",
                host="api.example.com",
                path="/users",
            )
        )
        await db.commit()

    scheduler = TestScheduler()
    trigger_result = await scheduler._trigger_run("schedule-1", ["tpl-1"], ["endpoint-1"], 1000000)

    # The scheduler never executes scans; api-sentinel-scan-worker claims the run.
    assert trigger_result["status"] == "queued"

    async with session_factory() as db:
        result = await db.execute(select(TestRun).where(TestRun.id == trigger_result["run_id"]))
        run = result.scalar_one()

    assert run.account_id == 1000000
    assert run.template_ids == ["tpl-1"]
    assert run.endpoint_ids == ["endpoint-1"]
    assert run.status == "PENDING"
    assert run.trigger_source == "schedule"


@pytest.mark.asyncio
async def test_scheduler_trigger_leaves_run_pending_in_queued_mode(test_engine, monkeypatch):
    from sentinel_core.models.core import APIEndpoint, TestRun
    from sentinel_scheduler.modules.scheduler.test_scheduler import TestScheduler

    session_factory = async_sessionmaker(bind=test_engine, expire_on_commit=False)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.AsyncSessionLocal", session_factory)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_SCAN_EXECUTION_MODE", "queued")

    async with session_factory() as db:
        db.add(
            APIEndpoint(
                id="endpoint-queued",
                account_id=1000200,
                method="GET",
                protocol="https",
                host="api.example.com",
                path="/queued",
            )
        )
        await db.commit()

    scheduler = TestScheduler()
    trigger_result = await scheduler._trigger_run("schedule-queued", ["tpl-queued"], ["endpoint-queued"], 1000200)

    assert trigger_result["status"] == "queued"

    async with session_factory() as db:
        result = await db.execute(select(TestRun).where(TestRun.id == trigger_result["run_id"]))
        run = result.scalar_one()

    assert run.account_id == 1000200
    assert run.status == "PENDING"
    assert run.template_ids == ["tpl-queued"]
    assert run.endpoint_ids == ["endpoint-queued"]
    assert run.trigger_source == "schedule"


@pytest.mark.asyncio
async def test_scheduler_trigger_records_queue_audit_with_auth_context(test_engine, monkeypatch):
    from sentinel_core.config import settings as app_settings
    from sentinel_core.models.core import APIEndpoint, AuditLog, AuthProfile, PentestProfile, TestRun
    from sentinel_scheduler.modules.scheduler.test_scheduler import TestScheduler

    account_id = 1000201
    session_factory = async_sessionmaker(bind=test_engine, expire_on_commit=False)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.AsyncSessionLocal", session_factory)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_SCAN_EXECUTION_MODE", "queued")
    monkeypatch.setattr(
        "sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_REQUIRE_AUTH_PROFILE_FOR_ACTIVE_SCANS",
        True,
    )

    async with session_factory() as db:
        endpoint = APIEndpoint(
            id="endpoint-queued-auth",
            account_id=account_id,
            method="GET",
            protocol="https",
            host="api.example.com",
            path="/queued-auth",
        )
        auth_profile = AuthProfile(
            account_id=account_id,
            name="scheduler queued bearer",
            auth_mode="bearer",
            token="Bearer scheduler-secret-token",
            scope_domains=["api.example.com"],
            is_active=True,
        )
        db.add_all([endpoint, auth_profile])
        await db.flush()
        db.add(
            PentestProfile(
                account_id=account_id,
                name=app_settings.PENTEST_DEFAULT_PROFILE_NAME,
                auth_profile_id=auth_profile.id,
            )
        )
        await db.commit()
        auth_profile_id = auth_profile.id

    scheduler = TestScheduler()
    trigger_result = await scheduler._trigger_run(
        "schedule-queued-auth",
        ["tpl-auth-1", "tpl-auth-2"],
        ["endpoint-queued-auth"],
        account_id,
    )

    assert trigger_result["status"] == "queued"
    assert trigger_result["source_schedule_id"] == "schedule-queued-auth"
    async with session_factory() as db:
        run = (
            await db.execute(select(TestRun).where(TestRun.id == trigger_result["run_id"]))
        ).scalar_one()
        audit = (
            await db.execute(
                select(AuditLog).where(
                    AuditLog.resource_id == trigger_result["run_id"],
                    AuditLog.action == "SCAN_RUN_QUEUED",
                )
            )
        ).scalar_one()

    assert run.trigger_source == "schedule"
    assert run.source_schedule_id == "schedule-queued-auth"
    assert audit.details["source"] == "schedule"
    assert audit.details["schedule_id"] == "schedule-queued-auth"
    assert audit.details["template_count"] == 2
    assert audit.details["endpoint_count"] == 1
    assert audit.details["planned_tests"] == 2
    assert audit.details["execution_mode"] == "queued"
    assert audit.details["trigger_source"] == "schedule"
    assert audit.details["auth_required"] is True
    assert audit.details["auth_profile_id"] == auth_profile_id
    assert audit.details["auth_profile_present"] is True
    assert audit.details["auth_mode"] == "bearer"
    assert "scheduler-secret-token" not in str(audit.details)


@pytest.mark.asyncio
async def test_scheduler_trigger_uses_bound_pentest_profile(test_engine, monkeypatch):
    from sentinel_core.config import settings as app_settings
    from sentinel_core.models.core import APIEndpoint, AuditLog, AuthProfile, PentestProfile, TestRun, TestSchedule
    from sentinel_scheduler.modules.scheduler.test_scheduler import TestScheduler

    account_id = 1000202
    session_factory = async_sessionmaker(bind=test_engine, expire_on_commit=False)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.AsyncSessionLocal", session_factory)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_SCAN_EXECUTION_MODE", "queued")
    monkeypatch.setattr(
        "sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_REQUIRE_AUTH_PROFILE_FOR_ACTIVE_SCANS",
        True,
    )

    async with session_factory() as db:
        endpoint = APIEndpoint(
            id="endpoint-bound-profile",
            account_id=account_id,
            method="GET",
            protocol="https",
            host="api.example.com",
            path="/bound-profile",
        )
        default_auth = AuthProfile(
            account_id=account_id,
            name="default schedule bearer",
            auth_mode="bearer",
            token="Bearer default-scheduler-token",
            scope_domains=["api.example.com"],
            is_active=True,
        )
        bound_auth = AuthProfile(
            account_id=account_id,
            name="bound schedule bearer",
            auth_mode="header",
            header_value="X-Api-Key bound-scheduler-token",
            scope_domains=["api.example.com"],
            is_active=True,
        )
        db.add_all([endpoint, default_auth, bound_auth])
        await db.flush()
        default_profile = PentestProfile(
            account_id=account_id,
            name=app_settings.PENTEST_DEFAULT_PROFILE_NAME,
            auth_profile_id=default_auth.id,
        )
        bound_profile = PentestProfile(
            account_id=account_id,
            name="Bound schedule profile",
            auth_profile_id=bound_auth.id,
        )
        db.add_all([default_profile, bound_profile])
        await db.flush()
        db.add(
            TestSchedule(
                id="schedule-bound-profile",
                account_id=account_id,
                name="Bound profile schedule",
                cron_expression="0 2 * * *",
                template_ids=["tpl-bound"],
                endpoint_ids=["endpoint-bound-profile"],
                pentest_profile_id=bound_profile.id,
            )
        )
        await db.commit()
        bound_profile_id = bound_profile.id
        bound_auth_id = bound_auth.id

    scheduler = TestScheduler()
    trigger_result = await scheduler._trigger_run(
        "schedule-bound-profile",
        ["tpl-bound"],
        ["endpoint-bound-profile"],
        account_id,
    )

    assert trigger_result["status"] == "queued"
    assert trigger_result["source_schedule_id"] == "schedule-bound-profile"
    async with session_factory() as db:
        run = (
            await db.execute(select(TestRun).where(TestRun.id == trigger_result["run_id"]))
        ).scalar_one()
        audit = (
            await db.execute(
                select(AuditLog).where(
                    AuditLog.resource_id == trigger_result["run_id"],
                    AuditLog.action == "SCAN_RUN_QUEUED",
                )
            )
        ).scalar_one()

    assert run.source_schedule_id == "schedule-bound-profile"
    assert run.pentest_profile_id == bound_profile_id
    assert audit.details["pentest_profile_id"] == bound_profile_id
    assert audit.details["auth_profile_id"] == bound_auth_id
    assert audit.details["auth_mode"] == "header"
    assert "default-scheduler-token" not in str(audit.details)
    assert "bound-scheduler-token" not in str(audit.details)


@pytest.mark.asyncio
async def test_scheduler_trigger_queues_bound_profile_for_worker(test_engine, monkeypatch):
    from sentinel_core.config import settings as app_settings
    from sentinel_core.models.core import APIEndpoint, AuthProfile, PentestProfile, TestRun, TestSchedule
    from sentinel_scheduler.modules.scheduler.test_scheduler import TestScheduler

    account_id = 1000203
    session_factory = async_sessionmaker(bind=test_engine, expire_on_commit=False)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.AsyncSessionLocal", session_factory)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_SCAN_EXECUTION_MODE", "background")
    monkeypatch.setattr(
        "sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_REQUIRE_AUTH_PROFILE_FOR_ACTIVE_SCANS",
        True,
    )

    async with session_factory() as db:
        endpoint = APIEndpoint(
            id="endpoint-background-bound-profile",
            account_id=account_id,
            method="GET",
            protocol="https",
            host="api.example.com",
            path="/background-bound-profile",
        )
        auth_profile = AuthProfile(
            account_id=account_id,
            name="background bound bearer",
            auth_mode="bearer",
            token="Bearer background-bound-token",
            scope_domains=["api.example.com"],
            is_active=True,
        )
        db.add_all([endpoint, auth_profile])
        await db.flush()
        bound_profile = PentestProfile(
            account_id=account_id,
            name="Background bound schedule profile",
            auth_profile_id=auth_profile.id,
        )
        db.add_all([
            PentestProfile(
                account_id=account_id,
                name=app_settings.PENTEST_DEFAULT_PROFILE_NAME,
                auth_profile_id=auth_profile.id,
            ),
            bound_profile,
        ])
        await db.flush()
        db.add(
            TestSchedule(
                id="schedule-background-bound-profile",
                account_id=account_id,
                name="Background bound profile schedule",
                cron_expression="0 2 * * *",
                template_ids=["tpl-background-bound"],
                endpoint_ids=["endpoint-background-bound-profile"],
                pentest_profile_id=bound_profile.id,
            )
        )
        await db.commit()
        bound_profile_id = bound_profile.id

    scheduler = TestScheduler()
    trigger_result = await scheduler._trigger_run(
        "schedule-background-bound-profile",
        ["tpl-background-bound"],
        ["endpoint-background-bound-profile"],
        account_id,
    )

    assert trigger_result["status"] == "queued"
    async with session_factory() as db:
        run = (await db.execute(select(TestRun).where(TestRun.id == trigger_result["run_id"]))).scalar_one()
    assert run.status == "PENDING"
    assert run.pentest_profile_id == bound_profile_id


@pytest.mark.asyncio
async def test_scheduler_trigger_blocks_when_kill_switch_enabled(test_engine, monkeypatch):
    from sentinel_core.models.core import TestRun
    from sentinel_scheduler.modules.scheduler.test_scheduler import TestScheduler

    session_factory = async_sessionmaker(bind=test_engine, expire_on_commit=False)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.AsyncSessionLocal", session_factory)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_KILL_SWITCH_ENABLED", True)

    scheduler = TestScheduler()
    result = await scheduler._trigger_run("schedule-kill", ["tpl-kill"], ["endpoint-kill"], 1000700)

    assert result == {
        "status": "blocked",
        "reason": "pentest_kill_switch_enabled",
        "schedule_id": "schedule-kill",
    }

    async with session_factory() as db:
        runs = (
            await db.execute(select(TestRun).where(TestRun.account_id == 1000700))
        ).scalars().all()

    assert runs == []


@pytest.mark.asyncio
async def test_scheduler_trigger_blocks_when_endpoint_target_guard_fails(test_engine, monkeypatch):
    from sentinel_core.models.core import APIEndpoint, TestRun
    from sentinel_scheduler.modules.scheduler.test_scheduler import TestScheduler

    session_factory = async_sessionmaker(bind=test_engine, expire_on_commit=False)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.AsyncSessionLocal", session_factory)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_SCAN_EXECUTION_MODE", "queued")

    async with session_factory() as db:
        db.add(
            APIEndpoint(
                id="endpoint-private",
                account_id=1000800,
                method="GET",
                protocol="https",
                host="api.example.com",
                path="/private",
            )
        )
        await db.commit()

    monkeypatch.setattr(
        "sentinel_scheduler.modules.scheduler.test_scheduler.blocked_endpoint_targets",
        lambda _endpoints: [
            {
                "endpoint_id": "endpoint-private",
                "url": "https://api.example.com/private",
                "reason": "target host resolved to private address",
            }
        ],
    )

    scheduler = TestScheduler()
    result = await scheduler._trigger_run(
        "schedule-target-guard",
        ["tpl-private"],
        ["endpoint-private"],
        1000800,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "target_guard_blocked"
    blocked_endpoint = result["blocked_endpoints"][0]
    assert blocked_endpoint["endpoint_id"] == "endpoint-private"
    assert blocked_endpoint["target_guard_policy"]["policy"] == "target_guard"
    assert blocked_endpoint["target_guard_policy"]["blocked"] is True
    assert blocked_endpoint["target_guard_policy"]["url"] == "https://api.example.com/private"
    assert "private" in blocked_endpoint["target_guard_policy"]["reason"]

    async with session_factory() as db:
        runs = (
            await db.execute(select(TestRun).where(TestRun.account_id == 1000800))
        ).scalars().all()

    assert runs == []


@pytest.mark.asyncio
async def test_scheduler_trigger_blocks_when_auth_profile_required(test_engine, monkeypatch):
    from sentinel_core.models.core import APIEndpoint, TestRun
    from sentinel_scheduler.modules.scheduler.test_scheduler import TestScheduler

    account_id = 1000810
    session_factory = async_sessionmaker(bind=test_engine, expire_on_commit=False)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.AsyncSessionLocal", session_factory)
    monkeypatch.setattr(
        "sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_REQUIRE_AUTH_PROFILE_FOR_ACTIVE_SCANS",
        True,
    )

    async with session_factory() as db:
        db.add(
            APIEndpoint(
                id="endpoint-auth-required",
                account_id=account_id,
                method="GET",
                protocol="https",
                host="api.example.com",
                path="/auth-required",
            )
        )
        await db.commit()

    scheduler = TestScheduler()
    result = await scheduler._trigger_run(
        "schedule-auth-required",
        ["tpl-auth"],
        ["endpoint-auth-required"],
        account_id,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "auth_profile_required"

    async with session_factory() as db:
        runs = (
            await db.execute(select(TestRun).where(TestRun.account_id == account_id))
        ).scalars().all()

    assert runs == []


@pytest.mark.asyncio
async def test_scheduler_trigger_blocks_when_auth_profile_scope_rejects_endpoint(test_engine, monkeypatch):
    from sentinel_core.config import settings as app_settings
    from sentinel_core.models.core import APIEndpoint, AuthProfile, PentestProfile, TestRun
    from sentinel_scheduler.modules.scheduler.test_scheduler import TestScheduler

    account_id = 1000820
    session_factory = async_sessionmaker(bind=test_engine, expire_on_commit=False)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.AsyncSessionLocal", session_factory)
    monkeypatch.setattr("sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_SCAN_EXECUTION_MODE", "queued")
    monkeypatch.setattr(
        "sentinel_scheduler.modules.scheduler.test_scheduler.settings.PENTEST_REQUIRE_AUTH_PROFILE_FOR_ACTIVE_SCANS",
        True,
    )

    async with session_factory() as db:
        endpoint = APIEndpoint(
            id="endpoint-auth-scope",
            account_id=account_id,
            method="GET",
            protocol="https",
            host="api.example.com",
            path="/auth-scope",
        )
        auth_profile = AuthProfile(
            account_id=account_id,
            name="scheduler bearer",
            auth_mode="bearer",
            token="Bearer runtime-token",
            scope_domains=["other.example.com"],
            is_active=True,
        )
        db.add_all([endpoint, auth_profile])
        await db.flush()
        db.add(
            PentestProfile(
                account_id=account_id,
                name=app_settings.PENTEST_DEFAULT_PROFILE_NAME,
                auth_profile_id=auth_profile.id,
            )
        )
        await db.commit()

    scheduler = TestScheduler()
    result = await scheduler._trigger_run(
        "schedule-auth-scope",
        ["tpl-auth"],
        ["endpoint-auth-scope"],
        account_id,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "auth_profile_scope_blocked"
    blocked_endpoint = result["blocked_endpoints"][0]
    assert blocked_endpoint["endpoint_id"] == "endpoint-auth-scope"
    policy = blocked_endpoint["auth_profile_scope_policy"]
    assert policy["policy"] == "auth_profile_scope_guard"
    assert policy["blocked"] is True
    assert policy["url"] == "https://api.example.com/auth-scope"
    assert policy["base_url"] == "https://api.example.com/auth-scope"
    assert policy["scope_domains_configured"] is True
    assert policy["scope_domain_count"] == 1
    assert "runtime-token" not in str(policy)

    async with session_factory() as db:
        runs = (
            await db.execute(select(TestRun).where(TestRun.account_id == account_id))
        ).scalars().all()

    assert runs == []
