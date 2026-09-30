"""
APScheduler runtime for api-sentinel-scheduler.

Registers cron jobs for enabled rows in ``test_schedules`` (see ``schedule_store``)
and enqueues a scan run for the scan-worker each time one fires.
"""
import asyncio
import uuid

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    APScheduler_AVAILABLE = True
except ImportError:
    APScheduler_AVAILABLE = False

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sentinel_core.config import settings
from sentinel_core.models.core import APIEndpoint, TestRun, TestSchedule
from sentinel_core.modules.auth.audit import log_action
from sentinel_core.modules.persistence.database import AsyncSessionLocal
from sentinel_core.modules.pentest.auth_preflight import (
    ActiveScanAuthError,
    active_scan_auth_audit_context,
    load_profile_and_auth_for_active_scan,
)
from sentinel_core.modules.pentest.auth_scope import blocked_auth_profile_targets
from sentinel_core.modules.test_executor.kill_switch import KILL_SWITCH_REASON, kill_switch_enabled
from sentinel_core.modules.test_executor.target_guard import blocked_endpoint_targets
from sentinel_core.modules.scheduler.schedule_store import (
    _blocked_targets_with_policy,
    create_schedule,
    enabled_schedules,
    validate_cron_expression,
    validate_schedule_plan,
)


class TestScheduler:
    """
    Manages cron-based scheduling of security test runs.
    Falls back gracefully if APScheduler is not installed.
    """

    def __init__(self):
        self._scheduler = None
        if APScheduler_AVAILABLE:
            self._scheduler = AsyncIOScheduler()

    def start(self):
        if self._scheduler:
            self._scheduler.start()
            print("[Scheduler] APScheduler started.")

    def stop(self):
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown()

    async def sync_persisted_schedules(self, db: AsyncSession | None = None) -> dict[str, int]:
        """Reconcile APScheduler jobs with the durable schedule table.

        The API may create or toggle schedules while the scheduler runs in a
        separate process.  Re-reading the tenant-scoped rows makes scheduling
        durable across restarts and keeps the API stateless; ``replace_existing``
        makes this safe to call periodically.
        """
        if not self._scheduler:
            return {"enabled": 0, "registered": 0, "removed": 0}

        owns_session = db is None
        session = db or AsyncSessionLocal()
        try:
            result = await session.execute(
                select(TestSchedule).where(TestSchedule.enabled == True)
            )
            enabled = result.scalars().all()
            enabled_ids = {str(row.id) for row in enabled}
            registered = 0
            for row in enabled:
                job = self._scheduler.get_job(str(row.id))
                expected_args = [
                    str(row.id), list(row.template_ids or []), list(row.endpoint_ids or []),
                    int(row.account_id), row.pentest_profile_id,
                ]
                expected_trigger = CronTrigger.from_crontab(str(row.cron_expression))
                if job is not None and list(job.args) == expected_args and str(job.trigger) == str(expected_trigger):
                    continue
                self._register_job(
                    str(row.id),
                    str(row.cron_expression),
                    list(row.template_ids or []),
                    list(row.endpoint_ids or []),
                    int(row.account_id),
                    row.pentest_profile_id,
                )
                registered += 1

            removed = 0
            for job in list(self._scheduler.get_jobs()):
                if str(job.id) not in enabled_ids:
                    self._scheduler.remove_job(job.id)
                    removed += 1
            return {"enabled": len(enabled), "registered": registered, "removed": removed}
        finally:
            if owns_session:
                await session.close()

    async def schedule(
        self,
        name: str,
        cron_expression: str,
        template_ids: list,
        endpoint_ids: list,
        account_id: int,
        db: AsyncSession,
        pentest_profile_id: str | None = None,
    ) -> str:
        """Persist a schedule and register it with this process's APScheduler."""
        schedule_id = await create_schedule(
            db,
            name=name,
            cron_expression=cron_expression,
            template_ids=template_ids,
            endpoint_ids=endpoint_ids,
            account_id=account_id,
            pentest_profile_id=pentest_profile_id,
        )
        self._register_job(
            schedule_id,
            cron_expression,
            template_ids,
            endpoint_ids,
            account_id,
            pentest_profile_id,
        )
        return schedule_id

    def _validate_cron_expression(self, cron_expression: str) -> None:
        validate_cron_expression(cron_expression)

    async def _validate_schedule_plan(self, db: AsyncSession, **kwargs) -> None:
        await validate_schedule_plan(db, **kwargs)

    def _register_job(
        self,
        schedule_id: str,
        cron_expr: str,
        template_ids: list,
        endpoint_ids: list,
        account_id: int,
        pentest_profile_id: str | None = None,
    ):
        if not self._scheduler:
            print(f"[Scheduler] APScheduler not available — schedule {schedule_id} not registered.")
            return
        parts = cron_expr.split()
        if len(parts) == 5:
            minute, hour, day, month, day_of_week = parts
        else:
            minute, hour, day, month, day_of_week = "0", "0", "*", "*", "*"

        trigger = CronTrigger(
            minute=minute, hour=hour, day=day,
            month=month, day_of_week=day_of_week,
        )
        self._scheduler.add_job(
            self._trigger_run,
            trigger=trigger,
            id=schedule_id,
            args=[schedule_id, template_ids, endpoint_ids, account_id, pentest_profile_id],
            replace_existing=True,
        )

    async def _trigger_run(
        self,
        schedule_id: str,
        template_ids: list,
        endpoint_ids: list,
        account_id: int,
        pentest_profile_id: str | None = None,
        trigger_source: str = "schedule",
    ):
        """Called by APScheduler: validate the schedule and enqueue a run for the scan-worker."""
        if kill_switch_enabled():
            return {
                "status": "blocked",
                "reason": KILL_SWITCH_REASON,
                "schedule_id": schedule_id,
            }

        run_id = str(uuid.uuid4())
        # Scheduled runs are always queued; api-sentinel-scan-worker executes them.
        execution_mode = "queued"
        async with AsyncSessionLocal() as db:
            endpoint_result = await db.execute(
                select(APIEndpoint).where(
                    APIEndpoint.id.in_(endpoint_ids),
                    APIEndpoint.account_id == account_id,
                )
            )
            endpoints = endpoint_result.scalars().all()
            if len(endpoints) < len(endpoint_ids):
                return {
                    "status": "blocked",
                    "reason": "endpoint_scope_invalid",
                    "schedule_id": schedule_id,
                }
            blocked_targets = _blocked_targets_with_policy(blocked_endpoint_targets(endpoints))
            if blocked_targets:
                return {
                    "status": "blocked",
                    "reason": "target_guard_blocked",
                    "schedule_id": schedule_id,
                    "blocked_endpoints": blocked_targets,
                }
            if pentest_profile_id is None:
                schedule = await db.get(TestSchedule, schedule_id)
                if schedule is not None and schedule.account_id == account_id:
                    pentest_profile_id = schedule.pentest_profile_id
            try:
                pentest_profile, auth_profile = await load_profile_and_auth_for_active_scan(
                    db,
                    account_id=account_id,
                    pentest_profile_id=pentest_profile_id,
                )
            except ActiveScanAuthError as exc:
                return {
                    "status": "blocked",
                    "reason": exc.reason,
                    "schedule_id": schedule_id,
                    "detail": exc.detail,
                }
            blocked_auth_targets = blocked_auth_profile_targets(auth_profile, endpoints)
            if blocked_auth_targets:
                return {
                    "status": "blocked",
                    "reason": "auth_profile_scope_blocked",
                    "schedule_id": schedule_id,
                    "blocked_endpoints": blocked_auth_targets,
                }

            is_schedule = trigger_source == "schedule"
            db.add(
                TestRun(
                    id=run_id,
                    account_id=account_id,
                    status="PENDING",
                    template_ids=template_ids,
                    endpoint_ids=endpoint_ids,
                    pentest_profile_id=pentest_profile.id if pentest_profile is not None else None,
                    trigger_source=trigger_source,
                    source_schedule_id=schedule_id if is_schedule else None,
                )
            )
            await log_action(
                db=db,
                account_id=account_id,
                action="SCAN_RUN_QUEUED",
                resource_type="test_run",
                resource_id=run_id,
                details={
                    "source": trigger_source,
                    "schedule_id": schedule_id if is_schedule else None,
                    "source_schedule_id": schedule_id if is_schedule else None,
                    "template_count": len(template_ids or []),
                    "endpoint_count": len(endpoint_ids or []),
                    "planned_tests": len(template_ids or []) * len(endpoint_ids or []),
                    "pentest_profile_id": pentest_profile.id if pentest_profile is not None else None,
                    **active_scan_auth_audit_context(pentest_profile, auth_profile),
                    "execution_mode": execution_mode,
                    "trigger_source": trigger_source,
                },
            )
            await db.commit()

        return {"status": "queued", "run_id": run_id, "source_schedule_id": schedule_id}

    async def trigger_continuous_discovery_scan(
        self,
        account_id: int,
        *,
        max_endpoints: int | None = None,
        pentest_profile_id: str | None = None,
    ) -> dict:
        """Auto-scan newly-discovered (never-tested) endpoints.

        Closes the Discovery -> Testing pipeline gap. Finds endpoints with no
        ``last_tested`` stamp and runs a scan over them, reusing the full safety
        path (target guard, auth scope, kill switch) via the standard run flow.
        Gated by ``CONTINUOUS_TESTING_ENABLED`` at the call site.
        """
        if kill_switch_enabled():
            return {"status": "blocked", "reason": KILL_SWITCH_REASON}

        limit = max_endpoints or settings.CONTINUOUS_TESTING_MAX_ENDPOINTS_PER_SWEEP
        profile_id = pentest_profile_id or (settings.CONTINUOUS_TESTING_PROFILE_ID or None)

        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(APIEndpoint)
                .where(
                    APIEndpoint.account_id == account_id,
                    APIEndpoint.last_tested.is_(None),
                )
                .order_by(APIEndpoint.last_seen.desc())
                .limit(limit)
            )
            endpoints = result.scalars().all()

        if not endpoints:
            return {"status": "noop", "reason": "no_untested_endpoints", "endpoint_count": 0}

        endpoint_ids = [ep.id for ep in endpoints]
        template_ids = self._all_active_template_ids()
        if not template_ids:
            return {"status": "noop", "reason": "no_templates", "endpoint_count": len(endpoint_ids)}

        return await self._trigger_run(
            schedule_id=f"continuous-discovery-{account_id}",
            template_ids=template_ids,
            endpoint_ids=endpoint_ids,
            account_id=account_id,
            pentest_profile_id=profile_id,
            trigger_source="continuous_discovery",
        )

    @staticmethod
    def _all_active_template_ids() -> list:
        from sentinel_core.modules.test_executor.wordlist_manager import WordlistManager

        templates = WordlistManager.get_instance().templates
        return [
            str(t.get("id"))
            for t in templates
            if isinstance(t, dict) and t.get("id")
        ]

    async def cancel(self, schedule_id: str, db: AsyncSession) -> None:
        if self._scheduler:
            try:
                self._scheduler.remove_job(schedule_id)
            except Exception:
                pass
        record = await db.get(TestSchedule, schedule_id)
        if record:
            record.enabled = False
            await db.commit()

    async def list_schedules(self, db: AsyncSession) -> list:
        return await enabled_schedules(db)


# Singleton
_scheduler_instance: TestScheduler = None


def get_scheduler() -> TestScheduler:
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = TestScheduler()
    return _scheduler_instance
