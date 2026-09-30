"""Periodic recon loop run by api-sentinel-scheduler."""
from __future__ import annotations

import asyncio
import datetime

import structlog
from sqlalchemy import select

from sentinel_core.config import settings
from sentinel_core.models.core import ReconSourceConfig
from sentinel_core.modules.persistence.database import AsyncSessionLocal, apply_tenant_context
from sentinel_core.modules.recon.source_runner import ReconSourceRunner
from sentinel_core.modules.tenancy.context import set_current_account_id

logger = structlog.get_logger(__name__)


class ReconScheduler:
    def __init__(self, interval_sec: int = 300) -> None:
        self.interval = interval_sec
        self._running = False
        self._task: asyncio.Task | None = None
        self._runner = ReconSourceRunner()

    async def start(self) -> None:
        if self._running or not settings.RECON_SCHEDULER_ENABLED:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._run_due_sources()
            except Exception as exc:
                logger.error("recon_scheduler_error", error=str(exc))
            await asyncio.sleep(self.interval)

    async def _run_due_sources(self) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(ReconSourceConfig).where(
                    ReconSourceConfig.enabled == True,
                    (ReconSourceConfig.next_run_at.is_(None))
                    | (ReconSourceConfig.next_run_at <= now),
                )
            )
            sources = result.scalars().all()
            for source in sources:
                set_current_account_id(source.account_id)
                await apply_tenant_context(db)
                await self._runner.run_source(db, source)
            await db.commit()
