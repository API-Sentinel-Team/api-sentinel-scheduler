# api-sentinel-scheduler

Runs the cron scheduler for saved scan schedules plus the recon, continuous-testing and OpenAPI-drift loops. It only **enqueues** runs; the scan-worker executes them.

Part of API Sentinel. This repo contains **only this service's code**; everything shared
(database models, migrations, config, tenancy, audit, redaction, pentest policy, scan planning,
the security-test template library) lives in
[`api-sentinel-core`](https://github.com/API-Sentinel-Team/api-sentinel-core), installed as the
`sentinel-core` dependency and pinned to a released tag in `pyproject.toml`.

## Boundaries

- Never import another service's package. Services cooperate only through the database run
  queue and Redis pub/sub. `tests/unit/test_service_boundaries.py` enforces this in the
  api repo; the same rule holds here.
- Schema changes are made in `api-sentinel-core` (the single owner of migrations), never here.

## Run

```bash
python -m sentinel_scheduler.services.scheduler_service
```

## Develop

```bash
pip install -e ../api-sentinel-core           # or the pinned tag from pyproject.toml
pip install --no-deps -e ".[test]"
DEBUG=true pytest -q
```

`DEBUG=true` is required by tests: without it `sentinel_core.config` refuses to build settings
(production validation).
