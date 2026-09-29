# api-sentinel-scheduler

Dedicated scheduling process: APScheduler, persisted-schedule sync, and the
periodic recon / continuous-testing / OpenAPI-drift processors.

## Status: vendored build, decoupling pending

The scheduler currently shares models and configuration with the API runtime
(vendored under `server/`). It starts and syncs today, but becoming an
independent microservice requires extracting the shared-contracts package
first (tracked as the next stage).

## Run

```bash
docker build -t api-sentinel/scheduler:local .
docker run --rm api-sentinel/scheduler:local
```

Entry point: `python -m server.services.scheduler_service` (needs Postgres,
Redis, and the standard API environment variables).
