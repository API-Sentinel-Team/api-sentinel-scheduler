# syntax=docker/dockerfile:1.7
# Build (sentinel-core is private; pass a token as a BuildKit secret):
#   DOCKER_BUILDKIT=1 docker build --secret id=gh_token,env=GH_TOKEN -t api-sentinel-scheduler .
ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim-bookworm AS builder
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1
RUN apt-get update && apt-get install -y --no-install-recommends build-essential gcc git libpq-dev \
    && rm -rf /var/lib/apt/lists/*
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
WORKDIR /build
# sentinel-core is a private repo: the token comes from a BuildKit secret and never lands in a layer.
RUN --mount=type=secret,id=gh_token \
    git config --global url."https://x-access-token:$(cat /run/secrets/gh_token)@github.com/".insteadOf "https://github.com/" \
    && pip install "sentinel-core @ git+https://github.com/API-Sentinel-Team/api-sentinel-core.git@v0.3.0" \
    ; rc=$?; git config --global --unset-all url."https://x-access-token:$(cat /run/secrets/gh_token)@github.com/".insteadof || true; exit $rc

COPY pyproject.toml ./
COPY sentinel_scheduler/ ./sentinel_scheduler/
# Service-only runtime dependency (deliberately not part of sentinel-core):
RUN pip install "APScheduler>=3.10,<4.0"
RUN pip install --no-deps .
# Fail the build, not the deployment, if a required import is missing.
RUN python -c "from sentinel_scheduler.modules.scheduler.test_scheduler import APScheduler_AVAILABLE; assert APScheduler_AVAILABLE, 'APScheduler missing: schedules would never fire'; import sentinel_scheduler.services.scheduler_service"

FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PATH="/opt/venv/bin:$PATH"
LABEL org.opencontainers.image.title="api-sentinel-scheduler" \
      org.opencontainers.image.source="https://github.com/API-Sentinel-Team/api-sentinel-scheduler"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libpq5 tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 appsentinel \
    && useradd --system --uid 10001 --gid appsentinel --home-dir /app --shell /usr/sbin/nologin appsentinel
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
RUN mkdir -p /app/data/archives /app/models && chown -R appsentinel:appsentinel /app
USER appsentinel
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "sentinel_scheduler.services.scheduler_service"]
