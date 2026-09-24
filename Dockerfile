# syntax=docker/dockerfile:1
#
# Agent Relay container image.
#
# Stage 1 builds the virtualenv from the committed uv.lock so the image is
# reproducible without a build toolchain. Stage 2 is a slim runtime that carries
# only CPython and the venv.
#
# The relay stores everything in PostgreSQL, so this image needs a database
# server: compose.yaml provides one and passes RELAY_DATABASE_URL. The process
# runs unprivileged and writes nothing to disk.

FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

# Copy rather than hardlink across the cache mount, and precompile bytecode so
# the non-root runtime never writes into the image.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependencies first, so this layer is reused until the lockfile changes.
# The dev group is installed on purpose: worker.py (the documented client loop)
# imports httpx, which pyproject.toml currently declares under [dependency-groups].
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

COPY . .

# Installs the project itself; --frozen keeps it locked to uv.lock.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen


FROM python:3.11-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

RUN useradd --create-home --uid 10001 relay

WORKDIR /app
COPY --from=builder --chown=relay:relay /app /app

# The relay never needs to write to its own code, so it runs unprivileged.
USER relay

EXPOSE 8000

# /ready verifies database connectivity and schema, so a wiped or unwritable
# volume reports unhealthy instead of silently accepting tasks.
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/ready', timeout=2)"]

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
