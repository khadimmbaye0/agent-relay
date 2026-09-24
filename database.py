"""PostgreSQL setup and durable Agent Relay models.

This module is intentionally the only place that knows about the connection
URL, the engine, and the transactional primitives.  The rest of the application
talks to the models through :mod:`storage`.

Concurrency: SQLite's starter version serialized every writer behind a global
``BEGIN IMMEDIATE`` reservation because SQLite has no row locking.  PostgreSQL
does, so claims and recovery use ``SELECT ... FOR UPDATE SKIP LOCKED`` instead:
several API processes and workers can claim concurrently, each gets a different
task, and none of them blocks on the others.
"""

from __future__ import annotations

import logging
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, create_engine, select, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker


LOGGER = logging.getLogger("agent_relay.database")

# Matches the host port published by the bundled compose.yaml, so `uv run
# uvicorn main:app` on a developer machine talks to `docker compose up postgres`
# without extra configuration.  Override with RELAY_DATABASE_URL.
DEFAULT_DATABASE_URL = "postgresql+psycopg://relay:relay@127.0.0.1:55432/relay"
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_.\-]+$")


def _database_url() -> str:
    return os.getenv("RELAY_DATABASE_URL") or os.getenv("DATABASE_URL") or DEFAULT_DATABASE_URL


def positive_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        return default
    return value if value > 0 else default


DATABASE_URL = _database_url()
LEASE_SECONDS = positive_int("RELAY_LEASE_SECONDS", 60)
MAX_ATTEMPTS = positive_int("RELAY_MAX_ATTEMPTS", 5)
RECOVERY_INTERVAL_SECONDS = max(1, positive_int("RELAY_RECOVERY_INTERVAL_SECONDS", 5))
MAX_BODY_BYTES = positive_int("RELAY_MAX_BODY_BYTES", 256 * 1024)
STARTUP_RETRY_SECONDS = positive_int("RELAY_STARTUP_RETRY_SECONDS", 30)
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 100

# Errors that are worth retrying: the connection may simply not be ready yet, or
# two transactions collided in a way PostgreSQL expects the client to retry.
TRANSIENT_ERROR_MARKERS = (
    "deadlock",
    "could not serialize",
    "connection",
    "server closed the connection",
    "shutting down",
    "too many clients",
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def as_db_time(value: datetime) -> datetime:
    """Columns store naive UTC so timestamps stay directly comparable in SQL."""

    return value.astimezone(timezone.utc).replace(tzinfo=None)


def db_time(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def iso_time(value: datetime | None) -> str | None:
    value = db_time(value)
    if value is None:
        return None
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


class Base(DeclarativeBase):
    pass


class Agent(Base):
    __tablename__ = "agents"

    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    sent_tasks: Mapped[list[Task]] = relationship(
        "Task", foreign_keys="Task.sender_id", back_populates="sender", passive_deletes=True
    )
    received_tasks: Mapped[list[Task]] = relationship(
        "Task", foreign_keys="Task.recipient_id", back_populates="recipient", passive_deletes=True
    )


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (UniqueConstraint("sender_id", "idempotency_key", name="uq_task_sender_idempotency"),)

    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    sender_id: Mapped[str] = mapped_column(String(100), ForeignKey("agents.id"), nullable=False, index=True)
    recipient_id: Mapped[str] = mapped_column(String(100), ForeignKey("agents.id"), nullable=False, index=True)
    input: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    output: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    sender: Mapped[Agent] = relationship("Agent", foreign_keys=[sender_id], back_populates="sent_tasks")
    recipient: Mapped[Agent] = relationship("Agent", foreign_keys=[recipient_id], back_populates="received_tasks")
    attempts: Mapped[list[Attempt]] = relationship(
        "Attempt", back_populates="task", cascade="all, delete-orphan", order_by="Attempt.attempt_number"
    )


class Attempt(Base):
    __tablename__ = "attempts"
    __table_args__ = (UniqueConstraint("task_id", "attempt_number", name="uq_attempt_task_number"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(
        String(100), ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False, index=True
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    claim_token_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    claimed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    lease_expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    outcome: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    terminal_action: Mapped[str | None] = mapped_column(String(10), nullable=True)
    terminal_payload_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)

    task: Mapped[Task] = relationship("Task", back_populates="attempts")


engine: Engine = create_engine(
    DATABASE_URL,
    # Verification catches connections dropped by a database restart or an idle
    # timeout; the pool is generous because claims are short-lived transactions
    # and several workers poll at once.
    pool_pre_ping=True,
    pool_size=positive_int("RELAY_POOL_SIZE", 10),
    max_overflow=positive_int("RELAY_POOL_OVERFLOW", 20),
)


SessionLocal = sessionmaker(bind=engine, class_=Session, expire_on_commit=False, autoflush=True)


def is_transient_error(exc: Exception) -> bool:
    """True when retrying may succeed: restarting server, deadlock, and so on."""

    message = str(getattr(exc, "orig", exc)).lower()
    return any(marker in message for marker in TRANSIENT_ERROR_MARKERS)


def ensure_database(url: str = DATABASE_URL) -> None:
    """Create the target PostgreSQL database when it does not exist yet.

    The relay ships no migration tool, so pointing it at a fresh server should
    not require a manual ``createdb``.  This connects to the server's
    maintenance database and creates the target only when it is missing.
    """

    parsed = make_url(url)
    if parsed.get_backend_name() != "postgresql" or not parsed.database:
        return
    if not SAFE_IDENTIFIER.match(parsed.database):
        raise ValueError(f"refusing to create unsafely named database {parsed.database!r}")
    admin = create_engine(parsed.set(database="postgres"), isolation_level="AUTOCOMMIT", pool_pre_ping=True)
    try:
        with admin.connect() as connection:
            present = connection.execute(
                text("select 1 from pg_database where datname = :name"), {"name": parsed.database}
            ).scalar()
            if not present:
                # CREATE DATABASE takes no bind parameter; the identifier is
                # validated by SAFE_IDENTIFIER before it reaches this string.
                connection.exec_driver_sql(f'CREATE DATABASE "{parsed.database}"')
                LOGGER.info("created database %s", parsed.database)
    finally:
        admin.dispose()


def init_db() -> None:
    """Create the database and schema, retrying while the server comes up."""

    deadline = time.monotonic() + STARTUP_RETRY_SECONDS
    while True:
        try:
            ensure_database()
            Base.metadata.create_all(engine)
            return
        except OperationalError as exc:
            if not is_transient_error(exc) or time.monotonic() >= deadline:
                raise
            LOGGER.warning("database not ready yet (%s); retrying", exc.orig)
            time.sleep(1)


@contextmanager
def db_session() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def recover_expired_in_session(db: Session, now: datetime) -> int:
    """Expire active leases and requeue/fail their tasks within ``db``."""

    now_db = as_db_time(now)
    expired = list(
        db.scalars(
            select(Attempt)
            .where(Attempt.outcome == "processing", Attempt.lease_expires_at <= now_db)
            .order_by(Attempt.lease_expires_at, Attempt.id)
            # A concurrent recovery pass skips an attempt another process
            # already locked instead of waiting for it; that process requeues
            # the task itself.
            .with_for_update(skip_locked=True)
        )
    )
    count = 0
    for attempt in expired:
        task = db.get(Task, attempt.task_id)
        if task is None or attempt.outcome != "processing":
            continue
        attempt.outcome = "expired"
        attempt.finished_at = now_db
        if task.status == "processing":
            if task.attempt_count >= MAX_ATTEMPTS:
                task.status = "failed"
                task.error = "attempts_exhausted"
                task.output = None
                task.finished_at = now_db
            else:
                task.status = "queued"
                task.finished_at = None
        count += 1
    return count


def recover_expired() -> int:
    """Run one recovery pass and return the number of expired attempts."""

    with db_session() as db:
        return recover_expired_in_session(db, utcnow())


__all__ = [
    "Agent",
    "Attempt",
    "Base",
    "DATABASE_URL",
    "DEFAULT_DATABASE_URL",
    "DEFAULT_PAGE_SIZE",
    "LEASE_SECONDS",
    "MAX_ATTEMPTS",
    "MAX_BODY_BYTES",
    "MAX_PAGE_SIZE",
    "RECOVERY_INTERVAL_SECONDS",
    "Task",
    "as_db_time",
    "db_session",
    "db_time",
    "engine",
    "ensure_database",
    "init_db",
    "is_transient_error",
    "iso_time",
    "recover_expired",
    "recover_expired_in_session",
    "utcnow",
]
