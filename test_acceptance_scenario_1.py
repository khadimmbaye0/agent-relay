"""SPEC.md acceptance scenario 1 as an API integration test.

> 1. Register two agents. One sends a task; the other claims and completes it;
>    the sender reads the result.

Unlike ``test_agent_relay.py`` (which drives ``main.app`` in-process through
``TestClient``), this test exercises the real thing:

* a real ``uvicorn`` process serving ``main:app`` over TCP,
* a real PostgreSQL database that the test then inspects with SQL (``init_db()``
  creates the scratch database, so only a running server is required),
* the real worker client loop from :mod:`worker` for claim/execute/complete,
* the same authorized endpoints the dashboard calls.

It is self-contained: it boots its own relay against a scratch database and
never touches the relay's own data. To run it against an already-running relay
instead, set ``RELAY_TEST_BASE_URL`` (and optionally ``RELAY_TEST_DB_URL`` to
keep the database assertions):

```bash
uv run pytest test_acceptance_scenario_1.py -q
RELAY_TEST_BASE_URL=http://127.0.0.1:8000 pytest test_acceptance_scenario_1.py -q
```
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import create_engine, text

from worker import run_worker

PROJECT_ROOT = Path(__file__).resolve().parent
STARTUP_TIMEOUT_SECONDS = 30.0
WORKER_TIMEOUT_SECONDS = 60.0
# Matches the host port published by compose.yaml; the database is created on
# demand by init_db(), so `docker compose up -d postgres` is enough.
DEFAULT_TEST_DATABASE_URL = "postgresql+psycopg://relay:relay@127.0.0.1:55432/relay_acceptance"


@dataclass(frozen=True)
class Relay:
    base_url: str
    db_url: str | None

    def rows(self, sql: str, parameters: dict[str, Any] | None = None) -> list[tuple]:
        assert self.db_url is not None, "set RELAY_TEST_DB_URL to assert database rows"
        engine = create_engine(self.db_url, pool_pre_ping=True)
        try:
            with engine.connect() as connection:
                return [tuple(row) for row in connection.execute(text(sql), parameters or {})]
        finally:
            engine.dispose()


@dataclass(frozen=True)
class Exchange:
    """One completed scenario-1 exchange, plus everything needed to assert on it."""

    relay: Relay
    task_id: str
    idempotency_key: str
    payload: str
    sender: dict[str, str]
    recipient: dict[str, str]
    worker_id: str


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_until_ready(base_url: str, process: subprocess.Popen) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    with httpx.Client(base_url=base_url, timeout=2) as client:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                output = process.stdout.read() if process.stdout else ""
                raise AssertionError(f"uvicorn exited during startup:\n{output}")
            try:
                if client.get("/ready").status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
    raise AssertionError(f"the relay at {base_url} never became ready")


@pytest.fixture(scope="module")
def relay() -> Relay:
    """A real relay process on its own port and its own scratch database."""

    external = os.getenv("RELAY_TEST_BASE_URL")
    if external:
        yield Relay(external.rstrip("/"), os.getenv("RELAY_TEST_DB_URL"))
        return

    db_url = os.getenv("RELAY_TEST_DATABASE_URL") or DEFAULT_TEST_DATABASE_URL
    port = free_port()
    env = {**os.environ, "RELAY_DATABASE_URL": db_url}
    process = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "main:app",
            "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
        ],
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_until_ready(f"http://127.0.0.1:{port}", process)
        yield Relay(f"http://127.0.0.1:{port}", db_url)
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def register(client: httpx.Client, name: str, description: str | None = None) -> dict[str, str]:
    response = client.post("/api/v1/agents", json={"name": name, "description": description})
    assert response.status_code == 201, response.text
    data = response.json()
    assert data["token"].startswith("agt_")
    return data


def bearer(agent: dict[str, str]) -> dict[str, str]:
    return {"Authorization": f"Bearer {agent['token']}"}


def is_sha256_hex(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def run_worker_once(relay: Relay, agent: dict[str, str], worker_id: str) -> None:
    """Run the real worker client loop until it completes one task."""

    failures: list[BaseException] = []

    def target() -> None:
        try:
            asyncio.run(
                run_worker(
                    relay.base_url, agent["agent_id"], agent["token"], worker_id,
                    wait_seconds=10, stop_after=1,
                )
            )
        except BaseException as exc:  # surfaced in the main thread below
            failures.append(exc)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout=WORKER_TIMEOUT_SECONDS)
    assert not thread.is_alive(), "the worker did not finish within the timeout"
    assert not failures, f"the worker failed: {failures[0]!r}"


@pytest.fixture(scope="module")
def exchange(relay: Relay) -> Exchange:
    """Register two agents, exchange one task and its result. SPEC.md scenario 1."""

    suffix = uuid.uuid4().hex[:8]
    payload = "review this python function"
    worker_id = f"laptop-{suffix}"
    idempotency_key = f"scenario-1-{suffix}"
    with httpx.Client(base_url=relay.base_url, timeout=30) as client:
        sender = register(client, f"alice-{suffix}", "Sends review requests")
        recipient = register(client, f"uppercase-{suffix}", "Deterministic uppercase worker")

        created = client.post(
            "/api/v1/tasks",
            headers={**bearer(sender), "Idempotency-Key": idempotency_key},
            json={"to": recipient["agent_id"], "input": payload},
        )
        assert created.status_code == 201, created.text
        assert created.json()["status"] == "queued"

    run_worker_once(relay, recipient, worker_id)

    return Exchange(
        relay=relay,
        task_id=created.json()["task_id"],
        idempotency_key=idempotency_key,
        payload=payload,
        sender=sender,
        recipient=recipient,
        worker_id=worker_id,
    )


def test_acceptance_scenario_1_task_and_result_over_http(exchange: Exchange) -> None:
    """The sender submits a task, the recipient works it, the sender reads the result."""

    with httpx.Client(base_url=exchange.relay.base_url, timeout=30) as client:
        # The sender reads the result the recipient produced.
        response = client.get(f"/api/v1/tasks/{exchange.task_id}", headers=bearer(exchange.sender))
        assert response.status_code == 200, response.text
        task = response.json()
        assert task["status"] == "completed"
        assert task["output"] == exchange.payload.upper()
        assert task["error"] is None
        assert task["attempt_count"] == 1
        assert task["from"] == exchange.sender["agent_id"]
        assert task["to"] == exchange.recipient["agent_id"]
        assert task["finished_at"] is not None

        # ...and so does the recipient, because both are participants.
        assert client.get(
            f"/api/v1/tasks/{exchange.task_id}", headers=bearer(exchange.recipient)
        ).status_code == 200

        # Both see the task in the direction they own.
        sent = client.get("/api/v1/tasks?direction=sent&limit=100", headers=bearer(exchange.sender))
        received = client.get(
            "/api/v1/tasks?direction=received&limit=100", headers=bearer(exchange.recipient)
        )
        assert exchange.task_id in {item["task_id"] for item in sent.json()["items"]}
        assert exchange.task_id in {item["task_id"] for item in received.json()["items"]}

        # Delivery history records exactly one completed attempt, without secrets.
        attempts = client.get(
            f"/api/v1/tasks/{exchange.task_id}/attempts", headers=bearer(exchange.sender)
        ).json()["items"]
        assert [item["attempt"] for item in attempts] == [1]
        assert attempts[0]["outcome"] == "completed"
        assert attempts[0]["worker_id"] == exchange.worker_id
        assert attempts[0]["lease_expires_at"] is not None
        assert attempts[0]["finished_at"] is not None
        assert "claim_token" not in attempts[0]

        # The task is terminal: the recipient's inbox has nothing left to claim.
        idle = client.post(
            "/api/v1/tasks/claim",
            headers=bearer(exchange.recipient),
            json={"worker_id": "idle", "wait_seconds": 0},
        )
        assert idle.status_code == 204

        # Credentials are required, and unrelated agents cannot read the task.
        assert client.get(f"/api/v1/tasks/{exchange.task_id}").status_code == 401
        outsider = register(client, f"outsider-{uuid.uuid4().hex[:8]}")
        assert client.get(
            f"/api/v1/tasks/{exchange.task_id}", headers=bearer(outsider)
        ).status_code == 404
        assert client.get(
            f"/api/v1/tasks/{exchange.task_id}/attempts", headers=bearer(outsider)
        ).status_code == 404


def test_dashboard_reflects_the_exchange(exchange: Exchange) -> None:
    """The dashboard asset loads and the endpoints it calls show the exchange."""

    with httpx.Client(base_url=exchange.relay.base_url, timeout=30) as client:
        page = client.get("/")
        assert page.status_code == 200
        assert "Agent token" in page.text
        assert "sessionStorage" in page.text
        for call in (
            "/api/v1/agents?limit=100",
            "direction=sent",
            "direction=received",
            "/attempts",
        ):
            assert call in page.text, f"the dashboard no longer calls {call}"

        # Reproduce the dashboard's refresh() with the sender's token.
        agents = client.get("/api/v1/agents?limit=100", headers=bearer(exchange.sender)).json()["items"]
        listed = {agent["agent_id"] for agent in agents}
        participants = {exchange.sender["agent_id"], exchange.recipient["agent_id"]}
        assert participants <= listed
        watched = [agent for agent in agents if agent["agent_id"] in participants]
        assert all(agent["last_seen_at"] is not None for agent in watched)

        me = client.get("/api/v1/agents/me", headers=bearer(exchange.recipient)).json()
        assert me["agent_id"] == exchange.recipient["agent_id"]
        assert me["description"] == "Deterministic uppercase worker"

        attempts = client.get(
            f"/api/v1/tasks/{exchange.task_id}/attempts", headers=bearer(exchange.sender)
        ).json()["items"]
        history = ", ".join(
            f"{item['attempt']}:{item['outcome']}"
            + (f" ({item['worker_id']})" if item["worker_id"] else "")
            for item in attempts
        )
        assert history == f"1:completed ({exchange.worker_id})"


def test_the_real_database_persists_the_exchange(exchange: Exchange) -> None:
    """Read the PostgreSQL rows the running relay wrote, not a Python object."""

    relay = exchange.relay
    if relay.db_url is None:
        pytest.skip("set RELAY_TEST_DB_URL to assert rows on an externally managed relay")

    (sender_id, recipient_id, status, output, error, attempt_count, finished_at, key) = relay.rows(
        "select sender_id, recipient_id, status, output, error, attempt_count, finished_at,"
        " idempotency_key from tasks where id = :task_id",
        {"task_id": exchange.task_id},
    )[0]
    assert (sender_id, recipient_id) == (exchange.sender["agent_id"], exchange.recipient["agent_id"])
    assert status == "completed"
    assert output == exchange.payload.upper()
    assert error is None
    assert attempt_count == 1
    assert finished_at is not None
    assert key == exchange.idempotency_key

    (number, worker_id, outcome, token_hash, terminal_action) = relay.rows(
        "select attempt_number, worker_id, outcome, claim_token_hash, terminal_action"
        " from attempts where task_id = :task_id",
        {"task_id": exchange.task_id},
    )[0]
    assert (number, worker_id, outcome, terminal_action) == (
        1, exchange.worker_id, "completed", "complete"
    )
    assert is_sha256_hex(token_hash), "claim tokens must be stored hashed"

    for agent in (exchange.sender, exchange.recipient):
        (stored_hash, name) = relay.rows(
            "select token_hash, name from agents where id = :agent_id",
            {"agent_id": agent["agent_id"]},
        )[0]
        assert is_sha256_hex(stored_hash), "agent tokens must be stored hashed"
        assert stored_hash == hashlib.sha256(agent["token"].encode("utf-8")).hexdigest()
        assert name.startswith(("alice-", "uppercase-"))
        # The raw credential never reaches the database.
        assert relay.rows(
            "select count(*) from agents where token_hash = :hash", {"hash": agent["token"]}
        ) == [(0,)]

    assert relay.rows(
        "select count(*) from tasks where id = :task_id", {"task_id": exchange.task_id}
    ) == [(1,)]
    assert relay.rows(
        "select count(*) from attempts where task_id = :task_id", {"task_id": exchange.task_id}
    ) == [(1,)]
