# Agent Relay (PostgreSQL)

Agent Relay is a small FastAPI service for registering agents, delivering one
task at a time, and recording results. PostgreSQL persists the queue and
attempts, while workers execute tasks on their own machines. The included worker
deterministically returns `input.upper()`.

## Run it

The relay needs PostgreSQL. `compose.yaml` starts one with the credentials the
default URL expects; or point `RELAY_DATABASE_URL` at any PostgreSQL server:

```bash
docker compose up -d postgres
uv sync
uv run uvicorn main:app --reload
```

Open <http://127.0.0.1:8000/> for the token-based local dashboard. The default
URL is `postgresql+psycopg://relay:relay@127.0.0.1:55432/relay`, which is the
host port `compose.yaml` publishes (override it with `RELAY_POSTGRES_PORT`).
Startup calls `init_db()`, which creates the database and its tables when they
are missing and retries briefly while the server starts; the starter ships no
migration tool, and Alembic is the next step for real deployments. `GET /health`
is a liveness check and `GET /ready` verifies database connectivity and schema
(it queries the real tables, so a wiped database reports not-ready instead of
passing with zero tables).

Register two identities and send a task:

```bash
alice=$(curl -sS -X POST http://127.0.0.1:8000/api/v1/agents \
  -H 'content-type: application/json' -d '{"name":"alice"}')
bob=$(curl -sS -X POST http://127.0.0.1:8000/api/v1/agents \
  -H 'content-type: application/json' -d '{"name":"uppercase"}')
```

The response contains each agent's secret `token` once. Keep it outside source
control. Use `Authorization: Bearer <token>` for all subsequent API calls;
registration is the only unauthenticated endpoint. For a shared installation,
set `RELAY_ENROLLMENT_SECRET` and send it as `X-Enrollment-Secret` when
registering.

## Run it in a container

`compose.yaml` runs the relay and `postgres` together; the relay waits for the
database healthcheck before it starts:

```bash
docker compose up --build
```

The API is then on the host at <http://127.0.0.1:8000/>. The image installs from
`uv.lock`, runs unprivileged, and contains the worker, so an agent process can
serve an identity inside the running container:

```bash
docker compose exec relay python main.py worker \
  --agent-id agent_123 --token agt_… --worker-id container-1
```

`docker compose down` stops both services without losing data, because the
database lives in a named volume; `docker compose down -v` deletes it too. The
API port is `RELAY_PORT` and the published database port is
`RELAY_POSTGRES_PORT`, bound to `127.0.0.1` only. Both credentials default to
`relay`/`relay` for local use — change `POSTGRES_USER` and `POSTGRES_PASSWORD`
before exposing this anywhere shared.

The image can also be built and run on its own, but it then needs a reachable
server:

```bash
docker build -t agent-relay:local .
docker run --rm -p 8000:8000 \
  -e RELAY_DATABASE_URL=postgresql+psycopg://relay:relay@host.docker.internal:55432/relay \
  agent-relay:local
```

## Run the deterministic worker

The worker can register itself and save credentials in a mode-0600 JSON file:

```bash
uv run python main.py worker \
  --base-url http://127.0.0.1:8000 \
  --name uppercase \
  --credentials ./uppercase-credentials.json \
  --worker-id laptop-1
```

For failure/redelivery demonstrations, make local execution intentionally slow
and stop the process after one completion:

```bash
uv run python main.py worker --credentials ./uppercase-credentials.json \
  --slow-seconds 75 --worker-id slow-laptop
```

The worker heartbeats during long work. Killing it leaves the claim leased;
after the 60-second lease expires, another worker can claim the task with a new
token and incremented attempt number. `RELAY_LEASE_SECONDS` and
`RELAY_MAX_ATTEMPTS` are configurable server settings.

An existing credential can also be supplied explicitly (the token is not
written to disk):

```bash
uv run python main.py worker --agent-id agent_123 --token agt_… --worker-id laptop-2
```

## Storage and delivery behavior

`database.py` contains the SQLAlchemy models, the engine, and the transaction
helper. `storage.py` contains task/claim/recovery operations; routes and request
models are kept in `main.py` and `schemas.py`. Claims and recovery select their
rows `FOR UPDATE SKIP LOCKED`, so concurrent workers and several API processes
each take a different task without blocking on one another, while heartbeat and
terminal submission lock the attempt they act on. Which engine backs the store
does not change the HTTP protocol or lifecycle in `SPEC.md`.

Claims are at-least-once and leased for 60 seconds by default. Heartbeats extend
an active lease. A completion or failure must include the recipient's bearer
token and claim token. Repeating the exact terminal request with that claim
token is idempotent; a stale token or different result receives `409`.

## Verify

The test suite covers the main protocol, sender/recipient access boundaries,
hashed claim-token behavior, idempotent terminal retries, concurrent claims,
lease expiry before and after recovery, pagination/error shape, and dashboard
asset serving:

```bash
uv run pytest -q
```

Tests need PostgreSQL but no manual setup: `docker compose up -d postgres` is
enough, because each test module uses its own scratch database (`relay_test` and
`relay_acceptance`) that `init_db()` creates on demand.

```bash
docker compose up -d postgres
uv run pytest -q
```

The protocol tests drop and recreate every table in `relay_test` around each
test, so point `RELAY_DATABASE_URL` at a database you can afford to erase. Their
concurrent-claim test runs 16 threads against one inbox, which is the in-process
equivalent of several workers racing for the same queue; set
`RELAY_TEST_DATABASE_URL` to move the acceptance test elsewhere.

`test_acceptance_scenario_1.py` turns SPEC.md scenario 1 into an end-to-end
integration test instead of an in-process one: it boots a real `uvicorn` process
on a free port against `relay_acceptance`, exchanges a task using the real worker
client loop, then asserts the result through the API the dashboard uses and
directly in that database with SQL. It manages its own relay by default; point it
at an already-running relay with `RELAY_TEST_BASE_URL`, and add
`RELAY_TEST_DB_URL` to keep the database assertions:

```bash
RELAY_TEST_BASE_URL=http://127.0.0.1:8000 \
RELAY_TEST_DB_URL=postgresql+psycopg://relay:relay@127.0.0.1:55432/relay \
  uv run pytest test_acceptance_scenario_1.py -q
```

This starter intentionally does not include Kubernetes, CI, external brokers, an
LLM, or a migration tool. Those are deployment concerns rather than part of the
local relay protocol. `Dockerfile` and `compose.yaml` only package the service as
it is; they do not change the protocol.
