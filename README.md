# Digital Store Games Temporal Orchestrator

Standalone Temporal service for durable workflows used by Digital Store Games
and future projects.

The first production target is the remarketing/manageleads flow. The WhatsApp
chatbot flow is scaffolded as a second migration target.

## What this project owns

- Temporal workers and workflows.
- A small HTTP API that the existing backend can call later.
- Activities that call legacy backend endpoints through adapters.
- Durable timers, retries, pause/resume/cancel signals, and workflow queries.

The existing `digitalstoregamesbackend` project is not modified by this
project.

## Local development

1. Copy environment variables:

```powershell
Copy-Item .env.example .env
```

1. Start Temporal locally:

```powershell
docker compose up temporal temporal-ui postgres
```

1. Install the Python project:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

1. Run the worker:

```powershell
python -m dsg_temporal.worker
```

1. Run the API:

```powershell
uvicorn dsg_temporal.api:app --host 0.0.0.0 --port 8090
```

Temporal UI defaults to http://localhost:8080.

### Local without Docker

If Docker is not installed, use the SDK-powered local dev launcher:

```powershell
.\scripts\dev_local.ps1
```

It starts:

- Temporal dev server on `127.0.0.1:7233`
- Temporal UI on http://127.0.0.1:8233
- API on http://127.0.0.1:8090
- Worker on task queue `dsg-orchestrator`

By default it runs with `DRY_RUN=true`.

### Tests

```powershell
.\scripts\run_tests.ps1
```

The integration tests start an ephemeral Temporal test server through the
Temporal Python SDK, so they do not require Docker.

## Railway

Railway deployment notes live in [railway/README.md](railway/README.md).

For Railway, deploy the same project root twice:

- API service with `SERVICE_ROLE=api`
- Worker service with `SERVICE_ROLE=worker`

Deploy Temporal Server from `railway/temporal-server` and point it at Railway
PostgreSQL. In the current Railway setup, the service is named
`temporal-serve`, so service references use
`${{temporal-serve.RAILWAY_PRIVATE_DOMAIN}}:7233`. Keep the Temporal Server and
worker private; expose only the API.

## Main endpoints

- `GET /health`
- `POST /v1/remarketing/workflows`
- `POST /v1/remarketing/workflows/{workflow_id}/purchase`
- `POST /v1/remarketing/workflows/{workflow_id}/pause`
- `POST /v1/remarketing/workflows/{workflow_id}/resume`
- `POST /v1/remarketing/workflows/{workflow_id}/cancel`
- `POST /v1/remarketing/workflows/{workflow_id}/confirm-current-step`
- `POST /v1/remarketing/workflows/{workflow_id}/retry-current-step`
- `POST /v1/whatsapp/messages`

## Design rule

Workflows contain orchestration only. Network calls, database calls, LLM calls,
email sending, WhatsApp sending, and purchase checks belong in activities.

This matters because Temporal replays workflow code. Workflow code must remain
deterministic.
