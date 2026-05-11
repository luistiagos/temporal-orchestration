# Railway Deployment

This project is ready to run on Railway with Railway PostgreSQL.

## Recommended Railway services

Create these services in the same Railway project:

1. `temporal-postgres`
   - Railway PostgreSQL plugin/service.
   - Use a dedicated database for Temporal if possible.

2. `temporal-server`
   - Root directory: `railway/temporal-server`
   - No public domain required.
   - Exposes Temporal gRPC on port `7233`.

3. `dsg-temporal-api`
   - Root directory: project root.
   - Public domain enabled.
   - Healthcheck path: `/health`.
   - Variables:
     - `SERVICE_ROLE=api`
     - `TEMPORAL_ADDRESS=${{temporal-server.RAILWAY_PRIVATE_DOMAIN}}:7233`
     - `TEMPORAL_NAMESPACE=default`
     - `TEMPORAL_TASK_QUEUE=dsg-orchestrator`
     - `DRY_RUN=true` initially

4. `dsg-temporal-worker`
   - Root directory: project root.
   - No public domain required.
   - No HTTP healthcheck.
   - Variables:
     - `SERVICE_ROLE=worker`
     - `TEMPORAL_ADDRESS=${{temporal-server.RAILWAY_PRIVATE_DOMAIN}}:7233`
     - `TEMPORAL_NAMESPACE=default`
     - `TEMPORAL_TASK_QUEUE=dsg-orchestrator`
     - `DRY_RUN=true` initially

5. `temporal-ui` optional
   - Root directory: `railway/temporal-ui`
   - Public domain can be enabled, preferably protected by Railway access controls
     or a private team-only network.
   - Variables:
     - `TEMPORAL_ADDRESS=${{temporal-server.RAILWAY_PRIVATE_DOMAIN}}:7233`

## Temporal Server variables

For the `temporal-server` service, set these variables from the Railway Postgres
service:

```text
DB=postgres12
DB_PORT=${{temporal-postgres.PGPORT}}
POSTGRES_SEEDS=${{temporal-postgres.PGHOST}}
POSTGRES_USER=${{temporal-postgres.PGUSER}}
POSTGRES_PWD=${{temporal-postgres.PGPASSWORD}}
DBNAME=${{temporal-postgres.PGDATABASE}}
VISIBILITY_POSTGRES_SEEDS=${{temporal-postgres.PGHOST}}
VISIBILITY_POSTGRES_USER=${{temporal-postgres.PGUSER}}
VISIBILITY_POSTGRES_PWD=${{temporal-postgres.PGPASSWORD}}
VISIBILITY_DB_PORT=${{temporal-postgres.PGPORT}}
VISIBILITY_DBNAME=${{temporal-postgres.PGDATABASE}}
POSTGRES_TLS_ENABLED=true
POSTGRES_TLS_DISABLE_HOST_VERIFICATION=true
```

If your Railway Postgres is only reachable through the private network without
TLS, set `POSTGRES_TLS_ENABLED=false`.

## Important note about the Temporal image

The `railway/temporal-server` Dockerfile uses `temporalio/auto-setup` because it
bootstraps the Temporal schema automatically, which is practical for the first
Railway deployment. Temporal marks this image as deprecated for long-term
production use.

For a mature production setup, move to either:

- Temporal Cloud, or
- `temporalio/server` plus an explicit schema migration/init job.

## Railway private networking

Use Railway private networking between API, worker, Temporal Server, and
Postgres. Only the API needs a public domain for calls from the existing backend.

The existing backend will eventually call:

```text
https://<dsg-temporal-api-domain>/v1/remarketing/workflows
```
