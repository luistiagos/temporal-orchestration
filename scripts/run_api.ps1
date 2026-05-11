$ErrorActionPreference = "Stop"
$port = if ($env:API_PORT) { $env:API_PORT } else { "8090" }
uvicorn dsg_temporal.api:app --host 0.0.0.0 --port $port

