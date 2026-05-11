$ErrorActionPreference = "Stop"

if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
    python -m venv .venv
}

.\.venv\Scripts\python -m pip install -e ".[dev]"
.\.venv\Scripts\python -m dsg_temporal.dev_local

