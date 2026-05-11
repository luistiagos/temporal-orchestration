from __future__ import annotations

import os
import subprocess
import sys

from dsg_temporal.settings import get_settings


def main() -> None:
    role = os.getenv("SERVICE_ROLE", "api").strip().lower()
    settings = get_settings()

    if role == "worker":
        from dsg_temporal.worker import main as worker_main

        worker_main()
        return

    if role == "api":
        port = os.getenv("PORT") or str(settings.api_port)
        host = os.getenv("API_HOST", settings.api_host)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "dsg_temporal.api:app",
                "--host",
                host,
                "--port",
                port,
            ],
            check=True,
        )
        return

    raise SystemExit(f"Unknown SERVICE_ROLE={role!r}. Expected 'api' or 'worker'.")


if __name__ == "__main__":
    main()

