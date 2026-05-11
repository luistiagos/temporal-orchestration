from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import uvicorn
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from dsg_temporal.activities import (
    check_purchase,
    dispatch_remarketing_step,
    notify_remarketing_event,
    process_whatsapp_batch,
)
from dsg_temporal.logging_config import configure_logging
from dsg_temporal.settings import get_settings
from dsg_temporal.workflows import LeadRemarketingWorkflow, WhatsAppConversationWorkflow


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


async def run_dev_local() -> None:
    configure_logging()

    temporal_port = int(os.getenv("TEMPORAL_DEV_PORT", "7233"))
    temporal_ui = _bool_env("TEMPORAL_DEV_UI", True)
    temporal_ui_port = int(os.getenv("TEMPORAL_DEV_UI_PORT", "8233"))
    temporal_db = os.getenv("TEMPORAL_DEV_DB", ".temporal/dev-server.sqlite")
    Path(temporal_db).parent.mkdir(parents=True, exist_ok=True)

    os.environ.setdefault("DRY_RUN", "true")
    os.environ.setdefault("TEMPORAL_NAMESPACE", "default")
    os.environ.setdefault("TEMPORAL_TASK_QUEUE", "dsg-orchestrator")
    os.environ.setdefault("API_HOST", "0.0.0.0")
    os.environ.setdefault("API_PORT", os.getenv("PORT", "8090"))

    env = await WorkflowEnvironment.start_local(
        namespace=os.environ["TEMPORAL_NAMESPACE"],
        ip="127.0.0.1",
        port=temporal_port,
        ui=temporal_ui,
        ui_port=temporal_ui_port,
        dev_server_database_filename=temporal_db,
        dev_server_log_level="warn",
    )
    target_host = env.client.service_client.config.target_host
    os.environ["TEMPORAL_ADDRESS"] = target_host
    get_settings.cache_clear()
    settings = get_settings()

    from dsg_temporal.api import app

    print(f"Temporal dev server: {target_host}")
    if temporal_ui:
        print(f"Temporal UI: http://127.0.0.1:{temporal_ui_port}")
    print(f"API: http://127.0.0.1:{settings.api_port}")
    print(f"Worker task queue: {settings.temporal_task_queue}")
    print("DRY_RUN=true" if settings.dry_run else "DRY_RUN=false")

    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=settings.api_host,
            port=settings.api_port,
            log_level="info",
        )
    )

    with ThreadPoolExecutor(max_workers=settings.activity_max_workers) as activity_executor:
        worker = Worker(
            env.client,
            task_queue=settings.temporal_task_queue,
            workflows=[LeadRemarketingWorkflow, WhatsAppConversationWorkflow],
            activities=[
                check_purchase,
                dispatch_remarketing_step,
                notify_remarketing_event,
                process_whatsapp_batch,
            ],
            activity_executor=activity_executor,
        )
        async with worker:
            try:
                await server.serve()
            finally:
                await env.shutdown()


def main() -> None:
    asyncio.run(run_dev_local())


if __name__ == "__main__":
    main()

