from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
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


async def run_worker() -> None:
    configure_logging()
    settings = get_settings()
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )

    with ThreadPoolExecutor(max_workers=settings.activity_max_workers) as activity_executor:
        worker = Worker(
            client,
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
        await worker.run()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()

