from __future__ import annotations

import asyncio
import contextlib
import logging
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
from dsg_temporal.version import summary as version_summary
from dsg_temporal.workflows import LeadRemarketingWorkflow, WhatsAppConversationWorkflow

logger = logging.getLogger(__name__)


async def run_worker() -> None:
    configure_logging()
    # O worker não serve HTTP: logar a versão VIVA no boot é o único jeito de
    # confirmar por fora QUAL commit está rodando (mata "fix fantasma" no worker).
    logger.info("dsg-temporal worker boot version=%s", version_summary())
    settings = get_settings()
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )

    wpp_queue = settings.temporal_wpp_task_queue
    isolate_wpp = bool(wpp_queue) and wpp_queue != settings.temporal_task_queue

    with contextlib.ExitStack() as stack:
        main_executor = stack.enter_context(
            ThreadPoolExecutor(max_workers=settings.activity_max_workers)
        )
        # Worker principal: workflows + remarketing. Continua registrando
        # process_whatsapp_batch para servir conversas legadas (in-flight) que
        # foram agendadas na fila principal ANTES do deploy do isolamento.
        main_worker = Worker(
            client,
            task_queue=settings.temporal_task_queue,
            workflows=[LeadRemarketingWorkflow, WhatsAppConversationWorkflow],
            activities=[
                check_purchase,
                dispatch_remarketing_step,
                notify_remarketing_event,
                process_whatsapp_batch,
            ],
            activity_executor=main_executor,
        )
        runners = [main_worker.run()]

        if isolate_wpp:
            # Worker dedicado às atividades de conversa, com pool PRÓPRIO. Assim os
            # sleeps de pacing do remarketing (que ocupam threads do pool principal)
            # nunca starvam a resposta ao cliente ao vivo.
            wpp_executor = stack.enter_context(
                ThreadPoolExecutor(max_workers=settings.wpp_activity_max_workers)
            )
            wpp_worker = Worker(
                client,
                task_queue=wpp_queue,
                activities=[process_whatsapp_batch],
                activity_executor=wpp_executor,
            )
            runners.append(wpp_worker.run())
            logger.info(
                "dsg-temporal: conversas isoladas na task_queue=%s (pool=%d)",
                wpp_queue,
                settings.wpp_activity_max_workers,
            )

        await asyncio.gather(*runners)


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()

