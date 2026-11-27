"""Entrega de uma mensagem de WhatsApp ao workflow da conversa.

Fora de `api.py` pela mesma regra de `diagnose.py`: a rota e' so a casca HTTP e o
teste roda sem fastapi.

Antes do signal, sonda o run atual: workflow com o workflow task falhando em loop
(`stuck_task_failure`) aceita o signal e nunca o processa, e a mensagem se perdia
respondida como `signaled`. Preso -> termina aquele run e inicia um novo com a
mensagem como `initial_message` (`status=restarted`; o backend alerta o dono).
Doc: docs/modules/chatbot-whatsapp/bugs/2026-11-27-mensagem-aceita-como-signaled-por-workflow-preso-se-perde-sem-alerta.md
"""

from __future__ import annotations

import logging
from typing import Any

from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from dsg_temporal import diagnose
from dsg_temporal.schemas import WhatsAppInboundMessage, WhatsAppWorkflowInput
from dsg_temporal.workflows import WhatsAppConversationWorkflow

logger = logging.getLogger(__name__)

STUCK_RESTART_REASON = "workflow task preso: reiniciado pela entrada de mensagem nova"


async def deliver_whatsapp_message(
    client: Client,
    *,
    workflow_id: str,
    workflow_input: WhatsAppWorkflowInput,
    message: WhatsAppInboundMessage,
    task_queue: str,
) -> dict[str, Any]:
    """`started` (run novo), `restarted` (run preso trocado) ou `signaled`.

    A sonda nunca bloqueia a entrada: qualquer excecao dela cai no signal de antes.
    Excecao do `start_workflow`/`signal` propaga (a rota devolve 500 e o backend usa
    o fallback dele).
    """
    try:
        await client.start_workflow(
            WhatsAppConversationWorkflow.run,
            workflow_input,
            id=workflow_id,
            task_queue=task_queue,
        )
        return {"workflow_id": workflow_id, "status": "started"}
    except WorkflowAlreadyStartedError:
        pass

    stuck = None
    try:
        stuck = await diagnose.terminate_if_stuck(client, workflow_id, STUCK_RESTART_REASON)
    except Exception:
        logger.exception("[whatsapp-ingest] sonda do workflow %s falhou; segue no signal", workflow_id)

    if stuck is not None:
        logger.warning(
            "[whatsapp-ingest] workflow %s preso (attempt=%s, run=%s, terminate=%s): reiniciando",
            workflow_id, stuck["attempt"], stuck["previous_run_id"], stuck["terminate"],
        )
        try:
            # id_reuse_policy default (ALLOW_DUPLICATE): o id do run terminado aceita
            # run novo.
            await client.start_workflow(
                WhatsAppConversationWorkflow.run,
                workflow_input,
                id=workflow_id,
                task_queue=task_queue,
            )
            return {
                "workflow_id": workflow_id,
                "status": "restarted",
                "previous_run_id": stuck["previous_run_id"],
                "stuck_attempt": stuck["attempt"],
            }
        except WorkflowAlreadyStartedError:
            # Corrida: outro request ja reiniciou. A mensagem vai ao run novo dele.
            pass

    handle = client.get_workflow_handle(workflow_id)
    await handle.signal(WhatsAppConversationWorkflow.message_received, message)
    return {"workflow_id": workflow_id, "status": "signaled"}
