"""Testes das correções de robustez do WhatsAppConversationWorkflow.

Cobrem os defeitos que travaram a conversa do "Dario" (silêncio de >1h):
- dedup por msg_id já visto (reenvio do reconcile não vira 2ª resposta);
- isolamento da atividade numa task_queue dedicada (pool separado do remarketing);
- falha da atividade NÃO perde a mensagem (re-enfileira + degrada, sem matar o workflow).

Rodam no test server em-memória do Temporal (mesma infra dos testes de integração).
"""
from __future__ import annotations

import asyncio
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from dsg_temporal.activities import process_whatsapp_batch
from dsg_temporal.schemas import (
    WhatsAppBatchResult,
    WhatsAppInboundMessage,
    WhatsAppWorkflowInput,
)
from dsg_temporal.workflows import WhatsAppConversationWorkflow

pytestmark = pytest.mark.integration


def _q(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def _msg(msg_id: str, text: str = "oi") -> WhatsAppInboundMessage:
    return WhatsAppInboundMessage(
        tenant_id="test",
        instance_id="agent",
        jid="5541999990000@lid",
        jid_alt=None,
        msg_id=msg_id,
        text=text,
    )


async def _wait_batches(handle, target: int, tries: int = 40) -> int:
    processed = 0
    for _ in range(tries):
        state = await handle.query(WhatsAppConversationWorkflow.state)
        processed = state.processed_batches
        if processed >= target:
            break
        await asyncio.sleep(0.1)
    return processed


def test_duplicate_msg_id_is_not_reprocessed():
    """Reenviar o MESMO msg_id depois do batch já ter saído não gera 2ª resposta."""

    async def run_test() -> tuple[int, int]:
        task_queue = _q("wpp-dedup")
        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client,
                    task_queue=task_queue,
                    workflows=[WhatsAppConversationWorkflow],
                    activities=[process_whatsapp_batch],
                    activity_executor=executor,
                ):
                    handle = await env.client.start_workflow(
                        WhatsAppConversationWorkflow.run,
                        WhatsAppWorkflowInput(
                            tenant_id="test",
                            conversation_id="5541999990000",
                            debounce_seconds=0.1,
                            initial_message=_msg("dup-1", "vou usar pendrive"),
                        ),
                        id=f"wpp-dedup-{uuid.uuid4().hex}",
                        task_queue=task_queue,
                        execution_timeout=timedelta(minutes=5),
                    )
                    # 1ª mensagem processada e buffer esvaziado.
                    assert await _wait_batches(handle, 1) == 1

                    # Reenvio do MESMO msg_id (simula o reconcile) — deve ser ignorado.
                    await handle.signal(
                        WhatsAppConversationWorkflow.message_received,
                        _msg("dup-1", "vou usar pendrive"),
                    )
                    await asyncio.sleep(0.5)
                    state_after_dup = await handle.query(WhatsAppConversationWorkflow.state)

                    # Uma mensagem NOVA continua sendo processada normalmente.
                    await handle.signal(
                        WhatsAppConversationWorkflow.message_received,
                        _msg("new-2", "qual valor"),
                    )
                    batches_after_new = await _wait_batches(handle, 2)

                    await handle.signal(WhatsAppConversationWorkflow.cancel, "done")
                    await handle.result()
                    return state_after_dup.processed_batches, batches_after_new

    dup_batches, new_batches = asyncio.run(run_test())
    assert dup_batches == 1  # duplicado NÃO gerou novo processamento
    assert new_batches == 2  # mensagem nova ainda funciona


def test_activity_runs_on_dedicated_task_queue():
    """Com activity_task_queue setada, a atividade roda no worker DEDICADO.

    O worker principal (fila do workflow) NÃO registra process_whatsapp_batch —
    então, se o batch é processado, foi obrigatoriamente pelo worker da fila wpp.
    """

    async def run_test() -> int:
        main_queue = _q("wpp-main")
        wpp_queue = _q("wpp-activities")
        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as main_ex, ThreadPoolExecutor(
                max_workers=4
            ) as wpp_ex:
                async with Worker(
                    env.client,
                    task_queue=main_queue,
                    workflows=[WhatsAppConversationWorkflow],
                    activities=[],  # sem a atividade de propósito
                    activity_executor=main_ex,
                ), Worker(
                    env.client,
                    task_queue=wpp_queue,
                    activities=[process_whatsapp_batch],
                    activity_executor=wpp_ex,
                ):
                    handle = await env.client.start_workflow(
                        WhatsAppConversationWorkflow.run,
                        WhatsAppWorkflowInput(
                            tenant_id="test",
                            conversation_id="5541999990000",
                            debounce_seconds=0.1,
                            initial_message=_msg("iso-1"),
                            activity_task_queue=wpp_queue,
                        ),
                        id=f"wpp-iso-{uuid.uuid4().hex}",
                        task_queue=main_queue,
                        execution_timeout=timedelta(minutes=5),
                    )
                    processed = await _wait_batches(handle, 1)
                    await handle.signal(WhatsAppConversationWorkflow.cancel, "done")
                    await handle.result()
                    return processed

    assert asyncio.run(run_test()) == 1


# --- Atividade falsa que falha as primeiras N chamadas, depois tem sucesso. ---
_fail_lock = threading.Lock()
_fail_calls = {"n": 0}
_FAIL_UNTIL = 5  # esgota as 5 tentativas do 1o execute_activity


@activity.defn(name="process_whatsapp_batch")
def flaky_process_whatsapp_batch(payload) -> WhatsAppBatchResult:
    with _fail_lock:
        _fail_calls["n"] += 1
        n = _fail_calls["n"]
    if n <= _FAIL_UNTIL:
        raise RuntimeError(f"backend indisponivel (call {n})")
    return WhatsAppBatchResult(status="processed", reason=f"ok on call {n}")


def test_activity_failure_does_not_lose_message():
    """Se a atividade falha, o workflow NÃO morre e a mensagem NÃO é perdida:
    re-enfileira, degrada (observável) e reprocessa quando recupera."""

    async def run_test() -> tuple[int, str, bool]:
        with _fail_lock:
            _fail_calls["n"] = 0
        task_queue = _q("wpp-flaky")
        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client,
                    task_queue=task_queue,
                    workflows=[WhatsAppConversationWorkflow],
                    activities=[flaky_process_whatsapp_batch],
                    activity_executor=executor,
                ):
                    handle = await env.client.start_workflow(
                        WhatsAppConversationWorkflow.run,
                        WhatsAppWorkflowInput(
                            tenant_id="test",
                            conversation_id="5541999990000",
                            debounce_seconds=0.1,
                            initial_message=_msg("flaky-1", "vou usar pendrive"),
                        ),
                        id=f"wpp-flaky-{uuid.uuid4().hex}",
                        task_queue=task_queue,
                        execution_timeout=timedelta(minutes=20),
                    )
                    # Avança o relógio (time-skipping) o suficiente para esgotar as 5
                    # tentativas da atividade (backoff ~75s) + o backoff de re-fila do
                    # workflow (30s) + a 6a chamada (sucesso). Apesar das falhas, a
                    # mensagem é preservada e acaba sendo processada.
                    await env.sleep(timedelta(seconds=240))
                    state = await handle.query(WhatsAppConversationWorkflow.state)
                    processed = state.processed_batches
                    had_failure_event = any(
                        e.event_type == "batch_failed" for e in state.events
                    )
                    await handle.signal(WhatsAppConversationWorkflow.cancel, "done")
                    result = await handle.result()
                    return processed, result.status, had_failure_event

    processed, status, had_failure_event = asyncio.run(run_test())
    assert processed == 1  # a mensagem foi processada (não perdida)
    assert status == "canceled"  # workflow seguiu vivo até o cancel
    assert had_failure_event  # a falha foi registrada de forma observável
