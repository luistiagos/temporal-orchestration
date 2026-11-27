"""Testes da sonda de workflow preso (`dsg_temporal/diagnose.py`).

Divisao deliberada:
- unit: extracao da falha a partir de eventos de historico MONTADOS a mao — e'
  a parte que decide o diagnostico (`cause` non-determinismo x excecao) e nao
  precisa de servidor;
- integration: veredicto contra um Temporal de verdade, incluindo um workflow
  que falha o workflow task de proposito (o caso que o bug descreve).
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
from temporalio import workflow
from temporalio.api.enums.v1 import EventType, WorkflowTaskFailedCause
from temporalio.api.history.v1 import HistoryEvent
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from dsg_temporal.diagnose import (
    STUCK_TASK_ATTEMPT_THRESHOLD,
    VERDICT_CLOSED,
    VERDICT_NOT_FOUND,
    VERDICT_RUNNING,
    VERDICT_STUCK,
    diagnose_workflow,
    summarize_task_failures,
    terminate_workflow,
    verdict_for,
)


# --- unit --------------------------------------------------------------------


def _task_failed_event(event_id: int, cause: int, message: str, stack: str = "") -> HistoryEvent:
    event = HistoryEvent(event_id=event_id)
    event.event_type = EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
    attrs = event.workflow_task_failed_event_attributes
    attrs.cause = cause
    attrs.identity = "worker@railway"
    attrs.failure.message = message
    attrs.failure.stack_trace = stack
    return event


@pytest.mark.unit
def test_extrai_mensagem_e_cause_de_non_determinismo():
    events = [
        _task_failed_event(
            10,
            WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR,
            "Nondeterminism error: Timer machine does not handle this event",
            "Traceback...",
        )
    ]

    summary = summarize_task_failures(events)

    assert summary["task_failures_total"] == 1
    failure = summary["task_failures"][0]
    assert failure["cause"] == "WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR"
    assert "Nondeterminism error" in failure["message"]
    assert failure["stack_trace"] == "Traceback..."
    assert failure["identity"] == "worker@railway"


@pytest.mark.unit
def test_devolve_apenas_as_ultimas_falhas_mas_conta_todas():
    events = [
        _task_failed_event(
            i,
            WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
            f"falha {i}",
        )
        for i in range(1, 9)
    ]

    summary = summarize_task_failures(events, limit=2)

    assert summary["task_failures_total"] == 8
    assert [f["message"] for f in summary["task_failures"]] == ["falha 7", "falha 8"]


@pytest.mark.unit
def test_evento_sem_falha_nao_quebra_a_extracao():
    event = HistoryEvent(event_id=1)
    event.event_type = EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
    event.workflow_task_failed_event_attributes.cause = (
        WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW
    )

    summary = summarize_task_failures([event])

    assert summary["task_failures"][0]["message"] == ""
    assert summary["task_failures"][0]["cause_chain"] == []


@pytest.mark.unit
def test_timer_e_activity_nao_entram_na_contagem():
    event = HistoryEvent(event_id=1)
    event.event_type = EventType.EVENT_TYPE_TIMER_STARTED

    summary = summarize_task_failures([event])

    assert summary["task_failures_total"] == 0
    assert summary["task_timeouts_total"] == 0


@pytest.mark.unit
def test_veredicto_running_com_attempt_alto_e_preso():
    # O ponto cego que originou o bug: para o Temporal o workflow esta RUNNING.
    assert verdict_for(1, 1) == VERDICT_RUNNING
    assert verdict_for(1, STUCK_TASK_ATTEMPT_THRESHOLD - 1) == VERDICT_RUNNING
    assert verdict_for(1, STUCK_TASK_ATTEMPT_THRESHOLD) == VERDICT_STUCK
    assert verdict_for(1, 14622) == VERDICT_STUCK
    # Fechado e' fechado, mesmo que a ultima tentativa tenha falhado.
    assert verdict_for(2, 0) == VERDICT_CLOSED
    assert verdict_for(3, 9999) == VERDICT_CLOSED


# --- integration -------------------------------------------------------------


@workflow.defn(name="TesteWorkflowQueSempreDorme")
class _SleepyWorkflow:
    @workflow.run
    async def run(self) -> str:
        await workflow.sleep(timedelta(days=30))
        return "acordou"


@workflow.defn(name="TesteWorkflowQueFalhaWorkflowTask")
class _BrokenWorkflow:
    @workflow.run
    async def run(self) -> str:
        # RuntimeError nao e' FailureError: o Temporal falha o workflow TASK
        # (nao o workflow) e retenta para sempre. E' a forma do bug.
        raise RuntimeError("estouro deterministico dentro do workflow")


@pytest.mark.integration
def test_diagnostico_de_workflow_vivo_fechado_e_inexistente():
    async def run_test() -> None:
        task_queue = f"diagnose-{uuid.uuid4().hex}"
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client, task_queue=task_queue, workflows=[_SleepyWorkflow]
            ):
                handle = await env.client.start_workflow(
                    _SleepyWorkflow.run,
                    id=f"vivo-{uuid.uuid4().hex}",
                    task_queue=task_queue,
                )
                alive = await diagnose_workflow(env.client, handle.id)
                assert alive["verdict"] == VERDICT_RUNNING
                assert alive["alive"] is True
                assert alive["status_name"] == "RUNNING"
                assert alive["pending_workflow_task"]["attempt"] <= 1
                # Sem `with_failures` explicito, um workflow saudavel nao paga
                # a leitura do historico.
                assert "task_failures" not in alive

                await handle.cancel()
                with pytest.raises(Exception):
                    await handle.result()
                closed = await diagnose_workflow(env.client, handle.id)
                assert closed["verdict"] == VERDICT_CLOSED
                assert closed["alive"] is False
                assert closed["status_name"] == "CANCELED"

            missing = await diagnose_workflow(env.client, f"nao-existe-{uuid.uuid4().hex}")
            assert missing["verdict"] == VERDICT_NOT_FOUND
            assert missing["alive"] is False

    asyncio.run(run_test())


@pytest.mark.integration
def test_workflow_task_em_retry_traz_veredicto_preso_e_a_mensagem():
    async def run_test() -> None:
        task_queue = f"diagnose-stuck-{uuid.uuid4().hex}"
        # Servidor local (sem time-skipping): o retry do workflow task tem
        # backoff e a versao com salto de tempo o dispararia em rajada.
        async with await WorkflowEnvironment.start_local() as env:
            async with Worker(
                env.client, task_queue=task_queue, workflows=[_BrokenWorkflow]
            ):
                handle = await env.client.start_workflow(
                    _BrokenWorkflow.run,
                    id=f"preso-{uuid.uuid4().hex}",
                    task_queue=task_queue,
                )
                report = {}
                for _ in range(90):
                    report = await diagnose_workflow(env.client, handle.id, with_failures=True)
                    if report["verdict"] == VERDICT_STUCK:
                        break
                    await asyncio.sleep(1)

                assert report["verdict"] == VERDICT_STUCK, report
                # PRESO, e ainda assim RUNNING para o Temporal.
                assert report["status_name"] == "RUNNING"
                assert report["alive"] is False
                assert report["pending_workflow_task"]["attempt"] >= (
                    STUCK_TASK_ATTEMPT_THRESHOLD
                )
                assert report["task_failures_total"] >= 1
                failure = report["task_failures"][-1]
                assert "estouro deterministico" in failure["message"]
                assert failure["cause"] == (
                    "WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE"
                )

                # Terminar e' a unica saida: signal nao chega em quem nao
                # processa workflow task.
                killed = await terminate_workflow(env.client, handle.id, "teste")
                assert killed["status"] == "terminated"
                after = await diagnose_workflow(env.client, handle.id)
                assert after["verdict"] == VERDICT_CLOSED
                assert after["status_name"] == "TERMINATED"

    asyncio.run(run_test())


@pytest.mark.integration
def test_terminate_de_workflow_inexistente_nao_estoura():
    async def run_test() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            out = await terminate_workflow(env.client, f"nada-{uuid.uuid4().hex}", "teste")
            assert out["status"] == "not_found"

    asyncio.run(run_test())


# --- reinicio do run preso antes do signal (bug 2026-11-27) ------------------
# Doc: docs/modules/chatbot-whatsapp/bugs/2026-11-27-mensagem-aceita-como-signaled-por-workflow-preso-se-perde-sem-alerta.md

from temporalio.client import WorkflowExecutionStatus  # noqa: E402

from dsg_temporal import whatsapp_ingest  # noqa: E402
from dsg_temporal.diagnose import terminate_if_stuck, terminate_run  # noqa: E402
from dsg_temporal.schemas import WhatsAppInboundMessage, WhatsAppWorkflowInput  # noqa: E402


async def _espera_preso(client, workflow_id: str) -> dict:
    report = {}
    for _ in range(90):
        report = await diagnose_workflow(client, workflow_id, with_failures=False)
        if report["verdict"] == VERDICT_STUCK:
            return report
        await asyncio.sleep(1)
    raise AssertionError(f"nao ficou preso: {report}")


@workflow.defn(name="WhatsAppConversationWorkflow")
class _WhatsAppQuebrado:
    """Mesmo NOME do workflow de producao, com o workflow task falhando sempre:
    e' o que um nao-determinismo faz com o run da conversa."""

    @workflow.run
    async def run(self, payload: WhatsAppWorkflowInput) -> None:
        raise RuntimeError("nao-determinismo simulado")

    @workflow.signal
    async def message_received(self, message: WhatsAppInboundMessage) -> None:
        return None


def _mensagem(msg_id: str) -> WhatsAppInboundMessage:
    return WhatsAppInboundMessage(
        tenant_id="t", instance_id="i", jid="5511999990000@s.whatsapp.net",
        jid_alt=None, msg_id=msg_id, text="oi",
    )


def _input(msg: WhatsAppInboundMessage) -> WhatsAppWorkflowInput:
    return WhatsAppWorkflowInput(
        tenant_id="t", conversation_id="5511999990000", initial_message=msg
    )


@pytest.mark.integration
def test_terminate_if_stuck_nao_toca_workflow_vivo_nem_inexistente():
    async def run_test() -> None:
        task_queue = f"stuck-vivo-{uuid.uuid4().hex}"
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(env.client, task_queue=task_queue, workflows=[_SleepyWorkflow]):
                handle = await env.client.start_workflow(
                    _SleepyWorkflow.run, id=f"vivo-{uuid.uuid4().hex}", task_queue=task_queue,
                )
                assert await terminate_if_stuck(env.client, handle.id, "teste") is None
                desc = await handle.describe()
                assert desc.status == WorkflowExecutionStatus.RUNNING

            assert await terminate_if_stuck(env.client, f"nada-{uuid.uuid4().hex}", "t") is None

    asyncio.run(run_test())


@pytest.mark.integration
def test_terminate_if_stuck_termina_o_run_preso_e_libera_o_id():
    async def run_test() -> None:
        task_queue = f"stuck-{uuid.uuid4().hex}"
        workflow_id = f"preso-{uuid.uuid4().hex}"
        async with await WorkflowEnvironment.start_local() as env:
            async with Worker(
                env.client, task_queue=task_queue, workflows=[_BrokenWorkflow, _SleepyWorkflow]
            ):
                old = await env.client.start_workflow(
                    _BrokenWorkflow.run, id=workflow_id, task_queue=task_queue,
                )
                await _espera_preso(env.client, workflow_id)

                out = await terminate_if_stuck(env.client, workflow_id, "teste")
                assert out is not None
                assert out["previous_run_id"] == old.result_run_id
                assert out["attempt"] >= STUCK_TASK_ATTEMPT_THRESHOLD
                assert out["terminate"] == "terminated"
                old_desc = await env.client.get_workflow_handle(
                    workflow_id, run_id=old.result_run_id
                ).describe()
                assert old_desc.status == WorkflowExecutionStatus.TERMINATED

                # ALLOW_DUPLICATE (default): o mesmo id aceita run novo.
                new = await env.client.start_workflow(
                    _SleepyWorkflow.run, id=workflow_id, task_queue=task_queue,
                )
                assert new.result_run_id != old.result_run_id

                # Corrida: request atrasado que descreveu o run velho termina pelo
                # run_id velho -> not_found, e o run novo segue vivo.
                late = await terminate_run(env.client, workflow_id, old.result_run_id, "atrasado")
                assert late["status"] == "not_found"
                new_desc = await env.client.get_workflow_handle(workflow_id).describe()
                assert new_desc.run_id == new.result_run_id
                assert new_desc.status == WorkflowExecutionStatus.RUNNING

    asyncio.run(run_test())


@pytest.mark.integration
def test_entrega_reinicia_o_workflow_da_conversa_preso():
    async def run_test() -> None:
        task_queue = f"wpp-preso-{uuid.uuid4().hex}"
        workflow_id = f"whatsapp-teste-{uuid.uuid4().hex}"
        async with await WorkflowEnvironment.start_local() as env:
            async with Worker(env.client, task_queue=task_queue, workflows=[_WhatsAppQuebrado]):
                m1 = _mensagem("m1")
                first = await whatsapp_ingest.deliver_whatsapp_message(
                    env.client, workflow_id=workflow_id, workflow_input=_input(m1),
                    message=m1, task_queue=task_queue,
                )
                assert first["status"] == "started"
                stuck = await _espera_preso(env.client, workflow_id)

                m2 = _mensagem("m2")
                second = await whatsapp_ingest.deliver_whatsapp_message(
                    env.client, workflow_id=workflow_id, workflow_input=_input(m2),
                    message=m2, task_queue=task_queue,
                )
                assert second["status"] == "restarted", second
                assert second["previous_run_id"] == stuck["run_id"]
                assert second["stuck_attempt"] >= STUCK_TASK_ATTEMPT_THRESHOLD

                # O run novo nasceu com a mensagem que disparou o reinicio.
                handle = env.client.get_workflow_handle(workflow_id)
                desc = await handle.describe()
                assert desc.run_id != stuck["run_id"]
                events = [e async for e in handle.fetch_history_events()]
                started = events[0].workflow_execution_started_event_attributes
                assert b'"m2"' in started.input.payloads[0].data

                # Mensagem seguinte, com o run novo ainda nao preso: signal normal.
                m3 = _mensagem("m3")
                third = await whatsapp_ingest.deliver_whatsapp_message(
                    env.client, workflow_id=workflow_id, workflow_input=_input(m3),
                    message=m3, task_queue=task_queue,
                )
                assert third["status"] == "signaled"

    asyncio.run(run_test())


@pytest.mark.integration
def test_sonda_levantando_cai_no_signal(monkeypatch):
    async def explode(*a, **k):
        raise RuntimeError("temporal fora para o describe")

    monkeypatch.setattr(whatsapp_ingest.diagnose, "terminate_if_stuck", explode)

    async def run_test() -> None:
        workflow_id = f"whatsapp-teste-{uuid.uuid4().hex}"
        task_queue = f"sem-worker-{uuid.uuid4().hex}"
        async with await WorkflowEnvironment.start_time_skipping() as env:
            m1 = _mensagem("m1")
            await whatsapp_ingest.deliver_whatsapp_message(
                env.client, workflow_id=workflow_id, workflow_input=_input(m1),
                message=m1, task_queue=task_queue,
            )
            m2 = _mensagem("m2")
            out = await whatsapp_ingest.deliver_whatsapp_message(
                env.client, workflow_id=workflow_id, workflow_input=_input(m2),
                message=m2, task_queue=task_queue,
            )
            assert out == {"workflow_id": workflow_id, "status": "signaled"}

    asyncio.run(run_test())


@pytest.mark.integration
def test_worker_ausente_nao_conta_como_preso():
    """Risco da passada 2: sem poller, o `attempt` sobe por timeout e o reinicio
    terminaria runs saudaveis? Mede no servidor local: workflow numa fila sem
    worker, com signal, sondado por ~30 s."""

    async def run_test() -> None:
        workflow_id = f"sem-worker-{uuid.uuid4().hex}"
        async with await WorkflowEnvironment.start_local() as env:
            m1 = _mensagem("m1")
            await env.client.start_workflow(
                "WhatsAppConversationWorkflow", _input(m1), id=workflow_id,
                task_queue=f"fila-vazia-{uuid.uuid4().hex}",
            )
            handle = env.client.get_workflow_handle(workflow_id)
            await handle.signal("message_received", _mensagem("m2"))
            attempts = []
            for _ in range(15):
                report = await diagnose_workflow(env.client, workflow_id, with_failures=False)
                attempts.append(report["pending_workflow_task"]["attempt"])
                await asyncio.sleep(2)
            assert max(attempts) < STUCK_TASK_ATTEMPT_THRESHOLD, attempts
            assert await terminate_if_stuck(env.client, workflow_id, "t") is None

    asyncio.run(run_test())
