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
