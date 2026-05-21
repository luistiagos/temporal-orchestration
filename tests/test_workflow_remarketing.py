"""Camada 2 — testes de workflow do LeadRemarketingWorkflow.

Usa WorkflowEnvironment.start_time_skipping() com activities MOCKADAS para
exercitar a lógica do workflow de forma determinística: timing, retry, cap
diário, signals, falhas.

Cobre: TIME-01/02, CHK-02/04/05, WPP-02/04/08, SIG-01/02/03, EDGE-03.
"""
from __future__ import annotations

import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from dsg_temporal.schemas import (
    DispatchResult,
    DispatchStepInput,
    LeadRemarketingInput,
    NotifyRemarketingEventInput,
    PurchaseCheckInput,
    PurchaseCheckResult,
    RemarketingStep,
)
from dsg_temporal.workflows import LeadRemarketingWorkflow

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fábricas de activities mockadas (nome registrado = nome da activity real)
# ---------------------------------------------------------------------------

def _make_check_purchase(*, purchased=False, raises=False):
    @activity.defn(name="check_purchase")
    def _check(payload: PurchaseCheckInput) -> PurchaseCheckResult:
        if raises:
            raise RuntimeError("purchase check failed (mock)")
        return PurchaseCheckResult(purchased=purchased)
    return _check


def _make_dispatch(results):
    """results: lista de DispatchResult retornados em ordem; o último repete."""
    calls = {"n": 0}

    @activity.defn(name="dispatch_remarketing_step")
    def _dispatch(payload: DispatchStepInput) -> DispatchResult:
        idx = min(calls["n"], len(results) - 1)
        calls["n"] += 1
        return results[idx]

    _dispatch.calls = calls  # type: ignore[attr-defined]
    return _dispatch


@activity.defn(name="notify_remarketing_event")
def _mock_notify(payload: NotifyRemarketingEventInput) -> None:
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _task_queue() -> str:
    return f"rmkt-{uuid.uuid4().hex}"


def _email_step(step_id="email-1", delay_minutes=0):
    return RemarketingStep(
        step_id=step_id, order=1, channel="email",
        template="remarket/email.html", subject="Volte!", delay_minutes=delay_minutes,
    )


def _whatsapp_step(step_id="wpp-1", delay_minutes=0):
    return RemarketingStep(
        step_id=step_id, order=2, channel="whatsapp",
        template="oi", delay_minutes=delay_minutes,
    )


def _input(sequence, *, max_cycles=1, stop_on_step_failure=True):
    return LeadRemarketingInput(
        tenant_id="test",
        lead_id=999,
        campaign_id="cart",
        email="emuladores.emuladores@gmail.com",
        phone="(41) 98531-1304",
        max_cycles=max_cycles,
        stop_on_step_failure=stop_on_step_failure,
        sequence=sequence,
    )


async def _run(input_obj, check_fn, dispatch_fn, *, timeout_days=2):
    """Executa o workflow até o fim e retorna o RemarketingWorkflowState."""
    task_queue = _task_queue()
    async with await WorkflowEnvironment.start_time_skipping() as env:
        with ThreadPoolExecutor(max_workers=4) as executor:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[LeadRemarketingWorkflow],
                activities=[check_fn, dispatch_fn, _mock_notify],
                activity_executor=executor,
            ):
                return await env.client.execute_workflow(
                    LeadRemarketingWorkflow.run,
                    input_obj,
                    id=f"test-{uuid.uuid4().hex}",
                    task_queue=task_queue,
                    execution_timeout=timedelta(days=timeout_days),
                )


# ---------------------------------------------------------------------------
# Happy path  (TIME-01, TIME-02)
# ---------------------------------------------------------------------------

def test_workflow_completa_e_despacha_email():
    async def run():
        return await _run(
            _input([_email_step(delay_minutes=15)]),
            _make_check_purchase(purchased=False),
            _make_dispatch([DispatchResult(status="sent")]),
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    assert [s.step_id for s in result.sent_steps] == ["email-1"]
    assert result.sent_steps[0].status == "sent"


def test_delay_zero_tambem_completa():
    async def run():
        return await _run(
            _input([_email_step(delay_minutes=0)]),
            _make_check_purchase(purchased=False),
            _make_dispatch([DispatchResult(status="sent")]),
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    assert len(result.sent_steps) == 1


# ---------------------------------------------------------------------------
# check_purchase  (CHK-02, CHK-04, CHK-05)
# ---------------------------------------------------------------------------

def test_lead_que_comprou_encerra_como_purchased():
    async def run():
        return await _run(
            _input([_email_step()]),
            _make_check_purchase(purchased=True),
            _make_dispatch([DispatchResult(status="sent")]),
        )

    result = asyncio.run(run())
    assert result.status == "purchased"
    assert result.sent_steps == []  # nenhum step despachado


def test_check_purchase_falha_resulta_em_status_error():
    async def run():
        return await _run(
            _input([_email_step()]),
            _make_check_purchase(raises=True),
            _make_dispatch([DispatchResult(status="sent")]),
        )

    result = asyncio.run(run())
    assert result.status == "error"
    assert "purchase_check failed" in result.last_error
    assert result.sent_steps == []


# ---------------------------------------------------------------------------
# Dispatch — skipped / cap / falhas  (WPP-02, WPP-04, WPP-08)
# ---------------------------------------------------------------------------

def test_whatsapp_desabilitado_step_skipped_e_workflow_continua():
    async def run():
        return await _run(
            _input([_whatsapp_step()]),
            _make_check_purchase(purchased=False),
            _make_dispatch([DispatchResult(status="skipped", reason="whatsapp disabled by config")]),
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    assert result.sent_steps[0].status == "skipped"


def test_cap_diario_atingido_depois_despacha():
    async def run():
        # 1ª chamada: cap_reached (workflow dorme); 2ª: sent.
        return await _run(
            _input([_whatsapp_step()]),
            _make_check_purchase(purchased=False),
            _make_dispatch([
                DispatchResult(status="cap_reached", raw={"next_window_seconds": 60}),
                DispatchResult(status="queued"),
            ]),
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    assert result.sent_steps[0].status == "queued"
    event_types = [e.event_type for e in result.events]
    assert "whatsapp_cap_reached" in event_types


def test_email_falha_nao_retryable_para_workflow():
    async def run():
        return await _run(
            _input([_email_step()], stop_on_step_failure=True),
            _make_check_purchase(purchased=False),
            _make_dispatch([DispatchResult(status="failed", retryable=False, reason="http 500")]),
        )

    result = asyncio.run(run())
    assert result.status == "failed"


def test_whatsapp_falha_nao_para_o_workflow():
    async def run():
        # WhatsApp ignora stop_on_step_failure — step falha mas workflow segue.
        return await _run(
            _input([_whatsapp_step()], stop_on_step_failure=True),
            _make_check_purchase(purchased=False),
            _make_dispatch([DispatchResult(status="failed", retryable=False, reason="number_not_exists")]),
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    assert result.sent_steps == []  # falhou, mas não interrompeu


def test_falha_retryable_retenta_e_depois_envia():
    async def run():
        return await _run(
            _input([_email_step()]),
            _make_check_purchase(purchased=False),
            _make_dispatch([
                DispatchResult(status="failed", retryable=True, reason="http 503"),
                DispatchResult(status="failed", retryable=True, reason="http 503"),
                DispatchResult(status="sent"),
            ]),
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    assert result.sent_steps[0].status == "sent"
    retries = [e for e in result.events if e.event_type == "step_retry"]
    assert len(retries) == 2


# ---------------------------------------------------------------------------
# Sequência vazia  (EDGE-03)
# ---------------------------------------------------------------------------

def test_sequencia_vazia_completa_sem_erro():
    async def run():
        return await _run(
            _input([]),
            _make_check_purchase(purchased=False),
            _make_dispatch([DispatchResult(status="sent")]),
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    assert result.sent_steps == []


# ---------------------------------------------------------------------------
# Signals  (SIG-01, SIG-02, SIG-03)
# ---------------------------------------------------------------------------

def test_cancel_signal_encerra_workflow():
    async def run():
        task_queue = _task_queue()
        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client,
                    task_queue=task_queue,
                    workflows=[LeadRemarketingWorkflow],
                    activities=[
                        _make_check_purchase(purchased=False),
                        _make_dispatch([DispatchResult(status="sent")]),
                        _mock_notify,
                    ],
                    activity_executor=executor,
                ):
                    handle = await env.client.start_workflow(
                        LeadRemarketingWorkflow.run,
                        # delay grande para o workflow ficar esperando
                        _input([_email_step(delay_minutes=60 * 24 * 30)]),
                        id=f"test-{uuid.uuid4().hex}",
                        task_queue=task_queue,
                        execution_timeout=timedelta(days=60),
                    )
                    await handle.signal(LeadRemarketingWorkflow.cancel, "teste")
                    return await handle.result()

    result = asyncio.run(run())
    assert result.status == "canceled"


def test_pause_e_resume():
    async def run():
        task_queue = _task_queue()
        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client,
                    task_queue=task_queue,
                    workflows=[LeadRemarketingWorkflow],
                    activities=[
                        _make_check_purchase(purchased=False),
                        _make_dispatch([DispatchResult(status="sent")]),
                        _mock_notify,
                    ],
                    activity_executor=executor,
                ):
                    handle = await env.client.start_workflow(
                        LeadRemarketingWorkflow.run,
                        _input([_email_step(delay_minutes=60 * 24 * 30)]),
                        id=f"test-{uuid.uuid4().hex}",
                        task_queue=task_queue,
                        execution_timeout=timedelta(days=60),
                    )
                    await handle.signal(LeadRemarketingWorkflow.pause, "teste")
                    # Espera o estado refletir 'paused'
                    paused = False
                    for _ in range(40):
                        st = await handle.query(LeadRemarketingWorkflow.state)
                        if st.status == "paused":
                            paused = True
                            break
                        await asyncio.sleep(0.1)
                    await handle.signal(LeadRemarketingWorkflow.resume)
                    result = await handle.result()
                    return paused, result.status

    paused, status = asyncio.run(run())
    assert paused is True
    assert status == "completed"
