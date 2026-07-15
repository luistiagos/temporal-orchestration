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


def _input(sequence, *, max_cycles=1, stop_on_step_failure=True,
           window_start_hour=None, window_end_hour=None):
    # window 0–24 nos testes de pacing = "janela sempre aberta", para o
    # resultado não depender da hora do dia em que a suíte roda.
    return LeadRemarketingInput(
        tenant_id="test",
        lead_id=999,
        campaign_id="cart",
        email="emuladores.emuladores@gmail.com",
        phone="(41) 98531-1304",
        max_cycles=max_cycles,
        stop_on_step_failure=stop_on_step_failure,
        window_start_hour=window_start_hour,
        window_end_hour=window_end_hour,
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


def test_whatsapp_pacing_workflow_sleep():
    calls = []

    @activity.defn(name="dispatch_remarketing_step")
    def _dispatch(payload: DispatchStepInput) -> DispatchResult:
        calls.append(payload.bypass_pacing)
        if not payload.bypass_pacing:
            return DispatchResult(status="pacing_required", raw={"wait_seconds": 120})
        return DispatchResult(status="sent")

    async def run():
        return await _run(
            _input([_whatsapp_step()], window_start_hour=0, window_end_hour=24),
            _make_check_purchase(purchased=False),
            _dispatch,
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    assert len(result.sent_steps) == 1
    assert result.sent_steps[0].status == "sent"
    assert calls == [False, True]

    event_types = [e.event_type for e in result.events]
    assert "whatsapp_pacing_wait" in event_types


def test_pacing_apos_cap_volta_para_a_fila_sem_bypass():
    """G1: quem passou por pacing e caiu no cap NÃO pode acordar às 09:00
    com bypass ligado — o dispatch pós-cap deve voltar com bypass_pacing=False
    (entra na fila de pacing em vez de disparar em rajada)."""
    calls = []

    @activity.defn(name="dispatch_remarketing_step")
    def _dispatch(payload: DispatchStepInput) -> DispatchResult:
        calls.append(payload.bypass_pacing)
        if len(calls) == 1:
            return DispatchResult(status="pacing_required", raw={"wait_seconds": 5})
        if len(calls) == 2:
            return DispatchResult(status="cap_reached", raw={"next_window_seconds": 60})
        return DispatchResult(status="sent")

    async def run():
        return await _run(
            _input([_whatsapp_step()], window_start_hour=0, window_end_hour=24),
            _make_check_purchase(purchased=False),
            _dispatch,
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    assert result.sent_steps[0].status == "sent"
    assert calls == [False, True, False]


def test_pacing_que_cruza_a_janela_re_adia_e_reseta_bypass():
    """G3: a espera na fila de pacing pode cruzar o fim da janela de envio
    (20:00 BRT). O workflow deve re-adiar para a próxima janela e voltar para
    a fila (bypass off), nunca enviar fora do horário comercial."""
    calls = []

    @activity.defn(name="dispatch_remarketing_step")
    def _dispatch(payload: DispatchStepInput) -> DispatchResult:
        calls.append(payload.bypass_pacing)
        if len(calls) == 1:
            # 12h de espera > 11h de janela (09–20) => cruza o fim SEMPRE,
            # independente da hora em que o teste roda.
            return DispatchResult(status="pacing_required", raw={"wait_seconds": 12 * 3600})
        return DispatchResult(status="sent")

    async def run():
        return await _run(
            _input([_whatsapp_step()], window_start_hour=9, window_end_hour=20),
            _make_check_purchase(purchased=False),
            _dispatch,
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    assert result.sent_steps[0].status == "sent"
    # O 2º dispatch veio SEM bypass (voltou para a fila após re-adiar).
    assert calls == [False, False]
    event_types = [e.event_type for e in result.events]
    idx_pacing = event_types.index("whatsapp_pacing_wait")
    assert "send_window_wait" in event_types[idx_pacing:]


def test_purchase_confirmed_durante_pacing_nao_envia():
    """G2: signal purchase_confirmed recebido DURANTE o sleep de pacing deve
    impedir o envio (antes o branch de pacing não checava signals)."""
    calls = []

    @activity.defn(name="dispatch_remarketing_step")
    def _dispatch(payload: DispatchStepInput) -> DispatchResult:
        calls.append(payload.bypass_pacing)
        if len(calls) == 1:
            return DispatchResult(status="pacing_required", raw={"wait_seconds": 100000})
        return DispatchResult(status="sent")

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
                        _dispatch,
                        _mock_notify,
                    ],
                    activity_executor=executor,
                ):
                    handle = await env.client.start_workflow(
                        LeadRemarketingWorkflow.run,
                        _input([_whatsapp_step()], window_start_hour=0, window_end_hour=24),
                        id=f"test-{uuid.uuid4().hex}",
                        task_queue=task_queue,
                        execution_timeout=timedelta(days=60),
                    )
                    # Espera o workflow entrar no sleep de pacing.
                    for _ in range(100):
                        st = await handle.query(LeadRemarketingWorkflow.state)
                        if st.status == "waiting_pacing":
                            break
                        await asyncio.sleep(0.1)
                    await handle.signal(LeadRemarketingWorkflow.purchase_confirmed)
                    return await handle.result()

    result = asyncio.run(run())
    # Só o 1º dispatch aconteceu; o envio pós-sleep foi suprimido.
    assert calls == [False]
    assert all(s.status != "sent" for s in result.sent_steps)


def test_espera_longa_recheca_compra_e_encerra_purchased():
    """G4: espera de pacing >= PACING_RECHECK_PURCHASE_SECONDS deve re-checar
    a compra antes do envio — o lead pode ter comprado enquanto esperava na
    fila (nada no backend emite o signal purchase_confirmed)."""
    dispatch_calls = []
    check_calls = {"n": 0}

    @activity.defn(name="check_purchase")
    def _check(payload: PurchaseCheckInput) -> PurchaseCheckResult:
        check_calls["n"] += 1
        # 1ª checagem (portão do step): não comprou. Re-check pós-fila: comprou.
        return PurchaseCheckResult(purchased=check_calls["n"] >= 2)

    @activity.defn(name="dispatch_remarketing_step")
    def _dispatch(payload: DispatchStepInput) -> DispatchResult:
        dispatch_calls.append(payload.bypass_pacing)
        if len(dispatch_calls) == 1:
            return DispatchResult(status="pacing_required", raw={"wait_seconds": 700})
        return DispatchResult(status="sent")

    async def run():
        return await _run(
            _input(
                [_whatsapp_step("wpp-1"), _whatsapp_step("wpp-2")],
                window_start_hour=0, window_end_hour=24,
            ),
            _check,
            _dispatch,
        )

    result = asyncio.run(run())
    # Comprou durante a fila: nada foi enviado e a sequência parou.
    assert result.status == "purchased"
    assert dispatch_calls == [False]
    assert check_calls["n"] == 2
    assert all(s.status != "sent" for s in result.sent_steps)


def test_pacing_required_repetido_no_bypass_re_dorme():
    """G5 (lado workflow): a activity pode devolver pacing_required de novo
    mesmo com bypass=True (piso contra o DB pós-restart do worker). O
    workflow deve dormir de novo e re-tentar, não falhar."""
    calls = []

    @activity.defn(name="dispatch_remarketing_step")
    def _dispatch(payload: DispatchStepInput) -> DispatchResult:
        calls.append(payload.bypass_pacing)
        if len(calls) <= 2:
            return DispatchResult(status="pacing_required", raw={"wait_seconds": 30})
        return DispatchResult(status="sent")

    async def run():
        return await _run(
            _input([_whatsapp_step()], window_start_hour=0, window_end_hour=24),
            _make_check_purchase(purchased=False),
            _dispatch,
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    assert result.sent_steps[0].status == "sent"
    assert calls == [False, True, True]
    event_types = [e.event_type for e in result.events]
    assert event_types.count("whatsapp_pacing_wait") == 2
