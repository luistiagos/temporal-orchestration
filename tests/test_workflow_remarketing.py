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


def test_email_falha_registra_step_e_workflow_segue():
    """Cada step é independente: a falha do e-mail fica registrada com o
    motivo e o WhatsApp seguinte sai (antes: workflow `failed` no e-mail)."""
    async def run():
        return await _run(
            _input([_email_step(), _whatsapp_step()], stop_on_step_failure=True, **_always_open()),
            _make_check_purchase(purchased=False),
            _make_dispatch([
                DispatchResult(
                    status="failed", retryable=False,
                    reason="http 400: INVALID_EMAIL_OR_TEMPLATE",
                    recipient="lead@example.com",
                ),
                DispatchResult(status="sent", recipient="5541985311304"),
            ]),
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    assert [(s.step_id, s.status) for s in result.sent_steps] == [
        ("email-1", "failed"), ("wpp-1", "sent"),
    ]
    email = result.sent_steps[0]
    assert email.reason == "http 400: INVALID_EMAIL_OR_TEMPLATE"
    assert email.recipient == "lead@example.com"
    assert result.sent_steps[1].recipient == "5541985311304"
    failed = [e for e in result.events if e.event_type == "step_failed"]
    assert failed and failed[0].metadata == {"step_id": "email-1", "cycle": 1}


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
    # falhou, não interrompeu, e o step aparece com o motivo
    assert [(s.step_id, s.status, s.reason) for s in result.sent_steps] == [
        ("wpp-1", "failed", "number_not_exists"),
    ]


def test_email_nao_enviado_por_regra_registra_motivo_e_destinatario():
    """skipped com motivo: o step guarda reason/recipient/override e o evento
    não diz só "skipped"."""
    async def run():
        return await _run(
            _input([_email_step()]),
            _make_check_purchase(purchased=False),
            _make_dispatch([DispatchResult(
                status="skipped", reason="user_has_purchase",
                recipient="dono@example.com", recipient_overridden=True,
                raw={"ok": True, "sent": False, "reason": "user_has_purchase"},
            )]),
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    step = result.sent_steps[0]
    assert (step.status, step.reason, step.recipient, step.recipient_overridden) == (
        "skipped", "user_has_purchase", "dono@example.com", True,
    )
    msgs = [e.message for e in result.events if e.event_type == "step_dispatched"]
    assert msgs == ["email step email-1 skipped — user_has_purchase"]


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


# ---------------------------------------------------------------------------
# Checkout em andamento: PIX gerado e não pago adia o WhatsApp (T3 de
# docs/modules/remarketing/bugs/2026-11-06-abandono-whatsapp-de-1-minuto-dispara-com-o-cliente-no-checkout-e-em-conversa-com-o-agente.md)
# ---------------------------------------------------------------------------

from pathlib import Path

from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from dsg_temporal.workflows.remarketing import (
    CHECKOUT_PENDING_MARGIN_SECONDS,
    CHECKOUT_PENDING_MAX_ROUNDS,
)

_FIXTURES = Path(__file__).parent / "fixtures"


def _make_check_sequence(results):
    """results: lista de (purchased, checkout_pending_seconds); o último repete."""
    calls = {"n": 0}

    @activity.defn(name="check_purchase")
    def _check(payload: PurchaseCheckInput) -> PurchaseCheckResult:
        idx = min(calls["n"], len(results) - 1)
        calls["n"] += 1
        purchased, pending = results[idx]
        return PurchaseCheckResult(purchased=purchased, checkout_pending_seconds=pending)

    _check.calls = calls  # type: ignore[attr-defined]
    return _check


def _checkout_in_progress(defer_seconds=600):
    # Forma exata que a activity devolve para o {"ok": false, ...} do backend (T2).
    return DispatchResult(
        status="failed", retryable=True, reason="checkout_in_progress",
        raw={"ok": False, "retryable": True, "reason": "checkout_in_progress",
             "defer_seconds": defer_seconds, "sent": False},
    )


def _always_open():
    return dict(window_start_hour=0, window_end_hour=24)


def _waits(result):
    return [e for e in result.events if e.event_type == "checkout_pending_wait"]


def test_pix_aberto_espera_e_comprou_encerra_sem_dispatch():
    check = _make_check_sequence([(False, 900), (True, 0)])
    dispatch = _make_dispatch([DispatchResult(status="sent")])

    result = asyncio.run(_run(_input([_whatsapp_step()], **_always_open()), check, dispatch))

    assert result.status == "purchased"
    assert dispatch.calls["n"] == 0
    assert check.calls["n"] == 2
    waits = _waits(result)
    assert len(waits) == 1
    assert waits[0].metadata["wait_seconds"] == 900 + CHECKOUT_PENDING_MARGIN_SECONDS
    assert waits[0].metadata["source"] == "purchase_check"


def test_pix_aberto_espera_e_nao_pagou_envia_uma_vez():
    check = _make_check_sequence([(False, 900), (False, 0)])
    dispatch = _make_dispatch([DispatchResult(status="sent")])

    result = asyncio.run(_run(_input([_whatsapp_step()], **_always_open()), check, dispatch))

    assert result.status == "completed"
    assert dispatch.calls["n"] == 1
    assert [(s.step_id, s.status) for s in result.sent_steps] == [("wpp-1", "sent")]
    assert len(_waits(result)) == 1


def test_dispatch_checkout_in_progress_adia_em_vez_de_pular():
    """Antes do T3: 3 tentativas de 5/10 s e o step era pulado. Agora: espera
    o defer_seconds, re-checa a compra e envia (1 dispatch extra, não 3)."""
    check = _make_check_sequence([(False, 0)])
    dispatch = _make_dispatch([_checkout_in_progress(600), DispatchResult(status="sent")])

    result = asyncio.run(_run(_input([_whatsapp_step()], **_always_open()), check, dispatch))

    assert result.status == "completed"
    assert dispatch.calls["n"] == 2
    assert check.calls["n"] == 2  # re-check depois da espera
    assert [(s.step_id, s.status) for s in result.sent_steps] == [("wpp-1", "sent")]
    waits = _waits(result)
    assert len(waits) == 1
    assert waits[0].metadata["source"] == "dispatch"
    assert waits[0].metadata["wait_seconds"] == 600 + CHECKOUT_PENDING_MARGIN_SECONDS
    assert not [e for e in result.events if e.event_type == "step_retry"]


def test_dispatch_checkout_in_progress_e_depois_comprou():
    check = _make_check_sequence([(False, 0), (True, 0)])
    dispatch = _make_dispatch([_checkout_in_progress(600), DispatchResult(status="sent")])

    result = asyncio.run(_run(_input([_whatsapp_step()], **_always_open()), check, dispatch))

    assert result.status == "purchased"
    assert dispatch.calls["n"] == 1
    assert result.sent_steps == []


def test_teto_de_adiamentos_pula_o_step_e_segue():
    """PIX regerado sem parar: no máximo CHECKOUT_PENDING_MAX_ROUNDS esperas;
    depois o step vai ao dispatch e, com o backend ainda vendo PIX aberto, o
    step de WhatsApp é pulado (o workflow continua)."""
    check = _make_check_sequence([(False, 900)])
    dispatch = _make_dispatch([_checkout_in_progress(900)])

    result = asyncio.run(_run(
        _input([_whatsapp_step()], **_always_open()), check, dispatch, timeout_days=5,
    ))

    assert result.status == "completed"
    # step pulado fica registrado com o motivo (antes: sumia de sent_steps)
    assert [(s.step_id, s.status, s.reason) for s in result.sent_steps] == [
        ("wpp-1", "checkout_pending", "checkout_in_progress"),
    ]
    assert len(_waits(result)) == CHECKOUT_PENDING_MAX_ROUNDS
    assert check.calls["n"] == CHECKOUT_PENDING_MAX_ROUNDS + 1
    assert dispatch.calls["n"] == 1  # teto esgotado: sem 3 retentativas
    assert result.last_error == "checkout_in_progress"


def test_espera_unica_limitada_a_uma_hora():
    check = _make_check_sequence([(False, 10 * 3600), (False, 0)])
    dispatch = _make_dispatch([DispatchResult(status="sent")])

    result = asyncio.run(_run(_input([_whatsapp_step()], **_always_open()), check, dispatch))

    assert _waits(result)[0].metadata["wait_seconds"] == 3600
    assert result.status == "completed"


def test_email_nao_adia_com_pix_aberto():
    check = _make_check_sequence([(False, 900)])
    dispatch = _make_dispatch([DispatchResult(status="sent")])

    result = asyncio.run(_run(_input([_email_step()], **_always_open()), check, dispatch))

    assert result.status == "completed"
    assert check.calls["n"] == 1
    assert dispatch.calls["n"] == 1
    assert _waits(result) == []


def test_status_durante_a_espera_e_ativo_para_o_planner():
    """O status gravado no backend durante a espera precisa estar no conjunto
    de ativos de manageleads_temporal.active_tracking_sql, senão o planner
    recria o workflow."""
    seen = []

    @activity.defn(name="notify_remarketing_event")
    def _notify(payload: NotifyRemarketingEventInput) -> None:
        if payload.event.event_type == "checkout_pending_wait":
            seen.append(payload.state.status)

    async def run():
        task_queue = _task_queue()
        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client, task_queue=task_queue,
                    workflows=[LeadRemarketingWorkflow],
                    activities=[_make_check_sequence([(False, 900), (False, 0)]),
                                _make_dispatch([DispatchResult(status="sent")]), _notify],
                    activity_executor=executor,
                ):
                    return await env.client.execute_workflow(
                        LeadRemarketingWorkflow.run,
                        _input([_whatsapp_step()], **_always_open()),
                        id=f"test-{uuid.uuid4().hex}", task_queue=task_queue,
                        execution_timeout=timedelta(days=2),
                    )

    asyncio.run(run())
    assert seen == ["waiting"]


def test_replay_de_historico_anterior_ao_t3_e_deterministico():
    """Workflow em andamento no deploy: o histórico gravado pelo código de
    ae35d99 (check sem o campo, dispatch checkout_in_progress x3 e step
    pulado) re-executa com o código novo sem não-determinismo."""
    history = WorkflowHistory.from_json(
        "pre-t3",
        (_FIXTURES / "history_pre_t3_checkout_in_progress.json").read_text(encoding="utf-8"),
    )

    async def run():
        await Replayer(workflows=[LeadRemarketingWorkflow]).replay_workflow(history)

    asyncio.run(run())


def test_replay_de_historico_novo_com_espera_e_deterministico():
    async def run():
        task_queue = _task_queue()
        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client, task_queue=task_queue,
                    workflows=[LeadRemarketingWorkflow],
                    activities=[
                        _make_check_sequence([(False, 900), (False, 0)]),
                        _make_dispatch([_checkout_in_progress(600), DispatchResult(status="sent")]),
                        _mock_notify,
                    ],
                    activity_executor=executor,
                ):
                    handle = await env.client.start_workflow(
                        LeadRemarketingWorkflow.run,
                        _input([_whatsapp_step()], **_always_open()),
                        id=f"test-{uuid.uuid4().hex}", task_queue=task_queue,
                        execution_timeout=timedelta(days=2),
                    )
                    result = await handle.result()
                    history = await handle.fetch_history()
        await Replayer(workflows=[LeadRemarketingWorkflow]).replay_workflow(history)
        return result

    result = asyncio.run(run())
    assert len(_waits(result)) == 2
    assert [(s.step_id, s.status) for s in result.sent_steps] == [("wpp-1", "sent")]


# ---------------------------------------------------------------------------
# T4 — cliente em conversa com o agente adia o step de WhatsApp
# docs/modules/remarketing/bugs/2026-11-06-abandono-whatsapp-de-1-minuto-dispara-com-o-cliente-no-checkout-e-em-conversa-com-o-agente.md
# ---------------------------------------------------------------------------

from dsg_temporal.workflows.remarketing import CONVERSATION_ACTIVE_MAX_ROUNDS


def _make_check_conversa(results):
    """results: lista de (purchased, checkout_pending_seconds,
    conversation_active_seconds); o último repete."""
    calls = {"n": 0}

    @activity.defn(name="check_purchase")
    def _check(payload: PurchaseCheckInput) -> PurchaseCheckResult:
        idx = min(calls["n"], len(results) - 1)
        calls["n"] += 1
        purchased, pending, conversa = results[idx]
        return PurchaseCheckResult(
            purchased=purchased,
            checkout_pending_seconds=pending,
            conversation_active_seconds=conversa,
        )

    _check.calls = calls  # type: ignore[attr-defined]
    return _check


def _customer_in_conversation(defer_seconds=420):
    # Forma que a activity devolve para o {"ok": false, ...} do backend (T6).
    return DispatchResult(
        status="failed", retryable=True, reason="customer_in_conversation",
        raw={"ok": False, "retryable": True, "reason": "customer_in_conversation",
             "defer_seconds": defer_seconds, "sent": False},
    )


def _conversa_waits(result):
    return [e for e in result.events if e.event_type == "conversation_active_wait"]


def test_conversa_ativa_espera_e_envia_uma_vez():
    check = _make_check_conversa([(False, 0, 420), (False, 0, 0)])
    dispatch = _make_dispatch([DispatchResult(status="sent")])

    result = asyncio.run(_run(_input([_whatsapp_step()], **_always_open()), check, dispatch))

    assert result.status == "completed"
    assert check.calls["n"] == 2
    assert dispatch.calls["n"] == 1
    assert [(s.step_id, s.status) for s in result.sent_steps] == [("wpp-1", "sent")]
    waits = _conversa_waits(result)
    assert len(waits) == 1
    assert waits[0].metadata["wait_seconds"] == 420 + CHECKOUT_PENDING_MARGIN_SECONDS
    assert waits[0].metadata["source"] == "purchase_check"
    assert waits[0].metadata["round"] == 1
    assert _waits(result) == []


def test_conversa_ativa_espera_e_comprou_encerra_sem_dispatch():
    check = _make_check_conversa([(False, 0, 420), (True, 0, 0)])
    dispatch = _make_dispatch([DispatchResult(status="sent")])

    result = asyncio.run(_run(_input([_whatsapp_step()], **_always_open()), check, dispatch))

    assert result.status == "purchased"
    assert dispatch.calls["n"] == 0


def test_dispatch_customer_in_conversation_adia_em_vez_de_pular():
    """Sem o T4, o worker trata o reason como falha retryable: 3 tentativas
    em 30 s e o step pulado (a opcao C, que o dono recusou)."""
    check = _make_check_conversa([(False, 0, 0)])
    dispatch = _make_dispatch([_customer_in_conversation(420), DispatchResult(status="sent")])

    result = asyncio.run(_run(_input([_whatsapp_step()], **_always_open()), check, dispatch))

    assert result.status == "completed"
    assert dispatch.calls["n"] == 2
    assert check.calls["n"] == 2
    assert [(s.step_id, s.status) for s in result.sent_steps] == [("wpp-1", "sent")]
    waits = _conversa_waits(result)
    assert len(waits) == 1
    assert waits[0].metadata["source"] == "dispatch"
    assert waits[0].metadata["wait_seconds"] == 420 + CHECKOUT_PENDING_MARGIN_SECONDS
    assert not [e for e in result.events if e.event_type == "step_retry"]


def test_teto_de_adiamentos_da_conversa_pula_o_step_e_segue():
    check = _make_check_conversa([(False, 0, 600)])
    dispatch = _make_dispatch([_customer_in_conversation(600), DispatchResult(status="sent")])
    email_depois = RemarketingStep(
        step_id="email-2", order=3, channel="email",
        template="remarket/email.html", subject="Volte!", delay_minutes=0,
    )
    seq = [_whatsapp_step(), email_depois]  # steps rodam por `order`

    result = asyncio.run(_run(
        _input(seq, stop_on_step_failure=False, **_always_open()), check, dispatch, timeout_days=5,
    ))

    assert result.status == "completed"
    assert len(_conversa_waits(result)) == CONVERSATION_ACTIVE_MAX_ROUNDS
    # 1 dispatch do WhatsApp (teto esgotado, sem 3 retentativas) + 1 do e-mail
    assert dispatch.calls["n"] == 2
    assert [(s.step_id, s.status) for s in result.sent_steps] == [
        ("wpp-1", "conversation_active"), ("email-2", "sent"),
    ]
    assert [e.message for e in result.events if e.event_type == "step_failed"] == [
        "customer_in_conversation"
    ]


def test_checkout_e_conversa_ao_mesmo_tempo_esperam_o_maior():
    check = _make_check_conversa([(False, 900, 420), (False, 0, 0)])
    dispatch = _make_dispatch([DispatchResult(status="sent")])

    result = asyncio.run(_run(_input([_whatsapp_step()], **_always_open()), check, dispatch))

    assert result.status == "completed"
    assert check.calls["n"] == 2  # UMA espera, nao duas
    waits = _waits(result)
    assert len(waits) == 1 and _conversa_waits(result) == []
    assert waits[0].metadata["wait_seconds"] == 900 + CHECKOUT_PENDING_MARGIN_SECONDS
    assert waits[0].metadata["conversation_active_seconds"] == 420

    check = _make_check_conversa([(False, 300, 600), (False, 0, 0)])
    result = asyncio.run(_run(_input([_whatsapp_step()], **_always_open()), check,
                              _make_dispatch([DispatchResult(status="sent")])))
    waits = _conversa_waits(result)
    assert len(waits) == 1 and _waits(result) == []
    assert waits[0].metadata["wait_seconds"] == 600 + CHECKOUT_PENDING_MARGIN_SECONDS


def test_email_nao_adia_com_conversa_ativa():
    check = _make_check_conversa([(False, 0, 600)])
    dispatch = _make_dispatch([DispatchResult(status="sent")])

    result = asyncio.run(_run(_input([_email_step()], **_always_open()), check, dispatch))

    assert result.status == "completed"
    assert check.calls["n"] == 1
    assert dispatch.calls["n"] == 1
    assert _conversa_waits(result) == []


def test_replay_de_historico_do_t3_e_deterministico():
    """Workflow em andamento no deploy do T4: historico gravado pelo codigo
    de a89d238 (checkout_pending_wait pelo check e pelo dispatch, com o
    marcador `checkout-pending-wait`) re-executa sem nao-determinismo."""
    history = WorkflowHistory.from_json(
        "t3",
        (_FIXTURES / "history_t3_checkout_pending_wait.json").read_text(encoding="utf-8"),
    )

    async def run():
        await Replayer(workflows=[LeadRemarketingWorkflow]).replay_workflow(history)

    asyncio.run(run())


def test_replay_de_historico_novo_com_conversa_e_deterministico():
    async def run():
        task_queue = _task_queue()
        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client, task_queue=task_queue,
                    workflows=[LeadRemarketingWorkflow],
                    activities=[
                        _make_check_conversa([(False, 900, 420), (False, 0, 300), (False, 0, 0)]),
                        _make_dispatch([_customer_in_conversation(200), DispatchResult(status="sent")]),
                        _mock_notify,
                    ],
                    activity_executor=executor,
                ):
                    handle = await env.client.start_workflow(
                        LeadRemarketingWorkflow.run,
                        _input([_whatsapp_step()], **_always_open()),
                        id=f"test-{uuid.uuid4().hex}", task_queue=task_queue,
                        execution_timeout=timedelta(days=2),
                    )
                    result = await handle.result()
                    history = await handle.fetch_history()
        await Replayer(workflows=[LeadRemarketingWorkflow]).replay_workflow(history)
        return result

    result = asyncio.run(run())
    assert len(_waits(result)) == 1
    assert len(_conversa_waits(result)) == 2
    assert [(s.step_id, s.status) for s in result.sent_steps] == [("wpp-1", "sent")]


# ---------------------------------------------------------------------------
# Steps independentes: falha de e-mail não para o workflow
# docs/modules/remarketing/bugs/2026-11-24-detalhe-do-workflow-mostra-email-como-enviado-sem-destinatario-nem-motivo.md
# ---------------------------------------------------------------------------

def test_replay_de_historico_anterior_aos_steps_independentes_e_deterministico():
    """Histórico gravado pelo código de 1d8e524 (e-mail falha 400 não
    retentável + WhatsApp; o workflow termina `failed` no e-mail, sem o
    marcador do patch) reexecuta pelo caminho antigo sem não-determinismo."""
    history = WorkflowHistory.from_json(
        "pre-step-independent",
        (_FIXTURES / "history_pre_step_independent_email_failed.json").read_text(encoding="utf-8"),
    )

    async def run():
        await Replayer(workflows=[LeadRemarketingWorkflow]).replay_workflow(history)

    asyncio.run(run())


def test_replay_de_historico_novo_com_falha_de_email_e_deterministico():
    async def run():
        task_queue = _task_queue()
        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client, task_queue=task_queue,
                    workflows=[LeadRemarketingWorkflow],
                    activities=[
                        _make_check_purchase(purchased=False),
                        _make_dispatch([
                            DispatchResult(status="failed", retryable=False, reason="sendmail_failed"),
                            DispatchResult(status="sent"),
                        ]),
                        _mock_notify,
                    ],
                    activity_executor=executor,
                ):
                    handle = await env.client.start_workflow(
                        LeadRemarketingWorkflow.run,
                        _input([_email_step(), _whatsapp_step()], **_always_open()),
                        id=f"test-{uuid.uuid4().hex}", task_queue=task_queue,
                        execution_timeout=timedelta(days=2),
                    )
                    result = await handle.result()
                    history = await handle.fetch_history()
        await Replayer(workflows=[LeadRemarketingWorkflow]).replay_workflow(history)
        return result

    result = asyncio.run(run())
    assert result.status == "completed"
    assert [(s.step_id, s.status) for s in result.sent_steps] == [
        ("email-1", "failed"), ("wpp-1", "sent"),
    ]


# ---------------------------------------------------------------------------
# Step relativo ao anterior (step de cupom uma hora depois do WhatsApp)
# docs/modules/remarketing/changes/2026-11-27-step-de-cupom-uma-hora-depois-do-whatsapp/spec.md
#
# O step relativo recebe um `send_at_iso` de proposito: sem ele,
# `compute_due_at` ja usa max(base, now) e o teste passaria no codigo antigo.
# ---------------------------------------------------------------------------

from datetime import UTC, datetime

from dsg_temporal.schedule import parse_iso_datetime


def _cupom_step(*, delay_minutes=60, send_at_iso=None, relative=True, order=3):
    return RemarketingStep(
        step_id="wpp-cupom", order=order, channel="whatsapp",
        template="remarket/wpp_remarket_cupom_ps2.txt", delay_minutes=delay_minutes,
        send_at_iso=send_at_iso, delay_from_previous=relative,
    )


def _wpp2(step_id="wpp-2"):
    return RemarketingStep(step_id=step_id, order=2, channel="whatsapp", template="oi",
                           delay_minutes=1)


def _sent_at(result, step_id):
    return [parse_iso_datetime(s.sent_at_iso) for s in result.sent_steps if s.step_id == step_id]


def _inicio(result):
    return next(parse_iso_datetime(e.at_iso) for e in result.events
                if e.event_type == "workflow_started")


def _iso_em(minutos):
    """Instante absoluto como o planner manda (lead + atraso), a partir do relogio de agora:
    o ambiente time-skipping comeca no relogio real."""
    return (datetime.now(UTC) + timedelta(minutes=minutos)).isoformat()


def test_step_relativo_ao_anterior_conta_do_despacho_anterior():
    """EARS-1: o anterior so despacha 30 min depois (pacing); o relativo sai 60 min
    depois do REGISTRO do anterior, nao no send_at_iso (lead + 60)."""
    calls = []

    @activity.defn(name="dispatch_remarketing_step")
    def _dispatch(payload: DispatchStepInput) -> DispatchResult:
        calls.append(payload.step.step_id)
        if len(calls) == 1:
            return DispatchResult(status="pacing_required", raw={"wait_seconds": 30 * 60})
        return DispatchResult(status="sent")

    async def run():
        return await _run(
            _input([_wpp2(), _cupom_step(send_at_iso=_iso_em(60))], **_always_open()),
            _make_check_purchase(purchased=False),
            _dispatch,
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    (anterior,) = _sent_at(result, "wpp-2")
    (cupom,) = _sent_at(result, "wpp-cupom")
    assert anterior - _inicio(result) >= timedelta(minutes=30)
    assert cupom - anterior >= timedelta(minutes=60)
    assert cupom - anterior < timedelta(minutes=61)


def test_step_relativo_conta_mesmo_com_o_anterior_falho():
    """EARS-1: falho tambem conta como despacho do anterior."""
    @activity.defn(name="dispatch_remarketing_step")
    def _dispatch(payload: DispatchStepInput) -> DispatchResult:
        if payload.step.step_id == "wpp-2":
            return DispatchResult(status="failed", retryable=False, reason="numero_inexistente")
        return DispatchResult(status="sent")

    async def run():
        return await _run(
            _input([_wpp2(), _cupom_step(send_at_iso=_iso_em(1))], **_always_open()),
            _make_check_purchase(purchased=False),
            _dispatch,
        )

    result = asyncio.run(run())
    assert [(s.step_id, s.status) for s in result.sent_steps] == [
        ("wpp-2", "failed"), ("wpp-cupom", "sent"),
    ]
    assert _sent_at(result, "wpp-cupom")[0] - _sent_at(result, "wpp-2")[0] >= timedelta(minutes=60)


def test_step_relativo_conta_depois_do_adiamento_da_janela():
    """EARS-2: o anterior e adiado pela janela (12 h de pacing > 11 h de janela 09-20
    cruza o fim sempre); o relativo conta do despacho real, depois do adiamento."""
    calls = []

    @activity.defn(name="dispatch_remarketing_step")
    def _dispatch(payload: DispatchStepInput) -> DispatchResult:
        calls.append(payload.step.step_id)
        if len(calls) == 1:
            return DispatchResult(status="pacing_required", raw={"wait_seconds": 12 * 3600})
        return DispatchResult(status="sent")

    async def run():
        return await _run(
            _input([_wpp2(), _cupom_step(delay_minutes=60, send_at_iso=_iso_em(60))],
                   window_start_hour=9, window_end_hour=20),
            _make_check_purchase(purchased=False),
            _dispatch,
            timeout_days=4,
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    event_types = [e.event_type for e in result.events]
    assert "send_window_wait" in event_types
    anterior = _sent_at(result, "wpp-2")[0]
    cupom = _sent_at(result, "wpp-cupom")[0]
    assert anterior - _inicio(result) >= timedelta(hours=12)
    assert cupom - anterior >= timedelta(minutes=60)


def test_step_relativo_respeita_a_janela():
    """EARS-3: 12 h depois do anterior pode cair fora de 09-20 BRT; o relativo sai sempre
    dentro da janela e nunca antes do atraso."""
    async def run():
        return await _run(
            _input([_wpp2(), _cupom_step(delay_minutes=12 * 60, send_at_iso=_iso_em(1))],
                   window_start_hour=9, window_end_hour=20),
            _make_check_purchase(purchased=False),
            _make_dispatch([DispatchResult(status="sent")]),
            timeout_days=4,
        )

    result = asyncio.run(run())
    assert result.status == "completed"
    anterior = _sent_at(result, "wpp-2")[0]
    cupom = _sent_at(result, "wpp-cupom")[0]
    assert cupom - anterior >= timedelta(hours=12)
    hora_brt = (cupom - timedelta(hours=3)).hour
    assert 9 <= hora_brt < 20


def test_step_relativo_nao_envia_apos_compra():
    """EARS-5: comprou entre o anterior e o relativo -> purchased, cupom nao despacha."""
    dispatch = _make_dispatch([DispatchResult(status="sent")])

    async def run():
        return await _run(
            _input([_wpp2(), _cupom_step(send_at_iso=_iso_em(60))], **_always_open()),
            _make_check_sequence([(False, 0), (True, 0)]),
            dispatch,
        )

    result = asyncio.run(run())
    assert result.status == "purchased"
    assert [s.step_id for s in result.sent_steps] == ["wpp-2"]
    assert dispatch.calls["n"] == 1


def test_step_relativo_primeiro_da_sequencia_conta_do_lead():
    """Sem anterior, o relativo conta da criacao do lead (cycle_anchor), nao do send_at_iso."""
    async def run():
        return await _run(
            _input([_cupom_step(delay_minutes=60, send_at_iso=_iso_em(5), order=1)],
                   **_always_open()),
            _make_check_purchase(purchased=False),
            _make_dispatch([DispatchResult(status="sent")]),
        )

    result = asyncio.run(run())
    assert _sent_at(result, "wpp-cupom")[0] - _inicio(result) >= timedelta(minutes=60)


def test_step_absoluto_inalterado():
    """EARS-4: sem a flag, o send_at_iso explicito continua vencendo (sai em ~5 min,
    mesmo com delay de 60)."""
    async def run():
        return await _run(
            _input([_wpp2(), _cupom_step(send_at_iso=_iso_em(5), relative=False)],
                   **_always_open()),
            _make_check_purchase(purchased=False),
            _make_dispatch([DispatchResult(status="sent")]),
        )

    result = asyncio.run(run())
    assert _sent_at(result, "wpp-cupom")[0] - _inicio(result) < timedelta(minutes=30)


def test_api_aceita_o_campo_e_o_ausente_vira_falso():
    # api.py importa fastapi no topo; o CI instala `.[dev]`, a maquina local pode nao ter.
    pytest.importorskip("fastapi")
    from dsg_temporal.api import RemarketingStepRequest

    base = dict(step_id="1", order=1, channel="whatsapp", template="t")
    assert RemarketingStepRequest(**base).to_dataclass().delay_from_previous is False
    assert RemarketingStepRequest(**base, delay_from_previous=True).to_dataclass().delay_from_previous is True


def test_replay_de_historico_novo_com_step_relativo_e_deterministico():
    async def run():
        task_queue = _task_queue()
        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client, task_queue=task_queue,
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
                        _input([_email_step(), _wpp2(), _cupom_step(send_at_iso=_iso_em(60))],
                               **_always_open()),
                        id=f"test-{uuid.uuid4().hex}", task_queue=task_queue,
                        execution_timeout=timedelta(days=2),
                    )
                    result = await handle.result()
                    history = await handle.fetch_history()
        await Replayer(workflows=[LeadRemarketingWorkflow]).replay_workflow(history)
        return result

    result = asyncio.run(run())
    assert [s.step_id for s in result.sent_steps] == ["email-1", "wpp-2", "wpp-cupom"]
