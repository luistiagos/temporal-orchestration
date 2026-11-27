from __future__ import annotations

import asyncio
from datetime import datetime, time, timedelta, timezone
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

# Janela de envio permitida em horário de Brasília (UTC-3). Vale para TODOS os
# canais de remarketing (e-mail + WhatsApp). A janela é configurável por
# campanha (loja) — os valores chegam no payload do workflow; estes são apenas
# o DEFAULT quando a campanha não tem janela própria. Envios fora da janela são
# adiados para o próximo horário de início (ex.: 09:00 BRT).
WHATSAPP_TZ_OFFSET_HOURS = -3
WHATSAPP_WINDOW_START_HOUR = 9
WHATSAPP_WINDOW_END_HOUR = 20  # 20:00 = limite superior (não envia >= 20:00)

# Espera de pacing a partir da qual a compra é re-checada antes do envio.
# check_purchase roda antes de entrar na fila de pacing; com ~1 posição de
# fila em prod (min..max 400..800s) o lead pode comprar enquanto espera.
PACING_RECHECK_PURCHASE_SECONDS = 600

# Checkout em andamento (PIX gerado e não pago, ver /user_has_purchase):
# o step de WhatsApp é ADIADO até a janela do PIX fechar, não pulado.
# docs/modules/remarketing/bugs/2026-11-06-abandono-whatsapp-de-1-minuto-dispara-com-o-cliente-no-checkout-e-em-conversa-com-o-agente.md
CHECKOUT_PENDING_PATCH = "checkout-pending-wait"
CHECKOUT_PENDING_REASON = "checkout_in_progress"  # reason do dispatch (backend)
CHECKOUT_PENDING_STATUS = "checkout_pending"  # DispatchResult interno do workflow
# Teto de adiamentos por step: quem regera PIX sem parar não segura o step
# para sempre (~3 h com a janela de 30 min do backend).
CHECKOUT_PENDING_MAX_ROUNDS = 6
# Folga sobre o tempo devolvido pelo backend: quem escreve a janela é o
# relógio dele, não o do workflow.
CHECKOUT_PENDING_MARGIN_SECONDS = 15
CHECKOUT_PENDING_MAX_WAIT_SECONDS = 3600

# Cliente em conversa com o agente (última mensagem DO CLIENTE nos últimos
# N min, ver /user_has_purchase): o step de WhatsApp também é ADIADO, com
# patch e contador próprios — o histórico gravado entre o deploy deste
# worker e o do backend tem de dar replay. Decisão do dono (opção B) no doc
# acima, seção "T4 — decisao do dono e design".
CONVERSATION_ACTIVE_PATCH = "conversation-active-wait"
CONVERSATION_ACTIVE_REASON = "customer_in_conversation"  # reason do dispatch (backend)
CONVERSATION_ACTIVE_STATUS = "conversation_active"  # DispatchResult interno do workflow
# ~1 h de conversa contínua (6 x (600 s + folga)); esgotado, o step é pulado
# (o agente já está atendendo) e os seguintes continuam.
CONVERSATION_ACTIVE_MAX_ROUNDS = 6

# Falha de step em canal != WhatsApp não para mais o workflow: o step fica
# registrado em sent_steps com o motivo e o workflow segue para os próximos.
# docs/modules/remarketing/bugs/2026-11-24-detalhe-do-workflow-mostra-email-como-enviado-sem-destinatario-nem-motivo.md
STEP_FAILURE_INDEPENDENT_PATCH = "step-failure-independent"


def _normalize_window(start_hour: int | None, end_hour: int | None) -> tuple[int, int]:
    """Normaliza (start, end) em horas inteiras BRT. start 0–23, end 1–24, e
    exige start < end. Qualquer inconsistência cai no default 09:00–20:00."""
    try:
        s = int(start_hour)
        e = int(end_hour)
    except (TypeError, ValueError):
        return WHATSAPP_WINDOW_START_HOUR, WHATSAPP_WINDOW_END_HOUR
    s = max(0, min(s, 23))
    e = max(1, min(e, 24))
    if s >= e:
        return WHATSAPP_WINDOW_START_HOUR, WHATSAPP_WINDOW_END_HOUR
    return s, e


def _whatsapp_next_allowed(
    now_utc: datetime,
    start_hour: int | None = None,
    end_hour: int | None = None,
) -> datetime:
    """Se o horário (em BRT) cai fora da janela [start, end), retorna o próximo
    `start` BRT; caso contrário retorna o próprio now_utc (sem adiamento).
    Sem start/end usa o default 09:00–20:00."""
    start_hour, end_hour = _normalize_window(
        WHATSAPP_WINDOW_START_HOUR if start_hour is None else start_hour,
        WHATSAPP_WINDOW_END_HOUR if end_hour is None else end_hour,
    )
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    brt = now_utc + timedelta(hours=WHATSAPP_TZ_OFFSET_HOURS)
    hour = brt.hour
    if start_hour <= hour < end_hour:
        return now_utc
    target_brt = brt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
        hours=start_hour
    )
    if hour >= end_hour:
        target_brt = target_brt + timedelta(days=1)
    # Volta para UTC
    return (target_brt - timedelta(hours=WHATSAPP_TZ_OFFSET_HOURS)).replace(
        tzinfo=timezone.utc
    )


def _whatsapp_resume_after_cap(
    now_utc: datetime,
    cap_reset_seconds: float,
    start_hour: int | None = None,
    end_hour: int | None = None,
) -> datetime:
    """Instante (UTC) em que o envio pode retomar após o cap diário ser atingido.

    O cap reseta à meia-noite BRT (`cap_reset_seconds` à frente), mas 00:00 BRT
    está FORA da janela de envio. A retomada precisa satisfazer as DUAS janelas:
    cap já resetado E dentro do horário comercial. Compondo: avança até o reset
    do cap e então aplica a guarda de janela — o que empurra a meia-noite para
    o próximo horário de início (ex.: 09:00 BRT).
    """
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    cap_reset_at = now_utc + timedelta(seconds=max(0.0, cap_reset_seconds))
    return _whatsapp_next_allowed(cap_reset_at, start_hour, end_hour)


def _int_or_zero(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


from dsg_temporal.ids import remarketing_idempotency_key
from dsg_temporal.schedule import compute_due_at, parse_iso_datetime
from dsg_temporal.schemas import (
    DispatchResult,
    DispatchStepInput,
    LeadRemarketingInput,
    NotifyRemarketingEventInput,
    PurchaseCheckInput,
    RemarketingWorkflowState,
    SentRemarketingStep,
    WorkflowEvent,
)

with workflow.unsafe.imports_passed_through():
    from dsg_temporal.activities.remarketing import (
        check_purchase,
        dispatch_remarketing_step,
        notify_remarketing_event,
    )


@workflow.defn
class LeadRemarketingWorkflow:
    def __init__(self) -> None:
        self._state = RemarketingWorkflowState(
            status="created",
            tenant_id="",
            lead_id="",
            campaign_id="",
        )
        self._input: LeadRemarketingInput | None = None
        self._pause_requested = False
        self._cancel_requested = False
        self._purchase_confirmed = False
        self._confirm_current_step = False
        self._retry_current_step = False

    @workflow.run
    async def run(self, payload: LeadRemarketingInput) -> RemarketingWorkflowState:
        self._input = payload
        self._state = RemarketingWorkflowState(
            status="running",
            tenant_id=payload.tenant_id,
            lead_id=payload.lead_id,
            campaign_id=payload.campaign_id,
        )
        self._add_event("workflow_started", "Remarketing workflow started")
        await self._notify_last_event()

        sequence = sorted(payload.sequence, key=lambda step: step.order)
        if not sequence:
            self._state.status = "completed"
            self._add_event("workflow_completed", "No remarketing steps configured")
            await self._notify_last_event()
            return self._state

        max_cycles = int(payload.max_cycles or 1)
        unlimited = max_cycles <= 0
        cycle = 1
        cycle_anchor = parse_iso_datetime(payload.lead_created_at_iso) or workflow.now()
        last_dispatched_at_by_step: dict[str, Any] = {}

        while unlimited or cycle <= max_cycles:
            cycle_sequence = sequence
            if cycle > 1:
                cycle_sequence = [
                    step
                    for step in sequence
                    if step.repeat_after_days and step.repeat_after_days > 0
                ]
                if not cycle_sequence:
                    break

            self._state.current_cycle = cycle
            # Instante do registro do step anterior NESTE ciclo (qualquer
            # desfecho): base do step relativo. None = ainda nao houve anterior.
            previous_recorded_at = None
            for step in cycle_sequence:
                if await self._stop_if_requested():
                    return self._state

                self._state.current_step_id = step.step_id
                self._state.status = "waiting"
                base_at = cycle_anchor
                delay_minutes = step.delay_minutes
                send_at_iso = step.send_at_iso if cycle == 1 else None
                # Step relativo: conta do registro do anterior, nunca do horario
                # explicito. Sem anterior (e o primeiro), conta do lead. Ramo so
                # entra com o campo verdadeiro: historico antigo nao passa aqui.
                if cycle == 1 and getattr(step, "delay_from_previous", False):
                    base_at = previous_recorded_at or cycle_anchor
                    send_at_iso = None
                if cycle > 1:
                    base_at = last_dispatched_at_by_step.get(step.step_id, workflow.now())
                    delay_minutes = int(step.repeat_after_days or 0) * 24 * 60
                    delay_minutes += int(step.delay_minutes or 0)
                due_at = compute_due_at(
                    now=workflow.now(),
                    base_at=base_at,
                    delay_minutes=delay_minutes,
                    send_at_iso=send_at_iso,
                    preferred_time=step.preferred_time,
                    preferred_day=step.preferred_day,
                )
                await self._wait_until_ready(due_at)
                if await self._stop_if_requested():
                    return self._state

                is_whatsapp = (step.channel or "").strip().lower() == "whatsapp"
                checkout_rounds = 0
                conversation_rounds = 0
                while True:
                    try:
                        purchase = await self._run_purchase_check()
                    except Exception as exc:
                        # Todas as retentativas esgotadas. NÃO assumimos compra —
                        # o status fica 'error' para tornar a falha visível e
                        # diferenciá-la de 'purchased'. O planner pode recriar o
                        # workflow em uma próxima execução.
                        self._state.status = "error"
                        self._state.last_error = f"purchase_check failed: {str(exc)[:300]}"
                        self._add_event("purchase_check_failed", self._state.last_error)
                        await self._notify_last_event()
                        return self._state

                    if purchase.purchased:
                        self._state.status = "purchased"
                        self._add_event("workflow_stopped", purchase.reason or "Lead purchased")
                        await self._notify_last_event()
                        return self._state

                    # PIX gerado e não pago: o cliente está no checkout. Adia o
                    # WhatsApp até a janela do PIX fechar e re-checa a compra.
                    # Lê o CAMPO (não o raw): histórico anterior ao patch não o
                    # tem, então o replay dele nunca entra aqui.
                    # Cliente em conversa com o agente: mesmo adiamento, patch e
                    # contador próprios. Os dois ao mesmo tempo: UMA espera, a
                    # maior. A ordem das chamadas a `patched` (checkout antes)
                    # é a do código do T3, e histórico gravado por ele tem
                    # conversation_active_seconds == 0: o segundo patch nunca é
                    # consultado no replay dele.
                    wait_checkout = 0
                    wait_conversation = 0
                    if (
                        is_whatsapp
                        and purchase.checkout_pending_seconds > 0
                        and checkout_rounds < CHECKOUT_PENDING_MAX_ROUNDS
                        and workflow.patched(CHECKOUT_PENDING_PATCH)
                    ):
                        checkout_rounds += 1
                        wait_checkout = purchase.checkout_pending_seconds
                    if (
                        is_whatsapp
                        and purchase.conversation_active_seconds > 0
                        and conversation_rounds < CONVERSATION_ACTIVE_MAX_ROUNDS
                        and workflow.patched(CONVERSATION_ACTIVE_PATCH)
                    ):
                        conversation_rounds += 1
                        wait_conversation = purchase.conversation_active_seconds
                    if wait_checkout or wait_conversation:
                        await self._wait_deferral(
                            step, cycle, wait_checkout, wait_conversation,
                            checkout_rounds, conversation_rounds, "purchase_check",
                        )
                        if await self._stop_if_requested():
                            return self._state
                        continue

                    # Janela de envio (TODOS os canais): se o horário atual cai fora
                    # da janela da campanha (default 09:00–20:00 BRT), adia o
                    # despacho para o próximo início de janela.
                    now_utc = workflow.now()
                    allowed_at = _whatsapp_next_allowed(
                        now_utc, payload.window_start_hour, payload.window_end_hour
                    )
                    if allowed_at > now_utc:
                        wait_seconds = (allowed_at - now_utc).total_seconds()
                        self._state.status = "waiting_window"
                        self._add_event(
                            "send_window_wait",
                            f"Postponed {int(wait_seconds // 60)} min until send window",
                            {
                                "allowed_at_iso": allowed_at.isoformat(),
                                "channel": (step.channel or "").strip().lower(),
                            },
                        )
                        await self._notify_last_event()
                        await self._wait_until_ready(allowed_at)
                        if await self._stop_if_requested():
                            return self._state

                    dispatch_result = await self._dispatch_with_manual_unknown_gate(step, cycle)
                    # O backend viu o PIX aberto no portão da rota (abriu
                    # depois do check, ou durante a fila de pacing): mesmo
                    # adiamento, mesma contagem de voltas.
                    if (
                        dispatch_result.status == CHECKOUT_PENDING_STATUS
                        and checkout_rounds < CHECKOUT_PENDING_MAX_ROUNDS
                    ):
                        checkout_rounds += 1
                        await self._wait_deferral(
                            step, cycle,
                            _int_or_zero((dispatch_result.raw or {}).get("defer_seconds")), 0,
                            checkout_rounds, conversation_rounds, "dispatch",
                        )
                        if await self._stop_if_requested():
                            return self._state
                        continue
                    # O backend viu o cliente conversando com o agente no
                    # portão da rota: adia com o contador da conversa.
                    if (
                        dispatch_result.status == CONVERSATION_ACTIVE_STATUS
                        and conversation_rounds < CONVERSATION_ACTIVE_MAX_ROUNDS
                    ):
                        conversation_rounds += 1
                        await self._wait_deferral(
                            step, cycle,
                            0, _int_or_zero((dispatch_result.raw or {}).get("defer_seconds")),
                            checkout_rounds, conversation_rounds, "dispatch",
                        )
                        if await self._stop_if_requested():
                            return self._state
                        continue
                    break

                # Todo desfecho vira um item de sent_steps (o falho também),
                # com motivo e destinatário: a tela mostra o que aconteceu com
                # cada step, não só os que deram certo.
                sent_at = self._record_step(step, cycle, dispatch_result)
                previous_recorded_at = sent_at
                if dispatch_result.status in {"sent", "queued", "skipped"}:
                    last_dispatched_at_by_step[step.step_id] = sent_at
                    self._state.status = "running"
                    message = f"{step.channel} step {step.step_id} {dispatch_result.status}"
                    if dispatch_result.status not in {"sent", "queued"} and dispatch_result.reason:
                        message += f" — {dispatch_result.reason}"
                    self._add_event(
                        "step_dispatched",
                        message,
                        {"step_id": step.step_id, "cycle": cycle},
                    )
                    await self._notify_last_event()
                    continue

                self._state.last_error = dispatch_result.reason
                self._add_event(
                    "step_failed",
                    dispatch_result.reason,
                    {"step_id": step.step_id, "cycle": cycle},
                )
                await self._notify_last_event()
                # Cada step é independente: a falha de um não para os
                # seguintes nem o workflow (decisão do dono, 2026-11-24).
                # WhatsApp sempre foi assim. Os demais canais paravam com
                # `stop_on_step_failure`; o histórico gravado antes do patch
                # reexecuta por esse caminho antigo.
                channel_l = (step.channel or "").strip().lower()
                if channel_l == "whatsapp":
                    continue
                if payload.stop_on_step_failure and not workflow.patched(
                    STEP_FAILURE_INDEPENDENT_PATCH
                ):
                    self._state.status = "failed"
                    return self._state

            if not unlimited and cycle >= max_cycles:
                break
            cycle_anchor = workflow.now()
            cycle += 1

        self._state.status = "completed"
        self._state.current_step_id = None
        self._add_event("workflow_completed", "Remarketing sequence completed")
        await self._notify_last_event()
        return self._state

    @workflow.signal
    async def purchase_confirmed(self, payload: dict[str, Any] | None = None) -> None:
        self._purchase_confirmed = True
        self._add_event("purchase_confirmed", "Purchase signal received", payload or {})

    @workflow.signal
    async def pause(self, reason: str = "") -> None:
        self._pause_requested = True
        self._state.paused = True
        self._state.pause_reason = reason
        self._add_event("workflow_paused", reason or "Paused")

    @workflow.signal
    async def resume(self) -> None:
        self._pause_requested = False
        self._state.paused = False
        self._state.pause_reason = ""
        self._add_event("workflow_resumed", "Resumed")

    @workflow.signal
    async def cancel(self, reason: str = "") -> None:
        self._cancel_requested = True
        self._add_event("workflow_cancel_requested", reason or "Cancel requested")

    @workflow.signal
    async def confirm_current_step(self, provider_message_id: str | None = None) -> None:
        self._confirm_current_step = True
        self._add_event(
            "manual_step_confirmed",
            "Current step confirmed manually",
            {"provider_message_id": provider_message_id},
        )

    @workflow.signal
    async def retry_current_step(self) -> None:
        self._retry_current_step = True
        self._add_event("manual_step_retry_requested", "Retry current step requested")

    @workflow.query
    def state(self) -> RemarketingWorkflowState:
        return self._state

    async def _run_purchase_check(self):
        """Executa a activity check_purchase com a política de retry padrão.
        Fonte única do portão de compra — usada antes de cada step e no
        re-check pós-espera longa de pacing. Propaga a exceção se todas as
        retentativas falharem."""
        assert self._input is not None
        payload = self._input
        return await workflow.execute_activity(
            check_purchase,
            PurchaseCheckInput(
                tenant_id=payload.tenant_id,
                lead_id=payload.lead_id,
                email=payload.email,
                phone=payload.phone,
                userip=payload.userip,
                fbp=payload.fbp,
                product_id=payload.product_id,
                metadata=payload.metadata,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=10),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(minutes=5),
                maximum_attempts=5,
            ),
        )

    async def _dispatch_with_manual_unknown_gate(self, step, cycle: int) -> DispatchResult:
        assert self._input is not None
        payload = self._input
        channel = (step.channel or "").strip().lower()
        idem_key = remarketing_idempotency_key(
            payload.tenant_id,
            payload.lead_id,
            payload.campaign_id,
            step.step_id,
            cycle,
        )
        # Política de retry específica do canal:
        #   WhatsApp: 3 tentativas com pausas curtas (5s, 10s, 15s) entre elas.
        #   Demais:   3 tentativas com 5min entre elas (comportamento anterior).
        if channel == "whatsapp":
            retry_waits_seconds = [5, 10, 15]
        else:
            retry_waits_seconds = [300, 300, 300]
        max_attempts = len(retry_waits_seconds)

        attempts = 0
        bypass_pacing = False
        while True:
            attempts += 1
            self._state.status = "dispatching"
            # A activity não dorme mais (pacing devolve pacing_required na
            # hora): o que resta são 2 HTTPs de ~20s. 3 min de folga deixa
            # falha real visível em minutos, não em meia hora.
            timeout = timedelta(minutes=3) if channel == "whatsapp" else timedelta(minutes=10)
            result = await workflow.execute_activity(
                dispatch_remarketing_step,
                DispatchStepInput(
                    tenant_id=payload.tenant_id,
                    lead_id=payload.lead_id,
                    campaign_id=payload.campaign_id,
                    step=step,
                    cycle=cycle,
                    idempotency_key=idem_key,
                    email=payload.email,
                    phone=payload.phone,
                    product_id=payload.product_id,
                    bypass_pacing=bypass_pacing,
                    metadata=payload.metadata,
                ),
                start_to_close_timeout=timeout,
                retry_policy=RetryPolicy(maximum_attempts=1),
            )

            if channel == "whatsapp" and result.status == "pacing_required":
                wait_s = float((result.raw or {}).get("wait_seconds", 0.0))
                if wait_s > 0:
                    self._state.status = "waiting_pacing"
                    self._add_event(
                        "whatsapp_pacing_wait",
                        f"WhatsApp pacing: sleeping {int(wait_s)}s before dispatch",
                        {
                            "step_id": step.step_id,
                            "cycle": cycle,
                            "wait_seconds": wait_s,
                        },
                    )
                    await self._notify_last_event()
                    await workflow.sleep(timedelta(seconds=wait_s))
                if self._cancel_requested or self._purchase_confirmed:
                    return DispatchResult(status="skipped", reason="workflow stopped")

                # A espera na fila pode ter cruzado o fim da janela de envio
                # (cauda da fila passando das 20:00 BRT). Dorme até a próxima
                # janela e volta para a FILA de pacing (bypass off) em vez de
                # furar o horário comercial.
                now_utc = workflow.now()
                allowed_at = _whatsapp_next_allowed(
                    now_utc, payload.window_start_hour, payload.window_end_hour
                )
                if allowed_at > now_utc:
                    window_wait = (allowed_at - now_utc).total_seconds()
                    self._state.status = "waiting_window"
                    self._add_event(
                        "send_window_wait",
                        f"Pacing wait crossed send window — postponed {int(window_wait // 60)} min",
                        {
                            "allowed_at_iso": allowed_at.isoformat(),
                            "step_id": step.step_id,
                            "cycle": cycle,
                        },
                    )
                    await self._notify_last_event()
                    await workflow.sleep(timedelta(seconds=window_wait))
                    if self._cancel_requested or self._purchase_confirmed:
                        return DispatchResult(status="skipped", reason="workflow stopped")
                    bypass_pacing = False
                    attempts = 0
                    continue

                # Espera longa: o lead pode ter comprado enquanto aguardava na
                # fila (nada no backend emite o signal purchase_confirmed hoje).
                # Best-effort: se o re-check falhar após as retentativas, envia
                # mesmo assim — o portão duro antes da fila continua fail-closed.
                if wait_s >= PACING_RECHECK_PURCHASE_SECONDS:
                    try:
                        purchase = await self._run_purchase_check()
                    except Exception as exc:
                        purchase = None
                        self._add_event(
                            "purchase_recheck_failed",
                            f"purchase re-check after pacing wait failed: {str(exc)[:200]}",
                            {"step_id": step.step_id, "cycle": cycle},
                        )
                        await self._notify_last_event()
                    if purchase is not None and purchase.purchased:
                        self._purchase_confirmed = True
                        self._add_event(
                            "workflow_stopped",
                            purchase.reason or "Lead purchased during pacing wait",
                            {"step_id": step.step_id, "cycle": cycle},
                        )
                        await self._notify_last_event()
                        return DispatchResult(
                            status="skipped", reason="lead purchased during pacing wait"
                        )

                bypass_pacing = True
                attempts -= 1
                continue

            # WhatsApp: cap diário atingido → o workflow dorme até a próxima
            # janela de ENVIO e tenta de novo o mesmo step. O cap reseta à
            # meia-noite BRT, mas 00:00 está fora da janela 09:00–20:00 — por
            # isso compomos as duas janelas (`_whatsapp_resume_after_cap`):
            # dormir só até 00:00 BRT fazia o re-despacho cair de madrugada,
            # fora do horário comercial.
            if channel == "whatsapp" and result.status == "cap_reached":
                cap_reset_seconds = int((result.raw or {}).get("next_window_seconds", 0)) or 3600
                resume_at = _whatsapp_resume_after_cap(
                    workflow.now(),
                    cap_reset_seconds,
                    payload.window_start_hour,
                    payload.window_end_hour,
                )
                wait_seconds = max(0, int((resume_at - workflow.now()).total_seconds()))
                self._state.status = "waiting_window"
                self._add_event(
                    "whatsapp_cap_reached",
                    f"Daily cap reached — sleeping {wait_seconds // 60} min until 09:00 BRT window",
                    {
                        "step_id": step.step_id,
                        "cycle": cycle,
                        "wait_seconds": wait_seconds,
                        "resume_at_iso": resume_at.isoformat(),
                    },
                )
                await self._notify_last_event()
                await workflow.sleep(timedelta(seconds=wait_seconds))
                if self._cancel_requested or self._purchase_confirmed:
                    return DispatchResult(status="skipped", reason="workflow stopped")
                # Reset attempts: a espera não conta como tentativa real.
                # bypass_pacing volta a False: quem acorda na abertura da
                # janela entra na FILA de pacing — sem isso, N workflows que
                # passaram por pacing antes do cap disparariam em rajada às
                # 09:00 sem intervalo entre si.
                attempts = 0
                bypass_pacing = False
                continue

            # PIX aberto visto pelo backend: não é falha, é adiamento. Sai na
            # hora (sem as 3 tentativas de 5/10 s, que esgotavam e pulavam o
            # step) e o run espera a janela do PIX e re-checa a compra.
            if (
                channel == "whatsapp"
                and result.status == "failed"
                and result.reason == CHECKOUT_PENDING_REASON
                and workflow.patched(CHECKOUT_PENDING_PATCH)
            ):
                return DispatchResult(
                    status=CHECKOUT_PENDING_STATUS,
                    retryable=False,
                    reason=CHECKOUT_PENDING_REASON,
                    raw=result.raw,
                )
            # Cliente em conversa com o agente visto pelo backend: idem.
            if (
                channel == "whatsapp"
                and result.status == "failed"
                and result.reason == CONVERSATION_ACTIVE_REASON
                and workflow.patched(CONVERSATION_ACTIVE_PATCH)
            ):
                return DispatchResult(
                    status=CONVERSATION_ACTIVE_STATUS,
                    retryable=False,
                    reason=CONVERSATION_ACTIVE_REASON,
                    raw=result.raw,
                )

            if result.status != "unknown":
                if result.status == "failed" and result.retryable and attempts < max_attempts:
                    wait_s = retry_waits_seconds[attempts - 1]
                    self._add_event(
                        "step_retry",
                        f"{channel} retry {attempts}/{max_attempts} in {wait_s}s — {result.reason}",
                        {"step_id": step.step_id, "cycle": cycle, "attempt": attempts},
                    )
                    await self._notify_last_event()
                    await workflow.sleep(timedelta(seconds=wait_s))
                    continue
                return result

            self._state.status = "waiting_manual_review"
            self._state.last_error = result.reason
            self._confirm_current_step = False
            self._retry_current_step = False
            self._add_event(
                "step_unknown",
                "Dispatch result is unknown; waiting for manual confirm or retry",
                {"step_id": step.step_id, "cycle": cycle},
            )
            await self._notify_last_event()

            await workflow.wait_condition(
                lambda: self._confirm_current_step
                or self._retry_current_step
                or self._cancel_requested
                or self._purchase_confirmed
            )
            if self._cancel_requested or self._purchase_confirmed:
                return DispatchResult(status="skipped", reason="workflow stopped")
            if self._confirm_current_step:
                return DispatchResult(status="sent", reason="manual confirmation")
            if self._retry_current_step:
                self._retry_current_step = False
                continue

    async def _wait_deferral(
        self,
        step,
        cycle: int,
        checkout_seconds: int,
        conversation_seconds: int,
        checkout_round: int,
        conversation_round: int,
        source: str,
    ) -> None:
        """Adia o step: espera a MAIOR das janelas (PIX aberto, cliente em
        conversa) mais a folga, respeitando pause/cancel/purchase_confirmed.
        O evento leva o nome do motivo que define a espera (empate: checkout).
        Status 'waiting': já está no conjunto de status ATIVOS do planner
        (`manageleads_temporal.active_tracking_sql`) — um status novo faria o
        planner ver o workflow como encerrado."""
        checkout_seconds = max(0, int(checkout_seconds))
        conversation_seconds = max(0, int(conversation_seconds))
        wait_s = min(
            max(checkout_seconds, conversation_seconds) + CHECKOUT_PENDING_MARGIN_SECONDS,
            CHECKOUT_PENDING_MAX_WAIT_SECONDS,
        )
        resume_at = workflow.now() + timedelta(seconds=wait_s)
        if conversation_seconds > checkout_seconds:
            event_type = "conversation_active_wait"
            round_n = conversation_round
            message = (
                f"Customer talking to the agent — postponed {wait_s}s "
                f"({round_n}/{CONVERSATION_ACTIVE_MAX_ROUNDS})"
            )
        else:
            event_type = "checkout_pending_wait"
            round_n = checkout_round
            message = (
                f"Checkout in progress (PIX pending) — postponed {wait_s}s "
                f"({round_n}/{CHECKOUT_PENDING_MAX_ROUNDS})"
            )
        self._state.status = "waiting"
        self._add_event(
            event_type,
            message,
            {
                "step_id": step.step_id,
                "cycle": cycle,
                "wait_seconds": wait_s,
                "round": round_n,
                "source": source,
                "resume_at_iso": resume_at.isoformat(),
                "checkout_pending_seconds": checkout_seconds,
                "conversation_active_seconds": conversation_seconds,
            },
        )
        await self._notify_last_event()
        await self._wait_until_ready(resume_at)

    async def _wait_until_ready(self, due_at) -> None:
        while True:
            while self._pause_requested and not self._cancel_requested and not self._purchase_confirmed:
                self._state.status = "paused"
                await workflow.wait_condition(
                    lambda: not self._pause_requested
                    or self._cancel_requested
                    or self._purchase_confirmed
                )

            if self._cancel_requested or self._purchase_confirmed:
                return

            remaining = due_at - workflow.now()
            if remaining <= timedelta(0):
                return

            try:
                await workflow.wait_condition(
                    lambda: self._pause_requested
                    or self._cancel_requested
                    or self._purchase_confirmed,
                    timeout=remaining,
                )
            except asyncio.TimeoutError:
                return

    def _record_step(self, step, cycle: int, dispatch_result: DispatchResult):
        """Registra o desfecho do step em sent_steps (qualquer status) e
        devolve o instante do registro."""
        assert self._input is not None
        payload = self._input
        at = workflow.now()
        self._state.sent_steps.append(
            SentRemarketingStep(
                step_id=step.step_id,
                channel=step.channel,
                cycle=cycle,
                status=dispatch_result.status,
                idempotency_key=remarketing_idempotency_key(
                    payload.tenant_id,
                    payload.lead_id,
                    payload.campaign_id,
                    step.step_id,
                    cycle,
                ),
                sent_at_iso=at.isoformat(),
                provider_message_id=dispatch_result.provider_message_id,
                metadata=dispatch_result.raw,
                reason=dispatch_result.reason,
                recipient=dispatch_result.recipient,
                recipient_overridden=dispatch_result.recipient_overridden,
            )
        )
        return at

    async def _stop_if_requested(self) -> bool:
        if self._purchase_confirmed:
            self._state.status = "purchased"
            self._add_event("workflow_stopped", "Purchase confirmed")
            await self._notify_last_event()
            return True
        if self._cancel_requested:
            self._state.status = "canceled"
            self._add_event("workflow_canceled", "Canceled")
            await self._notify_last_event()
            return True
        return False

    def _add_event(
        self,
        event_type: str,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._state.events.append(
            WorkflowEvent(
                event_type=event_type,
                message=message,
                at_iso=workflow.now().isoformat(),
                metadata=metadata or {},
            )
        )
        self._state.events = self._state.events[-50:]

    async def _notify_last_event(self) -> None:
        if not self._state.events:
            return
        try:
            await workflow.execute_activity(
                notify_remarketing_event,
                NotifyRemarketingEventInput(
                    workflow_id=workflow.info().workflow_id,
                    state=self._state,
                    event=self._state.events[-1],
                ),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        except Exception as exc:
            workflow.logger.warning("remarketing event callback ignored: %s", exc)
