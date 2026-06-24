from __future__ import annotations

import asyncio
from datetime import datetime, time, timedelta, timezone
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

# Janela de envio permitida para WhatsApp em horário de Brasília (UTC-3).
# Mensagens com due_at fora desta janela são adiadas para o próximo 09:00 BRT.
WHATSAPP_TZ_OFFSET_HOURS = -3
WHATSAPP_WINDOW_START_HOUR = 9
WHATSAPP_WINDOW_END_HOUR = 20  # 20:00 = limite superior (não envia >= 20:00)


def _whatsapp_next_allowed(now_utc: datetime) -> datetime:
    """Se o horário (em BRT) cai fora de 09:00–20:00, retorna o próximo 09:00 BRT;
    caso contrário retorna o próprio now_utc (sem adiamento)."""
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    brt = now_utc + timedelta(hours=WHATSAPP_TZ_OFFSET_HOURS)
    hour = brt.hour
    if WHATSAPP_WINDOW_START_HOUR <= hour < WHATSAPP_WINDOW_END_HOUR:
        return now_utc
    target_brt = brt.replace(
        hour=WHATSAPP_WINDOW_START_HOUR, minute=0, second=0, microsecond=0
    )
    if hour >= WHATSAPP_WINDOW_END_HOUR:
        target_brt = target_brt + timedelta(days=1)
    # Volta para UTC
    return (target_brt - timedelta(hours=WHATSAPP_TZ_OFFSET_HOURS)).replace(
        tzinfo=timezone.utc
    )


def _whatsapp_resume_after_cap(now_utc: datetime, cap_reset_seconds: float) -> datetime:
    """Instante (UTC) em que o envio pode retomar após o cap diário ser atingido.

    O cap reseta à meia-noite BRT (`cap_reset_seconds` à frente), mas 00:00 BRT
    está FORA da janela de envio (09:00–20:00). A retomada precisa satisfazer as
    DUAS janelas: cap já resetado E dentro do horário comercial. Compondo:
    avança até o reset do cap e então aplica a guarda de quiet hours — o que
    empurra a meia-noite para o próximo 09:00 BRT.
    """
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    cap_reset_at = now_utc + timedelta(seconds=max(0.0, cap_reset_seconds))
    return _whatsapp_next_allowed(cap_reset_at)


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
            for step in cycle_sequence:
                if await self._stop_if_requested():
                    return self._state

                self._state.current_step_id = step.step_id
                self._state.status = "waiting"
                base_at = cycle_anchor
                delay_minutes = step.delay_minutes
                if cycle > 1:
                    base_at = last_dispatched_at_by_step.get(step.step_id, workflow.now())
                    delay_minutes = int(step.repeat_after_days or 0) * 24 * 60
                    delay_minutes += int(step.delay_minutes or 0)
                due_at = compute_due_at(
                    now=workflow.now(),
                    base_at=base_at,
                    delay_minutes=delay_minutes,
                    send_at_iso=step.send_at_iso if cycle == 1 else None,
                    preferred_time=step.preferred_time,
                    preferred_day=step.preferred_day,
                )
                await self._wait_until_ready(due_at)
                if await self._stop_if_requested():
                    return self._state

                try:
                    purchase = await workflow.execute_activity(
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

                # Quiet hours para WhatsApp: 20:00 BRT a 09:00 BRT do dia seguinte.
                # Se o dispatch caísse fora dessa janela, adiamos.
                if (step.channel or "").strip().lower() == "whatsapp":
                    now_utc = workflow.now()
                    allowed_at = _whatsapp_next_allowed(now_utc)
                    if allowed_at > now_utc:
                        wait_seconds = (allowed_at - now_utc).total_seconds()
                        self._state.status = "waiting_window"
                        self._add_event(
                            "whatsapp_quiet_hours",
                            f"Postponed {int(wait_seconds // 60)} min until 09:00 BRT",
                            {"allowed_at_iso": allowed_at.isoformat()},
                        )
                        await self._notify_last_event()
                        await self._wait_until_ready(allowed_at)
                        if await self._stop_if_requested():
                            return self._state

                dispatch_result = await self._dispatch_with_manual_unknown_gate(step, cycle)
                if dispatch_result.status in {"sent", "queued", "skipped"}:
                    sent_at = workflow.now()
                    idem_key = remarketing_idempotency_key(
                        payload.tenant_id,
                        payload.lead_id,
                        payload.campaign_id,
                        step.step_id,
                        cycle,
                    )
                    self._state.sent_steps.append(
                        SentRemarketingStep(
                            step_id=step.step_id,
                            channel=step.channel,
                            cycle=cycle,
                            status=dispatch_result.status,
                            idempotency_key=idem_key,
                            sent_at_iso=sent_at.isoformat(),
                            provider_message_id=dispatch_result.provider_message_id,
                            metadata=dispatch_result.raw,
                        )
                    )
                    last_dispatched_at_by_step[step.step_id] = sent_at
                    self._state.status = "running"
                    self._add_event(
                        "step_dispatched",
                        f"{step.channel} step {step.step_id} {dispatch_result.status}",
                        {"step_id": step.step_id, "cycle": cycle},
                    )
                    await self._notify_last_event()
                    continue

                self._state.last_error = dispatch_result.reason
                self._add_event("step_failed", dispatch_result.reason)
                await self._notify_last_event()
                # WhatsApp: após 3 retentativas esgotadas, sempre pula o
                # step e continua o workflow (não interrompe). Outros canais
                # mantêm o comportamento de `stop_on_step_failure`.
                channel_l = (step.channel or "").strip().lower()
                if channel_l == "whatsapp":
                    continue
                if payload.stop_on_step_failure:
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
        while True:
            attempts += 1
            self._state.status = "dispatching"
            # WhatsApp pode esperar até max_interval (default 300s) + HTTP +
            # heartbeat — 30 min é folga confortável.
            timeout = timedelta(minutes=30) if channel == "whatsapp" else timedelta(minutes=10)
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
                    metadata=payload.metadata,
                ),
                start_to_close_timeout=timeout,
                heartbeat_timeout=timedelta(minutes=2) if channel == "whatsapp" else None,
                retry_policy=RetryPolicy(maximum_attempts=1),
            )

            # WhatsApp: cap diário atingido → o workflow dorme até a próxima
            # janela de ENVIO e tenta de novo o mesmo step. O cap reseta à
            # meia-noite BRT, mas 00:00 está fora da janela 09:00–20:00 — por
            # isso compomos as duas janelas (`_whatsapp_resume_after_cap`):
            # dormir só até 00:00 BRT fazia o re-despacho cair de madrugada,
            # fora do horário comercial.
            if channel == "whatsapp" and result.status == "cap_reached":
                cap_reset_seconds = int((result.raw or {}).get("next_window_seconds", 0)) or 3600
                resume_at = _whatsapp_resume_after_cap(workflow.now(), cap_reset_seconds)
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
                attempts = 0
                continue

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
