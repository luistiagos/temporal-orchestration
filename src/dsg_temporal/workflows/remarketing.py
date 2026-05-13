from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

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
                        initial_interval=timedelta(seconds=5),
                        maximum_interval=timedelta(minutes=1),
                        maximum_attempts=3,
                    ),
                )
                if purchase.purchased:
                    self._state.status = "purchased"
                    self._add_event("workflow_stopped", purchase.reason or "Lead purchased")
                    await self._notify_last_event()
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
        idem_key = remarketing_idempotency_key(
            payload.tenant_id,
            payload.lead_id,
            payload.campaign_id,
            step.step_id,
            cycle,
        )
        attempts = 0
        while True:
            attempts += 1
            self._state.status = "dispatching"
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
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )

            if result.status != "unknown":
                if result.status == "failed" and result.retryable and attempts < 3:
                    await workflow.sleep(timedelta(minutes=5))
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
