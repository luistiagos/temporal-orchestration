from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from dsg_temporal.schemas import (
    WhatsAppBatchInput,
    WhatsAppInboundMessage,
    WhatsAppWorkflowInput,
    WhatsAppWorkflowState,
    WorkflowEvent,
)

with workflow.unsafe.imports_passed_through():
    from dsg_temporal.activities.whatsapp import process_whatsapp_batch


@workflow.defn
class WhatsAppConversationWorkflow:
    def __init__(self) -> None:
        self._state = WhatsAppWorkflowState(
            status="created",
            tenant_id="",
            conversation_id="",
        )
        self._input: WhatsAppWorkflowInput | None = None
        self._pending: list[WhatsAppInboundMessage] = []
        self._cancel_requested = False

    @workflow.run
    async def run(self, payload: WhatsAppWorkflowInput) -> WhatsAppWorkflowState:
        self._input = payload
        self._state = WhatsAppWorkflowState(
            status="running",
            tenant_id=payload.tenant_id,
            conversation_id=payload.conversation_id,
        )
        if payload.initial_message:
            self._pending.append(payload.initial_message)
            self._state.last_msg_id = payload.initial_message.msg_id
        self._add_event("workflow_started", "WhatsApp conversation workflow started")

        while not self._cancel_requested:
            self._state.pending_count = len(self._pending)
            await workflow.wait_condition(lambda: bool(self._pending) or self._cancel_requested)
            if self._cancel_requested:
                break

            await workflow.sleep(timedelta(seconds=max(0.1, payload.debounce_seconds)))
            batch = list(self._pending)
            self._pending.clear()
            self._state.pending_count = 0
            self._state.status = "processing"

            try:
                result = await workflow.execute_activity(
                    process_whatsapp_batch,
                    WhatsAppBatchInput(
                        tenant_id=payload.tenant_id,
                        conversation_id=payload.conversation_id,
                        messages=batch,
                        metadata=payload.metadata,
                    ),
                    start_to_close_timeout=timedelta(minutes=3),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=5),
                        maximum_interval=timedelta(minutes=1),
                        maximum_attempts=3,
                    ),
                )
                self._state.processed_batches += 1
                self._state.status = "running"
                self._add_event("batch_processed", result.status, {"batch_size": len(batch)})
            except Exception as exc:
                self._state.status = "failed"
                self._state.last_error = str(exc)
                self._add_event("batch_failed", str(exc))
                raise

        self._state.status = "canceled"
        self._add_event("workflow_canceled", "Canceled")
        return self._state

    @workflow.signal
    async def message_received(self, message: WhatsAppInboundMessage) -> None:
        existing_ids = {item.msg_id for item in self._pending}
        if message.msg_id not in existing_ids:
            self._pending.append(message)
            self._state.last_msg_id = message.msg_id
            self._state.pending_count = len(self._pending)
            self._add_event("message_received", message.msg_id)

    @workflow.signal
    async def cancel(self, reason: str = "") -> None:
        self._cancel_requested = True
        self._add_event("workflow_cancel_requested", reason or "Cancel requested")

    @workflow.query
    def state(self) -> WhatsAppWorkflowState:
        return self._state

    def _add_event(self, event_type: str, message: str, metadata: dict | None = None) -> None:
        self._state.events.append(
            WorkflowEvent(
                event_type=event_type,
                message=message,
                at_iso=workflow.now().isoformat(),
                metadata=metadata or {},
            )
        )
        self._state.events = self._state.events[-50:]

