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


# Máximo de msg_ids lembrados para deduplicação (ao longo da conversa). Limita o
# crescimento do estado; conversas reais não repetem msg_ids fora dessa janela.
_SEEN_MSG_ID_LIMIT = 500

# Bounds da atividade de processamento do batch. schedule_to_start limita a espera
# NA FILA (starvation do pool / worker ausente vira falha VISÍVEL, não hang eterno);
# start_to_close limita cada tentativa; schedule_to_close é o teto TOTAL incluindo
# retries. Sem esses timeouts, um pool exausto deixava o workflow preso para sempre
# em "processing" (ver docs/bugs/open/2026-07-06-temporal-workflow-trava-sem-desbloqueio.md).
_ACTIVITY_SCHEDULE_TO_START = timedelta(seconds=90)
_ACTIVITY_START_TO_CLOSE = timedelta(minutes=3)
_ACTIVITY_SCHEDULE_TO_CLOSE = timedelta(minutes=15)


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
        # msg_ids já aceitos (pending + já processados). Dedup contra ISTO, não só
        # contra o buffer atual — senão o reenvio do reconcile (mesmo msg_id, depois
        # do batch já ter sido retirado) entra como 2ª cópia e o cliente é
        # respondido 2x. Foi o que aconteceu no caso "Dario" (msg_id repetido).
        self._seen_msg_ids: list[str] = []
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
            self._accept_message(payload.initial_message)
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

            activity_kwargs: dict = {}
            if payload.activity_task_queue:
                activity_kwargs["task_queue"] = payload.activity_task_queue

            try:
                result = await workflow.execute_activity(
                    process_whatsapp_batch,
                    WhatsAppBatchInput(
                        tenant_id=payload.tenant_id,
                        conversation_id=payload.conversation_id,
                        messages=batch,
                        metadata=payload.metadata,
                    ),
                    schedule_to_start_timeout=_ACTIVITY_SCHEDULE_TO_START,
                    start_to_close_timeout=_ACTIVITY_START_TO_CLOSE,
                    schedule_to_close_timeout=_ACTIVITY_SCHEDULE_TO_CLOSE,
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=5),
                        maximum_interval=timedelta(minutes=1),
                        maximum_attempts=5,
                    ),
                    **activity_kwargs,
                )
            except Exception as exc:
                # NÃO dar raise: isso mataria o workflow e PERDERIA a mensagem do
                # cliente. Em vez disso: devolve o batch pro buffer, marca o estado
                # como observável ("degraded") e faz backoff antes de retentar. Se
                # o backend estava fora/pool exausto, o próximo ciclo reprocessa
                # quando recupera — sem silêncio permanente nem perda de mensagem.
                self._state.consecutive_failures += 1
                self._state.last_error = str(exc)[:500]
                self._state.status = "degraded"
                self._add_event(
                    "batch_failed",
                    str(exc)[:200],
                    {
                        "batch_size": len(batch),
                        "consecutive_failures": self._state.consecutive_failures,
                    },
                )
                self._requeue_front(batch)
                backoff = min(self._state.consecutive_failures * 30, 300)
                await workflow.sleep(timedelta(seconds=backoff))
                continue

            self._state.consecutive_failures = 0
            self._state.processed_batches += 1
            self._state.status = "running"
            self._add_event("batch_processed", result.status, {"batch_size": len(batch)})

        self._state.status = "canceled"
        self._add_event("workflow_canceled", "Canceled")
        return self._state

    def _accept_message(self, message: WhatsAppInboundMessage) -> bool:
        """Enfileira uma mensagem NOVA (dedup por msg_id já visto). Retorna se aceitou."""
        if message.msg_id in self._seen_msg_ids:
            return False
        self._pending.append(message)
        self._seen_msg_ids.append(message.msg_id)
        if len(self._seen_msg_ids) > _SEEN_MSG_ID_LIMIT:
            self._seen_msg_ids = self._seen_msg_ids[-_SEEN_MSG_ID_LIMIT:]
        self._state.last_msg_id = message.msg_id
        self._state.pending_count = len(self._pending)
        return True

    def _requeue_front(self, batch: list[WhatsAppInboundMessage]) -> None:
        """Devolve um batch que falhou para a FRENTE do buffer, preservando ordem.
        Não passa pelo dedup: são mensagens já aceitas sendo re-tentadas."""
        self._pending[:0] = batch
        self._state.pending_count = len(self._pending)

    @workflow.signal
    async def message_received(self, message: WhatsAppInboundMessage) -> None:
        if self._accept_message(message):
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

