from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


Metadata = dict[str, Any]


@dataclass
class RemarketingStep:
    step_id: str
    order: int
    channel: str
    template: str
    subject: str = ""
    delay_minutes: int = 0
    send_at_iso: str | None = None
    preferred_time: str | None = None
    preferred_day: int | None = None
    repeat_after_days: int | None = None
    metadata: Metadata = field(default_factory=dict)


@dataclass
class LeadRemarketingInput:
    tenant_id: str
    lead_id: int | str
    campaign_id: str
    store_id: int | str | None = None
    product_id: int | str | None = None
    email: str | None = None
    phone: str | None = None
    userip: str | None = None
    fbp: str | None = None
    lead_created_at_iso: str | None = None
    max_cycles: int = 1
    stop_on_step_failure: bool = True
    # Janela de envio (hora BRT) por campanha. None => default 09:00–20:00.
    # Vale para todos os canais — o workflow adia o despacho para dentro dela.
    window_start_hour: int | None = None
    window_end_hour: int | None = None
    sequence: list[RemarketingStep] = field(default_factory=list)
    metadata: Metadata = field(default_factory=dict)


@dataclass
class PurchaseCheckInput:
    tenant_id: str
    lead_id: int | str
    email: str | None = None
    phone: str | None = None
    userip: str | None = None
    fbp: str | None = None
    product_id: int | str | None = None
    metadata: Metadata = field(default_factory=dict)


@dataclass
class PurchaseCheckResult:
    purchased: bool
    reason: str = ""
    raw: Metadata = field(default_factory=dict)


@dataclass
class DispatchStepInput:
    tenant_id: str
    lead_id: int | str
    campaign_id: str
    step: RemarketingStep
    cycle: int
    idempotency_key: str
    email: str | None = None
    phone: str | None = None
    product_id: int | str | None = None
    metadata: Metadata = field(default_factory=dict)


@dataclass
class DispatchResult:
    status: str
    provider_message_id: str | None = None
    retryable: bool = False
    reason: str = ""
    raw: Metadata = field(default_factory=dict)


@dataclass
class WorkflowEvent:
    event_type: str
    message: str
    at_iso: str
    metadata: Metadata = field(default_factory=dict)


@dataclass
class SentRemarketingStep:
    step_id: str
    channel: str
    cycle: int
    status: str
    idempotency_key: str
    sent_at_iso: str
    provider_message_id: str | None = None
    metadata: Metadata = field(default_factory=dict)


@dataclass
class RemarketingWorkflowState:
    status: str
    tenant_id: str
    lead_id: int | str
    campaign_id: str
    current_step_id: str | None = None
    current_cycle: int = 0
    paused: bool = False
    pause_reason: str = ""
    last_error: str = ""
    sent_steps: list[SentRemarketingStep] = field(default_factory=list)
    events: list[WorkflowEvent] = field(default_factory=list)


@dataclass
class NotifyRemarketingEventInput:
    workflow_id: str
    state: RemarketingWorkflowState
    event: WorkflowEvent


@dataclass
class WhatsAppInboundMessage:
    tenant_id: str
    instance_id: str
    jid: str
    jid_alt: str | None
    msg_id: str
    text: str
    from_me: bool = False
    push_name: str | None = None
    timestamp_iso: str | None = None
    metadata: Metadata = field(default_factory=dict)


@dataclass
class WhatsAppWorkflowInput:
    tenant_id: str
    conversation_id: str
    debounce_seconds: float = 2.5
    initial_message: WhatsAppInboundMessage | None = None
    # Fila dedicada onde a atividade process_whatsapp_batch é agendada. Quando
    # None, a atividade roda na própria fila do workflow (comportamento legado).
    # Isolar a fila/pool das conversas impede que os sleeps de pacing do
    # remarketing (mesmo ThreadPoolExecutor) starvem a resposta ao cliente.
    activity_task_queue: str | None = None
    metadata: Metadata = field(default_factory=dict)


@dataclass
class WhatsAppBatchInput:
    tenant_id: str
    conversation_id: str
    messages: list[WhatsAppInboundMessage]
    metadata: Metadata = field(default_factory=dict)


@dataclass
class WhatsAppBatchResult:
    status: str
    reason: str = ""
    raw: Metadata = field(default_factory=dict)


@dataclass
class WhatsAppWorkflowState:
    status: str
    tenant_id: str
    conversation_id: str
    pending_count: int = 0
    processed_batches: int = 0
    last_msg_id: str | None = None
    last_error: str = ""
    # Falhas consecutivas do batch atual (0 quando saudável). >0 sinaliza que a
    # conversa está "degraded" e re-tentando — observável por um monitor.
    consecutive_failures: int = 0
    events: list[WorkflowEvent] = field(default_factory=list)

