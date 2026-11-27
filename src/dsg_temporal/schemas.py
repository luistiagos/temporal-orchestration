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
    # True: no ciclo 1 o atraso conta do registro do step anterior da sequencia
    # (enviado, pulado ou falho), e `send_at_iso` e ignorado. Ausente/False: como
    # sempre foi. Historico antigo decodifica False.
    # docs/modules/remarketing/changes/2026-11-27-step-de-cupom-uma-hora-depois-do-whatsapp/design.md
    delay_from_previous: bool = False


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
    # Segundos até fechar a janela do PIX gerado e não pago (checkout em
    # andamento); 0 = nada aberto. Default 0: histórico antigo sem o campo
    # decodifica como "nada aberto" e o replay segue o caminho antigo.
    checkout_pending_seconds: int = 0
    # Segundos até fechar a janela de "cliente em conversa com o agente"
    # (última mensagem DO CLIENTE nos últimos N min); 0 = não está conversando.
    # Mesmo default pelo mesmo motivo (replay de histórico sem o campo).
    conversation_active_seconds: int = 0


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
    bypass_pacing: bool = False
    metadata: Metadata = field(default_factory=dict)


@dataclass
class DispatchResult:
    status: str
    provider_message_id: str | None = None
    retryable: bool = False
    reason: str = ""
    raw: Metadata = field(default_factory=dict)
    # Destino efetivo da tentativa (e-mail ou telefone, depois de override de
    # teste) e se ele foi trocado. Default: historico antigo desserializa.
    recipient: str | None = None
    recipient_overridden: bool = False


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
    # Todo step despachado entra aqui, inclusive o que nao enviou (status
    # skipped/failed/...): `reason` diz por que, `recipient` para quem foi a
    # tentativa. Defaults: estado antigo desserializa.
    reason: str = ""
    recipient: str | None = None
    recipient_overridden: bool = False


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
class WhatsAppImageDescriptionInput:
    """Pedido de leitura de UMA imagem recebida no WhatsApp.

    O trabalho em si roda no backend legado (fonte única da visão:
    `wpp_vision.describe_whatsapp_image`); esta atividade só existe para que o
    Temporal seja o dono do agendamento/retry/timeout dessa leitura. Antes ela
    rodava numa `threading.Thread` do web worker do PythonAnywhere — que sobe SEM
    `--enable-threads` e por isso congelava a thread entre requisições
    (docs/bugs/.../2026-08-19-agentes-nao-leem-imagem-do-cliente.md).
    """
    tenant_id: str
    conversation_id: str
    msg_id: str
    text: str
    jid: str
    jid_alt: str | None = None
    metadata: Metadata = field(default_factory=dict)


@dataclass
class WhatsAppImageDescriptionResult:
    status: str
    text: str = ""
    description: str = ""
    reason: str = ""
    raw: Metadata = field(default_factory=dict)


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

