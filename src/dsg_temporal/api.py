from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from dsg_temporal.ids import canonical_phone, remarketing_workflow_id, whatsapp_workflow_id
from dsg_temporal.schemas import (
    LeadRemarketingInput,
    RemarketingStep,
    WhatsAppInboundMessage,
    WhatsAppWorkflowInput,
)
from dsg_temporal.settings import get_settings
from dsg_temporal.workflows import LeadRemarketingWorkflow, WhatsAppConversationWorkflow


class RemarketingStepRequest(BaseModel):
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
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_dataclass(self) -> RemarketingStep:
        return RemarketingStep(**self.model_dump())


class StartRemarketingRequest(BaseModel):
    tenant_id: str = "digitalstoregames"
    lead_id: int | str
    campaign_id: str = "default"
    workflow_id: str | None = None
    store_id: int | str | None = None
    product_id: int | str | None = None
    email: str | None = None
    phone: str | None = None
    userip: str | None = None
    fbp: str | None = None
    lead_created_at_iso: str | None = None
    max_cycles: int = 1
    stop_on_step_failure: bool = True
    sequence: list[RemarketingStepRequest]
    metadata: dict[str, Any] = Field(default_factory=dict)

    def workflow_input(self) -> LeadRemarketingInput:
        data = self.model_dump(exclude={"workflow_id", "sequence"})
        return LeadRemarketingInput(
            **data,
            sequence=[step.to_dataclass() for step in self.sequence],
        )


class SignalPayload(BaseModel):
    reason: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class ConfirmStepPayload(BaseModel):
    provider_message_id: str | None = None


class WhatsAppMessageRequest(BaseModel):
    tenant_id: str = "digitalstoregames"
    instance_id: str = "default"
    jid: str
    jid_alt: str | None = None
    msg_id: str
    text: str
    from_me: bool = False
    push_name: str | None = None
    timestamp_iso: str | None = None
    debounce_seconds: float = 2.5
    metadata: dict[str, Any] = Field(default_factory=dict)

    def inbound_message(self) -> WhatsAppInboundMessage:
        data = self.model_dump(exclude={"debounce_seconds"})
        return WhatsAppInboundMessage(**data)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.temporal_client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )
    yield


app = FastAPI(title="DSG Temporal Orchestrator", version="0.1.0", lifespan=lifespan)


def temporal_client(request: Request) -> Client:
    return request.app.state.temporal_client


@app.get("/health")
async def health() -> dict[str, str]:
    settings = get_settings()
    return {
        "status": "ok",
        "temporal_address": settings.temporal_address,
        "namespace": settings.temporal_namespace,
        "task_queue": settings.temporal_task_queue,
    }


@app.post("/v1/remarketing/workflows")
async def start_remarketing_workflow(
    payload: StartRemarketingRequest,
    request: Request,
) -> dict[str, str]:
    settings = get_settings()
    workflow_id = payload.workflow_id or remarketing_workflow_id(
        payload.tenant_id,
        payload.lead_id,
        payload.campaign_id,
    )
    try:
        await temporal_client(request).start_workflow(
            LeadRemarketingWorkflow.run,
            payload.workflow_input(),
            id=workflow_id,
            task_queue=settings.temporal_task_queue,
        )
        return {"workflow_id": workflow_id, "status": "started"}
    except WorkflowAlreadyStartedError:
        return {"workflow_id": workflow_id, "status": "already_started"}


@app.get("/v1/remarketing/workflows/{workflow_id}")
async def get_remarketing_state(workflow_id: str, request: Request) -> Any:
    handle = temporal_client(request).get_workflow_handle(workflow_id)
    try:
        return await handle.query(LeadRemarketingWorkflow.state)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/v1/remarketing/workflows/{workflow_id}/purchase")
async def signal_purchase(workflow_id: str, payload: SignalPayload, request: Request) -> dict[str, str]:
    handle = temporal_client(request).get_workflow_handle(workflow_id)
    await handle.signal(LeadRemarketingWorkflow.purchase_confirmed, payload.model_dump())
    return {"workflow_id": workflow_id, "status": "signaled"}


@app.post("/v1/remarketing/workflows/{workflow_id}/pause")
async def pause_workflow(workflow_id: str, payload: SignalPayload, request: Request) -> dict[str, str]:
    handle = temporal_client(request).get_workflow_handle(workflow_id)
    await handle.signal(LeadRemarketingWorkflow.pause, payload.reason)
    return {"workflow_id": workflow_id, "status": "paused"}


@app.post("/v1/remarketing/workflows/{workflow_id}/resume")
async def resume_workflow(workflow_id: str, request: Request) -> dict[str, str]:
    handle = temporal_client(request).get_workflow_handle(workflow_id)
    await handle.signal(LeadRemarketingWorkflow.resume)
    return {"workflow_id": workflow_id, "status": "resumed"}


@app.post("/v1/remarketing/workflows/{workflow_id}/cancel")
async def cancel_workflow(workflow_id: str, payload: SignalPayload, request: Request) -> dict[str, str]:
    handle = temporal_client(request).get_workflow_handle(workflow_id)
    await handle.signal(LeadRemarketingWorkflow.cancel, payload.reason)
    return {"workflow_id": workflow_id, "status": "cancel_requested"}


@app.post("/v1/remarketing/workflows/{workflow_id}/confirm-current-step")
async def confirm_current_step(
    workflow_id: str,
    payload: ConfirmStepPayload,
    request: Request,
) -> dict[str, str]:
    handle = temporal_client(request).get_workflow_handle(workflow_id)
    await handle.signal(LeadRemarketingWorkflow.confirm_current_step, payload.provider_message_id)
    return {"workflow_id": workflow_id, "status": "current_step_confirmed"}


@app.post("/v1/remarketing/workflows/{workflow_id}/retry-current-step")
async def retry_current_step(workflow_id: str, request: Request) -> dict[str, str]:
    handle = temporal_client(request).get_workflow_handle(workflow_id)
    await handle.signal(LeadRemarketingWorkflow.retry_current_step)
    return {"workflow_id": workflow_id, "status": "current_step_retry_requested"}


@app.post("/v1/whatsapp/messages")
async def ingest_whatsapp_message(
    payload: WhatsAppMessageRequest,
    request: Request,
) -> dict[str, str]:
    settings = get_settings()
    conversation_id = canonical_phone(payload.jid) or payload.jid
    workflow_id = whatsapp_workflow_id(payload.tenant_id, conversation_id)
    message = payload.inbound_message()
    workflow_input = WhatsAppWorkflowInput(
        tenant_id=payload.tenant_id,
        conversation_id=conversation_id,
        debounce_seconds=payload.debounce_seconds,
        initial_message=message,
        metadata=payload.metadata,
    )

    client = temporal_client(request)
    try:
        await client.start_workflow(
            WhatsAppConversationWorkflow.run,
            workflow_input,
            id=workflow_id,
            task_queue=settings.temporal_task_queue,
        )
        return {"workflow_id": workflow_id, "status": "started"}
    except WorkflowAlreadyStartedError:
        handle = client.get_workflow_handle(workflow_id)
        await handle.signal(WhatsAppConversationWorkflow.message_received, message)
        return {"workflow_id": workflow_id, "status": "signaled"}


@app.get("/v1/whatsapp/workflows/{workflow_id}")
async def get_whatsapp_state(workflow_id: str, request: Request) -> Any:
    handle = temporal_client(request).get_workflow_handle(workflow_id)
    try:
        return await handle.query(WhatsAppConversationWorkflow.state)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/v1/whatsapp/workflows/{workflow_id}/cancel")
async def cancel_whatsapp_workflow(
    workflow_id: str,
    payload: SignalPayload,
    request: Request,
) -> dict[str, str]:
    handle = temporal_client(request).get_workflow_handle(workflow_id)
    await handle.signal(WhatsAppConversationWorkflow.cancel, payload.reason)
    return {"workflow_id": workflow_id, "status": "cancel_requested"}
