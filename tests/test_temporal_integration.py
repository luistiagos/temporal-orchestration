from __future__ import annotations

import asyncio
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from dsg_temporal.activities import (
    check_purchase,
    dispatch_remarketing_step,
    notify_remarketing_event,
    process_whatsapp_batch,
)
from dsg_temporal.schemas import (
    LeadRemarketingInput,
    RemarketingStep,
    WhatsAppInboundMessage,
    WhatsAppWorkflowInput,
)
from dsg_temporal.settings import get_settings
from dsg_temporal.workflows import LeadRemarketingWorkflow, WhatsAppConversationWorkflow


pytestmark = pytest.mark.integration


def _task_queue(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def test_remarketing_workflow_dispatches_email_and_whatsapp_in_dry_run():
    async def run_test() -> None:
        os.environ["DRY_RUN"] = "true"
        get_settings.cache_clear()
        task_queue = _task_queue("remarketing")

        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client,
                    task_queue=task_queue,
                    workflows=[LeadRemarketingWorkflow],
                    activities=[
                        check_purchase,
                        dispatch_remarketing_step,
                        notify_remarketing_event,
                    ],
                    activity_executor=executor,
                ):
                    result = await env.client.execute_workflow(
                        LeadRemarketingWorkflow.run,
                        LeadRemarketingInput(
                            tenant_id="test",
                            lead_id=123,
                            campaign_id="cart",
                            email="lead@example.com",
                            phone="+55 (41) 99999-0000",
                            max_cycles=1,
                            sequence=[
                                RemarketingStep(
                                    step_id="email-1",
                                    order=1,
                                    channel="email",
                                    subject="Teste",
                                    template="Email body",
                                    delay_minutes=60,
                                ),
                                RemarketingStep(
                                    step_id="whatsapp-1",
                                    order=2,
                                    channel="whatsapp",
                                    template="WhatsApp body",
                                ),
                            ],
                        ),
                        id=f"test-remarketing-{uuid.uuid4().hex}",
                        task_queue=task_queue,
                        execution_timeout=timedelta(days=1),
                    )

        assert result.status == "completed"
        assert [step.step_id for step in result.sent_steps] == ["email-1", "whatsapp-1"]
        assert [step.status for step in result.sent_steps] == ["sent", "queued"]

    asyncio.run(run_test())


def test_remarketing_purchase_signal_interrupts_waiting_timer():
    async def run_test() -> str:
        os.environ["DRY_RUN"] = "true"
        get_settings.cache_clear()
        task_queue = _task_queue("remarketing-signal")

        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client,
                    task_queue=task_queue,
                    workflows=[LeadRemarketingWorkflow],
                    activities=[
                        check_purchase,
                        dispatch_remarketing_step,
                        notify_remarketing_event,
                    ],
                    activity_executor=executor,
                ):
                    handle = await env.client.start_workflow(
                        LeadRemarketingWorkflow.run,
                        LeadRemarketingInput(
                            tenant_id="test",
                            lead_id=456,
                            campaign_id="cart",
                            email="buyer@example.com",
                            sequence=[
                                RemarketingStep(
                                    step_id="email-later",
                                    order=1,
                                    channel="email",
                                    template="Email body",
                                    delay_minutes=60 * 24 * 30,
                                )
                            ],
                        ),
                        id=f"test-remarketing-signal-{uuid.uuid4().hex}",
                        task_queue=task_queue,
                        execution_timeout=timedelta(days=60),
                    )
                    await handle.signal(
                        LeadRemarketingWorkflow.purchase_confirmed,
                        {"source": "test"},
                    )
                    result = await handle.result()
                    return result.status

    assert asyncio.run(run_test()) == "purchased"


def test_whatsapp_workflow_debounces_batch_and_can_be_canceled():
    async def run_test() -> tuple[int, str]:
        os.environ["DRY_RUN"] = "true"
        get_settings.cache_clear()
        task_queue = _task_queue("whatsapp")
        workflow_id = f"test-whatsapp-{uuid.uuid4().hex}"

        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client,
                    task_queue=task_queue,
                    workflows=[WhatsAppConversationWorkflow],
                    activities=[process_whatsapp_batch],
                    activity_executor=executor,
                ):
                    handle = await env.client.start_workflow(
                        WhatsAppConversationWorkflow.run,
                        WhatsAppWorkflowInput(
                            tenant_id="test",
                            conversation_id="5541999990000",
                            debounce_seconds=0.1,
                            initial_message=WhatsAppInboundMessage(
                                tenant_id="test",
                                instance_id="agent",
                                jid="5541999990000@lid",
                                jid_alt=None,
                                msg_id="msg-1",
                                text="oi",
                            ),
                        ),
                        id=workflow_id,
                        task_queue=task_queue,
                        execution_timeout=timedelta(minutes=5),
                    )
                    await handle.signal(
                        WhatsAppConversationWorkflow.message_received,
                        WhatsAppInboundMessage(
                            tenant_id="test",
                            instance_id="agent",
                            jid="5541999990000@lid",
                            jid_alt=None,
                            msg_id="msg-2",
                            text="quero xbox",
                        ),
                    )

                    processed_batches = 0
                    for _ in range(30):
                        state = await handle.query(WhatsAppConversationWorkflow.state)
                        processed_batches = state.processed_batches
                        if processed_batches >= 1:
                            break
                        await asyncio.sleep(0.1)

                    await handle.signal(WhatsAppConversationWorkflow.cancel, "test done")
                    result = await handle.result()
                    return processed_batches, result.status

    processed_batches, status = asyncio.run(run_test())
    assert processed_batches >= 1
    assert status == "canceled"
