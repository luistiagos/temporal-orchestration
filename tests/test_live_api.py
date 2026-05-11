from __future__ import annotations

import os
import time
import uuid

import pytest
import requests


pytestmark = pytest.mark.live


def _base_url() -> str:
    return os.getenv("API_BASE_URL", "http://127.0.0.1:8090").rstrip("/")


def _require_live_tests() -> None:
    if os.getenv("RUN_LIVE_TESTS", "").strip().lower() not in {"1", "true", "yes"}:
        pytest.skip("set RUN_LIVE_TESTS=true to run live API smoke tests")


def test_live_health_endpoint():
    _require_live_tests()
    response = requests.get(f"{_base_url()}/health", timeout=10)
    response.raise_for_status()
    body = response.json()
    assert body["status"] == "ok"
    assert body["task_queue"]


def test_live_remarketing_workflow_smoke():
    _require_live_tests()
    lead_id = int(uuid.uuid4().int % 1_000_000_000)
    payload = {
        "tenant_id": "live-test",
        "lead_id": lead_id,
        "campaign_id": "smoke",
        "email": "lead@example.com",
        "phone": "+55 (41) 99999-0000",
        "max_cycles": 1,
        "sequence": [
            {
                "step_id": "email-1",
                "order": 1,
                "channel": "email",
                "subject": "Smoke",
                "template": "Email smoke",
            },
            {
                "step_id": "whatsapp-1",
                "order": 2,
                "channel": "whatsapp",
                "template": "WhatsApp smoke",
            },
        ],
    }
    started = requests.post(
        f"{_base_url()}/v1/remarketing/workflows",
        json=payload,
        timeout=20,
    )
    started.raise_for_status()
    workflow_id = started.json()["workflow_id"]

    state = None
    for _ in range(30):
        response = requests.get(
            f"{_base_url()}/v1/remarketing/workflows/{workflow_id}",
            timeout=20,
        )
        response.raise_for_status()
        state = response.json()
        if state["status"] == "completed":
            break
        time.sleep(0.5)

    assert state is not None
    assert state["status"] == "completed"
    assert [step["step_id"] for step in state["sent_steps"]] == ["email-1", "whatsapp-1"]
    assert [step["status"] for step in state["sent_steps"]] == ["sent", "queued"]


def test_live_whatsapp_workflow_smoke():
    _require_live_tests()
    phone = "5541" + str(uuid.uuid4().int % 1_000_000_000).zfill(9)
    first = {
        "tenant_id": "live-test",
        "instance_id": "agent",
        "jid": f"{phone}@lid",
        "msg_id": f"msg-{uuid.uuid4().hex}",
        "text": "oi",
        "debounce_seconds": 0.1,
    }
    started = requests.post(f"{_base_url()}/v1/whatsapp/messages", json=first, timeout=20)
    started.raise_for_status()
    workflow_id = started.json()["workflow_id"]

    second = {**first, "msg_id": f"msg-{uuid.uuid4().hex}", "text": "quero xbox"}
    signaled = requests.post(f"{_base_url()}/v1/whatsapp/messages", json=second, timeout=20)
    signaled.raise_for_status()

    state = None
    for _ in range(30):
        response = requests.get(f"{_base_url()}/v1/whatsapp/workflows/{workflow_id}", timeout=20)
        response.raise_for_status()
        state = response.json()
        if state["processed_batches"] >= 1:
            break
        time.sleep(0.5)

    requests.post(
        f"{_base_url()}/v1/whatsapp/workflows/{workflow_id}/cancel",
        json={"reason": "test done"},
        timeout=20,
    ).raise_for_status()

    assert state is not None
    assert state["processed_batches"] >= 1

