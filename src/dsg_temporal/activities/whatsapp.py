from __future__ import annotations

from dataclasses import asdict, is_dataclass
import logging

import requests
from temporalio import activity

from dsg_temporal.activities.http_client import post_legacy_json, response_json_or_raw
from dsg_temporal.schemas import WhatsAppBatchInput, WhatsAppBatchResult
from dsg_temporal.settings import get_settings

logger = logging.getLogger(__name__)


def _message_to_dict(message):
    if is_dataclass(message):
        return asdict(message)
    if isinstance(message, dict):
        return message
    return dict(message)


@activity.defn
def process_whatsapp_batch(payload: WhatsAppBatchInput) -> WhatsAppBatchResult:
    settings = get_settings()
    if settings.dry_run:
        return WhatsAppBatchResult(
            status="processed",
            reason=f"dry_run batch_size={len(payload.messages)}",
        )

    body = {
        "tenant_id": payload.tenant_id,
        "conversation_id": payload.conversation_id,
        "messages": [_message_to_dict(message) for message in payload.messages],
        "metadata": payload.metadata,
    }
    try:
        response = post_legacy_json(settings.legacy_whatsapp_process_path, body)
    except requests.Timeout as exc:
        return WhatsAppBatchResult(status="unknown", reason=str(exc)[:500])
    except requests.RequestException as exc:
        raise RuntimeError(f"whatsapp batch request failed: {exc}") from exc

    raw = response_json_or_raw(response)
    if response.status_code in (200, 201, 202):
        return WhatsAppBatchResult(status="processed", raw=raw)
    if response.status_code == 409:
        return WhatsAppBatchResult(status="duplicate", raw=raw)
    raise RuntimeError(f"whatsapp batch http {response.status_code}: {raw}")
