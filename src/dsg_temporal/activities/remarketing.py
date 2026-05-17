from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urljoin

import requests
from temporalio import activity

from dsg_temporal.activities.http_client import response_json_or_raw
from dsg_temporal.schemas import (
    DispatchResult,
    DispatchStepInput,
    NotifyRemarketingEventInput,
    PurchaseCheckInput,
    PurchaseCheckResult,
)
from dsg_temporal.settings import get_settings

logger = logging.getLogger(__name__)


def _legacy_remarketing_headers(idempotency_key: str | None = None) -> dict[str, str]:
    settings = get_settings()
    headers = {"User-Agent": "dsg-temporal-remarketing/0.1"}
    if settings.legacy_api_key:
        headers["Authorization"] = f"Bearer {settings.legacy_api_key}"
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
        headers["X-Idempotency-Key"] = idempotency_key
    return headers


def _legacy_remarketing_get(
    path: str,
    payload: dict[str, Any],
    *,
    idempotency_key: str | None = None,
) -> requests.Response:
    settings = get_settings()
    url = urljoin(settings.legacy_backend_base_url + "/", path.lstrip("/"))
    params = {
        key: value
        for key, value in payload.items()
        if value is not None and not isinstance(value, (dict, list, tuple))
    }
    return requests.get(
        url,
        params=params,
        headers=_legacy_remarketing_headers(idempotency_key),
        timeout=settings.http_timeout_seconds,
    )


@activity.defn
def check_purchase(payload: PurchaseCheckInput) -> PurchaseCheckResult:
    settings = get_settings()
    if settings.dry_run:
        return PurchaseCheckResult(purchased=False, reason="dry_run")

    # IMPORTANTE: só enviamos email e phone (identificadores únicos do lead).
    # userip e fbp são compartilhados (NAT, cookies de browser) e geram falsos
    # positivos — qualquer outro lead na mesma rede ou que usou o mesmo browser
    # ativaria 'purchased=true' indevidamente, fazendo o workflow encerrar antes
    # de despachar qualquer step.
    body = {
        "email": payload.email,
        "phone": payload.phone,
        "productid": payload.product_id,
        "tenant_id": payload.tenant_id,
        "lead_id": payload.lead_id,
        "metadata": payload.metadata,
    }
    try:
        response = _legacy_remarketing_get(settings.legacy_purchase_check_path, body)
        raw = response_json_or_raw(response)
        if response.status_code >= 400:
            reason = f"purchase check http {response.status_code}"
            if settings.assume_purchased_on_check_error:
                return PurchaseCheckResult(purchased=True, reason=reason, raw=raw)
            raise RuntimeError(reason)
        purchased = bool(raw.get("value", raw.get("purchased", raw.get("haspurchase", raw))))
        return PurchaseCheckResult(purchased=purchased, raw=raw)
    except Exception as exc:
        logger.exception("purchase check failed")
        if settings.assume_purchased_on_check_error:
            return PurchaseCheckResult(purchased=True, reason=str(exc)[:500])
        raise


@activity.defn
def dispatch_remarketing_step(payload: DispatchStepInput) -> DispatchResult:
    settings = get_settings()
    channel = (payload.step.channel or "").strip().lower()

    if settings.dry_run:
        status = "queued" if channel == "whatsapp" else "sent"
        return DispatchResult(
            status=status,
            provider_message_id=f"dry-run-{payload.idempotency_key}",
            reason="dry_run",
        )

    body = {
        "tenant_id": payload.tenant_id,
        "lead_id": payload.lead_id,
        "campaign_id": payload.campaign_id,
        "cycle": payload.cycle,
        "step_id": payload.step.step_id,
        "remarketstoreid": payload.step.metadata.get("remarket_store_id"),
        "idempotency_key": payload.idempotency_key,
        "email": payload.email,
        "phone": payload.phone,
        "title": payload.step.subject,
        "subject": payload.step.subject,
        "template": payload.step.template,
        "productid": payload.product_id,
        "metadata": {**payload.metadata, **payload.step.metadata},
    }

    if channel == "email":
        path = settings.legacy_email_path
    elif channel == "whatsapp":
        path = settings.legacy_whatsapp_ai_path or settings.legacy_whatsapp_path
    else:
        return DispatchResult(
            status="failed",
            retryable=False,
            reason=f"unsupported channel: {payload.step.channel}",
        )

    try:
        response = _legacy_remarketing_get(
            path,
            body,
            idempotency_key=payload.idempotency_key,
        )
    except requests.Timeout as exc:
        return DispatchResult(
            status="unknown",
            retryable=False,
            reason=f"timeout after side effect boundary: {exc}",
        )
    except requests.ConnectionError as exc:
        return DispatchResult(status="failed", retryable=True, reason=str(exc)[:500])
    except requests.RequestException as exc:
        return DispatchResult(status="failed", retryable=True, reason=str(exc)[:500])

    raw = response_json_or_raw(response)
    if response.status_code in (200, 201, 202):
        provider_id = (
            raw.get("message_id")
            or raw.get("msg_id")
            or raw.get("id")
            or raw.get("queue_id")
            or raw.get("outbox_id")
        )
        status = "queued" if channel == "whatsapp" else "sent"
        return DispatchResult(status=status, provider_message_id=provider_id, raw=raw)

    retryable = response.status_code >= 500 or response.status_code == 429
    return DispatchResult(
        status="failed",
        retryable=retryable,
        reason=f"http {response.status_code}",
        raw=raw,
    )


@activity.defn
def notify_remarketing_event(payload: NotifyRemarketingEventInput) -> None:
    settings = get_settings()
    if not settings.legacy_event_callback_path or settings.dry_run:
        return

    body = {
        "workflow_id": payload.workflow_id,
        "event": payload.event,
        "state": payload.state,
    }
    url = urljoin(settings.legacy_backend_base_url + "/", settings.legacy_event_callback_path.lstrip("/"))
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "dsg-temporal-remarketing/0.1",
    }
    if settings.legacy_event_callback_secret:
        headers["X-Callback-Secret"] = settings.legacy_event_callback_secret
    try:
        response = requests.post(
            url,
            json=body,
            headers=headers,
            timeout=settings.http_timeout_seconds,
        )
        if response.status_code >= 400:
            logger.warning("event callback failed with http %s", response.status_code)
    except Exception:
        logger.exception("event callback failed")
