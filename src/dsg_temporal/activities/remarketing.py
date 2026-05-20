from __future__ import annotations

import dataclasses
import logging
import random
import threading
import time
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


# Sentinela retornada pela activity quando o cap diário foi atingido. O
# workflow trata e dorme até a próxima janela em vez de manter a activity
# aberta por horas.
WHATSAPP_CAP_REACHED = "cap_reached"


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
    # ativaria 'purchased=true' indevidamente.
    body = {
        "email": payload.email,
        "phone": payload.phone,
        "productid": payload.product_id,
        "tenant_id": payload.tenant_id,
        "lead_id": payload.lead_id,
        "metadata": payload.metadata,
    }
    # Erros de rede e respostas retryables propagam para Temporal retentar
    # com backoff. NÃO usamos mais assume_purchased_on_check_error: se a
    # checagem falhar definitivamente, o workflow marca status='error' em
    # vez de assumir compra silenciosa.
    try:
        response = _legacy_remarketing_get(settings.legacy_purchase_check_path, body)
    except (requests.Timeout, requests.ConnectionError, requests.RequestException) as exc:
        logger.warning("purchase check transient error (will retry): %s", exc)
        raise

    raw = response_json_or_raw(response)
    if response.status_code in (429, 500, 502, 503, 504):
        logger.warning("purchase check retryable http %s", response.status_code)
        raise RuntimeError(f"purchase check retryable http {response.status_code}")
    if response.status_code >= 400:
        logger.error(
            "purchase check non-retryable http %s body=%s",
            response.status_code,
            raw,
        )
        raise RuntimeError(f"purchase check http {response.status_code}: {raw}")

    purchased = bool(raw.get("value", raw.get("purchased", raw.get("haspurchase", raw))))
    return PurchaseCheckResult(purchased=purchased, raw=raw)


# --- Global per-worker rate limit gate for dispatch ---
# Cada canal compartilha um lock e um "last sent at" entre as activities
# rodando no mesmo processo de worker. Não é distribuído: vale para a
# instância. Para múltiplas instâncias somar-se-iam — mantenha uma só.
_dispatch_locks: dict[str, threading.Lock] = {
    "email": threading.Lock(),
    "whatsapp": threading.Lock(),
}
_dispatch_last_sent_at: dict[str, float] = {"email": 0.0, "whatsapp": 0.0}


def _throttle_channel(channel: str, min_interval: float) -> float:
    """Bloqueia até passar pelo menos min_interval segundos desde o último
    envio do canal. Retorna quanto tempo foi esperado (para log)."""
    if min_interval <= 0 or channel not in _dispatch_locks:
        return 0.0
    waited = 0.0
    lock = _dispatch_locks[channel]
    with lock:
        now = time.monotonic()
        last = _dispatch_last_sent_at[channel]
        delta = now - last
        if delta < min_interval:
            waited = min_interval - delta
            time.sleep(waited)
        _dispatch_last_sent_at[channel] = time.monotonic()
    return waited


def _fetch_wpp_sender_snapshot() -> dict | None:
    """Busca a configuração + estatísticas atuais de envio WhatsApp no backend.
    Retorna None em caso de erro (worker faz fallback seguro: skip)."""
    settings = get_settings()
    if not settings.legacy_wpp_sender_snapshot_path:
        return None
    url = urljoin(
        settings.legacy_backend_base_url + "/",
        settings.legacy_wpp_sender_snapshot_path.lstrip("/"),
    )
    headers = {"User-Agent": "dsg-temporal-remarketing/0.1"}
    if settings.legacy_event_callback_secret:
        headers["X-Callback-Secret"] = settings.legacy_event_callback_secret
    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=settings.http_timeout_seconds,
        )
        if response.status_code >= 400:
            logger.warning(
                "wpp sender snapshot http=%s body=%s",
                response.status_code,
                response.text[:200],
            )
            return None
        return response.json()
    except Exception as exc:
        logger.warning("wpp sender snapshot failed: %s", exc)
        return None


def _heartbeat_safely(payload: Any = None) -> None:
    """Emite heartbeat — silencioso se a activity não tiver contexto (testes)."""
    try:
        activity.heartbeat(payload)
    except Exception:
        pass


def _sleep_with_heartbeat(total_seconds: float, chunk_seconds: float = 30.0) -> None:
    """Dorme em chunks emitindo heartbeats. Necessário para sleeps longos
    em activities com heartbeat_timeout configurado."""
    remaining = float(total_seconds)
    while remaining > 0:
        step = min(chunk_seconds, remaining)
        time.sleep(step)
        remaining -= step
        _heartbeat_safely({"sleep_remaining_seconds": remaining})


def _apply_whatsapp_pacing(snapshot: dict) -> None:
    """Aplica o pacing aleatório (min..max) entre dois envios WhatsApp no
    mesmo worker, usando o lock global do canal."""
    min_iv = max(1, int(snapshot.get("min_interval_seconds", 90)))
    max_iv = max(min_iv, int(snapshot.get("max_interval_seconds", 300)))
    target = random.uniform(min_iv, max_iv)
    lock = _dispatch_locks["whatsapp"]
    with lock:
        now = time.monotonic()
        last = _dispatch_last_sent_at["whatsapp"]
        elapsed = now - last
        if last > 0 and elapsed < target:
            wait = target - elapsed
            logger.info(
                "whatsapp pacing: waiting %.1fs (target=%.1fs, elapsed=%.1fs, range=%d..%d)",
                wait, target, elapsed, min_iv, max_iv,
            )
            _heartbeat_safely({"pacing_wait_seconds": wait})
            _sleep_with_heartbeat(wait)
        _dispatch_last_sent_at["whatsapp"] = time.monotonic()


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

    dispatch_email = payload.email
    dispatch_phone = payload.phone

    if channel == "whatsapp":
        # Toda a configuração de WhatsApp vem do backend (WhatsAppSenderConfig).
        snapshot = _fetch_wpp_sender_snapshot()
        if snapshot is None:
            # Sem snapshot disponível, falha para retry no workflow.
            return DispatchResult(
                status="failed",
                retryable=True,
                reason="wpp_sender snapshot unavailable",
            )

        if not snapshot.get("enabled"):
            logger.info("whatsapp dispatch skipped (sender disabled in admin)")
            return DispatchResult(
                status="skipped",
                retryable=False,
                reason="whatsapp sender disabled in admin",
            )

        # Cap diário: o workflow trata (dorme até a próxima janela).
        sent_today = int(snapshot.get("sent_today", 0))
        max_per_day = int(snapshot.get("max_per_day", 0))
        if max_per_day > 0 and sent_today >= max_per_day:
            wait_seconds = int(snapshot.get("next_window_starts_in_seconds", 0))
            logger.info(
                "whatsapp daily cap reached (%d/%d) — workflow will sleep %ds",
                sent_today, max_per_day, wait_seconds,
            )
            return DispatchResult(
                status=WHATSAPP_CAP_REACHED,
                retryable=False,
                reason=f"daily cap reached {sent_today}/{max_per_day}",
                raw={"next_window_seconds": wait_seconds},
            )

        # Modo teste: 100% dos envios vão para test_phone quando ativo.
        if snapshot.get("test_mode_enabled") and snapshot.get("test_phone"):
            override = str(snapshot["test_phone"]).strip()
            logger.info(
                "whatsapp test mode active: %s -> %s",
                payload.phone, override,
            )
            dispatch_phone = override

        # Aplica o pacing aleatório min..max (lock global do worker).
        _apply_whatsapp_pacing(snapshot)
    elif channel == "email":
        waited = _throttle_channel("email", settings.email_min_interval_seconds)
        if waited > 0:
            logger.info(
                "throttled email dispatch for %.1fs (min_interval=%ds)",
                waited, settings.email_min_interval_seconds,
            )
        # Modo de teste de email (separado do de WhatsApp).
        if settings.remarketing_email_override_to:
            dispatch_email = settings.remarketing_email_override_to
            logger.info(
                "email override active: %s -> %s (test mode)",
                payload.email, dispatch_email,
            )

    body = {
        "tenant_id": payload.tenant_id,
        "lead_id": payload.lead_id,
        "campaign_id": payload.campaign_id,
        "cycle": payload.cycle,
        "step_id": payload.step.step_id,
        "remarketstoreid": payload.step.metadata.get("remarket_store_id"),
        "idempotency_key": payload.idempotency_key,
        "email": dispatch_email,
        "phone": dispatch_phone,
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

    # Para WhatsApp, o backend retorna 200 mesmo em falhas funcionais (ok=false).
    # Precisamos diferenciar "enviado" de "número inválido" para evitar 3
    # retentativas inúteis quando o número simplesmente não existe.
    if channel == "whatsapp" and response.status_code == 200:
        ok_flag = raw.get("ok") if isinstance(raw, dict) else None
        if ok_flag is False:
            retryable = bool(raw.get("retryable", False))
            reason = str(raw.get("reason") or raw.get("error") or "whatsapp send failed")
            logger.info(
                "whatsapp dispatch returned ok=false retryable=%s reason=%s",
                retryable, reason,
            )
            return DispatchResult(
                status="failed",
                retryable=retryable,
                reason=reason[:500],
                raw=raw if isinstance(raw, dict) else {},
            )

    if response.status_code in (200, 201, 202):
        provider_id = None
        if isinstance(raw, dict):
            provider_id = (
                raw.get("message_id")
                or raw.get("msg_id")
                or raw.get("id")
                or raw.get("queue_id")
                or raw.get("outbox_id")
            )
        status = "sent"  # WhatsApp já é "sent" — o envio é síncrono ao Evolution API
        return DispatchResult(status=status, provider_message_id=provider_id, raw=raw if isinstance(raw, dict) else {})

    retryable = response.status_code >= 500 or response.status_code == 429
    return DispatchResult(
        status="failed",
        retryable=retryable,
        reason=f"http {response.status_code}",
        raw=raw if isinstance(raw, dict) else {},
    )


def _to_jsonable(value: Any) -> Any:
    """Converte dataclasses (recursivamente) em dict para serialização JSON."""
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if isinstance(value, list):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_jsonable(v) for k, v in value.items()}
    return value


@activity.defn
def notify_remarketing_event(payload: NotifyRemarketingEventInput) -> None:
    settings = get_settings()
    if not settings.legacy_event_callback_path or settings.dry_run:
        return

    # event vai como STRING (event_type) — é o que o endpoint legado consome
    # com .strip(). O dict completo do evento vai em 'event_details'.
    event_obj = _to_jsonable(payload.event)
    event_type = (
        event_obj.get("event_type")
        if isinstance(event_obj, dict)
        else str(event_obj or "event")
    )
    body = {
        "workflow_id": payload.workflow_id,
        "event": event_type or "event",
        "event_details": event_obj,
        "state": _to_jsonable(payload.state),
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
            logger.warning(
                "event callback failed http=%s body=%s",
                response.status_code,
                response.text[:300],
            )
    except Exception:
        logger.exception("event callback failed")
