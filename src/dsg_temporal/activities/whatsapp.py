from __future__ import annotations

from dataclasses import asdict, is_dataclass
import logging

import requests
from temporalio import activity

from dsg_temporal.activities.http_client import post_legacy_json, response_json_or_raw
from dsg_temporal.schemas import (
    WhatsAppBatchInput,
    WhatsAppBatchResult,
    WhatsAppImageDescriptionInput,
    WhatsAppImageDescriptionResult,
)
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


@activity.defn
def describe_whatsapp_image(
    payload: WhatsAppImageDescriptionInput,
) -> WhatsAppImageDescriptionResult:
    """Le UMA imagem do cliente chamando o backend legado.

    Por que passa pelo backend em vez de descrever aqui: a visao tem UM dono
    (`wpp_vision.describe_whatsapp_image`) — cache por msg_id, rotacao de chave em
    `vision_keys`, cooldown de 429 e telemetria em `vision_usage` vivem todos no
    backend, contra o mesmo banco. Duplicar isso no worker criaria duas fontes da
    mesma regra. O que MUDA de dono e o agendamento: quem retenta, quando desiste e
    qual o teto passa a ser o Temporal, e a leitura executa numa THREAD DE
    REQUISICAO do Flask — que o uWSGI do PythonAnywhere de fato escalona.

    Falha de leitura NAO levanta: devolve status="undescribed" e o fluxo segue com o
    "[IMAGEM]" cru, que o agente sabe tratar com honestidade (pedir em palavras ou
    escalar). So erro de TRANSPORTE levanta, para o Temporal retentar.
    """
    settings = get_settings()
    if settings.dry_run:
        return WhatsAppImageDescriptionResult(
            status="skipped", text=payload.text, reason="dry_run"
        )

    body = {
        "tenant_id": payload.tenant_id,
        "conversation_id": payload.conversation_id,
        "msg_id": payload.msg_id,
        "text": payload.text,
        "jid": payload.jid,
        "jid_alt": payload.jid_alt,
        "metadata": payload.metadata,
    }
    try:
        response = post_legacy_json(
            settings.legacy_whatsapp_describe_image_path,
            body,
            timeout=settings.describe_image_timeout_seconds,
        )
    except requests.Timeout as exc:
        # Estourou o teto: retentar repetiria a mesma espera e atrasaria ainda mais a
        # resposta ao cliente. Segue sem descricao — silencio seria pior que um agente
        # dizendo com honestidade que o print nao abriu.
        logger.warning("describe_whatsapp_image timeout msg_id=%s: %s", payload.msg_id, exc)
        return WhatsAppImageDescriptionResult(
            status="undescribed", text=payload.text, reason=f"timeout: {exc}"[:500]
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"describe image request failed: {exc}") from exc

    raw = response_json_or_raw(response)
    if response.status_code in (200, 201):
        description = str(raw.get("description") or "")
        text = str(raw.get("text") or payload.text)
        return WhatsAppImageDescriptionResult(
            status="described" if description else "undescribed",
            text=text,
            description=description,
            reason=str(raw.get("reason") or ""),
            raw=raw,
        )
    raise RuntimeError(f"describe image http {response.status_code}: {raw}")
