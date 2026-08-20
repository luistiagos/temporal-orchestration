"""Testes da leitura de imagem como ATIVIDADE do Temporal.

Contexto (docs/bugs/close/2026-08-19-agentes-nao-leem-imagem-do-cliente.md): a
descricao rodava numa `threading.Thread` do web worker do PythonAnywhere, cujo uWSGI
sobe SEM `--enable-threads` — a thread so avanca quando chega outra requisicao. Um
`time.sleep(1.5)` foi medido em 225s e 34% dos prints nunca eram lidos. A leitura
virou atividade: o Temporal agenda/retenta, e o trabalho executa numa thread de
REQUISICAO do Flask.

O que estes testes travam:
- o print e lido ANTES do batch, e o batch recebe o texto JA enriquecido;
- falha da leitura NAO cala a conversa (o batch roda com o "[IMAGEM]" cru);
- mensagem sem a marca do backend nao dispara leitura nenhuma;
- o teto por batch impede que uma rajada de prints atrase a resposta.
"""
from __future__ import annotations

import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from dsg_temporal.schemas import (
    WhatsAppBatchInput,
    WhatsAppBatchResult,
    WhatsAppImageDescriptionInput,
    WhatsAppImageDescriptionResult,
    WhatsAppInboundMessage,
    WhatsAppWorkflowInput,
)
from dsg_temporal.workflows import WhatsAppConversationWorkflow

pytestmark = pytest.mark.integration


def _img(msg_id: str, caption: str = "", needs: bool = True) -> WhatsAppInboundMessage:
    return WhatsAppInboundMessage(
        tenant_id="test",
        instance_id="agent",
        jid="5541999990000@lid",
        jid_alt=None,
        msg_id=msg_id,
        text=f"[IMAGEM] {caption}".rstrip(),
        metadata={"needs_image_description": True} if needs else {},
    )


async def _wait_batches(handle, target: int, tries: int = 60) -> int:
    processed = 0
    for _ in range(tries):
        state = await handle.query(WhatsAppConversationWorkflow.state)
        processed = state.processed_batches
        if processed >= target:
            break
        await asyncio.sleep(0.1)
    return processed


def _run(describe_impl, initial, extra=None, tries=60):
    """Sobe workflow + worker com a atividade de leitura mockada.

    Devolve (textos_vistos_pelo_batch, chamadas_de_leitura).
    """
    vistos: list[list[str]] = []
    lidas: list[str] = []

    @activity.defn(name="describe_whatsapp_image")
    def fake_describe(payload: WhatsAppImageDescriptionInput) -> WhatsAppImageDescriptionResult:
        lidas.append(payload.msg_id)
        return describe_impl(payload)

    @activity.defn(name="process_whatsapp_batch")
    def fake_batch(payload: WhatsAppBatchInput) -> WhatsAppBatchResult:
        vistos.append([m.text for m in payload.messages])
        return WhatsAppBatchResult(status="processed")

    async def run_test():
        task_queue = f"wpp-img-{uuid.uuid4().hex}"
        async with await WorkflowEnvironment.start_time_skipping() as env:
            with ThreadPoolExecutor(max_workers=4) as executor:
                async with Worker(
                    env.client,
                    task_queue=task_queue,
                    workflows=[WhatsAppConversationWorkflow],
                    activities=[fake_describe, fake_batch],
                    activity_executor=executor,
                ):
                    handle = await env.client.start_workflow(
                        WhatsAppConversationWorkflow.run,
                        WhatsAppWorkflowInput(
                            tenant_id="test",
                            conversation_id="5541999990000",
                            debounce_seconds=0.1,
                            initial_message=initial,
                        ),
                        id=f"wpp-img-{uuid.uuid4().hex}",
                        task_queue=task_queue,
                        execution_timeout=timedelta(minutes=5),
                    )
                    for msg in extra or []:
                        await handle.signal(
                            WhatsAppConversationWorkflow.message_received, msg
                        )
                    await _wait_batches(handle, 1, tries=tries)
                    await handle.signal(WhatsAppConversationWorkflow.cancel, "done")
                    await handle.result()

    asyncio.run(run_test())
    return vistos, lidas


def test_print_e_lido_antes_do_batch():
    """O batch recebe o texto JA com a [DESCRIÇÃO] — o agente enxerga a imagem."""

    def ok(payload):
        return WhatsAppImageDescriptionResult(
            status="described",
            text=payload.text + "\n[DESCRIÇÃO]: tela de erro 401 do Xbox 360",
            description="tela de erro 401 do Xbox 360",
        )

    vistos, lidas = _run(ok, _img("img-1", "olha isso"))

    assert lidas == ["img-1"], "a leitura tinha que ter sido pedida uma vez"
    assert len(vistos) == 1
    assert "[DESCRIÇÃO]: tela de erro 401 do Xbox 360" in vistos[0][0]


def test_falha_na_leitura_nao_cala_a_conversa():
    """Leitura que falha NAO pode sumir com a mensagem: silencio e o pior desfecho.

    O batch tem que rodar mesmo assim, com o "[IMAGEM]" cru — o prompt sabe pedir
    em palavras ou escalar nesse estado.
    """

    def explode(payload):
        raise RuntimeError("visao fora do ar")

    vistos, lidas = _run(explode, _img("img-2", "tá dando erro"))

    assert len(vistos) == 1, "o batch precisa rodar mesmo sem a descricao"
    assert vistos[0] == ["[IMAGEM] tá dando erro"]
    assert "[DESCRIÇÃO]" not in vistos[0][0]


def test_leitura_vazia_segue_com_texto_cru():
    """Visao que responde sem descricao (status undescribed) tambem nao trava nada."""

    def vazio(payload):
        return WhatsAppImageDescriptionResult(
            status="undescribed", text=payload.text, reason="sem base64"
        )

    vistos, _ = _run(vazio, _img("img-3", "print"))
    assert vistos[0] == ["[IMAGEM] print"]


def test_mensagem_de_texto_nao_dispara_leitura():
    """Sem a marca do backend, nenhuma leitura e pedida (nao gasta cota a toa)."""

    def nunca(payload):  # pragma: no cover - nao deve ser chamado
        raise AssertionError("nao deveria pedir leitura de texto puro")

    texto = WhatsAppInboundMessage(
        tenant_id="test",
        instance_id="agent",
        jid="5541999990000@lid",
        jid_alt=None,
        msg_id="txt-1",
        text="quanto custa?",
    )
    vistos, lidas = _run(nunca, texto)

    assert lidas == []
    assert vistos == [["quanto custa?"]]


def test_teto_de_imagens_por_batch():
    """Rajada de prints nao pode empurrar a resposta: acima do teto segue cru."""

    def ok(payload):
        return WhatsAppImageDescriptionResult(
            status="described",
            text=payload.text + "\n[DESCRIÇÃO]: ok",
            description="ok",
        )

    extra = [_img(f"img-b{i}") for i in range(2, 7)]
    vistos, lidas = _run(ok, _img("img-b1"), extra=extra)

    assert len(lidas) == 3, f"teto de 3 leituras por batch, veio {len(lidas)}"
    descritos = [t for t in vistos[0] if "[DESCRIÇÃO]" in t]
    assert len(descritos) == 3
    assert len(vistos[0]) == 6, "nenhuma mensagem pode ser perdida pelo teto"
