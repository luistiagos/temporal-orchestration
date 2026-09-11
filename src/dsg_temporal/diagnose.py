"""Sonda de workflow: diz se um workflow esta VIVO, MORTO ou PRESO.

Motivo de existir (bug `2026-08-18-temporal-workflow-task-em-retry-infinito`):
quando o *workflow task* falha, o Temporal retenta PARA SEMPRE. O servidor loga
`Critical attempts processing workflow task` com `attempt`, `wf-id` e `shard-id`
— e **nao** com a mensagem do erro. Sem a mensagem nao ha diagnostico:
`describe` do temporalio 1.27 nao expoe `pending_workflow_task.last_failure`, e
a query de estado devolve apenas "Unable to query workflow due to Workflow Task
in failed state". A unica fonte da mensagem e o evento `WorkflowTaskFailed` no
HISTORICO, que e o que este modulo le.

Vive fora de `api.py` de proposito: assim script, teste e endpoint usam a MESMA
funcao (o endpoint e' so a casca HTTP) e o teste nao precisa de fastapi.
"""

from __future__ import annotations

from typing import Any

import temporalio.service
from temporalio.api.enums.v1 import EventType, WorkflowTaskFailedCause
from temporalio.client import Client

# `attempt` do workflow task pendente a partir do qual chamamos de PRESO.
# 1 = primeira tentativa (normal). Uma falha isolada de infra pode chegar a 2-3
# e se resolver sozinha; a partir de 4 o padrao e' falha DETERMINISTICA, que
# retenta ate o fim dos tempos (o caso medido chegou a attempt 14.622).
STUCK_TASK_ATTEMPT_THRESHOLD = 4

# Teto de eventos lidos por chamada. Protege a API de um historico gigante:
# a resposta diz `history_scanned` < `history_length` quando bateu no teto.
MAX_HISTORY_EVENTS = 4000

WORKFLOW_STATUS_NAMES = {
    0: "UNSPECIFIED",
    1: "RUNNING",
    2: "COMPLETED",
    3: "FAILED",
    4: "CANCELED",
    5: "TERMINATED",
    6: "CONTINUED_AS_NEW",
    7: "TIMED_OUT",
}

# Veredictos. `stuck_task_failure` e' o unico que o Temporal reporta como
# RUNNING sendo, na pratica, morto — e' o ponto cego que originou o bug.
VERDICT_RUNNING = "running"
VERDICT_CLOSED = "closed"
VERDICT_NOT_FOUND = "not_found"
VERDICT_STUCK = "stuck_task_failure"

_TRUNCATE_MESSAGE = 2000
_TRUNCATE_STACK = 4000


def is_not_found(exc: BaseException) -> bool:
    """True se a excecao e' um NOT_FOUND do Temporal (workflow inexistente ou
    ja purgado pela retencao) — e nao uma falha de rede/servidor."""
    return (
        isinstance(exc, temporalio.service.RPCError)
        and exc.status == temporalio.service.RPCStatusCode.NOT_FOUND
    )


def verdict_for(status: int | None, pending_attempt: int) -> str:
    """Veredicto a partir do status do workflow e do `attempt` do workflow task
    pendente. Funcao pura: e' aqui que 'RUNNING com attempt alto' vira PRESO."""
    if status != 1:
        return VERDICT_CLOSED
    if pending_attempt >= STUCK_TASK_ATTEMPT_THRESHOLD:
        return VERDICT_STUCK
    return VERDICT_RUNNING


def _failure_chain(failure) -> list[dict[str, Any]]:
    """Mensagem + stack de cada elo da cadeia de causa da falha."""
    chain: list[dict[str, Any]] = []
    cur = failure
    while cur is not None and len(chain) < 5:
        chain.append(
            {
                "message": (cur.message or "")[:_TRUNCATE_MESSAGE],
                "source": cur.source or "",
                "stack_trace": (cur.stack_trace or "")[:_TRUNCATE_STACK],
            }
        )
        cur = cur.cause if cur.HasField("cause") else None
    return chain


def summarize_task_failures(events, limit: int = 5) -> dict[str, Any]:
    """Extrai de uma lista de eventos de historico as falhas e os timeouts de
    WORKFLOW TASK. Funcao pura — testavel sem servidor.

    Devolve as `limit` ULTIMAS ocorrencias de cada tipo: a que interessa e' a
    mais recente (retentativa transiente nao acrescenta evento novo, entao a
    ultima `WorkflowTaskFailed` do historico e' a que esta em loop agora).
    """
    failures: list[dict[str, Any]] = []
    timeouts: list[dict[str, Any]] = []
    for event in events:
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED:
            attrs = event.workflow_task_failed_event_attributes
            chain = _failure_chain(attrs.failure) if attrs.HasField("failure") else []
            failures.append(
                {
                    "event_id": event.event_id,
                    "event_time": event.event_time.ToJsonString() if event.event_time else None,
                    "cause": WorkflowTaskFailedCause.Name(attrs.cause),
                    "identity": attrs.identity,
                    "message": chain[0]["message"] if chain else "",
                    "stack_trace": chain[0]["stack_trace"] if chain else "",
                    "cause_chain": chain[1:],
                }
            )
        elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
            attrs = event.workflow_task_timed_out_event_attributes
            timeouts.append(
                {
                    "event_id": event.event_id,
                    "event_time": event.event_time.ToJsonString() if event.event_time else None,
                    "timeout_type": int(attrs.timeout_type),
                }
            )
    return {
        "task_failures": failures[-limit:],
        "task_failures_total": len(failures),
        "task_timeouts": timeouts[-limit:],
        "task_timeouts_total": len(timeouts),
    }


async def diagnose_workflow(
    client: Client,
    workflow_id: str,
    *,
    with_failures: bool | None = None,
    failure_limit: int = 5,
    max_history_events: int = MAX_HISTORY_EVENTS,
) -> dict[str, Any]:
    """Diagnostico de UM workflow: veredicto + `attempt` + mensagem da falha.

    `with_failures=None` (default) le o historico somente quando o veredicto e'
    `stuck_task_failure` — assim o reconciliador do backend pode varrer centenas
    de linhas pagando so um `describe` por workflow. `True` forca a leitura,
    `False` nunca le.
    """
    handle = client.get_workflow_handle(workflow_id)
    try:
        desc = await handle.describe()
    except Exception as exc:
        if is_not_found(exc):
            return {
                "workflow_id": workflow_id,
                "verdict": VERDICT_NOT_FOUND,
                "alive": False,
                "status": None,
                "status_name": "NOT_FOUND",
                "detail": str(exc)[:300],
            }
        raise

    info = desc.raw_description.workflow_execution_info
    status = int(desc.status) if desc.status is not None else None
    pending = desc.raw_description.pending_workflow_task
    pending_attempt = int(pending.attempt or 0)
    verdict = verdict_for(status, pending_attempt)

    result: dict[str, Any] = {
        "workflow_id": workflow_id,
        "run_id": info.execution.run_id,
        "verdict": verdict,
        # `alive` responde a pergunta do reconciliador: da' para esperar que
        # este workflow ainda avance? PRESO conta como morto.
        "alive": verdict == VERDICT_RUNNING,
        "status": status,
        "status_name": WORKFLOW_STATUS_NAMES.get(status, str(status)),
        "workflow_type": info.type.name,
        "task_queue": info.task_queue,
        "start_time": info.start_time.ToJsonString() if info.HasField("start_time") else None,
        "close_time": info.close_time.ToJsonString() if info.HasField("close_time") else None,
        "history_length": int(info.history_length or 0),
        "history_size_bytes": int(info.history_size_bytes or 0),
        "pending_workflow_task": {
            "attempt": pending_attempt,
            "state": int(pending.state),
            "scheduled_time": (
                pending.scheduled_time.ToJsonString()
                if pending.HasField("scheduled_time")
                else None
            ),
            "original_scheduled_time": (
                pending.original_scheduled_time.ToJsonString()
                if pending.HasField("original_scheduled_time")
                else None
            ),
        },
        "pending_activity_count": len(desc.raw_description.pending_activities),
    }

    read_history = with_failures if with_failures is not None else verdict == VERDICT_STUCK
    if not read_history:
        return result

    events = []
    truncated = False
    async for event in handle.fetch_history_events(page_size=200):
        events.append(event)
        if len(events) >= max_history_events:
            truncated = True
            break
    result.update(summarize_task_failures(events, limit=failure_limit))
    result["history_scanned"] = len(events)
    result["history_truncated"] = truncated
    return result


async def terminate_workflow(client: Client, workflow_id: str, reason: str) -> dict[str, Any]:
    """Termina o workflow. E' o unico jeito de libertar o `workflow_id` de um
    workflow PRESO: `cancel` e `pause` sao SIGNAL, e signal depende do workflow
    task rodar — justamente o que esta falhando. `reset` tambem nao serve:
    replay do mesmo historico com o mesmo codigo quebra no mesmo ponto.
    """
    handle = client.get_workflow_handle(workflow_id)
    try:
        await handle.terminate(reason=reason)
    except Exception as exc:
        if is_not_found(exc):
            return {"workflow_id": workflow_id, "status": "not_found", "detail": str(exc)[:300]}
        raise
    return {"workflow_id": workflow_id, "status": "terminated", "reason": reason}
