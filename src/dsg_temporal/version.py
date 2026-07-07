"""Identidade da versão VIVA do serviço (commit + boot do processo).

Mata a classe "fix fantasma" no orquestrador Temporal: os serviços Railway
(`dsg-temporal-api` e `dsg-temporal-worker`) buildam de um Dockerfile SEM o
diretório `.git`, então não dá pra descobrir o commit por `git rev-parse` em
produção. Railway injeta `RAILWAY_GIT_COMMIT_SHA` no runtime de serviços
conectados ao GitHub — essa é a fonte canônica aqui.

A API expõe isso em `GET /version`; o worker (que não serve HTTP) loga no
startup. Assim dá pra confirmar por fora QUAL commit está vivo em cada serviço,
sem precisar abrir o dashboard do Railway.
"""
from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from functools import lru_cache

from dsg_temporal import __version__

# Momento em que o processo importou este módulo (~boot do processo).
_BOOT_AT = datetime.now(timezone.utc)


def _clean(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


@lru_cache(maxsize=1)
def commit() -> str:
    """SHA do commit vivo. Ordem: override explícito → Railway → git local → unknown."""
    for env_name in ("DSG_GIT_COMMIT", "RAILWAY_GIT_COMMIT_SHA", "GIT_COMMIT_SHA"):
        value = _clean(env_name)
        if value:
            return value
    # Dev local: tenta o git da árvore de trabalho (não existe na imagem Docker).
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=3,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        if out.returncode == 0:
            sha = out.stdout.strip()
            if sha:
                return sha
    except Exception:
        pass
    return "unknown"


def short_commit() -> str:
    sha = commit()
    return sha[:7] if sha and sha != "unknown" else sha


def service_role() -> str:
    return (_clean("SERVICE_ROLE") or "api").lower()


def summary() -> dict:
    """Resumo estável usado pelo endpoint /version e pelo log de boot do worker."""
    return {
        "service": "dsg-temporal",
        "role": service_role(),
        "package_version": __version__,
        "commit": commit(),
        "commit_short": short_commit(),
        "commit_message": _clean("RAILWAY_GIT_COMMIT_MESSAGE"),
        "branch": _clean("RAILWAY_GIT_BRANCH"),
        "deployment_id": _clean("RAILWAY_DEPLOYMENT_ID"),
        "booted_at": _BOOT_AT.isoformat(),
    }
