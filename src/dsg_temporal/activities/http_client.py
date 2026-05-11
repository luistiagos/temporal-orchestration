from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

import requests

from dsg_temporal.settings import get_settings


def post_legacy_json(
    path: str,
    payload: dict[str, Any],
    *,
    idempotency_key: str | None = None,
) -> requests.Response:
    settings = get_settings()
    url = urljoin(settings.legacy_backend_base_url + "/", path.lstrip("/"))
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "dsg-temporal-orchestrator/0.1",
    }
    if settings.legacy_api_key:
        headers["Authorization"] = f"Bearer {settings.legacy_api_key}"
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
        headers["X-Idempotency-Key"] = idempotency_key

    return requests.post(
        url,
        json=payload,
        headers=headers,
        timeout=settings.http_timeout_seconds,
    )


def response_json_or_raw(response: requests.Response) -> dict[str, Any]:
    try:
        data = response.json()
        return data if isinstance(data, dict) else {"value": data}
    except ValueError:
        return {"raw": response.text[:1000]}

