from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


@dataclass(frozen=True)
class Settings:
    temporal_address: str
    temporal_namespace: str
    temporal_task_queue: str
    api_host: str
    api_port: int
    legacy_backend_base_url: str
    legacy_api_key: str
    legacy_email_path: str
    legacy_whatsapp_path: str
    legacy_purchase_check_path: str
    legacy_event_callback_path: str
    legacy_whatsapp_process_path: str
    dry_run: bool
    assume_purchased_on_check_error: bool
    http_timeout_seconds: int
    activity_max_workers: int


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        temporal_address=os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
        temporal_namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
        temporal_task_queue=os.getenv("TEMPORAL_TASK_QUEUE", "dsg-orchestrator"),
        api_host=os.getenv("API_HOST", "0.0.0.0"),
        api_port=_int_env("API_PORT", 8090),
        legacy_backend_base_url=os.getenv(
            "LEGACY_BACKEND_BASE_URL",
            "https://digitalstoregames.pythonanywhere.com",
        ).rstrip("/"),
        legacy_api_key=os.getenv("LEGACY_API_KEY", ""),
        legacy_email_path=os.getenv("LEGACY_EMAIL_PATH", "/remarket_v2"),
        legacy_whatsapp_path=os.getenv("LEGACY_WHATSAPP_PATH", "/remarket_whatsapp"),
        legacy_purchase_check_path=os.getenv(
            "LEGACY_PURCHASE_CHECK_PATH",
            "/user_has_purchase",
        ),
        legacy_event_callback_path=os.getenv("LEGACY_EVENT_CALLBACK_PATH", ""),
        legacy_whatsapp_process_path=os.getenv(
            "LEGACY_WHATSAPP_PROCESS_PATH",
            "/temporal/whatsapp/process",
        ),
        dry_run=_bool_env("DRY_RUN", True),
        assume_purchased_on_check_error=_bool_env(
            "ASSUME_PURCHASED_ON_CHECK_ERROR",
            True,
        ),
        http_timeout_seconds=_int_env("HTTP_TIMEOUT_SECONDS", 20),
        activity_max_workers=_int_env("ACTIVITY_MAX_WORKERS", 20),
    )

