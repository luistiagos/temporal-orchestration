from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


_TEMPORAL_ADDRESS_RAILWAY_HINT = (
    "Set TEMPORAL_ADDRESS to "
    "'${{<temporal-service-name>.RAILWAY_PRIVATE_DOMAIN}}:7233'. "
    "In the current Railway project screenshot, that is likely "
    "'${{temporal-serve.RAILWAY_PRIVATE_DOMAIN}}:7233'."
)


def _clean_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _running_on_railway() -> bool:
    return any(
        _clean_env(name)
        for name in (
            "RAILWAY_ENVIRONMENT_ID",
            "RAILWAY_PROJECT_ID",
            "RAILWAY_SERVICE_ID",
            "RAILWAY_SERVICE_NAME",
        )
    )


def _str_env(name: str, default: str) -> str:
    return _clean_env(name) or default


def _bool_env(name: str, default: bool = False) -> bool:
    value = _clean_env(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "y", "on"}


def _int_env(name: str, default: int) -> int:
    raw = _clean_env(name)
    if raw is None:
        return default
    return int(raw)


def _temporal_address_env() -> str:
    address = _clean_env("TEMPORAL_ADDRESS")
    if address is None:
        if _running_on_railway():
            raise RuntimeError(
                "TEMPORAL_ADDRESS is empty or missing. "
                f"{_TEMPORAL_ADDRESS_RAILWAY_HINT}"
            )
        return "localhost:7233"

    if address.startswith(("http://", "https://")):
        raise RuntimeError(
            "TEMPORAL_ADDRESS must be a Temporal gRPC target like 'host:7233', "
            "not an HTTP URL. "
            f"{_TEMPORAL_ADDRESS_RAILWAY_HINT}"
        )

    if address.startswith(":") or "${{" in address:
        raise RuntimeError(
            f"TEMPORAL_ADDRESS={address!r} is not a valid Temporal target. "
            f"{_TEMPORAL_ADDRESS_RAILWAY_HINT}"
        )

    return address


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
        temporal_address=_temporal_address_env(),
        temporal_namespace=_str_env("TEMPORAL_NAMESPACE", "default"),
        temporal_task_queue=_str_env("TEMPORAL_TASK_QUEUE", "dsg-orchestrator"),
        api_host=_str_env("API_HOST", "0.0.0.0"),
        api_port=_int_env("API_PORT", 8090),
        legacy_backend_base_url=_str_env(
            "LEGACY_BACKEND_BASE_URL",
            "https://digitalstoregames.pythonanywhere.com",
        ).rstrip("/"),
        legacy_api_key=_str_env("LEGACY_API_KEY", ""),
        legacy_email_path=_str_env("LEGACY_EMAIL_PATH", "/remarket_v2"),
        legacy_whatsapp_path=_str_env("LEGACY_WHATSAPP_PATH", "/remarket_whatsapp"),
        legacy_purchase_check_path=_str_env(
            "LEGACY_PURCHASE_CHECK_PATH",
            "/user_has_purchase",
        ),
        legacy_event_callback_path=_str_env("LEGACY_EVENT_CALLBACK_PATH", ""),
        legacy_whatsapp_process_path=_str_env(
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
