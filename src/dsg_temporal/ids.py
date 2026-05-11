from __future__ import annotations

import re


_SAFE_RE = re.compile(r"[^a-zA-Z0-9_.-]+")


def safe_id(value: object, fallback: str = "unknown") -> str:
    text = str(value or "").strip()
    if not text:
        text = fallback
    text = _SAFE_RE.sub("-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text[:180] or fallback


def canonical_phone(raw: str | None) -> str:
    if not raw:
        return ""
    return re.sub(r"\D+", "", raw)


def remarketing_workflow_id(
    tenant_id: str,
    lead_id: int | str,
    campaign_id: str | None = None,
) -> str:
    tenant = safe_id(tenant_id, "tenant")
    campaign = safe_id(campaign_id, "default")
    lead = safe_id(lead_id, "lead")
    return f"remarketing-{tenant}-{campaign}-{lead}"


def whatsapp_workflow_id(tenant_id: str, phone_or_jid: str) -> str:
    tenant = safe_id(tenant_id, "tenant")
    phone = canonical_phone(phone_or_jid) or safe_id(phone_or_jid, "jid")
    return f"whatsapp-{tenant}-{phone}"


def remarketing_idempotency_key(
    tenant_id: str,
    lead_id: int | str,
    campaign_id: str,
    step_id: str,
    cycle: int,
) -> str:
    return ":".join(
        [
            "remarketing",
            safe_id(tenant_id),
            safe_id(campaign_id),
            safe_id(lead_id),
            safe_id(step_id),
            str(cycle),
        ]
    )

