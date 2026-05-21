"""Camada 3 — testes das activities de remarketing com HTTP mockado (responses).

Cobre a lógica determinística que NÃO pode ser exercida contra produção:
erros 5xx/4xx, DRY_RUN, construção do body, serialização de dataclass.

Cobre: CHK-01/02/03/05/07, EMAIL-01, WPP-02/04/07, CB-10.
"""
from __future__ import annotations

import json

import pytest
import responses

from dsg_temporal.activities.remarketing import (
    check_purchase,
    dispatch_remarketing_step,
    notify_remarketing_event,
)
from dsg_temporal.schemas import (
    DispatchStepInput,
    NotifyRemarketingEventInput,
    PurchaseCheckInput,
    RemarketingStep,
    RemarketingWorkflowState,
    SentRemarketingStep,
    WorkflowEvent,
)
from dsg_temporal.settings import get_settings

pytestmark = pytest.mark.unit

BASE = "http://backend.test"


@pytest.fixture
def settings_env(monkeypatch):
    """Configura env de teste e limpa o cache de get_settings."""
    monkeypatch.setenv("LEGACY_BACKEND_BASE_URL", BASE)
    monkeypatch.setenv("DRY_RUN", "false")
    monkeypatch.setenv("LEGACY_PURCHASE_CHECK_PATH", "/user_has_purchase")
    monkeypatch.setenv("LEGACY_EMAIL_PATH", "/remarket_v2")
    monkeypatch.setenv("LEGACY_WHATSAPP_AI_PATH", "/remarket_whatsapp_ai")
    monkeypatch.setenv("LEGACY_WPP_SENDER_SNAPSHOT_PATH", "/admin/wpp-sender/snapshot")
    monkeypatch.setenv("LEGACY_EVENT_CALLBACK_PATH", "/internal/remarket/event")
    monkeypatch.setenv("LEGACY_EVENT_CALLBACK_SECRET", "test-secret")
    monkeypatch.setenv("EMAIL_MIN_INTERVAL_SECONDS", "0")
    monkeypatch.delenv("REMARKETING_EMAIL_OVERRIDE_TO", raising=False)
    get_settings.cache_clear()
    yield monkeypatch


def _purchase_input(**over):
    base = dict(tenant_id="digitalstoregames", lead_id=999,
                email="emuladores.emuladores@gmail.com", phone="(41) 98531-1304",
                userip="138.204.25.70", fbp="fb.1.xxx", product_id=600004)
    base.update(over)
    return PurchaseCheckInput(**base)


def _dispatch_input(channel="email", **over):
    step = RemarketingStep(
        step_id="rs-12", order=1, channel=channel,
        template="remarket/email.html", subject="Volte!",
        metadata={"remarket_store_id": 12},
    )
    base = dict(
        tenant_id="digitalstoregames", lead_id=999, campaign_id="cart",
        step=step, cycle=1, idempotency_key="idem-1",
        email="emuladores.emuladores@gmail.com", phone="(41) 98531-1304",
        product_id=600004,
    )
    base.update(over)
    return DispatchStepInput(**base)


# ===========================================================================
# check_purchase
# ===========================================================================

class TestCheckPurchase:
    def test_dry_run_nao_faz_http(self, monkeypatch):
        monkeypatch.setenv("DRY_RUN", "true")
        monkeypatch.setenv("LEGACY_BACKEND_BASE_URL", BASE)
        get_settings.cache_clear()
        result = check_purchase(_purchase_input())
        assert result.purchased is False
        assert result.reason == "dry_run"

    @responses.activate
    def test_http_200_value_true(self, settings_env):
        responses.add(responses.GET, f"{BASE}/user_has_purchase",
                      json={"value": True}, status=200)
        assert check_purchase(_purchase_input()).purchased is True

    @responses.activate
    def test_http_200_haspurchase_false(self, settings_env):
        responses.add(responses.GET, f"{BASE}/user_has_purchase",
                      json={"haspurchase": False}, status=200)
        assert check_purchase(_purchase_input()).purchased is False

    @responses.activate
    def test_http_200_bare_true(self, settings_env):
        responses.add(responses.GET, f"{BASE}/user_has_purchase",
                      json=True, status=200)
        assert check_purchase(_purchase_input()).purchased is True

    @responses.activate
    @pytest.mark.parametrize("status", [500, 502, 503, 504, 429])
    def test_http_5xx_e_429_levantam_erro(self, settings_env, status):
        responses.add(responses.GET, f"{BASE}/user_has_purchase",
                      json={"err": "x"}, status=status)
        with pytest.raises(RuntimeError):
            check_purchase(_purchase_input())

    @responses.activate
    def test_http_400_levanta_erro(self, settings_env):
        responses.add(responses.GET, f"{BASE}/user_has_purchase",
                      json={"err": "bad"}, status=400)
        with pytest.raises(RuntimeError):
            check_purchase(_purchase_input())

    @responses.activate
    def test_nao_envia_userip_nem_fbp(self, settings_env):
        # CHK-07: userip/fbp são compartilhados — não devem ir na query.
        responses.add(responses.GET, f"{BASE}/user_has_purchase",
                      json={"value": False}, status=200)
        check_purchase(_purchase_input())
        url = responses.calls[0].request.url
        assert "userip" not in url
        assert "fbp" not in url
        assert "email=" in url


# ===========================================================================
# dispatch_remarketing_step — email
# ===========================================================================

class TestDispatchEmail:
    def test_dry_run_email_retorna_sent(self, monkeypatch):
        monkeypatch.setenv("DRY_RUN", "true")
        monkeypatch.setenv("LEGACY_BACKEND_BASE_URL", BASE)
        get_settings.cache_clear()
        result = dispatch_remarketing_step(_dispatch_input("email"))
        assert result.status == "sent"

    @responses.activate
    def test_email_200_status_sent(self, settings_env):
        responses.add(responses.GET, f"{BASE}/remarket_v2",
                      json={"ok": True, "reason": "sent"}, status=200)
        result = dispatch_remarketing_step(_dispatch_input("email"))
        assert result.status == "sent"

    @responses.activate
    def test_email_500_failed_retryable(self, settings_env):
        responses.add(responses.GET, f"{BASE}/remarket_v2",
                      json={"error": "x"}, status=500)
        result = dispatch_remarketing_step(_dispatch_input("email"))
        assert result.status == "failed"
        assert result.retryable is True

    @responses.activate
    def test_email_400_failed_nao_retryable(self, settings_env):
        responses.add(responses.GET, f"{BASE}/remarket_v2",
                      json={"error": "bad"}, status=400)
        result = dispatch_remarketing_step(_dispatch_input("email"))
        assert result.status == "failed"
        assert result.retryable is False

    @responses.activate
    def test_email_override_redireciona_destinatario(self, settings_env):
        settings_env.setenv("REMARKETING_EMAIL_OVERRIDE_TO", "override@teste.com")
        get_settings.cache_clear()
        responses.add(responses.GET, f"{BASE}/remarket_v2",
                      json={"ok": True}, status=200)
        dispatch_remarketing_step(_dispatch_input("email"))
        url = responses.calls[0].request.url
        assert "override%40teste.com" in url or "override@teste.com" in url


# ===========================================================================
# dispatch_remarketing_step — whatsapp
# ===========================================================================

class TestDispatchWhatsApp:
    def _snapshot(self, **over):
        snap = dict(enabled=True, sent_today=0, max_per_day=30,
                    min_interval_seconds=1, max_interval_seconds=1,
                    test_mode_enabled=False, test_phone="",
                    next_window_starts_in_seconds=3600)
        snap.update(over)
        return snap

    @responses.activate
    def test_snapshot_indisponivel_failed_retryable(self, settings_env):
        responses.add(responses.GET, f"{BASE}/admin/wpp-sender/snapshot",
                      json={"err": "x"}, status=500)
        result = dispatch_remarketing_step(_dispatch_input("whatsapp"))
        assert result.status == "failed"
        assert result.retryable is True

    @responses.activate
    def test_whatsapp_desabilitado_skipped(self, settings_env):
        responses.add(responses.GET, f"{BASE}/admin/wpp-sender/snapshot",
                      json=self._snapshot(enabled=False), status=200)
        result = dispatch_remarketing_step(_dispatch_input("whatsapp"))
        assert result.status == "skipped"

    @responses.activate
    def test_cap_diario_atingido(self, settings_env):
        responses.add(responses.GET, f"{BASE}/admin/wpp-sender/snapshot",
                      json=self._snapshot(sent_today=30, max_per_day=30), status=200)
        result = dispatch_remarketing_step(_dispatch_input("whatsapp"))
        assert result.status == "cap_reached"
        assert result.raw.get("next_window_seconds") == 3600

    @responses.activate
    def test_whatsapp_200_ok_false_failed(self, settings_env):
        responses.add(responses.GET, f"{BASE}/admin/wpp-sender/snapshot",
                      json=self._snapshot(), status=200)
        responses.add(responses.GET, f"{BASE}/remarket_whatsapp_ai",
                      json={"ok": False, "retryable": False, "reason": "number_not_exists"},
                      status=200)
        result = dispatch_remarketing_step(_dispatch_input("whatsapp"))
        assert result.status == "failed"
        assert result.retryable is False
        assert "number_not_exists" in result.reason

    @responses.activate
    def test_whatsapp_sucesso(self, settings_env):
        responses.add(responses.GET, f"{BASE}/admin/wpp-sender/snapshot",
                      json=self._snapshot(), status=200)
        responses.add(responses.GET, f"{BASE}/remarket_whatsapp_ai",
                      json={"ok": True, "message_id": "abc123"}, status=200)
        result = dispatch_remarketing_step(_dispatch_input("whatsapp"))
        assert result.status == "sent"
        assert result.provider_message_id == "abc123"

    @responses.activate
    def test_modo_teste_redireciona_telefone(self, settings_env):
        responses.add(responses.GET, f"{BASE}/admin/wpp-sender/snapshot",
                      json=self._snapshot(test_mode_enabled=True, test_phone="5541985311304"),
                      status=200)
        responses.add(responses.GET, f"{BASE}/remarket_whatsapp_ai",
                      json={"ok": True}, status=200)
        dispatch_remarketing_step(_dispatch_input("whatsapp"))
        # 2 chamadas: snapshot e o envio. A última carrega o telefone de teste.
        send_url = responses.calls[-1].request.url
        assert "5541985311304" in send_url


# ===========================================================================
# notify_remarketing_event  (CB-10 — serialização)
# ===========================================================================

class TestNotifyRemarketingEvent:
    def _payload(self):
        state = RemarketingWorkflowState(
            status="completed", tenant_id="digitalstoregames",
            lead_id=999, campaign_id="cart",
            sent_steps=[SentRemarketingStep(
                step_id="rs-12", channel="email", cycle=1, status="sent",
                idempotency_key="idem-1", sent_at_iso="2026-05-20T12:00:00+00:00",
            )],
            events=[WorkflowEvent(event_type="workflow_completed",
                                  message="done", at_iso="2026-05-20T12:00:00+00:00")],
        )
        return NotifyRemarketingEventInput(
            workflow_id="remarketing-x-y-999",
            state=state,
            event=state.events[-1],
        )

    def test_dry_run_nao_faz_http(self, monkeypatch):
        monkeypatch.setenv("DRY_RUN", "true")
        monkeypatch.setenv("LEGACY_EVENT_CALLBACK_PATH", "/internal/remarket/event")
        get_settings.cache_clear()
        # Não deve levantar nem chamar HTTP (responses não ativo aqui).
        assert notify_remarketing_event(self._payload()) is None

    @responses.activate
    def test_envia_event_como_string_e_state_como_dict(self, settings_env):
        responses.add(responses.POST, f"{BASE}/internal/remarket/event",
                      json={"success": True}, status=200)
        notify_remarketing_event(self._payload())
        body = json.loads(responses.calls[0].request.body)
        # 'event' deve ser STRING (event_type), não dict.
        assert body["event"] == "workflow_completed"
        assert isinstance(body["event"], str)
        # 'state' deve ser dict serializável (dataclass convertida).
        assert isinstance(body["state"], dict)
        assert body["state"]["status"] == "completed"
        assert body["state"]["sent_steps"][0]["step_id"] == "rs-12"
        # 'event_details' carrega o objeto completo.
        assert body["event_details"]["event_type"] == "workflow_completed"

    @responses.activate
    def test_envia_header_de_secret(self, settings_env):
        responses.add(responses.POST, f"{BASE}/internal/remarket/event",
                      json={"success": True}, status=200)
        notify_remarketing_event(self._payload())
        assert responses.calls[0].request.headers.get("X-Callback-Secret") == "test-secret"

    @responses.activate
    def test_callback_http_500_nao_levanta(self, settings_env):
        # Falha do callback é best-effort — não deve propagar exceção.
        responses.add(responses.POST, f"{BASE}/internal/remarket/event",
                      json={"err": "x"}, status=500)
        assert notify_remarketing_event(self._payload()) is None
