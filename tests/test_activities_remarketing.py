"""Camada 3 — testes das activities de remarketing com HTTP mockado (responses).

Cobre a lógica determinística que NÃO pode ser exercida contra produção:
erros 5xx/4xx, DRY_RUN, construção do body, serialização de dataclass.

Cobre: CHK-01/02/03/05/07, EMAIL-01, WPP-02/04/07, CB-10.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
import responses

from dsg_temporal.activities import remarketing as remarketing_activities
from dsg_temporal.activities.remarketing import (
    _apply_whatsapp_pacing,
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


@pytest.fixture(autouse=True)
def _reset_wpp_pacing_state():
    """Zera o estado de pacing em memória entre testes: o pacing devolve
    pacing_required em vez de dormir, então estado residual de um teste
    anterior mudaria o resultado do seguinte."""
    remarketing_activities._dispatch_last_sent_at["whatsapp"] = 0.0
    remarketing_activities._dispatch_last_sent_at["email"] = 0.0
    yield


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
    def test_contrato_t2_le_purchased_e_checkout_pending(self, settings_env):
        """Resposta do backend desde o T2: dict com purchased + segundos do PIX
        aberto (docs/modules/remarketing/bugs/2026-11-06-abandono-...)."""
        responses.add(responses.GET, f"{BASE}/user_has_purchase",
                      json={"purchased": False, "checkout_pending_seconds": 1200},
                      status=200)
        result = check_purchase(_purchase_input())
        assert result.purchased is False
        assert result.checkout_pending_seconds == 1200

    @responses.activate
    def test_contrato_t2_comprou_sem_checkout(self, settings_env):
        responses.add(responses.GET, f"{BASE}/user_has_purchase",
                      json={"purchased": True, "checkout_pending_seconds": 0},
                      status=200)
        result = check_purchase(_purchase_input())
        assert result.purchased is True
        assert result.checkout_pending_seconds == 0

    @pytest.mark.parametrize("body", [
        True,                                          # bool cru (backend antigo)
        {"purchased": False},                          # sem o campo
        {"purchased": False, "checkout_pending_seconds": "x"},
        {"purchased": False, "checkout_pending_seconds": None},
        {"purchased": False, "checkout_pending_seconds": -30},
    ])
    @responses.activate
    def test_checkout_pending_ausente_ou_invalido_vira_zero(self, settings_env, body):
        responses.add(responses.GET, f"{BASE}/user_has_purchase", json=body, status=200)
        assert check_purchase(_purchase_input()).checkout_pending_seconds == 0

    @responses.activate
    def test_contrato_t6_le_conversation_active_seconds(self, settings_env):
        """Resposta do backend desde o T6: cliente em conversa com o agente
        (docs/modules/remarketing/bugs/2026-11-06-abandono-...)."""
        responses.add(responses.GET, f"{BASE}/user_has_purchase",
                      json={"purchased": False, "checkout_pending_seconds": 0,
                            "conversation_active_seconds": 420},
                      status=200)
        result = check_purchase(_purchase_input())
        assert result.purchased is False
        assert result.conversation_active_seconds == 420
        assert result.checkout_pending_seconds == 0

    @pytest.mark.parametrize("body", [
        True,                                          # bool cru (backend antigo)
        {"purchased": False, "checkout_pending_seconds": 900},  # backend do T2 (sem o campo)
        {"purchased": False, "conversation_active_seconds": "x"},
        {"purchased": False, "conversation_active_seconds": None},
        {"purchased": False, "conversation_active_seconds": -30},
    ])
    @responses.activate
    def test_conversation_active_ausente_ou_invalido_vira_zero(self, settings_env, body):
        responses.add(responses.GET, f"{BASE}/user_has_purchase", json=body, status=200)
        assert check_purchase(_purchase_input()).conversation_active_seconds == 0

    def test_dry_run_checkout_pending_zero(self, monkeypatch):
        monkeypatch.setenv("DRY_RUN", "true")
        monkeypatch.setenv("LEGACY_BACKEND_BASE_URL", BASE)
        get_settings.cache_clear()
        assert check_purchase(_purchase_input()).checkout_pending_seconds == 0

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
        result = dispatch_remarketing_step(_dispatch_input("email"))
        url = responses.calls[0].request.url
        assert "override%40teste.com" in url or "override@teste.com" in url
        assert result.recipient == "override@teste.com"
        assert result.recipient_overridden is True

    # T2 (docs/modules/remarketing/bugs/2026-11-24-detalhe-do-workflow-mostra-
    # email-como-enviado-sem-destinatario-nem-motivo.md): o /remarket_v2 diz
    # em HTTP 200 se enviou (ok/sent/reason); a activity tem de ler.
    @responses.activate
    def test_email_200_sent_true_vira_sent_com_destinatario_do_lead(self, settings_env):
        responses.add(responses.GET, f"{BASE}/remarket_v2",
                      json={"ok": True, "reason": "sent", "sent": True}, status=200)
        result = dispatch_remarketing_step(_dispatch_input("email"))
        assert result.status == "sent"
        assert result.recipient == "emuladores.emuladores@gmail.com"
        assert result.recipient_overridden is False

    @pytest.mark.parametrize("reason", ["user_has_purchase", "lead_status_purchase",
                                        "already_sent_dedup"])
    @responses.activate
    def test_email_200_nao_enviado_por_regra_vira_skipped_com_motivo(self, settings_env, reason):
        responses.add(responses.GET, f"{BASE}/remarket_v2",
                      json={"ok": True, "reason": reason, "sent": False}, status=200)
        result = dispatch_remarketing_step(_dispatch_input("email"))
        assert result.status == "skipped"
        assert result.reason == reason
        assert result.retryable is False
        assert result.recipient == "emuladores.emuladores@gmail.com"

    @responses.activate
    def test_email_200_sendmail_failed_vira_failed_nao_retentavel(self, settings_env):
        # A email_retry_queue do backend e a dona da retentativa.
        responses.add(responses.GET, f"{BASE}/remarket_v2",
                      json={"ok": False, "reason": "sendmail_failed", "sent": False},
                      status=200)
        result = dispatch_remarketing_step(_dispatch_input("email"))
        assert result.status == "failed"
        assert result.retryable is False
        assert result.reason == "sendmail_failed"
        assert result.recipient == "emuladores.emuladores@gmail.com"

    @responses.activate
    def test_email_200_sem_campo_sent_segue_sent(self, settings_env):
        # Contrato antigo (corpo sem "sent"): nada muda.
        responses.add(responses.GET, f"{BASE}/remarket_v2",
                      json={"ok": True}, status=200)
        result = dispatch_remarketing_step(_dispatch_input("email"))
        assert result.status == "sent"

    @responses.activate
    def test_email_falha_http_leva_destinatario(self, settings_env):
        responses.add(responses.GET, f"{BASE}/remarket_v2",
                      json={"error": "x"}, status=500)
        result = dispatch_remarketing_step(_dispatch_input("email"))
        assert result.status == "failed"
        assert result.recipient == "emuladores.emuladores@gmail.com"
        assert result.recipient_overridden is False


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
        result = dispatch_remarketing_step(_dispatch_input("whatsapp"))
        # 2 chamadas: snapshot e o envio. A última carrega o telefone de teste.
        send_url = responses.calls[-1].request.url
        assert "5541985311304" in send_url
        assert result.recipient == "5541985311304"
        assert result.recipient_overridden is True

    @responses.activate
    def test_whatsapp_sem_modo_teste_leva_telefone_do_lead(self, settings_env):
        responses.add(responses.GET, f"{BASE}/admin/wpp-sender/snapshot",
                      json=self._snapshot(), status=200)
        responses.add(responses.GET, f"{BASE}/remarket_whatsapp_ai",
                      json={"ok": False, "retryable": False, "reason": "number_not_exists"},
                      status=200)
        result = dispatch_remarketing_step(_dispatch_input("whatsapp"))
        assert result.status == "failed"
        assert result.recipient == "(41) 98531-1304"
        assert result.recipient_overridden is False

    @responses.activate
    def test_whatsapp_desabilitado_nao_leva_destinatario(self, settings_env):
        # Retorno anterior a troca de destino: nenhuma tentativa, nenhum destino.
        responses.add(responses.GET, f"{BASE}/admin/wpp-sender/snapshot",
                      json=self._snapshot(enabled=False), status=200)
        result = dispatch_remarketing_step(_dispatch_input("whatsapp"))
        assert result.status == "skipped"
        assert result.recipient is None


# ===========================================================================
# dispatch_remarketing_step — piso de pacing no bypass + semeadura pós-restart
# ===========================================================================

class TestWhatsAppPacingFloor:
    """bypass_pacing pula a FILA em memória, não a REGRA: o piso usa
    snapshot['last_sent_at_iso'] (DB) para segurar rajadas pós-restart."""

    def _snapshot(self, **over):
        snap = dict(enabled=True, sent_today=0, max_per_day=30,
                    min_interval_seconds=400, max_interval_seconds=800,
                    test_mode_enabled=False, test_phone="",
                    next_window_starts_in_seconds=3600)
        snap.update(over)
        return snap

    @responses.activate
    def test_bypass_com_envio_recente_no_db_devolve_pacing_required(self, settings_env):
        recent = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        responses.add(responses.GET, f"{BASE}/admin/wpp-sender/snapshot",
                      json=self._snapshot(last_sent_at_iso=recent), status=200)
        result = dispatch_remarketing_step(_dispatch_input("whatsapp", bypass_pacing=True))
        assert result.status == "pacing_required"
        wait = float(result.raw["wait_seconds"])
        # min_interval(400) - elapsed(~30) + jitter(1..30)
        assert 365 <= wait <= 405

    @responses.activate
    def test_bypass_com_envio_antigo_no_db_envia(self, settings_env):
        old = (datetime.now(timezone.utc) - timedelta(seconds=9999)).isoformat()
        responses.add(responses.GET, f"{BASE}/admin/wpp-sender/snapshot",
                      json=self._snapshot(last_sent_at_iso=old), status=200)
        responses.add(responses.GET, f"{BASE}/remarket_whatsapp_ai",
                      json={"ok": True, "message_id": "abc"}, status=200)
        result = dispatch_remarketing_step(_dispatch_input("whatsapp", bypass_pacing=True))
        assert result.status == "sent"

    @responses.activate
    def test_bypass_sem_last_sent_no_db_envia(self, settings_env):
        # Sem last_sent_at_iso no snapshot (nunca enviou), o piso não segura.
        responses.add(responses.GET, f"{BASE}/admin/wpp-sender/snapshot",
                      json=self._snapshot(), status=200)
        responses.add(responses.GET, f"{BASE}/remarket_whatsapp_ai",
                      json={"ok": True}, status=200)
        result = dispatch_remarketing_step(_dispatch_input("whatsapp", bypass_pacing=True))
        assert result.status == "sent"

    def test_semeadura_pos_restart_usa_o_db(self):
        # Memória zerada (worker recém-subido) + envio real há 40s no DB:
        # o pacing deve esperar target(100) - 40 ≈ 60s, não liberar na hora.
        recent = (datetime.now(timezone.utc) - timedelta(seconds=40)).isoformat()
        wait = _apply_whatsapp_pacing({
            "min_interval_seconds": 100,
            "max_interval_seconds": 100,
            "last_sent_at_iso": recent,
        })
        assert 55 <= wait <= 65

    def test_sem_memoria_e_sem_db_envia_imediato(self):
        wait = _apply_whatsapp_pacing({
            "min_interval_seconds": 100,
            "max_interval_seconds": 100,
        })
        assert wait == 0.0

    def test_pacing_queue_reserva_cumulativa_com_teto(self):
        # 10 chamadas consecutivas de pacing não podem explodir para 2 horas;
        # o teto limita o horizonte a max_interval_seconds * 2.5 + max_interval
        snapshot = {
            "min_interval_seconds": 100,
            "max_interval_seconds": 100,
            "last_sent_at_iso": datetime.now(timezone.utc).isoformat(),
        }
        waits = []
        for i in range(10):
            w = _apply_whatsapp_pacing(snapshot, f"key-{i}")
            waits.append(w)

        # O primeiro espera ~100s
        assert 95 <= waits[0] <= 105
        # O último deve ser limitado pelo teto (100 * 2.5 + 100 = 350s), longe de 1000s+
        assert waits[-1] <= 360


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
