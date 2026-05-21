"""Camada 1 — unitário. Janela de envio de WhatsApp (quiet hours 09:00–20:00 BRT).

Cobre os casos WPP-09 do plano de testes. Função pura, sem infra.
"""
from datetime import datetime, timedelta, timezone

from dsg_temporal.workflows.remarketing import _whatsapp_next_allowed

UTC = timezone.utc


def test_dentro_da_janela_retorna_o_proprio_horario():
    # 15:00 UTC = 12:00 BRT — dentro de 09–20.
    now = datetime(2026, 5, 20, 15, 0, tzinfo=UTC)
    assert _whatsapp_next_allowed(now) == now


def test_limite_inferior_09h_brt_esta_dentro():
    # 12:00 UTC = 09:00 BRT — 9 <= 9 < 20, dentro.
    now = datetime(2026, 5, 20, 12, 0, tzinfo=UTC)
    assert _whatsapp_next_allowed(now) == now


def test_limite_superior_20h_brt_esta_fora():
    # 23:00 UTC = 20:00 BRT — 20 não é < 20, fora. Posterga p/ 09:00 do dia seguinte.
    now = datetime(2026, 5, 20, 23, 0, tzinfo=UTC)
    # 09:00 BRT do dia 21 = 12:00 UTC do dia 21.
    assert _whatsapp_next_allowed(now) == datetime(2026, 5, 21, 12, 0, tzinfo=UTC)


def test_madrugada_posterga_para_09h_do_mesmo_dia():
    # 06:00 UTC = 03:00 BRT — antes das 09:00. Posterga p/ 09:00 BRT do mesmo dia.
    now = datetime(2026, 5, 20, 6, 0, tzinfo=UTC)
    assert _whatsapp_next_allowed(now) == datetime(2026, 5, 20, 12, 0, tzinfo=UTC)


def test_noite_posterga_para_09h_do_dia_seguinte():
    # 02:00 UTC = 23:00 BRT do dia anterior — fora. Posterga p/ 09:00 BRT seguinte.
    now = datetime(2026, 5, 20, 2, 0, tzinfo=UTC)
    # BRT = 2026-05-19 23:00 -> 09:00 BRT do dia 20 = 12:00 UTC do dia 20.
    assert _whatsapp_next_allowed(now) == datetime(2026, 5, 20, 12, 0, tzinfo=UTC)


def test_datetime_naive_e_tratado_como_utc():
    now_naive = datetime(2026, 5, 20, 15, 0)  # sem tzinfo
    result = _whatsapp_next_allowed(now_naive)
    assert result == datetime(2026, 5, 20, 15, 0, tzinfo=UTC)
    assert result.tzinfo is not None


def test_resultado_postergado_sempre_dentro_da_janela():
    # Qualquer horário fora da janela deve resultar num horário dentro dela.
    for hour_utc in range(24):
        now = datetime(2026, 5, 20, hour_utc, 30, tzinfo=UTC)
        allowed = _whatsapp_next_allowed(now)
        brt_hour = (allowed + timedelta(hours=-3)).hour
        assert 9 <= brt_hour < 20, f"hora UTC {hour_utc} resultou em BRT {brt_hour}"
