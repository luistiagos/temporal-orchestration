"""Camada 1 — unitário. Janela de envio de WhatsApp (quiet hours 09:00–20:00 BRT).

Cobre os casos WPP-09 do plano de testes. Função pura, sem infra.
"""
from datetime import datetime, timedelta, timezone

from dsg_temporal.workflows.remarketing import (
    _normalize_window,
    _whatsapp_next_allowed,
    _whatsapp_resume_after_cap,
)

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


# ---------------------------------------------------------------------------
# Retomada após cap diário — NÃO pode re-despachar de madrugada.
# Regressão de docs/bugs/close/2026-06-24-remarketing-whatsapp-fora-do-horario-comercial.md
# ---------------------------------------------------------------------------

def test_cap_reset_a_meia_noite_retoma_so_as_09h():
    # Cap batido às 15:00 BRT (18:00 UTC). Reset do cap = próx. 00:00 BRT, que
    # em segundos é 9h à frente (15:00 -> 00:00). Antes do fix o workflow
    # acordava à meia-noite; agora deve retomar só às 09:00 BRT (12:00 UTC).
    now = datetime(2026, 5, 20, 18, 0, tzinfo=UTC)          # 15:00 BRT
    seconds_to_midnight_brt = 9 * 3600                       # 15:00 -> 00:00 BRT
    resume = _whatsapp_resume_after_cap(now, seconds_to_midnight_brt)
    # 09:00 BRT do dia 21 = 12:00 UTC do dia 21.
    assert resume == datetime(2026, 5, 21, 12, 0, tzinfo=UTC)
    assert 9 <= (resume + timedelta(hours=-3)).hour < 20


def test_reset_dentro_da_janela_retoma_no_proprio_reset():
    # Reset do cap caindo às 10:00 BRT (dentro de 09–20) → retoma no reset.
    now = datetime(2026, 5, 20, 12, 0, tzinfo=UTC)          # 09:00 BRT
    one_hour = 3600                                          # 09:00 -> 10:00 BRT
    resume = _whatsapp_resume_after_cap(now, one_hour)
    assert resume == now + timedelta(seconds=one_hour)       # 10:00 BRT


def test_reset_logo_apos_as_20h_empurra_para_o_dia_seguinte():
    # Reset caindo às 20:30 BRT (>= 20, fora) → próximo 09:00 BRT do dia seguinte.
    now = datetime(2026, 5, 20, 22, 30, tzinfo=UTC)         # 19:30 BRT
    one_hour = 3600                                          # 19:30 -> 20:30 BRT
    resume = _whatsapp_resume_after_cap(now, one_hour)
    assert resume == datetime(2026, 5, 21, 12, 0, tzinfo=UTC)  # 09:00 BRT dia 21


def test_resume_nunca_antes_do_reset_do_cap():
    # Invariante: a retomada nunca pode ser antes do reset do cap (senão o
    # cap ainda estaria ativo e re-despacharíamos em loop).
    for hour_utc in range(24):
        now = datetime(2026, 5, 20, hour_utc, 30, tzinfo=UTC)
        cap_reset_at = now + timedelta(seconds=3600)
        resume = _whatsapp_resume_after_cap(now, 3600)
        assert resume >= cap_reset_at
        assert 9 <= (resume + timedelta(hours=-3)).hour < 20


# ---------------------------------------------------------------------------
# Janela configurável por campanha (parametrizada).
# Feature docs/funcionalidades/2026-06-24-janela-envio-por-campanha.md
# ---------------------------------------------------------------------------

def test_normalize_window_default_quando_invalido():
    assert _normalize_window(None, None) == (9, 20)        # ausente
    assert _normalize_window("x", 20) == (9, 20)           # não inteiro
    assert _normalize_window(20, 8) == (9, 20)             # start >= end
    assert _normalize_window(9, 9) == (9, 20)              # vazia
    assert _normalize_window(8, 22) == (8, 22)             # válida mantém
    assert _normalize_window(-5, 30) == (0, 24)            # clamp aos limites


def test_janela_custom_posterga_para_o_inicio_configurado():
    # Janela 08:00–22:00. 23:00 BRT (02:00 UTC) está fora → próximo 08:00 BRT.
    now = datetime(2026, 5, 20, 2, 0, tzinfo=UTC)          # 23:00 BRT dia 19
    allowed = _whatsapp_next_allowed(now, 8, 22)
    assert allowed == datetime(2026, 5, 20, 11, 0, tzinfo=UTC)  # 08:00 BRT dia 20


def test_janela_custom_dentro_nao_posterga():
    # 10:00 BRT (13:00 UTC) dentro de 08–22 → sem adiamento.
    now = datetime(2026, 5, 20, 13, 0, tzinfo=UTC)
    assert _whatsapp_next_allowed(now, 8, 22) == now


def test_janela_ate_meia_noite_end_24():
    # Janela 09:00–24:00: 22:00 BRT (01:00 UTC dia seguinte) está dentro.
    now = datetime(2026, 5, 21, 1, 0, tzinfo=UTC)          # 22:00 BRT dia 20
    assert _whatsapp_next_allowed(now, 9, 24) == now
    # 03:00 BRT (06:00 UTC) está fora → próximo 09:00 BRT do mesmo dia.
    madrugada = datetime(2026, 5, 20, 6, 0, tzinfo=UTC)
    assert _whatsapp_next_allowed(madrugada, 9, 24) == datetime(2026, 5, 20, 12, 0, tzinfo=UTC)


def test_cap_resume_respeita_janela_custom():
    # Cap batido às 15:00 BRT (18:00 UTC), reset à meia-noite (9h à frente),
    # janela 10:00–18:00 → retoma às 10:00 BRT do dia seguinte (não 00:00).
    now = datetime(2026, 5, 20, 18, 0, tzinfo=UTC)         # 15:00 BRT
    resume = _whatsapp_resume_after_cap(now, 9 * 3600, 10, 18)
    assert resume == datetime(2026, 5, 21, 13, 0, tzinfo=UTC)  # 10:00 BRT dia 21
