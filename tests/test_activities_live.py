"""Camada 3 (live) — testes de activity contra os serviços REAIS.

Marcados `live`: ficam FORA da execução padrão do CI. Rodar com:
    pytest -m live

Usam o backend de produção e o contato de teste:
    email   : emuladores.emuladores@gmail.com
    whatsapp: 5541985311304

`check_purchase` é read-only (apenas consulta) — seguro de rodar a qualquer
momento. Despachos reais de email/WhatsApp são cobertos pelo smoke test E2E
(Fase 7), pois geram efeito colateral (mensagem enviada) a cada execução.
"""
from __future__ import annotations

import pytest

from dsg_temporal.activities.remarketing import check_purchase
from dsg_temporal.schemas import PurchaseCheckInput, PurchaseCheckResult
from dsg_temporal.settings import get_settings

pytestmark = pytest.mark.live

BACKEND = "https://digitalstoregames.pythonanywhere.com"
TEST_EMAIL = "emuladores.emuladores@gmail.com"
TEST_PHONE = "(41) 98531-1304"


@pytest.fixture
def live_env(monkeypatch):
    monkeypatch.setenv("LEGACY_BACKEND_BASE_URL", BACKEND)
    monkeypatch.setenv("DRY_RUN", "false")
    monkeypatch.setenv("LEGACY_PURCHASE_CHECK_PATH", "/user_has_purchase")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_check_purchase_real_retorna_resultado(live_env):
    """check_purchase contra o /user_has_purchase real — read-only.

    Não assertamos True/False (depende do estado da base), apenas que a
    activity completa sem erro e devolve um PurchaseCheckResult válido.
    """
    result = check_purchase(
        PurchaseCheckInput(
            tenant_id="digitalstoregames",
            lead_id=0,
            email=TEST_EMAIL,
            phone=TEST_PHONE,
            product_id=600004,
        )
    )
    assert isinstance(result, PurchaseCheckResult)
    assert isinstance(result.purchased, bool)
