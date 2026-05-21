"""Fixtures compartilhadas dos testes do dsg-temporal."""
import pytest

from dsg_temporal.activities import remarketing as _rmkt
from dsg_temporal.settings import get_settings


@pytest.fixture(autouse=True)
def _reset_settings_cache():
    """get_settings() é lru_cache — limpar antes/depois de cada teste evita
    vazamento de configuração entre testes."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
def _reset_dispatch_globals():
    """Os locks de throttle/pacing guardam 'last_sent_at' em variáveis de
    módulo. Zerar antes de cada teste evita esperas reais (sleep) herdadas
    de um teste anterior."""
    _rmkt._dispatch_last_sent_at["whatsapp"] = 0.0
    _rmkt._dispatch_last_sent_at["email"] = 0.0
    yield
