import pytest

from dsg_temporal.settings import get_settings


def test_temporal_address_defaults_locally_when_missing(monkeypatch):
    monkeypatch.delenv("TEMPORAL_ADDRESS", raising=False)
    monkeypatch.delenv("RAILWAY_SERVICE_NAME", raising=False)
    get_settings.cache_clear()

    assert get_settings().temporal_address == "localhost:7233"


def test_temporal_address_empty_fails_on_railway(monkeypatch):
    monkeypatch.setenv("TEMPORAL_ADDRESS", "")
    monkeypatch.setenv("RAILWAY_SERVICE_NAME", "dsg-temporal-api")
    get_settings.cache_clear()

    with pytest.raises(RuntimeError, match="TEMPORAL_ADDRESS is empty or missing"):
        get_settings()


def test_temporal_address_rejects_http_url(monkeypatch):
    monkeypatch.setenv("TEMPORAL_ADDRESS", "https://temporal-serve.railway.internal:7233")
    get_settings.cache_clear()

    with pytest.raises(RuntimeError, match="not an HTTP URL"):
        get_settings()


def test_temporal_address_trims_valid_value(monkeypatch):
    monkeypatch.setenv("TEMPORAL_ADDRESS", " temporal-serve.railway.internal:7233 ")
    get_settings.cache_clear()

    assert get_settings().temporal_address == "temporal-serve.railway.internal:7233"
