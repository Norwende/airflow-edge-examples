"""Lecture de la configuration depuis l'environnement."""

import pytest
from pydantic import ValidationError

from notes_api.config import Settings

ENV_VARS = ("DATABASE_URL", "APP_ENV", "APP_VERSION", "APP_COMMIT", "LOG_LEVEL", "LOG_FORMAT")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_defaults() -> None:
    settings = Settings()
    assert settings.database_url is None
    assert settings.app_env == "local"
    assert settings.app_version == "0.0.0"
    assert settings.app_commit == "unknown"
    assert settings.log_level == "INFO"
    assert settings.log_format == "json"


def test_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:secret@db:5432/notes")
    monkeypatch.setenv("APP_ENV", "PROD")
    monkeypatch.setenv("APP_VERSION", "1.4.0")
    monkeypatch.setenv("APP_COMMIT", "deadbeef")
    monkeypatch.setenv("LOG_LEVEL", "warning")
    monkeypatch.setenv("LOG_FORMAT", "Console")

    settings = Settings()
    assert settings.database_url is not None
    assert settings.database_url.get_secret_value() == "postgresql://u:secret@db:5432/notes"
    assert "secret" not in repr(settings)  # l'URI (mot de passe) n'apparaît jamais en clair
    assert settings.app_env == "prod"
    assert settings.app_version == "1.4.0"
    assert settings.app_commit == "deadbeef"
    assert settings.log_level == "WARNING"
    assert settings.log_format == "console"


def test_blank_values_fall_back_to_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_VERSION", " ")
    monkeypatch.setenv("APP_COMMIT", "")
    settings = Settings()
    assert settings.app_version == "0.0.0"
    assert settings.app_commit == "unknown"


@pytest.mark.parametrize(
    ("name", "value"),
    [("APP_ENV", "staging"), ("LOG_LEVEL", "verbose"), ("LOG_FORMAT", "xml")],
)
def test_invalid_values(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        Settings()
