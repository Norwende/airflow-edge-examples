"""Fabrique d'application et lifespan (sans base réelle)."""

from dataclasses import dataclass

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from notes_api import db, main
from notes_api.config import Settings
from notes_api.repository import PostgresNotesRepository


@dataclass
class FakePool:
    """Remplace le ConnectionPool psycopg : seul le cycle de vie est vérifié ici."""

    conninfo: str
    max_size: int = 5
    schema_initialized: bool = False
    closed: bool = False

    def close(self) -> None:
        self.closed = True


def test_module_level_app_exists() -> None:
    assert main.app.title == "notes-api"


def test_lifespan_requires_database_url(settings: Settings) -> None:
    app = main.create_app(settings)  # pas de dépôt injecté, pas de DATABASE_URL
    with pytest.raises(RuntimeError, match="DATABASE_URL"), TestClient(app):
        pass


def test_lifespan_opens_and_closes_postgres_pool(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    pools: list[FakePool] = []

    def fake_open_pool(conninfo: str) -> FakePool:
        pools.append(FakePool(conninfo))
        return pools[-1]

    def fake_init_schema(pool: FakePool) -> None:
        pool.schema_initialized = True

    monkeypatch.setattr(db, "open_pool", fake_open_pool)
    monkeypatch.setattr(db, "init_schema", fake_init_schema)
    url = "postgresql://notes:notes@db:5432/notes"
    app = main.create_app(settings.model_copy(update={"database_url": SecretStr(url)}))

    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        (pool,) = pools
        assert pool.conninfo == url
        assert pool.schema_initialized
        assert isinstance(app.state.repository, PostgresNotesRepository)
        assert not pool.closed
    assert pool.closed


def test_pool_is_closed_when_schema_init_fails(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    pools: list[FakePool] = []

    def fake_open_pool(conninfo: str) -> FakePool:
        pools.append(FakePool(conninfo))
        return pools[-1]

    def failing_init_schema(pool: FakePool) -> None:
        raise ConnectionError("base injoignable")

    monkeypatch.setattr(db, "open_pool", fake_open_pool)
    monkeypatch.setattr(db, "init_schema", failing_init_schema)
    app = main.create_app(settings.model_copy(update={"database_url": SecretStr("postgresql://x")}))

    with pytest.raises(ConnectionError), TestClient(app):
        pass
    assert pools[0].closed
