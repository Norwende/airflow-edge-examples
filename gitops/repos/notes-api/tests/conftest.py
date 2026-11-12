"""Fixtures partagées par les tests unitaires et d'intégration."""

import os
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from notes_api.config import Settings
from notes_api.main import create_app
from notes_api.repository import InMemoryNotesRepository

DATABASE_URL_ENV = "DATABASE_URL"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Ignore les tests marqués ``integration`` quand ``DATABASE_URL`` n'est pas définie."""
    if os.environ.get(DATABASE_URL_ENV):
        return
    skip = pytest.mark.skip(reason=f"{DATABASE_URL_ENV} non définie : pas de PostgreSQL")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def settings() -> Settings:
    """Configuration de test, indépendante des variables d'environnement de la machine."""
    return Settings(
        database_url=None,
        app_env="local",
        app_version="1.2.3",
        app_commit="0123abcd",
        log_level="DEBUG",
        log_format="json",
    )


@pytest.fixture
def repository() -> InMemoryNotesRepository:
    return InMemoryNotesRepository()


@pytest.fixture
def client(settings: Settings, repository: InMemoryNotesRepository) -> Iterator[TestClient]:
    """Client HTTP sur l'application, avec un dépôt en mémoire (lifespan exécuté)."""
    app = create_app(settings, repository=repository)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def database_url() -> str:
    """URI PostgreSQL des tests d'intégration (le marqueur les ignore si elle est absente)."""
    url = os.environ.get(DATABASE_URL_ENV)
    if not url:  # pragma: no cover - garde-fou si le test n'est pas marqué ``integration``
        pytest.skip(f"{DATABASE_URL_ENV} non définie")
    return url
