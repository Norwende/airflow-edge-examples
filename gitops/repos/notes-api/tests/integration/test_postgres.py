"""Tests contre un PostgreSQL réel (``DATABASE_URL``), ignorés si la variable est absente."""

import uuid
from collections.abc import Iterator

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool, PoolTimeout
from pydantic import SecretStr

from notes_api import db
from notes_api.config import Settings
from notes_api.main import create_app
from notes_api.models import NoteCreate
from notes_api.repository import PostgresNotesRepository

pytestmark = pytest.mark.integration


@pytest.fixture
def pool(database_url: str) -> Iterator[ConnectionPool]:
    pool = db.open_pool(database_url)
    db.init_schema(pool)
    with pool.connection() as conn:
        conn.execute("TRUNCATE notes")
    yield pool
    pool.close()


@pytest.fixture
def pg_repository(pool: ConnectionPool) -> PostgresNotesRepository:
    return PostgresNotesRepository(pool)


def test_schema_init_is_idempotent(pool: ConnectionPool) -> None:
    db.init_schema(pool)
    db.init_schema(pool)
    with pool.connection() as conn:
        row = conn.execute("SELECT to_regclass('public.notes') IS NOT NULL").fetchone()
    assert row == (True,)


def test_ping(pg_repository: PostgresNotesRepository) -> None:
    pg_repository.ping()


def test_crud_cycle(pg_repository: PostgresNotesRepository) -> None:
    note = pg_repository.create_note(NoteCreate(title="titre", content="contenu"))
    assert note.title == "titre"
    assert note.content == "contenu"
    assert note.created_at.tzinfo is not None

    assert pg_repository.get_note(note.id) == note
    assert pg_repository.list_notes(limit=10, offset=0) == [note]
    assert pg_repository.delete_note(note.id) is True
    assert pg_repository.get_note(note.id) is None
    assert pg_repository.delete_note(note.id) is False


def test_get_unknown(pg_repository: PostgresNotesRepository) -> None:
    assert pg_repository.get_note(uuid.uuid4()) is None


def test_list_order_and_pagination(pg_repository: PostgresNotesRepository) -> None:
    notes = [pg_repository.create_note(NoteCreate(title=f"n{i}")) for i in range(3)]
    listed = pg_repository.list_notes(limit=10, offset=0)
    assert [n.id for n in listed] == [n.id for n in reversed(notes)]
    assert [n.id for n in pg_repository.list_notes(limit=1, offset=2)] == [notes[0].id]


def test_open_pool_fails_fast_on_unreachable_database(
    database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Le pool abandonne après POOL_OPEN_TIMEOUT_S si aucune connexion n'aboutit.

    On vise une base inexistante plutôt qu'un mauvais mot de passe : l'image officielle
    ``postgres`` fait confiance aux connexions locales (``host all all 127.0.0.1/32 trust``),
    donc en CI (service dans le même pod, joint via 127.0.0.1) un mauvais mot de passe est accepté.
    """
    monkeypatch.setattr(db, "POOL_OPEN_TIMEOUT_S", 2.0)
    conninfo = psycopg.conninfo.make_conninfo(database_url, dbname="does_not_exist")
    with pytest.raises(PoolTimeout):
        db.open_pool(conninfo)


def test_full_application(database_url: str, pool: ConnectionPool) -> None:
    """Application complète : lifespan réel (pool + schéma) et endpoints sur PostgreSQL."""
    settings = Settings(database_url=SecretStr(database_url), app_version="9.9.9")
    with TestClient(create_app(settings)) as client:
        assert client.get("/readyz").json() == {"status": "ok"}
        created = client.post("/notes", json={"title": "intégration", "content": "ok"})
        assert created.status_code == 201
        note_id = created.json()["id"]
        assert client.get(f"/notes/{note_id}").json()["title"] == "intégration"
        assert [n["id"] for n in client.get("/notes").json()] == [note_id]
        assert client.delete(f"/notes/{note_id}").status_code == 204
        assert client.get("/version").json()["version"] == "9.9.9"
