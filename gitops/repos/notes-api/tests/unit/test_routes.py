"""Endpoints HTTP, avec le dépôt en mémoire."""

import uuid

from fastapi.testclient import TestClient

from notes_api.repository import InMemoryNotesRepository


def test_healthz(client: TestClient) -> None:
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_healthz_ignores_storage(client: TestClient, repository: InMemoryNotesRepository) -> None:
    repository.healthy = False
    assert client.get("/healthz").status_code == 200


def test_readyz_ok(client: TestClient) -> None:
    response = client.get("/readyz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readyz_unavailable(client: TestClient, repository: InMemoryNotesRepository) -> None:
    repository.healthy = False
    response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json() == {"status": "unavailable"}


def test_version(client: TestClient) -> None:
    response = client.get("/version")
    assert response.status_code == 200
    assert response.json() == {"version": "1.2.3", "environment": "local", "commit": "0123abcd"}


def test_create_and_get_note(client: TestClient) -> None:
    response = client.post("/notes", json={"title": "  Courses  ", "content": "Pain"})
    assert response.status_code == 201
    note = response.json()
    assert note["title"] == "Courses"  # espaces retirés
    assert note["content"] == "Pain"
    assert uuid.UUID(note["id"])
    assert note["created_at"]
    assert response.headers["location"] == f"/notes/{note['id']}"

    fetched = client.get(f"/notes/{note['id']}")
    assert fetched.status_code == 200
    assert fetched.json() == note


def test_create_note_content_defaults_to_empty(client: TestClient) -> None:
    response = client.post("/notes", json={"title": "Sans contenu"})
    assert response.status_code == 201
    assert response.json()["content"] == ""


def test_create_note_validation(client: TestClient) -> None:
    assert client.post("/notes", json={"title": ""}).status_code == 422
    assert client.post("/notes", json={"title": "   "}).status_code == 422
    assert client.post("/notes", json={"content": "x"}).status_code == 422
    assert client.post("/notes", json={"title": "x" * 201}).status_code == 422
    assert client.post("/notes", json={"title": "ok", "extra": 1}).status_code == 422


def test_list_notes_newest_first_with_pagination(client: TestClient) -> None:
    assert client.get("/notes").json() == []
    ids = [client.post("/notes", json={"title": f"n{i}"}).json()["id"] for i in range(3)]

    listed = client.get("/notes").json()
    assert [n["id"] for n in listed] == list(reversed(ids))

    page = client.get("/notes", params={"limit": 1, "offset": 1}).json()
    assert [n["id"] for n in page] == [ids[1]]


def test_list_notes_rejects_bad_pagination(client: TestClient) -> None:
    assert client.get("/notes", params={"limit": 0}).status_code == 422
    assert client.get("/notes", params={"limit": 501}).status_code == 422
    assert client.get("/notes", params={"offset": -1}).status_code == 422


def test_get_unknown_note(client: TestClient) -> None:
    response = client.get(f"/notes/{uuid.uuid4()}")
    assert response.status_code == 404
    assert response.json() == {"detail": "note not found"}


def test_get_note_invalid_id(client: TestClient) -> None:
    assert client.get("/notes/not-a-uuid").status_code == 422


def test_delete_note(client: TestClient) -> None:
    note_id = client.post("/notes", json={"title": "à supprimer"}).json()["id"]
    response = client.delete(f"/notes/{note_id}")
    assert response.status_code == 204
    assert response.content == b""
    assert client.get(f"/notes/{note_id}").status_code == 404
    assert client.delete(f"/notes/{note_id}").status_code == 404


def test_openapi_lists_endpoints(client: TestClient) -> None:
    paths = client.get("/openapi.json").json()["paths"]
    assert {"/healthz", "/readyz", "/version", "/notes", "/notes/{note_id}"} <= set(paths)
