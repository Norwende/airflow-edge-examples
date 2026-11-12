"""Logs JSON (champs du contrat) et middleware de journalisation des requêtes."""

import io
import json
import logging
from collections.abc import Iterator
from typing import Any

import pytest
import structlog
from fastapi import FastAPI
from fastapi.testclient import TestClient

from notes_api.config import Settings
from notes_api.logging_config import configure_logging
from notes_api.main import create_app
from notes_api.repository import InMemoryNotesRepository

CONTRACT_FIELDS = {"@timestamp", "log.level", "message", "logger", "service.name"}


@pytest.fixture
def log_stream(settings: Settings) -> io.StringIO:
    stream = io.StringIO()
    configure_logging(settings, stream=stream)
    return stream


def read_lines(stream: io.StringIO) -> list[dict[str, Any]]:
    """Chaque ligne doit être un objet JSON autonome."""
    return [json.loads(line) for line in stream.getvalue().splitlines()]


@pytest.fixture
def app(settings: Settings, repository: InMemoryNotesRepository) -> FastAPI:
    app = create_app(settings, repository=repository)

    @app.get("/boom")
    def boom() -> None:
        raise RuntimeError("explosion")

    return app


@pytest.fixture
def logged_client(app: FastAPI, log_stream: io.StringIO) -> Iterator[TestClient]:
    # create_app a configuré les logs vers stdout ; log_stream (évalué après) les redirige.
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client


def request_logs(stream: io.StringIO) -> list[dict[str, Any]]:
    return [line for line in read_lines(stream) if line.get("logger") == "notes_api.access"]


def test_structlog_event_is_json_with_contract_fields(log_stream: io.StringIO) -> None:
    structlog.stdlib.get_logger("notes_api.test").info("bonjour", answer=42)
    (line,) = read_lines(log_stream)
    assert set(line) >= CONTRACT_FIELDS
    assert line["message"] == "bonjour"
    assert line["log.level"] == "info"
    assert line["logger"] == "notes_api.test"
    assert line["service.name"] == "notes-api"
    assert line["service.version"] == "1.2.3"
    assert line["service.environment"] == "local"
    assert line["answer"] == 42
    assert line["@timestamp"].endswith("Z")
    assert "event" not in line
    assert "level" not in line


def test_stdlib_logs_are_routed_through_structlog(log_stream: io.StringIO) -> None:
    logging.getLogger("uvicorn.error").warning("Started server process [%d]", 7)
    (line,) = read_lines(log_stream)
    assert line["message"] == "Started server process [7]"
    assert line["logger"] == "uvicorn.error"
    assert line["log.level"] == "warning"
    assert line["service.name"] == "notes-api"


def test_uvicorn_access_log_disabled(log_stream: io.StringIO) -> None:
    logging.getLogger("uvicorn.access").info("GET / 200")
    assert log_stream.getvalue() == ""


def test_level_filtering(settings: Settings) -> None:
    stream = io.StringIO()
    configure_logging(settings.model_copy(update={"log_level": "WARNING"}), stream=stream)
    logger = structlog.stdlib.get_logger("notes_api.test")
    logger.info("ignoré")
    logger.warning("gardé")
    assert [line["message"] for line in read_lines(stream)] == ["gardé"]


def test_exception_is_rendered_as_stack_trace(log_stream: io.StringIO) -> None:
    try:
        raise ValueError("mauvaise valeur")
    except ValueError:
        structlog.stdlib.get_logger("notes_api.test").exception("échec")
    (line,) = read_lines(log_stream)
    assert line["log.level"] == "error"
    assert "ValueError: mauvaise valeur" in line["error.stack_trace"]


def test_console_format(settings: Settings) -> None:
    stream = io.StringIO()
    configure_logging(settings.model_copy(update={"log_format": "console"}), stream=stream)
    structlog.stdlib.get_logger("notes_api.test").info("lisible", answer=42)
    output = stream.getvalue()
    assert "lisible" in output
    assert "answer" in output
    with pytest.raises(json.JSONDecodeError):
        json.loads(output)


def test_request_is_logged_with_http_fields(
    logged_client: TestClient, log_stream: io.StringIO
) -> None:
    response = logged_client.post("/notes", json={"title": "t"})
    assert response.status_code == 201
    request_id = response.headers["x-request-id"]

    (access,) = request_logs(log_stream)
    assert set(access) >= CONTRACT_FIELDS
    assert access["message"] == "request completed"
    assert access["log.level"] == "info"
    assert access["http.method"] == "POST"
    assert access["url.path"] == "/notes"
    assert access["http.status_code"] == 201
    assert access["duration_ms"] >= 0
    assert access["client.ip"] == "testclient"
    assert access["request_id"] == request_id

    # Les logs émis pendant la requête portent le même request_id.
    (created,) = [line for line in read_lines(log_stream) if line["message"] == "note created"]
    assert created["request_id"] == request_id


def test_incoming_request_id_is_reused(logged_client: TestClient, log_stream: io.StringIO) -> None:
    response = logged_client.get("/version", headers={"X-Request-ID": "abc-123"})
    assert response.headers["x-request-id"] == "abc-123"
    assert request_logs(log_stream)[0]["request_id"] == "abc-123"


def test_unreasonable_request_id_is_replaced(logged_client: TestClient) -> None:
    response = logged_client.get("/version", headers={"X-Request-ID": "x" * 500})
    assert response.headers["x-request-id"] != "x" * 500
    assert len(response.headers["x-request-id"]) == 32


def test_probes_are_logged_at_debug(logged_client: TestClient, log_stream: io.StringIO) -> None:
    logged_client.get("/healthz")
    (access,) = request_logs(log_stream)
    assert access["log.level"] == "debug"
    assert access["url.path"] == "/healthz"


def test_client_error_is_logged_at_info(logged_client: TestClient, log_stream: io.StringIO) -> None:
    logged_client.get("/notes/not-a-uuid")
    (access,) = request_logs(log_stream)
    assert access["http.status_code"] == 422
    assert access["log.level"] == "info"


def test_unhandled_exception_is_logged_as_500(
    logged_client: TestClient, log_stream: io.StringIO
) -> None:
    response = logged_client.get("/boom")
    assert response.status_code == 500
    (access,) = request_logs(log_stream)
    assert access["message"] == "request failed"
    assert access["log.level"] == "error"
    assert access["http.status_code"] == 500
    assert "RuntimeError: explosion" in access["error.stack_trace"]


def test_server_error_response_is_logged_as_error(
    logged_client: TestClient, log_stream: io.StringIO, repository: InMemoryNotesRepository
) -> None:
    repository.healthy = False
    assert logged_client.get("/readyz").status_code == 503
    (access,) = request_logs(log_stream)
    assert access["log.level"] == "error"
    assert "error.stack_trace" not in access
