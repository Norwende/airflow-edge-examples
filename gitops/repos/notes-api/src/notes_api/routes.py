"""Endpoints HTTP : sondes, version et CRUD des notes.

Les routes sont synchrones (``def``) : FastAPI les exécute dans son pool de threads, ce qui
convient au pool psycopg synchrone sans bloquer la boucle d'événements.
"""

from typing import Annotated
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status

from notes_api.config import Settings
from notes_api.models import HealthStatus, Note, NoteCreate, VersionInfo
from notes_api.repository import NotesRepository

logger = structlog.stdlib.get_logger(__name__)

router = APIRouter()


def get_repository(request: Request) -> NotesRepository:
    """Dépôt de notes installé par le lifespan (PostgreSQL) ou injecté par les tests."""
    repository: NotesRepository = request.app.state.repository
    return repository


def get_app_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


RepositoryDep = Annotated[NotesRepository, Depends(get_repository)]
SettingsDep = Annotated[Settings, Depends(get_app_settings)]


# --------------------------------------------------------------------------- sondes / version
@router.get("/healthz", tags=["ops"])
def healthz() -> HealthStatus:
    """Liveness : le processus répond. Aucune dépendance (une base en panne ne doit pas faire
    redémarrer les pods)."""
    return HealthStatus(status="ok")


@router.get(
    "/readyz",
    tags=["ops"],
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": HealthStatus}},
)
def readyz(repository: RepositoryDep, response: Response) -> HealthStatus:
    """Readiness : la base répond à ``SELECT 1`` ; sinon 503 (le pod sort du Service)."""
    try:
        repository.ping()
    except Exception as exc:
        logger.warning("readiness check failed", error=repr(exc))
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return HealthStatus(status="unavailable")
    return HealthStatus(status="ok")


@router.get("/version", tags=["ops"])
def version(settings: SettingsDep) -> VersionInfo:
    """Version déployée (tag d'image), environnement et commit."""
    return VersionInfo(
        version=settings.app_version,
        environment=settings.app_env,
        commit=settings.app_commit,
    )


# --------------------------------------------------------------------------- notes
@router.get("/notes", tags=["notes"])
def list_notes(
    repository: RepositoryDep,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[Note]:
    """Notes de la plus récente à la plus ancienne (pagination ``limit``/``offset``)."""
    return repository.list_notes(limit=limit, offset=offset)


@router.post("/notes", status_code=status.HTTP_201_CREATED, tags=["notes"])
def create_note(data: NoteCreate, repository: RepositoryDep, response: Response) -> Note:
    """Crée une note ; l'en-tête ``Location`` pointe vers la nouvelle ressource."""
    note = repository.create_note(data)
    response.headers["Location"] = f"/notes/{note.id}"
    logger.info("note created", note_id=str(note.id))
    return note


@router.get("/notes/{note_id}", tags=["notes"], responses={404: {"description": "Note absente"}})
def get_note(note_id: UUID, repository: RepositoryDep) -> Note:
    note = repository.get_note(note_id)
    if note is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="note not found")
    return note


@router.delete(
    "/notes/{note_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["notes"],
    responses={404: {"description": "Note absente"}},
)
def delete_note(note_id: UUID, repository: RepositoryDep) -> None:
    if not repository.delete_note(note_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="note not found")
    logger.info("note deleted", note_id=str(note_id))
