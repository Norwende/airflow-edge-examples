"""Modèles pydantic exposés par l'API (entrées, sorties)."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class NoteCreate(BaseModel):
    """Corps de ``POST /notes``."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    title: str = Field(min_length=1, max_length=200, examples=["Courses"])
    content: str = Field(default="", max_length=10_000, examples=["Pain, lait, café"])


class Note(BaseModel):
    """Une note telle que stockée et renvoyée par l'API."""

    model_config = ConfigDict(frozen=True)

    id: UUID
    title: str
    content: str
    created_at: datetime


class VersionInfo(BaseModel):
    """Réponse de ``GET /version``."""

    version: str
    environment: str
    commit: str


class HealthStatus(BaseModel):
    """Réponse des sondes ``/healthz`` et ``/readyz``."""

    status: Literal["ok", "unavailable"]
