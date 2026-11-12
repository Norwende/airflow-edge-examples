"""Dépôt de notes : interface (Protocol) et implémentations PostgreSQL / mémoire."""

import threading
import uuid
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID

from psycopg_pool import ConnectionPool

from notes_api.models import Note, NoteCreate


class NotesRepository(Protocol):
    """Ce dont les routes ont besoin pour stocker des notes (injecté au démarrage)."""

    def ping(self) -> None:
        """Vérifie que le stockage répond ; lève une exception sinon."""
        ...

    def list_notes(self, *, limit: int, offset: int) -> list[Note]:
        """Notes de la plus récente à la plus ancienne."""
        ...

    def create_note(self, data: NoteCreate) -> Note:
        """Crée une note et la renvoie (identifiant et date générés)."""
        ...

    def get_note(self, note_id: UUID) -> Note | None:
        """Renvoie la note, ou ``None`` si elle n'existe pas."""
        ...

    def delete_note(self, note_id: UUID) -> bool:
        """Supprime la note ; ``False`` si elle n'existait pas."""
        ...


def _row_to_note(row: tuple[Any, ...]) -> Note:
    note_id, title, content, created_at = row
    return Note(id=note_id, title=title, content=content, created_at=created_at)


# Requêtes SQL (littéraux constants, valeurs toujours passées en paramètres : pas d'injection SQL).
_SQL_LIST = (
    "SELECT id, title, content, created_at FROM notes"
    " ORDER BY created_at DESC, id LIMIT %s OFFSET %s"
)
_SQL_INSERT = (
    "INSERT INTO notes (id, title, content) VALUES (%s, %s, %s)"
    " RETURNING id, title, content, created_at"
)
_SQL_GET = "SELECT id, title, content, created_at FROM notes WHERE id = %s"
_SQL_DELETE = "DELETE FROM notes WHERE id = %s"


class PostgresNotesRepository:
    """Implémentation PostgreSQL au-dessus d'un pool psycopg 3."""

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def ping(self) -> None:
        with self._pool.connection() as conn:
            conn.execute("SELECT 1")

    def list_notes(self, *, limit: int, offset: int) -> list[Note]:
        with self._pool.connection() as conn:
            rows = conn.execute(_SQL_LIST, (limit, offset)).fetchall()
        return [_row_to_note(row) for row in rows]

    def create_note(self, data: NoteCreate) -> Note:
        with self._pool.connection() as conn:
            row = conn.execute(_SQL_INSERT, (uuid.uuid4(), data.title, data.content)).fetchone()
        if row is None:  # pragma: no cover - INSERT ... RETURNING renvoie toujours une ligne
            raise RuntimeError("INSERT ... RETURNING n'a renvoyé aucune ligne")
        return _row_to_note(row)

    def get_note(self, note_id: UUID) -> Note | None:
        with self._pool.connection() as conn:
            row = conn.execute(_SQL_GET, (note_id,)).fetchone()
        return _row_to_note(row) if row is not None else None

    def delete_note(self, note_id: UUID) -> bool:
        with self._pool.connection() as conn:
            return conn.execute(_SQL_DELETE, (note_id,)).rowcount > 0


class InMemoryNotesRepository:
    """Implémentation en mémoire, pour les tests unitaires (thread-safe : FastAPI exécute les
    routes synchrones dans un pool de threads)."""

    def __init__(self) -> None:
        self._notes: dict[UUID, Note] = {}
        self._lock = threading.Lock()
        self.healthy = True  # passer à False simule une base indisponible

    def ping(self) -> None:
        if not self.healthy:
            raise ConnectionError("stockage en mémoire marqué indisponible")

    def list_notes(self, *, limit: int, offset: int) -> list[Note]:
        with self._lock:
            notes = sorted(self._notes.values(), key=lambda n: n.created_at, reverse=True)
        return notes[offset : offset + limit]

    def create_note(self, data: NoteCreate) -> Note:
        note = Note(
            id=uuid.uuid4(),
            title=data.title,
            content=data.content,
            created_at=datetime.now(UTC),
        )
        with self._lock:
            self._notes[note.id] = note
        return note

    def get_note(self, note_id: UUID) -> Note | None:
        with self._lock:
            return self._notes.get(note_id)

    def delete_note(self, note_id: UUID) -> bool:
        with self._lock:
            return self._notes.pop(note_id, None) is not None
