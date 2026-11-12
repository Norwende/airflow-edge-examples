"""Dépôt en mémoire (référence de comportement pour l'implémentation PostgreSQL)."""

import uuid

import pytest

from notes_api.models import NoteCreate
from notes_api.repository import InMemoryNotesRepository


def test_crud_cycle(repository: InMemoryNotesRepository) -> None:
    note = repository.create_note(NoteCreate(title="t", content="c"))
    assert repository.get_note(note.id) == note
    assert repository.list_notes(limit=10, offset=0) == [note]
    assert repository.delete_note(note.id) is True
    assert repository.get_note(note.id) is None
    assert repository.delete_note(note.id) is False


def test_get_unknown(repository: InMemoryNotesRepository) -> None:
    assert repository.get_note(uuid.uuid4()) is None


def test_ping(repository: InMemoryNotesRepository) -> None:
    repository.ping()
    repository.healthy = False
    with pytest.raises(ConnectionError):
        repository.ping()
