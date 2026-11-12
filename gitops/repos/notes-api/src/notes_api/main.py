"""Point d'entrée ASGI : ``uvicorn notes_api.main:app``.

``create_app`` est une fabrique : les tests l'appellent avec leurs propres ``Settings`` et un
dépôt de notes en mémoire ; en production, le lifespan ouvre le pool PostgreSQL et crée le schéma.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from notes_api import db
from notes_api.config import SERVICE_NAME, Settings
from notes_api.logging_config import RequestLoggingMiddleware, configure_logging
from notes_api.repository import NotesRepository, PostgresNotesRepository
from notes_api.routes import router

logger = structlog.stdlib.get_logger(__name__)


def create_app(
    settings: Settings | None = None,
    repository: NotesRepository | None = None,
) -> FastAPI:
    """Construit l'application.

    :param settings: configuration (par défaut lue depuis l'environnement).
    :param repository: dépôt de notes à utiliser ; si absent, un dépôt PostgreSQL est créé au
        démarrage à partir de ``DATABASE_URL``.
    """
    settings = settings if settings is not None else Settings()
    configure_logging(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logger.info(
            "application starting",
            version=settings.app_version,
            commit=settings.app_commit,
            environment=settings.app_env,
        )
        if repository is not None:
            app.state.repository = repository
            yield
        else:
            if settings.database_url is None:
                raise RuntimeError("DATABASE_URL n'est pas définie")
            pool = db.open_pool(settings.database_url.get_secret_value())
            try:
                db.init_schema(pool)
                logger.info("database ready", pool_max_size=pool.max_size)
                app.state.repository = PostgresNotesRepository(pool)
                yield
            finally:
                pool.close()
        logger.info("application stopped")

    app = FastAPI(
        title=SERVICE_NAME,
        version=settings.app_version,
        summary="API REST de notes (CRUD) sur PostgreSQL",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.include_router(router)
    app.add_middleware(RequestLoggingMiddleware)
    return app


app = create_app()
