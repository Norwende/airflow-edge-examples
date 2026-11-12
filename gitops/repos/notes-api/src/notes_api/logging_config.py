"""Logs structurés : structlog en JSON sur stdout (une ligne = un objet JSON).

Tout passe par le module ``logging`` de la stdlib, structlog servant de formateur :

* les logs émis via ``structlog.get_logger()`` (code de l'application) ;
* les logs stdlib des bibliothèques (uvicorn, psycopg…), dits « foreign », enrichis par la même
  chaîne de processeurs ``_SHARED_PROCESSORS``.

Le format JSON suit les champs du contrat (proches d'ECS, décodés tels quels par Filebeat) :
``@timestamp``, ``log.level``, ``message``, ``logger``, ``service.*``, ``request_id``, et pour
les requêtes HTTP ``http.method``, ``url.path``, ``http.status_code``, ``duration_ms``,
``client.ip``. L'access log d'uvicorn est désactivé : c'est ``RequestLoggingMiddleware`` qui
journalise les requêtes (avec l'identifiant de requête et la durée).
"""

import logging
import sys
import time
import uuid
from typing import Any, TextIO

import structlog
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from structlog.typing import EventDict, Processor, WrappedLogger

from notes_api.config import SERVICE_NAME, Settings

REQUEST_ID_HEADER = "x-request-id"
# Sondes Kubernetes : appelées toutes les quelques secondes, journalisées en DEBUG seulement.
QUIET_PATHS = frozenset({"/healthz", "/readyz"})
_LEADING_KEYS = ("@timestamp", "log.level", "message", "logger", "request_id")

# Processeurs communs aux logs structlog et aux logs stdlib.
_SHARED_PROCESSORS: list[Processor] = [
    structlog.contextvars.merge_contextvars,  # request_id lié par le middleware
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.TimeStamper(fmt="iso", utc=True, key="@timestamp"),
    structlog.processors.StackInfoRenderer(),
]


class _ServiceFields:
    """Ajoute ``service.name``, ``service.version`` et ``service.environment`` à chaque ligne."""

    def __init__(self, settings: Settings) -> None:
        self._fields = {
            "service.name": SERVICE_NAME,
            "service.version": settings.app_version,
            "service.environment": settings.app_env,
        }

    def __call__(self, _logger: WrappedLogger, _method: str, event_dict: EventDict) -> EventDict:
        for key, value in self._fields.items():
            event_dict.setdefault(key, value)
        return event_dict


def _to_contract_fields(_logger: WrappedLogger, _method: str, event_dict: EventDict) -> EventDict:
    """Renomme les clés structlog vers les noms du contrat (``event`` -> ``message``…)."""
    event_dict["message"] = event_dict.pop("event", "")
    event_dict["log.level"] = event_dict.pop("level", "info")
    if "exception" in event_dict:
        event_dict["error.stack_trace"] = event_dict.pop("exception")
    # Champs principaux en tête de ligne : plus lisible avec `kubectl logs`.
    ordered: EventDict = {key: event_dict.pop(key) for key in _LEADING_KEYS if key in event_dict}
    ordered.update(event_dict)
    return ordered


def configure_logging(settings: Settings, stream: TextIO | None = None) -> None:
    """Configure structlog et la stdlib (idempotent : peut être rappelée, ex. dans les tests).

    ``stream`` permet aux tests de capturer la sortie ; par défaut ``sys.stdout``.
    """
    renderer: list[Processor]
    if settings.log_format == "json":
        renderer = [
            _ServiceFields(settings),
            structlog.processors.format_exc_info,
            _to_contract_fields,
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ]
    else:
        renderer = [structlog.dev.ConsoleRenderer()]

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=_SHARED_PROCESSORS,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, *renderer],
    )
    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(settings.log_level)

    # uvicorn installe ses propres handlers (texte, stderr) avant d'importer l'application :
    # on les retire pour que ses logs remontent au handler JSON de la racine.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_logger = logging.getLogger(name)
        uv_logger.handlers.clear()
        uv_logger.propagate = True
        uv_logger.setLevel(settings.log_level)
    # Remplacé par RequestLoggingMiddleware (même si uvicorn est lancé sans --no-access-log).
    logging.getLogger("uvicorn.access").disabled = True

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *_SHARED_PROCESSORS,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        # Pas de cache : une reconfiguration (tests) s'applique aussi aux loggers déjà créés.
        cache_logger_on_first_use=False,
    )


class RequestLoggingMiddleware:
    """Middleware ASGI : identifiant de requête + une ligne de log par requête HTTP.

    * ``request_id`` : repris de l'en-tête ``X-Request-ID`` s'il est fourni (corrélation avec
      un proxy), sinon généré ; lié au contexte structlog (présent sur tous les logs émis pendant
      la requête) et renvoyé dans la réponse.
    * À la fin de la requête : ``http.method``, ``url.path``, ``http.status_code``,
      ``duration_ms``, ``client.ip``. Une exception non gérée est journalisée en 500.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app
        self.logger = structlog.stdlib.get_logger("notes_api.access")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = _incoming_request_id(scope) or uuid.uuid4().hex
        status_code = 500  # valeur retenue si l'application lève avant d'envoyer une réponse
        start = time.perf_counter()

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                MutableHeaders(scope=message).append(REQUEST_ID_HEADER, request_id)
            await send(message)

        with structlog.contextvars.bound_contextvars(request_id=request_id):
            try:
                await self.app(scope, receive, send_wrapper)
            except Exception:
                self._log(scope, status_code, start, exc_info=True)
                raise
            self._log(scope, status_code, start)

    def _log(self, scope: Scope, status_code: int, start: float, *, exc_info: bool = False) -> None:
        path: str = scope.get("path", "")
        client = scope.get("client")
        fields: dict[str, Any] = {
            "http.method": scope.get("method", ""),
            "url.path": path,
            "http.status_code": status_code,
            "duration_ms": round((time.perf_counter() - start) * 1000, 2),
            "client.ip": client[0] if client else None,
        }
        if exc_info or status_code >= 500:
            self.logger.error("request failed", exc_info=exc_info, **fields)
        elif path in QUIET_PATHS:
            self.logger.debug("request completed", **fields)
        else:
            self.logger.info("request completed", **fields)


def _incoming_request_id(scope: Scope) -> str | None:
    """Identifiant fourni par le client (``X-Request-ID``), s'il est raisonnable."""
    for name, value in scope.get("headers", []):
        if name == REQUEST_ID_HEADER.encode():
            candidate: str = value.decode("latin-1").strip()
            # Borne la taille : l'en-tête vient du client et finit dans les logs.
            if 0 < len(candidate) <= 128 and candidate.isprintable():
                return candidate
    return None
