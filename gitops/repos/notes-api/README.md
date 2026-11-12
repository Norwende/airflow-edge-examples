# notes-api

API REST de notes (CRUD) sur PostgreSQL : l'application du scénario GitOps.
Python 3.13, FastAPI, psycopg 3, structlog, géré avec [uv](https://docs.astral.sh/uv/).

| Endpoint | Rôle |
|---|---|
| `GET /healthz` | liveness (aucune dépendance) |
| `GET /readyz` | readiness (`SELECT 1` sur la base, 503 sinon) |
| `GET /version` | `{"version", "environment", "commit"}` |
| `GET /notes?limit=&offset=` | liste (plus récentes d'abord) |
| `POST /notes` | création (`{"title", "content"}`) → 201 |
| `GET /notes/{id}` / `DELETE /notes/{id}` | lecture / suppression (404 si absente) |

Documentation OpenAPI : `/docs`.

## Lancer en local

```bash
uv sync                                    # crée .venv avec les dépendances de dev
docker run -d --name notes-pg -e POSTGRES_USER=notes -e POSTGRES_PASSWORD=notes \
  -e POSTGRES_DB=notes -p 5432:5432 postgres:17-alpine

export DATABASE_URL=postgresql://notes:notes@127.0.0.1:5432/notes
export LOG_FORMAT=console                  # logs lisibles (JSON par défaut)
uv run uvicorn notes_api.main:app --reload
```

La table `notes` est créée au démarrage si elle n'existe pas.

## Tests et qualité

```bash
uv run ruff check .                        # lint (dont règles de sécurité S)
uv run ruff format --check .               # formatage
uv run mypy                                # typage strict (src + tests)
uv run pytest                              # tests unitaires (intégration ignorée)

# avec les tests d'intégration (PostgreSQL réel) et la couverture, comme en CI :
DATABASE_URL=postgresql://notes:notes@127.0.0.1:5432/notes \
  uv run pytest --cov=notes_api --cov-report=term --cov-fail-under=85
```

`tests/unit` utilise un dépôt de notes en mémoire ; `tests/integration` (marqueur
`integration`) n'est exécuté que si `DATABASE_URL` est définie.

## Image

```bash
docker build -t notes-api:dev --build-arg APP_VERSION=0.0.0-dev --build-arg APP_COMMIT=$(git rev-parse HEAD) .
docker run --rm -p 8000:8000 -e DATABASE_URL=postgresql://notes:notes@host.docker.internal:5432/notes notes-api:dev
```

Multi-stage : Python géré par uv + venv copiés dans `gcr.io/distroless/cc-debian12:nonroot`
(sans shell, uid 65532, compatible `readOnlyRootFilesystem`). Pas de BuildKit requis (kaniko).

## Variables d'environnement

| Variable | Défaut | Rôle |
|---|---|---|
| `DATABASE_URL` | — (obligatoire) | URI PostgreSQL (Secret CNPG `notes-pg-app`, clé `uri`) |
| `APP_ENV` | `local` | `dev`, `prod` ou `local` |
| `APP_VERSION` | `0.0.0` | version exposée par `/version` (= tag de l'image) |
| `APP_COMMIT` | `unknown` | sha du commit |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `LOG_FORMAT` | `json` | `json` (une ligne JSON par log, stdout) ou `console` |

Les sondes `/healthz` et `/readyz` ne sont journalisées qu'en `DEBUG`.

## Versions

La version publiée vient des tags git `vX.Y.Z` posés par semantic-release ; `version` dans
`pyproject.toml` reste à `0.0.0`. Les commits suivent Conventional Commits (`feat:`, `fix:`…).
