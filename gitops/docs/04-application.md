# 04 — L'application `notes-api`

Sources : `repos/notes-api/` (projet GitLab `gitops/notes-api`). API REST de notes (CRUD) sur
PostgreSQL : volontairement petite, mais équipée comme un service de production.

| Brique | Choix | Pourquoi |
|---|---|---|
| Python 3.13 + **uv** | `pyproject.toml` + `uv.lock` commités, `uv sync --frozen` partout (local, CI, image) | installation reproductible et rapide ; une seule source de vérité pour les versions (Renovate met à jour le lock) |
| FastAPI + uvicorn | routes synchrones + pool psycopg 3 (`ConnectionPool`, `check_connection`) | simple, typé (pydantic), OpenAPI gratuite (`/docs`) ; une connexion morte (bascule CNPG) est remplacée au lieu d'échouer |
| Configuration | pydantic-settings, variables d'environnement (`DATABASE_URL` en `SecretStr`, `APP_ENV`, `APP_VERSION`, `LOG_LEVEL`, `LOG_FORMAT`) | 12-factor : la même image pour tous les environnements, la différence est dans les values Helm |
| Sondes | `GET /healthz` (liveness, aucune dépendance) / `GET /readyz` (readiness, `SELECT 1`, 503 sinon) | une base en panne retire le pod du Service sans le faire redémarrer en boucle |
| Schéma | `CREATE TABLE IF NOT EXISTS` au démarrage sous verrou consultatif PostgreSQL | idempotent, sûr avec plusieurs réplicas ; Alembic serait l'étape suivante |
| Version | `GET /version` → `APP_VERSION` (= tag de l'image) ; `pyproject.toml` reste à `0.0.0` | la version est décidée par semantic-release à partir des commits, jamais à la main |
| Architecture | `repository.py` : Protocol `NotesRepository`, implémentation PostgreSQL et implémentation mémoire | les tests unitaires tournent sans base ; l'injection se fait par la fabrique `create_app()` |

## Logs structurés (structlog)

L'application écrit **une ligne JSON par évènement sur stdout** et rien d'autre : pas de client
Elasticsearch, pas de fichier, pas de buffer réseau. C'est le choix le plus léger pour l'application
et le plus standard pour Kubernetes (la collecte est l'affaire du nœud, voir [07](07-logs.md)).

```json
{"@timestamp": "2026-11-07T18:31:57.508124Z", "log.level": "info", "message": "request completed",
 "logger": "notes_api.access", "request_id": "demo-req-1", "http.method": "POST", "url.path": "/notes",
 "http.status_code": 201, "duration_ms": 5.7, "client.ip": "10.42.0.12",
 "service.name": "notes-api", "service.version": "1.0.0", "service.environment": "dev"}
```

- Noms de champs proches d'**ECS** (`@timestamp`, `log.level`, `http.*`, `url.path`, `service.*`) :
  Filebeat les décode tels quels, Kibana les comprend sans mapping particulier.
- Tous les logs **stdlib** (uvicorn, psycopg) passent par la même chaîne de processeurs : zéro ligne
  texte au milieu du JSON. L'access log d'uvicorn est remplacé par un middleware ASGI qui ajoute
  `request_id` (repris de `X-Request-ID` si fourni, renvoyé dans la réponse), méthode, chemin, code,
  durée, IP cliente.
- Les sondes `/healthz` et `/readyz` sont journalisées en `DEBUG` : pas de bruit en prod (`INFO`),
  visibles en dev (`DEBUG` dans `envs/dev/values.yaml`).
- `LOG_FORMAT=console` en local pour une sortie lisible.

## Tests

| Suite | Contenu | Quand |
|---|---|---|
| `tests/unit` (40 tests) | routes avec dépôt mémoire, format des logs (JSON, champs du contrat), configuration, cycle de vie de l'application, dépôt mémoire | toujours (local, MR, develop, main) |
| `tests/integration` (7 tests, marqueur `integration`) | schéma idempotent, ping, CRUD complet, tri/pagination, mauvais identifiants → `PoolTimeout`, application complète avec vrai lifespan | si `DATABASE_URL` est définie ; en CI via un **service** `postgres:17-alpine` |

Qualité : `ruff check` (dont règles `S` = bandit), `ruff format --check`, `mypy --strict` (src + tests,
plugin pydantic), couverture ≥ 85 % (`--cov-fail-under`, mesurée à 100 % unit + intégration).
Résultats remontés dans GitLab : JUnit (onglet *Tests* de la pipeline et widget de MR), Cobertura
(couverture par ligne dans le diff de la MR), Code Quality (ruff au format GitLab).

## Image : multi-stage, distroless, Python géré par uv

```
builder  ghcr.io/astral-sh/uv:0.10.8-python3.13-trixie-slim
   uv python install (Python autonome dans /python)  ->  élagage (-55 Mo : tests, Tk, pip, headers…)
   uv sync --frozen --no-dev --no-editable  (dépendances puis projet, deux couches)
runtime  gcr.io/distroless/cc-debian12:nonroot
   COPY /python, /app/.venv, libz.so.1 ; USER 65532 ; ENTRYPOINT python -m uvicorn …
```

- **Distroless** : ni shell, ni gestionnaire de paquets, ni `curl` dans l'image finale ; surface
  d'attaque minimale, scan de vulnérabilités quasi vide.
- **Python géré par uv** (python-build-standalone) : lié uniquement à la glibc, donc exécutable dans
  distroless/cc ; le Python Debian de l'image builder ne l'est pas (libssl, libsqlite…).
- Piège rencontré : le wheel `psycopg-binary` embarque libpq et OpenSSL mais pas **zlib** → le
  builder copie `libz.so.1` dans l'image finale (commenté dans le Dockerfile).
- Compatible **kaniko** (aucune instruction BuildKit) et **`readOnlyRootFilesystem`** (bytecode
  précompilé, `PYTHONDONTWRITEBYTECODE`).
- Taille : ~200 Mo décompressés (~48 Mo compressés). Labels OCI `source`, `revision`, `version`.
- Les tags des images de base sont épinglés ; Renovate propose les montées de version et peut
  épingler les digests.

## Conventional Commits

`feat:` → version mineure, `fix:`/`perf:` → correctif, `BREAKING CHANGE` → majeure ; `chore:`,
`docs:`, `test:`, `ci:` ne publient rien. Vérifié par **commitlint** dans les pipelines de MR, exploité
par **semantic-release** sur `main` (voir [05](05-pipeline-ci.md)).
