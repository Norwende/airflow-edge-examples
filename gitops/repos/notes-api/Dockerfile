# Image de notes-api : build multi-stage, image finale distroless (ni shell ni gestionnaire de
# paquets, uid 65532). Compatible kaniko : aucune fonctionnalité BuildKit (pas de RUN --mount).

# --------------------------------------------------------------------------- builder
FROM ghcr.io/astral-sh/uv:0.10.8-python3.13-trixie-slim AS builder

# Le Python de cette image (/usr/local, paquet Debian officiel) dépend de bibliothèques système
# (libssl, libsqlite, libexpat…) absentes de distroless. On utilise donc un Python géré par uv
# (python-build-standalone : autonome, lié seulement à la glibc, compatible debian12), installé
# dans /python, chemin identique dans l'image finale (le lien python du venv pointe dessus).
ENV UV_PYTHON_INSTALL_DIR=/python \
    UV_PYTHON_PREFERENCE=only-managed \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1

WORKDIR /app

# 1) Python + dépendances seules : couches en cache tant que uv.lock ne change pas.
#    Élagage du Python installé (~-55 Mo), rien de ce qui est retiré n'est utilisé à l'exécution :
#    - libpython3.13.so : l'exécutable python embarque l'interpréteur en statique et aucune
#      extension (stdlib ou wheels) n'est liée à cette bibliothèque (vérifié avec ldd) ;
#    - pip/ensurepip (uv gère les paquets), en-têtes C, terminfo/man, Tcl/Tk, IDLE, tests.
COPY .python-version pyproject.toml uv.lock ./
RUN uv python install \
 && cd /python/cpython-3.*.*-linux-*-gnu \
 && rm -rf include share lib/pkgconfig lib/libpython3*.so* lib/itcl* lib/thread* \
           lib/tcl* lib/tk* lib/libtcl* lib/libtk* \
           bin/idle3* bin/pip* bin/pydoc3* bin/python3*-config \
           lib/python3.*/site-packages/pip* \
           lib/python3.*/test lib/python3.*/idlelib lib/python3.*/tkinter \
           lib/python3.*/turtledemo lib/python3.*/ensurepip lib/python3.*/lib-dynload/_tkinter* \
 && cd /app \
 && uv sync --frozen --no-dev --no-editable --no-install-project

# 2) Le projet lui-même, installé comme paquet (non éditable) dans le venv.
COPY README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

# 3) Bibliothèque système manquante dans distroless/cc : le wheel psycopg-binary embarque libpq,
#    OpenSSL, Kerberos… mais pas zlib (« libz.so.1: cannot open shared object file » au démarrage).
#    On la reprend du builder : la libz de trixie n'exige que GLIBC <= 2.14, donc compatible avec
#    la glibc 2.36 de debian12. /usr/lib fait partie des répertoires par défaut du chargeur.
RUN mkdir -p /runtime-libs \
 && cp -L /usr/lib/*-linux-gnu/libz.so.1 /runtime-libs/

# --------------------------------------------------------------------------- runtime
FROM gcr.io/distroless/cc-debian12:nonroot

# Python géré par uv + venv (bytecode déjà compilé : compatible readOnlyRootFilesystem).
# Propriété root, lecture seule pour l'utilisateur 65532 : l'application ne peut pas se modifier.
COPY --from=builder /runtime-libs/ /usr/lib/
COPY --from=builder /python /python
COPY --from=builder /app/.venv /app/.venv

ARG APP_VERSION=0.0.0
ARG APP_COMMIT=unknown
ARG APP_SOURCE=http://gitlab.127-0-0-1.sslip.io/gitops/notes-api

LABEL org.opencontainers.image.title="notes-api" \
      org.opencontainers.image.description="API REST de notes (CRUD) sur PostgreSQL" \
      org.opencontainers.image.source="${APP_SOURCE}" \
      org.opencontainers.image.revision="${APP_COMMIT}" \
      org.opencontainers.image.version="${APP_VERSION}"

ENV APP_VERSION=${APP_VERSION} \
    APP_COMMIT=${APP_COMMIT} \
    PATH=/app/.venv/bin:/usr/local/bin:/usr/bin:/bin \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
# uid/gid numériques : permettent à Kubernetes de vérifier runAsNonRoot.
USER 65532:65532
EXPOSE 8000

ENTRYPOINT ["/app/.venv/bin/python","-m","uvicorn","notes_api.main:app","--host","0.0.0.0","--port","8000"]
