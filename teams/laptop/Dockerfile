# Worker edge "laptop" : environnement Python minimal, SANS l'image officielle Airflow.
FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV AIRFLOW_HOME=/opt/airflow \
    PYTHONUNBUFFERED=1 \
    UV_NO_CACHE=1 \
    UV_LINK_MODE=copy

ARG AIRFLOW_CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt

COPY requirements.txt /tmp/requirements.txt
RUN uv pip install --system -r /tmp/requirements.txt --constraint "${AIRFLOW_CONSTRAINTS}" \
 && rm /tmp/requirements.txt

# Utilisateur non-root, même uid que l'image officielle.
RUN useradd --uid 50000 --gid 0 --home-dir /opt/airflow --create-home --shell /bin/bash airflow \
 && mkdir -p /opt/airflow/dags/laptop /opt/airflow/logs /opt/airflow/.duckdb \
 && chown -R 50000:0 /opt/airflow && chmod -R g+rwX /opt/airflow
USER 50000
WORKDIR /opt/airflow

# Pré-installe l'extension duckdb httpfs (sinon téléchargée au 1er usage => internet requis au runtime).
# Non bloquant : sans réseau au build, le DAG retombe sur boto3 (chemin par défaut).
RUN python -c "import duckdb; duckdb.connect().sql('INSTALL httpfs')" \
 || echo "WARN: extension httpfs non pre-installee"

# DAGs en dernier : une modif de DAG ne réinvalide pas les couches lourdes.
COPY --chown=50000:0 dags/ /opt/airflow/dags/laptop/
COPY --chmod=755 entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
