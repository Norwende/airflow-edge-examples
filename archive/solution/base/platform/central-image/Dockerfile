# Image CENTRALE (api-server, scheduler, dag-processor, triggerer, jobs du chart).
#   docker build -t airflow-edge/airflow-central:3.3.2 platform/central-image
#
# L'image officielle n'embarque pas le provider edge3 : or l'EdgeExecutor tourne dans
# le scheduler et le plugin d'API Edge (/edge_worker/v1/rpcapi) dans l'api-server.
# Le provider Amazon (S3Hook) et pandas, utilisés par les DAGs "core", sont déjà dans l'image officielle.
FROM apache/airflow:3.3.2

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.13
ARG EDGE3_VERSION=4.3.2

# En utilisateur airflow (jamais root pour pip). On ré-épingle apache-airflow pour
# interdire toute montée/descente de version, et on applique les contraintes officielles.
USER airflow
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-edge3==${EDGE3_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# DAGs "ancienne idéologie" : cuits dans l'image (bundle "core").
COPY --chown=airflow:root dags/core/ /opt/airflow/dags/core/
