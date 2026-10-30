# team-iot — équipe capteurs (exercice « qualité de l'air »)

Équipe créée par l'exercice guidé [docs/09](../../docs/09-exercice-qualite-air.md) : elle ingère les CSV
des stations de mesure (`s3://datalake/air/...`), contrôle leur qualité (scikit-learn) et publie des
mesures validées. Ses tâches s'exécutent sur **son** edge worker (queue `team-iot`).

| | |
|---|---|
| Python | **3.10** (`apache/airflow:3.3.2-python3.10`) — choix pédagogique : edge3 5.0.0 abandonne 3.10 |
| Libs | `numpy`, `scikit-learn`, `boto3`, `apache-airflow-providers-amazon[s3fs]` (ObjectStoragePath, XCom/logs S3) |
| Airflow / edge3 | 3.3.2 / 4.3.2 (identiques au central, sinon HTTP 400 et le worker s'arrête) |
| Queue / hostname | `team-iot` / `team-iot-worker-1` (+ `team-iot-worker-2`, étape 13.3) |
| Bundle | `team-iot` → `/opt/airflow/dags/team-iot/..data` (ConfigMap `dags-team-iot` montée) |
| Pool | `team-iot` (8 slots) : `airflow pools set team-iot 8 "pool team-iot"` (CLI du scheduler) |

## Règle d'or (D3)

Le centre **n'a ni sklearn ni numpy** (ni s3fs) : ces imports se font **dans les fonctions de tâche**.
`tests/test_dags.py` parse chaque `dags/*.py` avec `sklearn`, `numpy`, `boto3`, `botocore`, `s3fs`
bloqués, et vérifie que **toutes** les tâches vont sur `EdgeExecutor` / `team-iot`.

## Build / test

```bash
docker build -t airflow-edge/worker-team-iot:dev teams/team-iot
docker run --rm airflow-edge/worker-team-iot:dev python -c "import sklearn, numpy, s3fs; print(sklearn.__version__)"
# Git Bash : MSYS_NO_PATHCONV=1 et $(pwd -W) pour le chemin Windows
MSYS_NO_PATHCONV=1 docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -v "$(pwd -W)/teams/team-iot:/work" -w /work \
  airflow-edge/worker-team-iot:dev python -m pytest -q -p no:cacheprovider tests
```

## Déploiement

Pré-requis au centre : bundle `team-iot` déclaré dans `platform/helm-values/airflow.yaml` (fait) et
`helm upgrade` passé.

```bash
kubectl apply -k teams/team-iot/k8s          # namespace, secret (JWT + connexion minio_s3), worker
uv run scripts/dags.py apply team-iot        # ConfigMap dags-team-iot : ns airflow-edge ET ns team-iot
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow edge list-workers
kubectl apply -f teams/team-iot/k8s/worker-2.yaml   # étape 13.3 seulement (2e worker, même queue)
```

Le pod reste `ContainerCreating` tant que la ConfigMap `team-iot/dags-team-iot` n'existe pas (volume non
optionnel). Le worker reçoit aussi `AIRFLOW_CONN_MINIO_S3` (pas de DB sur un worker), le logging distant
(`s3://datalake/logs`) et le backend XCom object storage (`s3://minio_s3@datalake/xcom`, > 64 Kio, gzip) :
section `[common.io]` → variables `AIRFLOW__COMMON_IO__*` (le point devient `_`).

## Les DAGs

| Fichier | dag_id | Rôle |
|---|---|---|
| `dags/iot_where_am_i.py` | `iot_where_am_i` | preuve d'exécution : hostname, Python 3.10, `import sklearn` dans la tâche |
| `dags/iot_slow_task.py` | `iot_slow_task` | tâche longue (`params.seconds`, défaut 90 s, `retries=1`) pour l'étape 13 |
| autres `dags/iot_*.py` | `iot_ingest_measurements`, ... | solution des étapes 3 à 11 (voir docs/09) |

Simulateur de stations : `uv run scripts/air_stations.py --stations 3 --hours 2 --negative st-01 --drift st-02`.
