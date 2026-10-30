# team-data — équipe data engineering

Équipe qui ingère et agrège des commandes dans le datalake MinIO (`s3://datalake/orders/...`).
Ses DAGs illustrent la **nouvelle idéologie Airflow 3** (Task SDK `airflow.sdk`, Assets, dynamic
task mapping, versioning) et s'exécutent sur **son propre edge worker** (queue `team-data`).

| | |
|---|---|
| Python | 3.12 (`apache/airflow:3.3.2-python3.12`) |
| Libs | `polars` (dataframes + parquet), `boto3` (S3/MinIO) |
| Airflow / edge3 | 3.3.2 / 4.3.2 (identiques au central, sinon le worker s'arrête) |
| Queue / hostname | `team-data` / `team-data-worker-1` |
| Bundle | `team-data` → `/opt/airflow/dags/team-data/..data` (ConfigMap `dags-team-data` montée) |

## Règle d'or (D3)

Le dag-processor central **n'a pas polars** : `import polars` / `import boto3` se font
**dans les fonctions de tâche**, jamais en tête de fichier. Les tests le vérifient.

## Build / test / exécution

```bash
# depuis la racine du repo
docker build -t airflow-edge/worker-team-data:dev teams/team-data
docker run --rm airflow-edge/worker-team-data:dev python -c "import airflow, polars, boto3; from airflow.providers.edge3 import __version__ as v; print(airflow.__version__, v, polars.__version__)"

# tests (Git Bash : MSYS_NO_PATHCONV=1 et $(pwd -W) pour le chemin Windows)
docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -v "$PWD/teams/team-data:/work" -w /work airflow-edge/worker-team-data:dev python -m pytest -q -p no:cacheprovider tests
```

## Déploiement

```bash
kubectl apply -k teams/team-data/k8s          # namespace, secret JWT, Deployment du worker
uv run scripts/dags.py apply team-data        # ConfigMap dags-team-data : ns airflow-edge ET ns team-data
```

L'ordre compte : le namespace doit exister avant `dags.py apply` (sinon seule la ConfigMap centrale est
écrite) ; tant que la ConfigMap `team-data/dags-team-data` n'existe pas, le pod reste en
`ContainerCreating` (volume non optionnel), puis démarre tout seul.

**Source unique** : `dags.py apply` publie la même ConfigMap `dags-team-data` au centre (parsing,
planification) et dans le namespace `team-data`, où elle est montée dans le worker sur
`/opt/airflow/dags/team-data` (bundle = `.../..data`, comme au centre). Kubelet propage une mise à jour
en ~1 min : **pas de rebuild ni de redémarrage du worker** pour changer de version de DAG
(`--version v2` superpose `versions/v2/*.py` à `dags/`). La copie `COPY dags/` de l'image ne sert plus
qu'aux tests locaux (`docker run ... pytest`). En production : `GitDagBundle` / git-sync.
Le pod a `hostname: team-data-worker-1` : c'est ce nom qui apparaît dans le champ `hostname` des tâches.

## Les DAGs

| Fichier | dag_id | Ce qu'il montre |
|---|---|---|
| `dags/data_ingest_orders.py` | `data_ingest_orders` | `@dag/@task`, `expand()` sur 3 régions, parquet polars → MinIO via `Connection.get("minio_s3")`, `yield Metadata(...)` sur l'asset `orders_raw` ; `logical_date` = `None` en run manuel |
| `dags/data_orders_daily.py` | `data_orders_daily` | `schedule=[orders_raw]` (déclenché par la donnée), lecture de `triggering_asset_events` / `inlet_events`, agrégat du CA, asset `orders_daily` via `outlet_events`, XCom implicite TaskFlow |
| `dags/asset_shorthand_demo.py` | `demo_shorthand` | raccourci `@asset` ; **limite** : pas d'`executor`/`queue` → tourne au **centre** (LocalExecutor), il affiche son hostname pour le prouver |
| `versions/v1/`, `versions/v2/` | `data_ingest_orders` | v2 ajoute `validate` (lignes > 0, schéma parquet) : `uv run scripts/dags.py apply team-data --version v2` (v2 superposée à `dags/`, retour : sans `--version`) → nouvelle **version de DAG** dans l'UI, worker à jour en ~1 min sans rebuild (`scenario.py versioning`). Bundle local = pas d'épinglage du code par run (c'est `GitDagBundle`) |
