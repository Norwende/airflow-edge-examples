# 04 — Ancienne et nouvelle idéologie, côte à côte

| | Ancienne (style Airflow 2) | Nouvelle (Airflow 3, Task SDK) |
|---|---|---|
| Fichiers | [`platform/central-image/dags/core/`](../platform/central-image/dags/core/) | [`teams/*/dags/`](../teams/) |
| DAGs | `classic_etl_pipeline` → `classic_report` | `data_ingest_orders` → `data_orders_daily` → `search_index_orders` + `laptop_duckdb_analytics` |
| Exécution | LocalExecutor, sous-process du pod scheduler | EdgeExecutor, worker de l'équipe |

Scénarios : `uv run scripts/scenario.py classic` et `uv run scripts/scenario.py assets`.
Les extraits ci-dessous sont copiés des fichiers du repo (raccourcis par `...`).

## 1. Définition du DAG

**Ancienne** — `with DAG(...)`, opérateurs **classes**, câblage impératif :

```python
# platform/central-image/dags/core/classic_etl_pipeline.py
with DAG(
    dag_id="classic_etl_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["core", "classic"],
    doc_md=__doc__,
) as dag:
    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    ...
    t_extract >> t_transform >> t_check_env >> t_load >> t_trigger
```

**Nouvelle** — `@dag` / `@task`, le graphe découle des appels de fonctions :

```python
# teams/team-data/dags/data_ingest_orders.py
@dag(
    dag_id="data_ingest_orders",
    schedule=None,  # déclenchement manuel pour la démo
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"executor": "EdgeExecutor", "queue": "team-data", "owner": "team-data"},
    tags=["team-data", "edge", "assets"],
    doc_md=__doc__,
)
def data_ingest_orders():
    @task
    def list_regions() -> list[str]:
        ...
    publish(generate_orders.expand(region=list_regions()))


data_ingest_orders()
```

Les opérateurs classiques restent utilisables dans un DAG "nouveau" : `laptop_where_am_i` mélange `@task`
et un `BashOperator` exécuté sur le laptop (`edge worker: laptop-chris` dans le log).

## 2. Passage de données

**Ancienne** — XCom **explicite** par `task_id` :

```python
def transform(**context):
    # XCom explicite : on va chercher le résultat de la tâche amont par son task_id.
    ti = context["ti"]
    orders = ti.xcom_pull(task_ids="extract")
```

…et dans un template Jinja : `conf={"key": "{{ ti.xcom_pull(task_ids='load_to_s3') }}"}`.

**Nouvelle** — valeurs de retour passées en argument (XCom implicite) :

```python
# teams/team-data/dags/data_orders_daily.py
    @task
    def log_summary(summary: dict) -> None:
        """Reçoit le dict de la tâche précédente via XCom, sans xcom_pull explicite."""
        print(f"[{summary['ds']}] {summary['rows']} lignes, CA total {summary['revenue']} -> {summary['key']}")

    log_summary(aggregate_daily())
```

## 3. Couplage entre DAGs

**Ancienne** — le producteur **appelle** le consommateur :

```python
    t_trigger = TriggerDagRunOperator(
        task_id="trigger_report",
        trigger_dag_id="classic_report",
        conf={"key": "{{ ti.xcom_pull(task_ids='load_to_s3') }}"},
        wait_for_completion=False,
    )
```

```python
# classic_report.py : schedule=None, reçoit la clé dans dag_run.conf
    conf = context["dag_run"].conf or {}
```

**Nouvelle** — le consommateur **s'abonne** à une donnée ; l'`extra` de l'événement remplace le `conf` :

```python
# producteur (team-data)
    @task(outlets=[ORDERS_RAW])
    def publish(results: list[dict]):
        ...
        yield Metadata(ORDERS_RAW, {"rows": total, "ds": ds, "keys": keys})

# consommateur (même équipe)
@dag(dag_id="data_orders_daily", schedule=[ORDERS_RAW], ...)

# consommateur d'une AUTRE équipe : par nom, sans importer le code de team-data
ORDERS_DAILY_REF = Asset.ref(name="orders_daily")
@dag(dag_id="search_index_orders", schedule=[ORDERS_DAILY_REF], ...)
```

Ajouter un consommateur ne modifie pas le producteur. Détails et limite `triggering_asset_events` /
`Asset.ref` : [05 §1](05-assets-versioning-hitl.md#1-assets).

## 4. Dépendances

**Ancienne** — hooks des **providers installés dans l'image centrale** :

```python
def load_to_s3(**context):
    # Hook du provider Amazon (installé dans l'image centrale) : il lit la connexion
    # `minio_s3` (login/password + extra endpoint_url/region_name) et crée le client boto3.
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    ...
    S3Hook(aws_conn_id="minio_s3").load_string(buf.getvalue(), key=key, bucket_name=BUCKET, replace=True)
```

Le DAG dépend des versions de providers et de libs (pandas dans `classic_report`) **du centre** :
toute nouvelle dépendance = rebuild de l'image centrale + redéploiement de la plateforme, pour tout le monde.

**Nouvelle** — la lib de l'équipe, dans **son** worker, importée dans la tâche (règle D3) :

```python
    @task
    def generate_orders(region: str) -> dict:
        ...
        import polars as pl  # import local : règle D3
```

```python
def _s3_client():
    """Client boto3 vers MinIO depuis la connexion Airflow `minio_s3` (lue via l'Execution API)."""
    import boto3  # import local : règle D3

    conn = Connection.get("minio_s3")
```

La connexion reste gérée au centre ; seule la lib change de place.
Voir [06-isolation-equipes.md](06-isolation-equipes.md).

## 5. Tâches dynamiques

**Ancienne** — boucle Python évaluée **au parsing** : le nombre de tâches est figé par le fichier.
Illustration (ce motif n'est pas dans le repo) :

```python
for region in ["eu", "us", "apac"]:
    PythonOperator(task_id=f"generate_{region}", python_callable=generate, op_kwargs={"region": region})
```

**Nouvelle** — **dynamic task mapping** : le nombre d'instances est décidé **à l'exécution** par la sortie
d'une tâche amont, et le résultat agrégé arrive en liste paresseuse :

```python
    publish(generate_orders.expand(region=list_regions()))
...
        results = list(results)  # LazyXComSequence -> list
```

Relevé : `generate_orders[0]`, `[1]`, `[2]` sur `team-data-worker-1`.

## 6. Contexte et dates

En Airflow 3, un run manuel peut avoir `logical_date = None` : `ds` et les macros dérivées
n'existent alors pas. Les deux idéologies utilisent le même repli :

```python
def _run_date(context) -> str:
    # `ds` n'existe pas pour un run déclenché sans logical_date (Airflow 3).
    ds = context.get("ds")
    return ds or context["dag_run"].run_after.strftime("%Y-%m-%d")
```

Relevé du scénario `assets` : run `logical_date=None run_after=2026-10-17T10:08:55.387475Z` → success.

Côté nouveau, `get_current_context()` donne le contexte sans `**context` ; `Variable.get` et
`Connection.get` viennent de `airflow.sdk` et passent par l'Execution API.

## 7. Imports

| Usage | Chemin Airflow 3 (utilisé ici) | Ancien chemin (alias déprécié) |
|---|---|---|
| DAG, `@dag`, `@task`, `Asset`, `Variable`, `Connection`, `Param`, `Metadata` | `from airflow.sdk import ...` | `airflow.models.DAG`, `airflow.decorators`, `airflow.datasets`, `airflow.models.Variable` |
| `PythonOperator`, `BashOperator`, `TriggerDagRunOperator` | `airflow.providers.standard.operators.*` | `airflow.operators.*` |
| `ApprovalOperator` | `airflow.providers.standard.operators.hitl` | — (3.1+) |
| `AirflowSkipException` | `airflow.sdk.exceptions` | `airflow.exceptions` |
| `TriggerRule` | `from airflow.sdk import TriggerRule` | `airflow.utils.trigger_rule` |

Même les DAGs "anciens" du repo utilisent les chemins 3.3 : l'idéologie, c'est le **style**
(classes, XCom explicite, TriggerDagRun, libs au centre), pas les imports.

## 8. Où ça s'exécute

| | Ancienne | Nouvelle |
|---|---|---|
| executor | défaut = `LocalExecutor` (écrit explicitement dans `default_args`) | `"executor": "EdgeExecutor"` + `"queue": "<équipe>"` dans `default_args` |
| lieu (relevé) | `airflow-scheduler-0.airflow-scheduler.airflow-edge.svc.cluster.local` | `team-data-worker-1`, `team-search-worker-1`, `laptop-chris` |
| Python | celui de l'image centrale (3.13) | celui de l'équipe (3.12 / 3.13 / 3.11) |
| accès DB | par l'Execution API (Task SDK) dans les deux cas | idem ; le worker n'a pas de DB du tout |

## 9. Synthèse

| Question | Ancienne idéologie | Nouvelle idéologie |
|---|---|---|
| Qui décide de l'enchaînement entre DAGs ? | le producteur (`TriggerDagRunOperator`) | le consommateur (`schedule=[Asset]`) |
| Comment passent les données ? | `xcom_pull(task_ids=...)`, `dag_run.conf` | valeurs de retour, `extra` d'événement d'asset |
| Combien de tâches ? | figé au parsing | décidé à l'exécution (`expand`) |
| Où sont les libs ? | image centrale, pour tous | image du worker, par équipe |
| Ajouter une lib ? | rebuild + redéploiement de la plateforme | rebuild du seul worker de l'équipe |
| Livrer un DAG ? | dans l'image centrale (bundle `core`) | bundle de l'équipe (`dags.py apply`, `GitDagBundle` en prod) |
| Où tourne la tâche ? | pod scheduler | worker edge de l'équipe, n'importe où |
| Validation humaine ? | — | `ApprovalOperator` (HITL) sans slot occupé |
| Historique de structure ? | versions de DAG aussi (Airflow 3) | idem, démontré par `versions/v2` |
