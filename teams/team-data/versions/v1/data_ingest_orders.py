"""
### data_ingest_orders — ingestion des commandes brutes (équipe team-data)

**Ce que montre ce DAG (Airflow 3, "nouvelle idéologie")**

- **Task SDK** : tout vient de `airflow.sdk` (`@dag`, `@task`, `Asset`, `Variable`, `Connection`,
  `Metadata`). Plus d'import de `airflow.models` / `airflow.operators` : le code DAG ne parle
  plus à la base de données, les tâches passent par l'Execution API.
- **Edge Executor** : `default_args` route chaque tâche vers `EdgeExecutor`, queue `team-data`
  → exécution sur le worker de l'équipe (Python 3.12 + polars + boto3), pas au centre.
- **Règle D3** : `polars` / `boto3` sont importés **dans** les fonctions de tâche ; le
  dag-processor central (qui n'a pas ces libs) peut donc parser ce fichier.
- **Dynamic task mapping** : `generate_orders.expand(region=list_regions())` crée une tâche
  par région à l'exécution (avant : boucle Python figée au parsing).
- **Assets + métadonnées** : `publish` émet un événement sur l'asset `orders_raw` avec un
  `extra` (lignes, date, clés S3) via `yield Metadata(...)`. Avant : `TriggerDagRunOperator`
  + XCom explicite entre DAGs ; ici le DAG consommateur (`data_orders_daily`) est planifié
  **par la donnée**.
- **`logical_date` peut être `None`** (run manuel en Airflow 3) : la date est dérivée de
  `dag_run.run_after` si `ds` est absent.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Asset, Connection, Metadata, Variable, dag, get_current_context, task

# Asset partagé (contrat) : nom stable + URI.
ORDERS_RAW = Asset(name="orders_raw", uri="s3://datalake/orders/raw")
REGIONS = ["eu", "us", "apac"]
ROWS_PER_REGION = 500


def _run_date(context) -> str:
    """Date du run (YYYY-MM-DD) ; `ds` est absent si logical_date est None (run manuel)."""
    ds = context.get("ds")
    if ds:
        return ds
    return context["dag_run"].run_after.strftime("%Y-%m-%d")


def _s3_client():
    """Client boto3 vers MinIO depuis la connexion Airflow `minio_s3` (lue via l'Execution API)."""
    import boto3  # import local : règle D3

    conn = Connection.get("minio_s3")
    extra = conn.extra_dejson
    return boto3.client(
        "s3",
        endpoint_url=extra["endpoint_url"],
        region_name=extra.get("region_name", "us-east-1"),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )


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
        """Liste des régions : sa taille décide du nombre de tâches mappées."""
        return REGIONS

    @task
    def generate_orders(region: str) -> dict:
        """Génère des commandes synthétiques (polars) et les écrit en parquet sur MinIO."""
        import io
        import random
        import zlib

        import polars as pl  # import local : règle D3

        ds = _run_date(get_current_context())
        rng = random.Random(zlib.crc32(f"{region}{ds}".encode()))  # graine déterministe région+date
        n = ROWS_PER_REGION
        df = pl.DataFrame(
            {
                "order_id": [f"{region}-{ds}-{i:05d}" for i in range(n)],
                "region": [region] * n,
                "order_date": [ds] * n,
                "quantity": [rng.randint(1, 5) for _ in range(n)],
                "unit_price": [round(rng.uniform(5, 200), 2) for _ in range(n)],
            }
        ).with_columns(pl.col("order_date").str.to_date())

        buf = io.BytesIO()
        df.write_parquet(buf)  # parquet en mémoire, puis put_object (pas de s3fs)
        bucket = Variable.get("datalake_bucket", default="datalake")
        key = f"orders/raw/{ds}/{region}.parquet"
        _s3_client().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
        print(f"s3://{bucket}/{key} : {df.height} lignes")
        return {"region": region, "rows": df.height, "key": key}

    @task(outlets=[ORDERS_RAW])
    def publish(results: list[dict]):
        """Agrège les résultats mappés et émet l'événement d'asset `orders_raw` avec métadonnées."""
        import importlib.metadata
        import socket
        import sys

        # Preuve du lieu d'exécution (lue par scripts/scenario.py dans le log de la tâche)
        print(
            f"worker={socket.gethostname()} python={sys.version.split()[0]} "
            f"polars={importlib.metadata.version('polars')}"
        )
        results = list(results)  # LazyXComSequence -> list
        ds = _run_date(get_current_context())
        total = sum(r["rows"] for r in results)
        keys = [r["key"] for r in results]
        # Le Task SDK consomme les `Metadata` produits : outlet_events[ORDERS_RAW].extra.update(...)
        yield Metadata(ORDERS_RAW, {"rows": total, "ds": ds, "keys": keys})
        return {"rows": total, "ds": ds, "keys": keys}  # valeur de retour -> XCom

    publish(generate_orders.expand(region=list_regions()))


data_ingest_orders()
