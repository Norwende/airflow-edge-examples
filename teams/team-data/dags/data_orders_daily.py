"""
### data_orders_daily — agrégat quotidien du chiffre d'affaires (équipe team-data)

**Ce que montre ce DAG (Airflow 3, "nouvelle idéologie")**

- **Planification par asset** : `schedule=[ORDERS_RAW]` ; le DAG démarre dès qu'un événement
  est émis sur `orders_raw` (par `data_ingest_orders`). Avant : `TriggerDagRunOperator` dans le
  producteur, qui devait connaître le consommateur.
- **Métadonnées de l'événement** : `triggering_asset_events` (l'événement qui a déclenché ce run),
  avec repli sur `inlet_events[ORDERS_RAW][-1]` (dernier événement connu, utile en run manuel).
  L'`extra` contient les clés parquet à charger : pas de XCom inter-DAG.
- **Asset en sortie** : émet `orders_daily` avec un `extra` (clé, lignes, date) via
  `outlet_events` (variante du `yield Metadata` utilisé dans l'ingestion).
- **TaskFlow** : le dict retourné par `aggregate_daily` arrive dans `log_summary` par XCom, implicitement.
- `Variable.get("datalake_bucket")` et `Connection.get("minio_s3")` passent par l'Execution API
  (le worker edge n'a pas de base de données).
- Règle D3 : `polars` / `boto3` importés dans les fonctions.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Asset, Connection, Variable, dag, task
from airflow.sdk.exceptions import AirflowSkipException

ORDERS_RAW = Asset(name="orders_raw", uri="s3://datalake/orders/raw")
ORDERS_DAILY = Asset(name="orders_daily", uri="s3://datalake/orders/daily")


def _s3_client():
    """Client boto3 vers MinIO depuis la connexion `minio_s3`."""
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


def _source_extra(context) -> dict:
    """Extra de l'événement `orders_raw` : celui du déclencheur, sinon le dernier connu."""
    triggering = context["triggering_asset_events"].get(ORDERS_RAW) or []
    if triggering:
        return dict(triggering[-1].extra or {})
    inlet = context["inlet_events"][ORDERS_RAW]
    if len(inlet):
        return dict(inlet[-1].extra or {})
    raise AirflowSkipException("Aucun événement sur orders_raw : lancer data_ingest_orders d'abord.")


@dag(
    dag_id="data_orders_daily",
    schedule=[ORDERS_RAW],  # piloté par la donnée
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"executor": "EdgeExecutor", "queue": "team-data", "owner": "team-data"},
    tags=["team-data", "edge", "assets"],
    doc_md=__doc__,
)
def data_orders_daily():
    @task(inlets=[ORDERS_RAW], outlets=[ORDERS_DAILY])
    def aggregate_daily(**context) -> dict:
        """Charge les parquets listés dans l'extra, agrège le CA par région/jour, écrit le résultat."""
        import io

        import polars as pl  # import local : règle D3

        src = _source_extra(context)
        ds, keys = src["ds"], src["keys"]
        bucket = Variable.get("datalake_bucket")
        s3 = _s3_client()

        frames = [
            pl.read_parquet(io.BytesIO(s3.get_object(Bucket=bucket, Key=k)["Body"].read())) for k in keys
        ]
        daily = (
            pl.concat(frames)
            .with_columns((pl.col("quantity") * pl.col("unit_price")).alias("revenue"))
            .group_by("region", "order_date")
            .agg(pl.len().alias("orders"), pl.col("revenue").sum().round(2))
            .sort("region")
        )
        print(daily)

        buf = io.BytesIO()
        daily.write_parquet(buf)
        key = f"orders/daily/{ds}.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

        # Autre façon de poser l'extra d'un événement d'asset (équivalente à `yield Metadata`)
        context["outlet_events"][ORDERS_DAILY].extra = {"key": key, "rows": daily.height, "ds": ds}
        return {
            "key": key,
            "rows": daily.height,
            "ds": ds,
            "revenue": round(float(daily["revenue"].sum()), 2),
        }

    @task
    def log_summary(summary: dict) -> None:
        """Reçoit le dict de la tâche précédente via XCom, sans xcom_pull explicite."""
        print(
            f"[{summary['ds']}] {summary['rows']} lignes, CA total {summary['revenue']} -> {summary['key']}"
        )

    log_summary(aggregate_daily())


data_orders_daily()
