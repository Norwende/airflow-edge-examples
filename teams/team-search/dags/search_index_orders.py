"""
### search_index_orders — indexe l'agrégat quotidien dans Elasticsearch (équipe team-search)

- **Dépendance inter-équipes par nom d'asset** : planifié sur `Asset.ref(name="orders_daily")`,
  produit par team-data. team-search n'importe **aucun** code de team-data : seul le nom compte.
- **inlet_events** : `fetch_daily` déclare l'asset en `inlets` et lit `extra["key"]` du dernier
  événement (`context["inlet_events"][ref][-1].extra`) → clé du parquet dans MinIO, sans XCom inter-DAG.
- **Déclenchement manuel** : param `key` (facultatif). Sans param ni événement, on prend le dernier
  objet sous `orders/daily/`.
- **Asset en sortie** : `orders_index` (`es://orders`) avec `extra={"docs": n}`.
- Exécuté sur l'edge worker `team-search` (Python 3.13, elasticsearch + pandas/pyarrow + boto3).
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import Param, dag, task

from search_common import (
    ORDERS_DAILY_REF,
    ORDERS_INDEX,
    TEAM_DEFAULT_ARGS,
    TEAM_TAGS,
    bulk_index,
    latest_daily_key,
    read_daily_docs,
)


def _resolve_key(context: dict[str, Any]) -> str:
    """Ordre : param `key` (manuel) > extra du dernier événement d'asset > dernier objet MinIO."""
    if key := context["params"].get("key"):
        print(f"clé fournie en paramètre : {key}")
        return key
    events = context["inlet_events"][ORDERS_DAILY_REF]
    if events and (key := (events[-1].extra or {}).get("key")):
        print(f"clé lue dans le dernier événement d'asset orders_daily : {key}")
        return key
    key = latest_daily_key()
    print(f"aucun événement exploitable : dernier objet MinIO {key}")
    return key


@dag(
    dag_id="search_index_orders",
    schedule=[ORDERS_DAILY_REF],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=TEAM_DEFAULT_ARGS,
    tags=TEAM_TAGS,
    params={
        "key": Param(
            None,
            type=["null", "string"],
            description="Clé S3 d'un parquet quotidien (ex. orders/daily/2026-10-17.parquet). Vide = auto.",
        )
    },
    doc_md=__doc__,
)
def search_index_orders():
    @task(inlets=[ORDERS_DAILY_REF])
    def fetch_daily(**context) -> list[dict[str, Any]]:
        """Résout la clé du parquet puis le convertit en documents."""
        return read_daily_docs(_resolve_key(context))

    @task
    def index_docs(docs: list[dict[str, Any]]) -> int:
        """Indexation bulk idempotente dans l'index `orders`."""
        return bulk_index(docs)

    @task(outlets=[ORDERS_INDEX])
    def publish(count: int, **context) -> None:
        """Émet l'événement d'asset `orders_index` avec le nombre de documents."""
        context["outlet_events"][ORDERS_INDEX].extra = {"docs": count}
        print(f"orders_index publié : {count} documents")

    publish(index_docs(fetch_daily()))


search_index_orders()
