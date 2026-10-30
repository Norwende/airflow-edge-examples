"""
Utilitaires partagés des DAGs team-search (module importable, ne définit aucun DAG).

Règle D3 : ce fichier est lu par le dag-processor central, qui n'a ni elasticsearch, ni pandas,
ni pyarrow, ni boto3 → ces libs sont importées **dans** les fonctions, jamais au niveau module.
Le dossier du bundle est ajouté au `sys.path` (BundleDagBag) côté central comme côté worker,
d'où `from search_common import ...` dans les DAGs (nom préfixé pour éviter les collisions entre bundles).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.sdk import Asset, Connection, Variable

if TYPE_CHECKING:  # annotations seulement : jamais évaluées au parsing
    from elasticsearch import Elasticsearch

# Assets du contrat (noms stables). L'asset de team-data est référencé par son **nom** uniquement.
ORDERS_DAILY_REF = Asset.ref(name="orders_daily")
ORDERS_INDEX = Asset(name="orders_index", uri="es://orders")

INDEX_NAME = "orders"
DAILY_PREFIX = "orders/daily/"
INDEX_MAPPING: dict[str, Any] = {
    "properties": {
        "region": {"type": "keyword"},
        "revenue": {"type": "float"},
        "ds": {"type": "date"},
        "orders": {"type": "integer"},
        "source_key": {"type": "keyword"},
    }
}
TEAM_DEFAULT_ARGS: dict[str, Any] = {
    "executor": "EdgeExecutor",
    "queue": "team-search",
    "owner": "team-search",
}
TEAM_TAGS = ["team-search", "edge", "assets"]


def s3_client() -> Any:
    """Client boto3 vers MinIO, construit depuis la connexion `minio_s3` (Execution API, pas de DB)."""
    import boto3

    conn = Connection.get("minio_s3")
    extra = conn.extra_dejson
    return boto3.client(
        "s3",
        endpoint_url=extra["endpoint_url"],
        region_name=extra.get("region_name", "us-east-1"),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )


def es_client() -> Elasticsearch:
    """Client Elasticsearch depuis la connexion `elasticsearch_default` (host/port/schema)."""
    from elasticsearch import Elasticsearch

    conn = Connection.get("elasticsearch_default")
    return Elasticsearch(f"{conn.schema or 'http'}://{conn.host}:{conn.port or 9200}", request_timeout=30)


def latest_daily_key() -> str:
    """Dernier objet parquet sous `orders/daily/` (repli quand aucun événement d'asset n'existe)."""
    bucket = Variable.get("datalake_bucket")
    pages = s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=DAILY_PREFIX)
    objects = [o for page in pages for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
    if not objects:
        raise FileNotFoundError(f"aucun parquet sous s3://{bucket}/{DAILY_PREFIX}")
    return max(objects, key=lambda o: o["LastModified"])["Key"]


def read_daily_docs(key: str) -> list[dict[str, Any]]:
    """Télécharge le parquet quotidien et le convertit en documents (pandas + pyarrow)."""
    import io

    import pandas as pd

    bucket = Variable.get("datalake_bucket")
    body = s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
    df = pd.read_parquet(io.BytesIO(body), engine="pyarrow")
    # ds : colonne `order_date` (schéma team-data) sinon la date tirée du nom de fichier
    fallback_ds = key.rsplit("/", 1)[-1].removesuffix(".parquet")
    ds = pd.to_datetime(df["order_date"]).dt.strftime("%Y-%m-%d") if "order_date" in df else fallback_ds
    out = pd.DataFrame(
        {
            "region": df["region"].astype(str),
            "revenue": df["revenue"].astype(float),
            "ds": ds,
            "orders": df["orders"].astype(int) if "orders" in df else 0,
            "source_key": key,
        }
    )
    print(f"s3://{bucket}/{key} : {len(out)} documents")
    # types Python natifs (int/float/str) → sérialisables en XCom
    return [
        {k: (v.item() if hasattr(v, "item") else v) for k, v in r.items()} for r in out.to_dict("records")
    ]


def ensure_index(recreate: bool = False) -> None:
    """Crée l'index `orders` avec son mapping (le supprime d'abord si `recreate`)."""
    es = es_client()
    if recreate and es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        print(f"index {INDEX_NAME} supprimé")
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, mappings=INDEX_MAPPING)
        print(f"index {INDEX_NAME} créé")


def bulk_index(docs: list[dict[str, Any]]) -> int:
    """Indexation bulk idempotente : `_id` = région + jour → rejouer écrase au lieu de dupliquer."""
    from elasticsearch import helpers

    ensure_index()
    actions = ({"_index": INDEX_NAME, "_id": f"{d['region']}_{d['ds']}", "_source": d} for d in docs)
    ok, _ = helpers.bulk(es_client(), actions, refresh="wait_for")
    print(f"{ok} documents indexés dans {INDEX_NAME}")
    return int(ok)


# --- Helpers génériques (index quelconque) : ajoutés pour search_index_air, signatures ci-dessus inchangées


def list_keys(prefix: str, suffix: str = ".parquet") -> list[dict[str, Any]]:
    """Objets (Key, LastModified, ...) sous `prefix` dans le bucket du datalake, filtrés par suffixe."""
    bucket = Variable.get("datalake_bucket")
    pages = s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return [o for page in pages for o in page.get("Contents", []) if o["Key"].endswith(suffix)]


def read_parquet_records(key: str) -> list[dict[str, Any]]:
    """Télécharge un parquet et le renvoie en liste de dicts (pandas + pyarrow, types numpy conservés)."""
    import io

    import pandas as pd

    bucket = Variable.get("datalake_bucket")
    body = s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
    df = pd.read_parquet(io.BytesIO(body), engine="pyarrow")
    print(f"s3://{bucket}/{key} : {len(df)} lignes, colonnes {list(df.columns)}")
    return df.to_dict("records")


def ensure_named_index(index: str, mapping: dict[str, Any]) -> bool:
    """Crée `index` avec `mapping` s'il n'existe pas ; renvoie True si l'index vient d'être créé."""
    es = es_client()
    if es.indices.exists(index=index):
        return False
    es.indices.create(index=index, mappings=mapping)
    print(f"index {index} créé")
    return True


def bulk_index_with_ids(index: str, docs: list[dict[str, Any]], id_field: str = "_id") -> int:
    """Bulk idempotent : `_id` lu dans `doc[id_field]` (retiré du `_source`) → rejouer écrase."""
    from elasticsearch import helpers

    actions = (
        {"_index": index, "_id": d[id_field], "_source": {k: v for k, v in d.items() if k != id_field}}
        for d in docs
    )
    ok, _ = helpers.bulk(es_client(), actions, refresh="wait_for")
    print(f"{ok} documents indexés dans {index}")
    return int(ok)
