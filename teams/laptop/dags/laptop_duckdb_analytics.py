"""
Analytique duckdb sur le laptop, déclenchée par l'asset ``orders_daily`` de team-data.

- Consomme l'asset **par nom** (``Asset.ref(name="orders_daily")``) sans importer le code de team-data.
- Lit l'``extra`` du dernier événement d'asset (clé S3 du parquet) via ``inlet_events`` ;
  en déclenchement manuel sans événement : prend le dernier objet sous ``orders/daily/``.
- Calcule le CA par région avec **duckdb** (schéma lu dynamiquement par ``DESCRIBE``).

Endpoint S3 « spécifique à l'environnement » : la connexion ``minio_s3`` porte l'endpoint
interne au cluster, injoignable depuis le laptop ; la variable d'environnement
``MINIO_ENDPOINT_URL`` (ex. ``http://minio.localhost``) le surcharge quand elle est définie.

Lecture : boto3 télécharge le parquet puis duckdb le lit en local (par défaut, robuste) ;
paramètre ``use_httpfs=True`` : duckdb lit directement ``s3://`` via l'extension httpfs.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Asset, Connection, Param, Variable, dag, task

ORDERS_DAILY = Asset.ref(name="orders_daily")
DAILY_PREFIX = "orders/daily/"
NUMERIC_TYPES = (
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "REAL",
)


def _s3_settings() -> dict:
    """Identifiants + endpoint S3 : env MINIO_ENDPOINT_URL prioritaire sur l'extra de la connexion."""
    import os

    conn = Connection.get("minio_s3")
    extra = conn.extra_dejson or {}
    return {
        "endpoint_url": os.environ.get("MINIO_ENDPOINT_URL") or extra.get("endpoint_url"),
        "region_name": extra.get("region_name", "us-east-1"),
        "access_key": conn.login,
        "secret_key": conn.password,
    }


def _key_from_extra(extra: dict, bucket: str) -> str | None:
    """Extrait la clé objet de l'extra d'événement (tolère key / s3_key / object_key / path / uri s3://...)."""
    for field in ("key", "s3_key", "object_key", "path", "uri"):
        value = (extra or {}).get(field)
        if isinstance(value, str) and value:
            prefix = f"s3://{bucket}/"
            return value[len(prefix) :] if value.startswith(prefix) else value.lstrip("/")
    return None


def _aggregate(con) -> tuple[str | None, list[str], list[dict]]:
    """Groupe par région (si présente) et somme toutes les colonnes numériques non-identifiants."""
    schema = con.sql("DESCRIBE orders").fetchall()  # (name, type, null, key, default, extra)
    print("schéma :", [(c[0], c[1]) for c in schema])
    names = [c[0] for c in schema]
    group_col = next((n for n in names if n.lower() in ("region", "country", "city")), None)
    measures = [
        c[0]
        for c in schema
        if c[1].upper().startswith(NUMERIC_TYPES)
        and c[0] != group_col
        and not (c[0].lower() == "id" or c[0].lower().endswith("_id"))
    ]
    aggs = ", ".join([f'ROUND(SUM("{m}"), 2) AS "{m}"' for m in measures] + ["COUNT(*) AS n_orders"])
    if group_col:
        query = f'SELECT "{group_col}", {aggs} FROM orders GROUP BY 1 ORDER BY 1'
    else:
        query = f"SELECT 'all' AS region, {aggs} FROM orders"
    rel = con.sql(query)
    rel.show()
    columns = list(rel.columns)
    rows = [
        {
            k: (float(v) if hasattr(v, "as_integer_ratio") and not isinstance(v, int) else v)
            for k, v in zip(columns, r, strict=True)
        }
        for r in rel.fetchall()
    ]
    return group_col, measures, rows


@dag(
    dag_id="laptop_duckdb_analytics",
    schedule=[ORDERS_DAILY],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"executor": "EdgeExecutor", "queue": "laptop", "owner": "laptop"},
    tags=["laptop", "edge"],
    params={
        "use_httpfs": Param(False, type="boolean", description="Lire s3:// directement via duckdb httpfs")
    },
    doc_md=__doc__,
)
def laptop_duckdb_analytics():
    @task(inlets=[ORDERS_DAILY])
    def revenue_by_region(**context) -> dict:
        """Agrège le parquet orders_daily par région avec duckdb et renvoie le résultat."""
        import os
        import tempfile

        import boto3
        import duckdb
        from botocore.config import Config

        bucket = Variable.get("datalake_bucket", default="datalake")
        s3cfg = _s3_settings()
        print(f"endpoint S3 utilisé : {s3cfg['endpoint_url']} (bucket {bucket})")
        s3 = boto3.client(
            "s3",
            endpoint_url=s3cfg["endpoint_url"],
            region_name=s3cfg["region_name"],
            aws_access_key_id=s3cfg["access_key"],
            aws_secret_access_key=s3cfg["secret_key"],
            # path-style : l'ingress route sur Host=minio.localhost (pas de <bucket>.minio.localhost)
            config=Config(s3={"addressing_style": "path"}),
        )

        # 1) Clé du parquet : dernier événement d'asset, sinon dernier objet sous orders/daily/.
        key = None
        events = context["inlet_events"][ORDERS_DAILY]
        if len(events) > 0:
            last = events[-1]
            print(f"dernier événement orders_daily : {last.timestamp} extra={last.extra}")
            key = _key_from_extra(last.extra, bucket)
        if not key:
            objs = []
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=DAILY_PREFIX):
                objs += [o for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
            if not objs:
                raise FileNotFoundError(f"aucun parquet sous s3://{bucket}/{DAILY_PREFIX}")
            key = max(objs, key=lambda o: o["LastModified"])["Key"]
            print(f"pas d'événement exploitable : dernier objet = {key}")

        # 2) Source duckdb : httpfs (s3:// direct) ou fichier local téléchargé par boto3.
        con = duckdb.connect()
        if context["params"].get("use_httpfs"):
            from urllib.parse import urlparse

            ep = urlparse(s3cfg["endpoint_url"] or "")
            con.sql("INSTALL httpfs; LOAD httpfs;")  # no-op si pré-installée dans l'image
            con.execute(
                "CREATE SECRET minio (TYPE s3, KEY_ID ?, SECRET ?, REGION ?, ENDPOINT ?, "
                "URL_STYLE 'path', USE_SSL ?)",
                [
                    s3cfg["access_key"],
                    s3cfg["secret_key"],
                    s3cfg["region_name"],
                    ep.netloc,
                    ep.scheme == "https",
                ],
            )
            source = f"s3://{bucket}/{key}"
        else:
            tmpdir = tempfile.mkdtemp(prefix="laptop-duckdb-")
            source = os.path.join(tmpdir, os.path.basename(key))
            s3.download_file(bucket, key, source)
        print(f"lecture duckdb : {source}")
        con.execute(f"CREATE VIEW orders AS SELECT * FROM read_parquet('{source}')")

        group_col, measures, rows = _aggregate(con)
        return {
            "source": f"s3://{bucket}/{key}",
            "group_by": group_col,
            "measures": measures,
            "rows": rows,
            "duckdb": duckdb.__version__,
        }

    revenue_by_region()


laptop_duckdb_analytics()
