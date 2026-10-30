"""
Code partagé des DAGs « bulletin qualité de l'air » du laptop (module importable, ne définit aucun DAG).

Utilisé par ``laptop_air_bulletin`` (planifié par partition d'asset, étapes 8/10 de l'exercice 09) et
``laptop_air_bulletin_cron`` (cron 07:00, pour l'exercice de backfill). Nom préfixé ``laptop_`` :
les bundles partagent le ``sys.path`` du dag-processor (règle D3, docs/06).

Règle D3 : lu par le dag-processor central (ni duckdb, ni boto3, ni matplotlib) → ces libs sont
importées **dans** les fonctions. matplotlib n'existe que dans le venv créé par ``@task.virtualenv``.
"""

from __future__ import annotations

from typing import Any

from airflow.sdk import Asset, Connection, Param, Variable, task

# Assets du contrat (docs/09 §1). air_hourly est produit par team-iot : référencé par son nom.
AIR_HOURLY_REF = Asset.ref(name="air_hourly")
AIR_BULLETIN = Asset(name="air_bulletin", uri="s3://datalake/air/bulletin")

HOURLY_PREFIX = "air/hourly/"
MEASUREMENTS_PREFIX = "air/measurements/"
BULLETIN_PREFIX = "air/bulletin/"
MEASURES = ("pm25", "pm10", "no2", "o3", "temp")
VENV_CACHE = "/opt/airflow/.venvs"

LAPTOP_DEFAULT_ARGS: dict[str, Any] = {"executor": "EdgeExecutor", "queue": "laptop", "owner": "laptop"}
AIR_TAGS = ["laptop", "edge", "air"]
AIR_PARAMS = {
    "ds": Param(
        None,
        type=["null", "string"],
        description="Jour du bulletin (AAAA-MM-JJ) ; vide = date de partition, sinon logical_date/run_after.",
    )
}


def _s3_settings() -> dict[str, Any]:
    """Endpoint/région/bucket S3 (non secrets) : env MINIO_ENDPOINT_URL prioritaire sur la connexion."""
    import os

    conn = Connection.get("minio_s3")
    extra = conn.extra_dejson or {}
    return {
        "endpoint_url": os.environ.get("MINIO_ENDPOINT_URL") or extra.get("endpoint_url"),
        "region_name": extra.get("region_name", "us-east-1"),
        "bucket": Variable.get("datalake_bucket", default="datalake"),
    }


def _bulletin_day(context: dict[str, Any]) -> tuple[str, str]:
    """Jour du bulletin et son origine : partition > param ``ds`` > logical_date > run_after."""
    dag_run = context["dag_run"]
    partition_date = context.get("partition_date") or getattr(dag_run, "partition_date", None)
    if partition_date is not None:
        return partition_date.strftime("%Y-%m-%d"), f"partition {context.get('partition_key')}"
    if ds := context["params"].get("ds"):
        return str(ds), "param ds"
    if (logical_date := context.get("logical_date")) is not None:
        return logical_date.strftime("%Y-%m-%d"), "logical_date"
    return dag_run.run_after.strftime("%Y-%m-%d"), "run_after (logical_date nulle)"


def _list(s3: Any, bucket: str, prefix: str) -> list[str]:
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return sorted(o["Key"] for page in pages for o in page.get("Contents", []))


def _daily_stats(con: Any, view: str) -> list[dict[str, Any]]:
    """Moyenne + max journaliers par station.

    Colonnes acceptées par mesure ``m`` : ``m`` (mesures brutes), ``m_mean`` / ``m_max``, ou
    ``mean_m`` (stats horaires de team-iot : ``station, hour, rows, mean_pm25, ...``). Pour des
    moyennes horaires, la moyenne du jour est pondérée par ``rows`` si la colonne existe, et le max
    est le max des moyennes horaires (pas de max horaire publié).
    """
    cols = {c[0] for c in con.sql(f"DESCRIBE {view}").fetchall()}
    aggs = []
    for m in MEASURES:
        mean_col = next((c for c in (m, f"{m}_mean", f"mean_{m}") if c in cols), None)
        if mean_col is None:
            continue
        max_col = f"{m}_max" if f"{m}_max" in cols else mean_col
        if mean_col != m and "rows" in cols:  # moyenne des moyennes horaires pondérée par le nb de lignes
            mean_expr = (
                f'SUM("{mean_col}" * "rows") / NULLIF(SUM(CASE WHEN "{mean_col}" IS NOT NULL '
                f'THEN "rows" END), 0)'
            )
        else:
            mean_expr = f'AVG("{mean_col}")'
        aggs += [
            f"ROUND(({mean_expr})::DOUBLE, 2) AS {m}_mean",
            f'ROUND(MAX("{max_col}")::DOUBLE, 2) AS {m}_max',
        ]
    if "station" not in cols or not aggs:
        raise ValueError(f"colonnes inattendues dans {view} : {sorted(cols)}")
    rel = con.sql(f"SELECT station, {', '.join(aggs)} FROM {view} GROUP BY station ORDER BY station")
    rel.show()
    return [
        {k: (float(v) if isinstance(v, float) else v) for k, v in zip(rel.columns, row, strict=True)}
        for row in rel.fetchall()
    ]


def _markdown(stats: dict[str, Any]) -> str:
    lines = [
        f"# Bulletin qualité de l'air — {stats['ds']}",
        "",
        f"Source : `{stats['source']}` ({stats['files']} fichier(s)), jour résolu par {stats['day_from']}.",
        "",
    ]
    rows = stats["stations"]
    if not rows:
        return "\n".join([*lines, "_Aucune donnée pour ce jour._", ""])
    headers = [k for k in rows[0] if k != "station"]
    lines += ["| station | " + " | ".join(headers) + " |", "|---|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(str(r[k]) for k in ["station", *headers]) + " |" for r in rows]
    return "\n".join([*lines, ""])


def _load_view(con: Any, s3: Any, bucket: str, keys: list[str], tmpdir: str, view: str) -> int:
    """Télécharge ``keys`` (boto3) dans ``tmpdir``, crée la vue DuckDB ``view``, renvoie son nb de lignes.

    Pas de httpfs : depuis le conteneur laptop, DuckDB ne résout pas ``minio.localhost`` (il ignore
    l'entrée host-gateway de /etc/hosts) et Traefik refuse l'IP sans en-tête Host. boto3 passe.
    """
    import os

    paths = []
    for i, key in enumerate(keys):
        local = os.path.join(tmpdir, f"{view}-{i:04d}{os.path.splitext(key)[1]}")  # sans ":" ni "/"
        s3.download_file(bucket, key, local)
        paths.append(local)
    parquet = [x for x in paths if x.endswith(".parquet")]
    reader = f"read_parquet({parquet!r}, union_by_name=true)" if parquet else f"read_json_auto({paths!r})"
    con.execute(f"CREATE VIEW {view} AS SELECT * FROM {reader}")
    return con.sql(f"SELECT COUNT(*) FROM {view}").fetchone()[0]


def collect_day(**context) -> dict[str, Any]:
    """Statistiques journalières (boto3 + DuckDB local) → ``air/bulletin/<ds>.md``. Renvoie un petit dict."""
    import tempfile

    import boto3
    import duckdb
    from botocore.config import Config

    ds, day_from = _bulletin_day(context)
    cfg = _s3_settings()
    conn = Connection.get("minio_s3")
    bucket = cfg["bucket"]
    print(f"bulletin du {ds} (jour résolu par {day_from}) ; endpoint S3 {cfg['endpoint_url']}")
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg["endpoint_url"],
        region_name=cfg["region_name"],
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        config=Config(s3={"addressing_style": "path"}),  # l'ingress route sur Host=minio.localhost
    )

    # 1) Sources : stats horaires du jour (hour=<ds>T..) si elles contiennent des lignes,
    #    sinon mesures brutes ds=<ds>/ (une heure sans donnée produit un stats.parquet à 0 ligne).
    hourly = [k for k in _list(s3, bucket, f"{HOURLY_PREFIX}hour={ds}") if k.endswith((".parquet", ".json"))]
    measurements = [k for k in _list(s3, bucket, f"{MEASUREMENTS_PREFIX}ds={ds}/") if k.endswith(".parquet")]
    print(f"{len(hourly)} fichier(s) horaires, {len(measurements)} parquet(s) de mesures")

    source, keys, stations = "none", [], []
    con = duckdb.connect()
    with tempfile.TemporaryDirectory(prefix="laptop-air-") as tmpdir:
        if hourly and (n := _load_view(con, s3, bucket, hourly, tmpdir, "hourly")) > 0:
            source, keys, view = "air_hourly", hourly, "hourly"
        else:
            if hourly:
                print("stats horaires présentes mais vides (0 ligne) : repli sur air/measurements")
            if measurements and (n := _load_view(con, s3, bucket, measurements, tmpdir, "meas")) > 0:
                source, keys, view = "air_measurements", measurements, "meas"
        if keys:
            print(f"source {source} : {len(keys)} fichier(s), {n} ligne(s)")
            stations = _daily_stats(con, view)
        else:
            print(f"aucune donnée air pour le {ds} : bulletin vide (mais valide)")

    stats = {"ds": ds, "day_from": day_from, "source": source, "files": len(keys), "stations": stations}
    md_key = f"{BULLETIN_PREFIX}{ds}.md"
    s3.put_object(Bucket=bucket, Key=md_key, Body=_markdown(stats).encode(), ContentType="text/markdown")
    print(f"écrit s3://{bucket}/{md_key}")
    return {**stats, "md": md_key, "s3": cfg}


@task.virtualenv(
    task_id="render_chart",
    requirements=["matplotlib", "boto3"],
    system_site_packages=False,
    venv_cache_path=VENV_CACHE,
    expect_airflow=False,  # pas d'Airflow dans le venv : pas de contexte, arguments simples uniquement
)
def render_chart(stats: dict, access_key: str, secret_key: str) -> str:
    """Dans un venv (matplotlib + boto3) : graphique PM2.5 moyen/max par station → ``<ds>.png``."""
    import io
    import sys

    import boto3
    import matplotlib
    from botocore.config import Config

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    print(f"venv : {sys.prefix} (python {sys.version.split()[0]}, matplotlib {matplotlib.__version__})")
    rows = stats["stations"]
    fig, ax = plt.subplots(figsize=(8, 4))
    if rows and "pm25_mean" in rows[0]:
        names = [r["station"] for r in rows]
        x = range(len(names))
        ax.bar([i - 0.2 for i in x], [r["pm25_mean"] or 0 for r in rows], width=0.4, label="moyenne")
        ax.bar([i + 0.2 for i in x], [r["pm25_max"] or 0 for r in rows], width=0.4, label="max")
        ax.axhline(15, color="grey", linestyle="--", linewidth=1, label="OMS 24 h (15 µg/m³)")
        ax.set_xticks(list(x), names, rotation=30, ha="right")
        ax.set_ylabel("PM2.5 (µg/m³)")
        ax.legend()
    else:
        ax.text(0.5, 0.5, "Aucune donnée", ha="center", va="center", fontsize=16, transform=ax.transAxes)
        ax.set_axis_off()
    ax.set_title(f"PM2.5 par station — {stats['ds']} (source {stats['source']})")
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100)

    cfg = stats["s3"]
    key = f"air/bulletin/{stats['ds']}.png"
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg["endpoint_url"],
        region_name=cfg["region_name"],
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(s3={"addressing_style": "path"}),
    )
    s3.put_object(Bucket=cfg["bucket"], Key=key, Body=buf.getvalue(), ContentType="image/png")
    print(f"écrit s3://{cfg['bucket']}/{key} ({len(buf.getvalue())} octets)")
    return key


def publish_bulletin(stats: dict[str, Any], png_key: str, **context) -> dict[str, Any]:
    """Émet l'événement ``air_bulletin`` avec les clés du .md et du .png."""
    extra = {"ds": stats["ds"], "md": stats["md"], "png": png_key}
    context["outlet_events"][AIR_BULLETIN].extra = extra
    print(f"air_bulletin publié : {extra}")
    return extra


def build_bulletin_tasks() -> None:
    """Assemble collect_day → render_chart (venv) → publish_bulletin dans le DAG courant."""
    stats = task(task_id="collect_day")(collect_day)()
    # Identifiants MinIO rendus par Jinja sur le worker (masqués dans « Rendered Template »),
    # plutôt que transportés en XCom ; endpoint/bucket (non secrets) passent par l'XCom de collect_day.
    png = render_chart(stats, "{{ conn.minio_s3.login }}", "{{ conn.minio_s3.password }}")
    task(task_id="publish_bulletin", outlets=[AIR_BULLETIN])(publish_bulletin)(stats, png)
