"""
Utilitaires partagés des DAGs team-iot (module importable, ne définit aucun DAG).

Règle D3 : ce fichier est lu par le dag-processor central, qui n'a ni numpy, ni scikit-learn, ni
s3fs, ni boto3 → ces libs (et pyarrow) sont importées **dans** les fonctions, jamais au niveau module.
Le dossier du bundle est dans le `sys.path` côté centre comme côté worker (`from iot_common import ...`).

Contrat (docs/09 §1) : bucket `datalake`, préfixes `air/...`, assets `air_inbox` (surveillé),
`air_measurements`, `air_quarantine`, `air_hourly` (partitionné), alias `air_stations`.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.providers.standard.operators.hitl import HITLBranchOperator, HITLOperator
from airflow.sdk import Asset, AssetAlias, AssetWatcher, BaseNotifier, Connection, Variable

from iot_triggers import S3NewObjectTrigger

if TYPE_CHECKING:  # annotations seulement : jamais évaluées au parsing
    from airflow.sdk import ObjectStoragePath

# --- Contrat S3 -------------------------------------------------------------------------------
CONN_ID = "minio_s3"
DEFAULT_BUCKET = "datalake"
INBOX_PREFIX = "air/inbox/"
PROCESSED_PREFIX = "air/processed/"
MEASUREMENTS_PREFIX = "air/measurements/"
QUARANTINE_PREFIX = "air/quarantine/"
HOURLY_PREFIX = "air/hourly/"
TMP_PREFIX = "air/tmp/"
ALERTS_PREFIX = "air/alerts/"

CSV_COLUMNS = ["ts", "station", "lat", "lon", "pm25", "pm10", "no2", "o3", "temp"]
NUMERIC_COLUMNS = ["lat", "lon", "pm25", "pm10", "no2", "o3", "temp"]
POLLUTANTS = ["pm25", "pm10", "no2", "o3"]  # une concentration négative est impossible (temp peut l'être)
# Seuil de dérive : base simulée pm25 5-35 (moyenne ~20), --drift ajoute +40 (moyenne ~60) → 45.
DRIFT_MEAN_PM25 = 45.0
OUTLIER_SCORE = -0.6  # score IsolationForest d'un point isolé (information seulement)

# --- Assets -----------------------------------------------------------------------------------
# Le trigger tourne dans le triggerer (centre) : il liste air/inbox/ et émet un événement par fichier.
AIR_INBOX = Asset(
    name="air_inbox",
    uri="s3://datalake/air/inbox",
    watchers=[AssetWatcher(name="inbox", trigger=S3NewObjectTrigger(prefix=INBOX_PREFIX))],
)
AIR_MEASUREMENTS = Asset(name="air_measurements", uri="s3://datalake/air/measurements")
AIR_QUARANTINE = Asset(name="air_quarantine", uri="s3://datalake/air/quarantine")
AIR_HOURLY = Asset(name="air_hourly", uri="s3://datalake/air/hourly")  # partitionné (heure)
AIR_STATIONS = AssetAlias("air_stations")  # un asset par station, créé à l'exécution

TEAM_DEFAULT_ARGS: dict[str, Any] = {"executor": "EdgeExecutor", "queue": "team-iot", "owner": "team-iot"}
TEAM_TAGS = ["team-iot", "edge", "air"]

UI_BASE_URL = "http://airflow-edge.localhost"
MAIL_FROM = "airflow@airflow-edge.local"
MAIL_TO = "team-iot@example.org"


# --- S3 ---------------------------------------------------------------------------------------
def bucket() -> str:
    """Bucket du datalake (variable `datalake_bucket`, défaut `datalake`)."""
    return Variable.get("datalake_bucket", default=DEFAULT_BUCKET)


def s3_path(rel: str) -> ObjectStoragePath:
    """`ObjectStoragePath` vers `s3://datalake/<rel>` avec la connexion `minio_s3` (s3fs sur le worker)."""
    from airflow.sdk import ObjectStoragePath

    return ObjectStoragePath(f"s3://{CONN_ID}@{bucket()}/{rel}")


def s3_client() -> Any:
    """Client boto3 vers MinIO depuis la connexion `minio_s3` (comme `_s3_client()` de team-data)."""
    import boto3  # import local : règle D3

    conn = Connection.get(CONN_ID)
    extra = conn.extra_dejson
    return boto3.client(
        "s3",
        endpoint_url=extra["endpoint_url"],
        region_name=extra.get("region_name", "us-east-1"),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )


def list_keys(prefix: str, suffix: str = "") -> list[str]:
    """Clés sous `prefix` (triées), filtrées par suffixe."""
    pages = s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket(), Prefix=prefix)
    return sorted(o["Key"] for p in pages for o in p.get("Contents", []) if o["Key"].endswith(suffix))


def move_object(src_key: str, dst_key: str) -> None:
    """Copie puis suppression (S3 n'a pas de « rename » atomique)."""
    client, b = s3_client(), bucket()
    client.copy_object(Bucket=b, Key=dst_key, CopySource={"Bucket": b, "Key": src_key})
    client.delete_object(Bucket=b, Key=src_key)
    print(f"déplacé s3://{b}/{src_key} -> {dst_key}")


def delete_prefix(prefix: str) -> int:
    """Supprime tous les objets sous `prefix` ; renvoie le nombre d'objets supprimés."""
    keys = list_keys(prefix)
    client, b = s3_client(), bucket()
    for i in range(0, len(keys), 1000):
        client.delete_objects(Bucket=b, Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]})
    return len(keys)


def put_json(key: str, payload: dict[str, Any]) -> None:
    """Écrit un petit objet JSON (boto3, sans s3fs : utilisable aussi au centre)."""
    body = json.dumps(payload, indent=2, default=str, ensure_ascii=False).encode()
    s3_client().put_object(Bucket=bucket(), Key=key, Body=body, ContentType="application/json")
    print(f"s3://{bucket()}/{key} écrit")


# --- Clés et fichiers -------------------------------------------------------------------------
def station_of(key: str) -> str:
    """`air/inbox/<station>/<AAAAMMJJTHHMM>.csv` → `<station>` (aussi sous air/processed/)."""
    return key.rstrip("/").split("/")[-2]


def stamp_of(key: str) -> str:
    """`.../<AAAAMMJJTHHMM>.csv` → `AAAAMMJJTHHMM`."""
    return key.rsplit("/", 1)[-1].split(".", 1)[0]


def measurement_key(station: str, stamp: str) -> str:
    """`air/measurements/ds=<AAAA-MM-JJ>/<station>-<HHMM>.parquet` (contrat docs/09)."""
    ds = f"{stamp[0:4]}-{stamp[4:6]}-{stamp[6:8]}"
    return f"{MEASUREMENTS_PREFIX}ds={ds}/{station}-{stamp[9:13]}.parquet"


def read_csv_rows(key: str) -> list[dict[str, Any]]:
    """Lit un CSV `ts,station,lat,lon,pm25,...` via ObjectStoragePath ; valeurs numériques en float."""
    import csv

    with s3_path(key).open("r") as f:
        rows = list(csv.DictReader(f))
    out = []
    for r in rows:
        row: dict[str, Any] = {"ts": r["ts"], "station": r["station"]}
        for col in NUMERIC_COLUMNS:
            row[col] = float(r[col]) if r.get(col) not in (None, "") else None
        out.append(row)
    return out


def write_parquet(rows: list[dict[str, Any]], key: str) -> None:
    """Écrit des lignes (dicts) en parquet sur MinIO (pyarrow, via ObjectStoragePath)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        [("ts", pa.string()), ("station", pa.string())] + [(c, pa.float64()) for c in NUMERIC_COLUMNS]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    with s3_path(key).open("wb") as f:
        pq.write_table(table, f)
    print(f"s3://{bucket()}/{key} : {table.num_rows} lignes")


def read_parquet_rows(key: str) -> list[dict[str, Any]]:
    """Relit un parquet de mesures en liste de dicts."""
    import pyarrow.parquet as pq

    with s3_path(key).open("rb") as f:
        return pq.read_table(f).to_pylist()


def drift_detected(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Dérive d'un capteur, déterministe : moyenne `pm25` > 45 (`DRIFT_MEAN_PM25`).

    `IsolationForest` (scikit-learn, `random_state=0`) ne sert qu'à **informer** : part des lignes
    isolées (score < -0.6) dans le log et le résultat. Il ne décide pas : un décalage de toute la
    station forme un groupe, pas des points isolés, et `contamination="auto"` isole 30 à 70 % de
    lignes saines sur ces petits fichiers.
    """
    import numpy as np  # import local : règle D3

    if not rows:
        return {"drift": False, "reason": "aucune ligne", "mean_pm25": None, "outlier_ratio": 0.0}
    pm25 = np.array([r["pm25"] for r in rows], dtype=float)
    mean_pm25 = round(float(pm25.mean()), 2)
    ratio = None
    if len(rows) >= 10:
        from sklearn.ensemble import IsolationForest  # import local : règle D3

        x = np.array([[r[c] for c in POLLUTANTS] for r in rows], dtype=float)
        scores = IsolationForest(random_state=0).fit(x).score_samples(x)
        ratio = round(float((scores < OUTLIER_SCORE).mean()), 3)
    drift = mean_pm25 > DRIFT_MEAN_PM25
    reason = f"moyenne pm25 {mean_pm25} {'>' if drift else '<='} {DRIFT_MEAN_PM25}"
    if ratio is not None:
        reason += f" ; IsolationForest : {ratio:.0%} de lignes isolées (info)"
    return {"drift": drift, "reason": reason, "mean_pm25": mean_pm25, "outlier_ratio": ratio}


# --- Statistiques horaires (partitions, étape 8) ---------------------------------------------
def hourly_stats(partition_key: str, hour_start: Any) -> dict[str, Any]:
    """
    Moyennes par station des mesures dont `ts` tombe dans `[hour_start, hour_start + 1 h[` ;
    écrit `air/hourly/hour=<partition_key>/stats.parquet` **même vide** (la partition doit exister :
    le bulletin et son backfill s'appuient dessus).
    """
    import pendulum
    import pyarrow as pa
    import pyarrow.parquet as pq

    start = pendulum.instance(hour_start).in_timezone("UTC").start_of("hour")
    end = start.add(hours=1)
    sums: dict[str, dict[str, float]] = {}
    keys = list_keys(f"{MEASUREMENTS_PREFIX}ds={start.to_date_string()}/", ".parquet")
    for key in keys:
        for r in read_parquet_rows(key):
            ts = pendulum.parse(r["ts"]).in_timezone("UTC")
            if not (start <= ts < end):
                continue
            acc = sums.setdefault(r["station"], {"n": 0.0, **{c: 0.0 for c in POLLUTANTS}})
            acc["n"] += 1
            for c in POLLUTANTS:
                acc[c] += r[c] or 0.0
    stats = [
        {
            "hour": start.isoformat(),
            "station": st,
            "rows": int(a["n"]),
            **{f"mean_{c}": round(a[c] / a["n"], 2) for c in POLLUTANTS},
        }
        for st, a in sorted(sums.items())
    ]
    schema = pa.schema(
        [("hour", pa.string()), ("station", pa.string()), ("rows", pa.int64())]
        + [(f"mean_{c}", pa.float64()) for c in POLLUTANTS]
    )
    out_key = f"{HOURLY_PREFIX}hour={partition_key}/stats.parquet"
    with s3_path(out_key).open("wb") as f:
        pq.write_table(pa.Table.from_pylist(stats, schema=schema), f)
    print(f"s3://{bucket()}/{out_key} : {len(stats)} stations, {len(keys)} fichiers lus, [{start} ; {end}[")
    return {"partition_key": partition_key, "stations": len(stats), "files": len(keys), "key": out_key}


# --- Notifications ----------------------------------------------------------------------------
class MinioAlertNotifier(BaseNotifier):
    """
    Notifier maison : écrit `air/alerts/<dag_id>-<run_id>.json` dans MinIO.

    Repli quand Mailpit n'est pas déployé, et trace persistante en plus du mail. En callback de DAG
    il s'exécute côté **dag-processor** (centre : provider Amazon, donc boto3 présent) ; en callback
    de tâche, sur le worker.
    """

    template_fields = ()

    def notify(self, context: Any) -> None:
        dag_run = context.get("dag_run")
        ti = context.get("ti") or context.get("task_instance")
        dag_id = getattr(dag_run, "dag_id", None) or getattr(context.get("dag"), "dag_id", "unknown")
        run_id = getattr(dag_run, "run_id", None) or context.get("run_id", "unknown")
        payload = {
            "dag_id": dag_id,
            "run_id": run_id,
            "state": str(getattr(dag_run, "state", "")),
            "task_id": getattr(ti, "task_id", None),
            "reason": str(context.get("reason") or context.get("exception") or ""),
        }
        put_json(f"{ALERTS_PREFIX}{dag_id}-{run_id}.json", payload)


class HitlLinkSmtpNotifier(SmtpNotifier):
    """`SmtpNotifier` qui ajoute au corps le lien UI de la tâche HITL (`generate_link_to_ui_from_context`)."""

    def notify(self, context: Any) -> None:
        link = HITLOperator.generate_link_to_ui_from_context(context=context, base_url=UI_BASE_URL)
        self.html_content = f'{self.html_content or ""}<p><a href="{link}">Répondre dans Airflow</a></p>'
        try:
            super().notify(context)
        except Exception as exc:  # mail = confort : son échec ne doit pas faire échouer la tâche HITL
            self.log.warning("mail HITL non envoyé (%r) ; lien : %s", exc, link)


class AirHITLBranchOperator(HITLBranchOperator):
    """
    `HITLBranchOperator` qui publie aussi la réponse complète en XCom (clé `hitl_response`).

    Vérifié dans le provider standard : la valeur de retour (XCom `return_value`) d'un
    `HITLBranchOperator` est la **liste des task_ids suivies** (`do_branch`), pas le dict
    `chosen_options`/`params_input` des autres opérateurs HITL. Or `apply_calibration` a besoin de
    `params_input["offset"]`. NB : en cas d'erreur ou de timeout, `event` porte les `defaults`.
    """

    def execute_complete(self, context: Any, event: dict[str, Any]) -> Any:
        # Push AVANT super() : le branchement lève `DownstreamTasksSkipped` (Task SDK) dès qu'il y a
        # des tâches à sauter, et rien de ce qui suit ne s'exécuterait (vérifié en dags test).
        chosen = event.get("chosen_options") or []
        branches = [self.options_mapping.get(o, o) for o in chosen]
        context["ti"].xcom_push(
            key="hitl_response",
            value={
                "chosen_options": event.get("chosen_options"),
                "params_input": event.get("params_input") or {},
                "responded_at": str(event.get("responded_at")),
                "responded_by_user": event.get("responded_by_user"),
                "branches": branches,
            },
        )
        return super().execute_complete(context=context, event=event)
