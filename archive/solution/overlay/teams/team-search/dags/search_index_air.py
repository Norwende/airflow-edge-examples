"""
### search_index_air — indexe les mesures de qualité de l'air dans Elasticsearch (équipe team-search)

Étape 9 de l'exercice `docs/09-exercice-qualite-air.md`.

- **Dépendance inter-équipes par nom** : planifié sur `Asset.ref(name="air_measurements")` (produit par
  team-iot). Aucun code de team-iot importé : seul le nom compte.
- **Deux sensors avant l'indexation**, à comparer :
  - `wait_es` : `HttpSensor(deferrable=True)` sur `_cluster/health` (connexion `es_http`). Le premier
    `poke` a lieu **sur l'edge worker** ; s'il échoue, la tâche passe `deferred`, **libère son slot** et le
    trigger (`HttpSensorTrigger`) tourne **au centre** (triggerer) : c'est lui qui doit joindre ES. À la
    reprise, un nouveau job edge est mis en file sur la queue `team-search`.
    Le statut attendu est passé à ES (`wait_for_status`, param `es_wait_for_status`) : ES répond 408 tant
    qu'il n'est pas atteint. Mettre `green` sur ce cluster mono-nœud (toujours `yellow`) pour **observer**
    l'état `deferred` jusqu'au timeout.
    Pas de `response_check` : dans provider http 6.1.0, `HttpSensor.execute` ignore `deferrable` dès
    qu'un `response_check` est fourni (il poke alors dans le worker).
  - `wait_es_poke` : `@task.sensor(mode="reschedule")` + `PokeReturnValue` : chaque poke est un job edge
    court ; entre deux pokes la tâche est `up_for_reschedule` (slot libéré aussi, mais pas de triggerer).
- **inlet_events** : `fetch_measurements` lit `extra["keys"]` du dernier événement `air_measurements`
  (`inlet_events[ref][-1]`). On logue aussi `triggering_asset_events` (observation de l'exercice : vide
  pour un DAG planifié par `Asset.ref` en 3.3.2, cf. docs/05 §1). Sans événement (run manuel) : tous les
  parquets du dernier préfixe `air/measurements/ds=<jour>/`, ou param `ds` pour forcer le jour ;
  rien à lire → tâche `skipped`.
- **Asset en sortie** : `air_index` (`es://air`), `extra={"indexed": n}`. `_id` = `<station>-<ts>` :
  rejouer un run écrase au lieu de dupliquer.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sdk import Asset, Param, PokeReturnValue, dag, task

from search_common import (
    TEAM_DEFAULT_ARGS,
    bulk_index_with_ids,
    ensure_named_index,
    list_keys,
    read_parquet_records,
)

AIR_MEASUREMENTS_REF = Asset.ref(name="air_measurements")
AIR_INDEX = Asset(name="air_index", uri="es://air")

AIR_INDEX_NAME = "air"
MEASUREMENTS_PREFIX = "air/measurements/"
MEASURES = ("pm25", "pm10", "no2", "o3", "temp")
AIR_MAPPING: dict[str, Any] = {
    "properties": {
        "station": {"type": "keyword"},
        "ts": {"type": "date"},
        "location": {"type": "geo_point"},
        **{m: {"type": "float"} for m in MEASURES},
        "source_key": {"type": "keyword"},
    }
}


def _keys_without_event(ds: str | None) -> list[str]:
    """Repli : parquets de `ds=<ds>/` (param) ou du dernier préfixe `ds=` présent sous air/measurements/."""
    objects = list_keys(MEASUREMENTS_PREFIX)
    if ds:
        prefix = f"{MEASUREMENTS_PREFIX}ds={ds}/"
        return sorted(o["Key"] for o in objects if o["Key"].startswith(prefix))
    days = sorted({o["Key"].split("/")[2] for o in objects if o["Key"].split("/")[2].startswith("ds=")})
    if not days:
        return []
    return sorted(o["Key"] for o in objects if o["Key"].split("/")[2] == days[-1])


def _to_doc(row: dict[str, Any], key: str) -> dict[str, Any] | None:
    """Ligne parquet → document ES (ts ISO UTC, location geo_point, `_id` déterministe)."""
    import math

    import pandas as pd

    ts = pd.to_datetime(row.get("ts"), utc=True)  # accepte chaîne ISO ou timestamp
    if pd.isna(ts):
        return None
    ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    station = str(row["station"])

    def num(v: Any) -> float | None:
        return None if v is None or (isinstance(v, float) and math.isnan(v)) else float(v)

    doc: dict[str, Any] = {
        "_id": f"{station}-{ts_iso}",
        "station": station,
        "ts": ts_iso,
        "source_key": key,
        **{m: num(row.get(m)) for m in MEASURES},
    }
    lat, lon = num(row.get("lat")), num(row.get("lon"))
    if lat is not None and lon is not None:
        doc["location"] = {"lat": lat, "lon": lon}
    return doc


@dag(
    dag_id="search_index_air",
    schedule=[AIR_MEASUREMENTS_REF],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=TEAM_DEFAULT_ARGS,
    tags=["team-search", "edge", "assets", "air"],
    params={
        "ds": Param(
            None,
            type=["null", "string"],
            description="Jour AAAA-MM-JJ (air/measurements/ds=<jour>/) si aucun événement. Vide = auto.",
        ),
        "es_wait_for_status": Param(
            "yellow",
            enum=["yellow", "green"],
            description="Statut ES attendu par les sensors ; `green` = attente forcée (cluster mono-nœud).",
        ),
    },
    doc_md=__doc__,
)
def search_index_air():
    # (a) Sensor deferrable : poke initial sur l'edge worker, puis trigger au centre (triggerer).
    wait_es = HttpSensor(
        task_id="wait_es",
        http_conn_id="es_http",
        endpoint="_cluster/health",
        # templaté : ES répond 408 tant que le statut n'est pas atteint (attente côté serveur : 5 s)
        request_params={"wait_for_status": "{{ params.es_wait_for_status }}", "timeout": "5s"},
        # 408 → poke() renvoie False (au lieu de lever) → defer ; 404 = valeur par défaut
        response_error_codes_allowlist=["404", "408"],
        deferrable=True,
        poke_interval=10,
        timeout=600,
        # ES injoignable (connexion refusée) : le poke ou le trigger lève → nouvelle tentative
        retries=3,
        retry_delay=timedelta(seconds=20),
    )

    # (b) Même contrôle en mode reschedule : chaque poke est un job edge court sur la queue team-search.
    @task.sensor(poke_interval=10, timeout=300, mode="reschedule")
    def wait_es_poke(**context) -> PokeReturnValue:
        """Interroge `_cluster/health` (urllib) ; terminé quand le statut attendu est atteint."""
        import json
        import urllib.request

        from airflow.sdk import Connection

        conn = Connection.get("es_http")
        host = conn.host if "://" in (conn.host or "") else f"{conn.schema or 'http'}://{conn.host}"
        url = f"{host.rstrip('/')}:{conn.port or 9200}/_cluster/health"
        wanted = context["params"]["es_wait_for_status"]
        accepted = ("green",) if wanted == "green" else ("green", "yellow")
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                status = json.load(resp).get("status")
        except OSError as exc:  # connexion refusée, timeout, HTTP 5xx
            print(f"{url} injoignable : {exc}")
            return PokeReturnValue(is_done=False)
        print(f"{url} : status={status} (attendu {accepted})")
        return PokeReturnValue(is_done=status in accepted, xcom_value=status)

    # (c) Clés des parquets : extra du dernier événement d'asset, sinon repli sur MinIO.
    @task(inlets=[AIR_MEASUREMENTS_REF])
    def fetch_measurements(**context) -> list[dict[str, Any]]:
        """Lit les parquets de mesures validées et les convertit en documents ES."""
        from airflow.sdk.exceptions import AirflowSkipException

        triggering = context.get("triggering_asset_events") or {}
        print(f"triggering_asset_events : {dict(triggering) if triggering else 'vide'}")
        events = context["inlet_events"][AIR_MEASUREMENTS_REF]
        keys: list[str] = []
        if len(events) > 0:
            last = events[-1]
            print(f"dernier événement air_measurements : {last.timestamp} extra={last.extra}")
            keys = list((last.extra or {}).get("keys") or [])
        if not keys:
            keys = _keys_without_event(context["params"].get("ds"))
            print(f"pas de clé dans un événement : repli MinIO → {len(keys)} fichier(s)")
        if not keys:
            raise AirflowSkipException(f"aucun parquet sous {MEASUREMENTS_PREFIX} : rien à indexer")

        docs = [d for key in keys for r in read_parquet_records(key) if (d := _to_doc(r, key))]
        print(f"{len(docs)} documents construits depuis {len(keys)} fichier(s)")
        return docs

    # (d) Indexation idempotente + événement d'asset air_index.
    @task(outlets=[AIR_INDEX])
    def index_air(docs: list[dict[str, Any]], **context) -> int:
        """Crée l'index `air` (mapping geo_point) si besoin puis indexe en bulk."""
        ensure_named_index(AIR_INDEX_NAME, AIR_MAPPING)
        n = bulk_index_with_ids(AIR_INDEX_NAME, docs) if docs else 0
        context["outlet_events"][AIR_INDEX].extra = {"indexed": n}
        print(f"air_index publié : {n} documents")
        return n

    indexed = index_air(fetch_measurements())
    [wait_es, wait_es_poke()] >> indexed


search_index_air()
