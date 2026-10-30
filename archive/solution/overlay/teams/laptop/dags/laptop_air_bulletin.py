"""
### laptop_air_bulletin — bulletin quotidien de qualité de l'air, planifié par **partition d'asset**

Étapes 8 et 10 de l'exercice `docs/09-exercice-qualite-air.md`.

- `schedule=PartitionedAssetTimetable(assets=Asset.ref(name="air_hourly"),
  default_partition_mapper=StartOfDayMapper())` : chaque événement **partitionné** de `air_hourly`
  (clé horaire `2026-10-17T10:00:00`, produit par `iot_hourly_stats` de team-iot) est ramené à la
  clé « jour » `2026-10-17`. Combien de runs pour 24 partitions horaires ? À comparer avec
  `RollupMapper(DayWindow())` (exercice, étape 8). Un événement **non** partitionné ne déclenche
  pas ce DAG.
- `collect_day` : jour = `dag_run.partition_date` (sinon param `ds`, sinon `logical_date`/`run_after`) ;
  boto3 (endpoint `MINIO_ENDPOINT_URL`) télécharge `air/hourly/hour=<jour>*` lus par DuckDB, en repli
  `air/measurements/ds=<jour>/*.parquet` ; écrit `air/bulletin/<jour>.md` (vide mais valide sans données).
- `render_chart` : `@task.virtualenv` (matplotlib + boto3, venv en cache sous `/opt/airflow/.venvs`) →
  `air/bulletin/<jour>.png`.
- `publish_bulletin` : asset `air_bulletin`, `extra={"ds", "md", "png"}`.

Variante temporelle pour le backfill : `laptop_air_bulletin_cron` (mêmes tâches, `laptop_air_common.py`).
"""

from __future__ import annotations

import pendulum
from airflow.sdk import PartitionedAssetTimetable, StartOfDayMapper, dag

from laptop_air_common import AIR_HOURLY_REF, AIR_PARAMS, AIR_TAGS, LAPTOP_DEFAULT_ARGS, build_bulletin_tasks


@dag(
    dag_id="laptop_air_bulletin",
    schedule=PartitionedAssetTimetable(assets=AIR_HOURLY_REF, default_partition_mapper=StartOfDayMapper()),
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    default_args=LAPTOP_DEFAULT_ARGS,
    tags=[*AIR_TAGS, "partitions"],
    params=AIR_PARAMS,
    doc_md=__doc__,
)
def laptop_air_bulletin():
    build_bulletin_tasks()


laptop_air_bulletin()
