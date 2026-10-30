"""
### iot_hourly_stats — statistiques horaires **partitionnées** (team-iot, docs/09 étape 8)

`schedule=CronPartitionTimetable("0 * * * *", timezone="UTC", run_offset=-1)` : un run par heure, chacun
porte une **clé de partition** (`dag_run.partition_key`, format par défaut `%Y-%m-%dT%H:%M:%S`) et sa date
(`dag_run.partition_date`, aussi `{{ partition_date | ds }}` dans les templates). L'événement émis
sur `air_hourly` porte la même clé : seuls les consommateurs **partition-aware**
(`PartitionedAssetTimetable`, ex. le bulletin du laptop) le reçoivent.

La tâche lit `air/measurements/ds=<jour>/*.parquet`, garde les lignes dont `ts` tombe dans l'heure
`[partition_date ; +1 h[` et écrit `air/hourly/hour=<partition_key>/stats.parquet` (moyennes par
station). Sans mesure dans l'heure, le fichier est écrit **vide** : la partition existe toujours
(le backfill du bulletin, étape 10, s'appuie dessus).

Vérifié avec `airflow dags test` : sans `run_offset`, la clé est l'instant du cron lui-même (run de
11:00 → clé `2026-10-17T11:00:00`) ; `run_offset=-1` fait porter au run de 09:00 l'heure écoulée 08:00.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import CronPartitionTimetable, dag, task

from iot_common import AIR_HOURLY, TEAM_DEFAULT_ARGS, TEAM_TAGS


@dag(
    dag_id="iot_hourly_stats",
    # run_offset=-1 : le run de 09:00 porte la partition 08:00 (heure [08:00 ; 09:00[ terminée).
    # Sans offset, la clé = l'instant du cron : le run de 09:00 viserait une heure pas encore écoulée.
    schedule=CronPartitionTimetable("0 * * * *", timezone="UTC", run_offset=-1),
    start_date=pendulum.datetime(2026, 10, 15, tz="UTC"),
    catchup=False,
    max_active_runs=2,
    default_args=TEAM_DEFAULT_ARGS,
    tags=[*TEAM_TAGS, "assets", "partitions"],
    doc_md=__doc__,
)
def iot_hourly_stats():
    @task(outlets=[AIR_HOURLY])
    def compute_hourly(partition_ds: str, **context) -> dict:
        """Moyennes par station pour l'heure de la partition ; écrit le fichier même vide."""
        from iot_common import hourly_stats

        dag_run = context["dag_run"]
        key, date = dag_run.partition_key, dag_run.partition_date
        print(f"partition_key={key!r} partition_date={date!r} (template partition_date|ds = {partition_ds})")
        if key is None or date is None:  # ex. run manuel sans partition
            date = pendulum.instance(dag_run.run_after).start_of("hour").subtract(hours=1)
            key = date.strftime("%Y-%m-%dT%H:%M:%S")
            print(f"pas de partition sur ce run : heure précédente {key}")
        return hourly_stats(key, date)

    compute_hourly(partition_ds="{{ partition_date | ds }}")


iot_hourly_stats()
