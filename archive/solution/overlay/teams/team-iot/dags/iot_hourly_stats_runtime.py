"""
### iot_hourly_stats_runtime — partitions décidées **à l'exécution** (team-iot, docs/09 étape 8)

Variante de `iot_hourly_stats` : `schedule=PartitionedAtRuntime()` ne planifie jamais rien
(déclenchement manuel) ; c'est la tâche qui choisit les clés et les attache à l'événement
`air_hourly` par `outlet_events[AIR_HOURLY].add_partitions([...])` (clés en double fusionnées ;
une seule clé → `dag_run.partition_key` est renseigné). Sans clé ajoutée, aucun événement partitionné.

Même asset `air_hourly` que le producteur cron : la doc n'interdit pas deux producteurs de timetables
différents sur un asset (elle dit seulement qu'**une** timetable est soit pré-déterminée, soit runtime)
— **à vérifier** en live. Les clés suivent le format du cron (`%Y-%m-%dT%H:%M:%S`) pour rester
lisibles par les mappers du consommateur (`StartOfDayMapper`, `RollupMapper`). Utile pour combler
des heures manquantes : la tâche (re)calcule aussi les fichiers `air/hourly/hour=<clé>/stats.parquet`.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Param, PartitionedAtRuntime, dag, task

from iot_common import AIR_HOURLY, TEAM_DEFAULT_ARGS, TEAM_TAGS

KEY_FORMAT = "%Y-%m-%dT%H:%M:%S"


@dag(
    dag_id="iot_hourly_stats_runtime",
    schedule=PartitionedAtRuntime(),
    start_date=pendulum.datetime(2026, 10, 15, tz="UTC"),
    catchup=False,
    default_args=TEAM_DEFAULT_ARGS,
    tags=[*TEAM_TAGS, "assets", "partitions"],
    doc_md=__doc__,
    params={
        "hours": Param(
            [],
            type="array",
            items={"type": "string"},
            title="Heures (clés de partition)",
            description_md="Ex. `2026-10-17T08:00:00`. Vide = l'heure écoulée.",
        )
    },
)
def iot_hourly_stats_runtime():
    @task(outlets=[AIR_HOURLY])
    def emit_partitions(**context) -> list[dict]:
        """Calcule les heures demandées et attache leurs clés à l'événement `air_hourly`."""
        from iot_common import hourly_stats

        hours = list(context["params"]["hours"]) or [
            pendulum.now("UTC").start_of("hour").subtract(hours=1).strftime(KEY_FORMAT)
        ]
        keys = [pendulum.parse(h).in_timezone("UTC").start_of("hour").strftime(KEY_FORMAT) for h in hours]
        results = [hourly_stats(k, pendulum.parse(k)) for k in keys]
        context["outlet_events"][AIR_HOURLY].add_partitions(keys)
        print(f"partitions émises : {keys}")
        return results

    emit_partitions()


iot_hourly_stats_runtime()
