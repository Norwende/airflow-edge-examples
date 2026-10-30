"""
### iot_weekly_recap — temps **ou** données : `AssetOrTimeSchedule` (team-iot, docs/09 étape 7)

Déclenché chaque **lundi à 6 h (Europe/Paris)** par `CronTriggerTimetable`, **et** à chaque mise à
jour de `air_measurements`. La tâche affiche l'`extra` du dernier événement `air_measurements`
(`inlet_events[...][-1]`), qu'il y ait ou non un événement déclencheur (run planifié par le temps).
"""

from __future__ import annotations

import pendulum
from airflow.sdk import AssetOrTimeSchedule, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

from iot_common import AIR_MEASUREMENTS, TEAM_DEFAULT_ARGS, TEAM_TAGS


@dag(
    dag_id="iot_weekly_recap",
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 6 * * 1", timezone="Europe/Paris"), assets=AIR_MEASUREMENTS
    ),
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Paris"),
    catchup=False,
    default_args=TEAM_DEFAULT_ARGS,
    tags=[*TEAM_TAGS, "assets"],
    doc_md=__doc__,
)
def iot_weekly_recap():
    @task(inlets=[AIR_MEASUREMENTS])
    def latest_measurements(**context) -> dict | None:
        """Dernier événement `air_measurements` (et type de run : planifié ou déclenché par asset)."""
        dag_run = context["dag_run"]
        print(f"run_type={dag_run.run_type} triggering={dict(context.get('triggering_asset_events') or {})}")
        events = context["inlet_events"][AIR_MEASUREMENTS]
        try:
            last = events[-1]
        except IndexError:
            print("aucun événement air_measurements pour l'instant")
            return None
        print(f"dernier air_measurements @ {last.timestamp} : {last.extra}")
        return dict(last.extra or {})

    latest_measurements()


iot_weekly_recap()
