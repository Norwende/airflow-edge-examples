"""
### iot_manual_reconcile — met à jour l'asset `air_manual_reconcile` (team-iot, docs/09 étape 7)

DAG manuel minuscule : sa tâche a pour outlet `air_manual_reconcile`, référencé **par nom**
(`Asset.ref`) dans le schedule de `iot_reconcile` → déclencher ce DAG suffit à lancer la réconciliation.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from iot_common import TEAM_DEFAULT_ARGS, TEAM_TAGS

AIR_MANUAL_RECONCILE = Asset(name="air_manual_reconcile", uri="s3://datalake/air/manual_reconcile")


@dag(
    dag_id="iot_manual_reconcile",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    default_args=TEAM_DEFAULT_ARGS,
    tags=[*TEAM_TAGS, "assets"],
    doc_md=__doc__,
)
def iot_manual_reconcile():
    @task(outlets=[AIR_MANUAL_RECONCILE])
    def request_reconcile(**context) -> str:
        """Émet un événement sur `air_manual_reconcile` (extra : qui et quand)."""
        context["outlet_events"][AIR_MANUAL_RECONCILE].extra = {"requested_by_run": context["run_id"]}
        return context["run_id"]

    request_reconcile()


iot_manual_reconcile()
