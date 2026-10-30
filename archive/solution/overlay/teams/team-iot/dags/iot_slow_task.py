"""
Tâche longue pour les exercices Edge Executor (docs/09, étape 13) : maintenance, drain,
deux workers sur la même queue, kill -9, retry.

Une seule tâche qui dort ``params.seconds`` secondes (défaut 90) en affichant le hostname
toutes les 10 s ; ``retries=1`` pour observer la reprise après la perte d'un worker.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, task


@dag(
    dag_id="iot_slow_task",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=10,  # 13.3 : plusieurs runs en parallèle répartis sur les workers
    default_args={
        "executor": "EdgeExecutor",
        "queue": "team-iot",
        "owner": "team-iot",
        "retries": 1,
        "retry_delay": timedelta(seconds=20),
    },
    params={
        "seconds": Param(90, type="integer", minimum=1, maximum=600, description="durée de la tâche (s)"),
    },
    tags=["team-iot", "edge", "exercice"],
    doc_md=__doc__,
)
def iot_slow_task():
    @task
    def sleep_a_while(params=None) -> str:
        import socket
        import time

        seconds = int(params["seconds"])
        host = socket.gethostname()
        start = time.monotonic()
        while (elapsed := time.monotonic() - start) < seconds:
            print(f"host={host} écoulé={elapsed:.0f}s / {seconds}s", flush=True)
            time.sleep(min(10, seconds - elapsed))
        print(f"host={host} terminé après {seconds}s")
        return host

    sleep_a_while()


iot_slow_task()
