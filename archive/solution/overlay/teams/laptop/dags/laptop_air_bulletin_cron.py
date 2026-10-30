"""
### laptop_air_bulletin_cron — bulletin quotidien de qualité de l'air, planifié par **cron** (07:00 UTC)

Étape 10 de l'exercice `docs/09-exercice-qualite-air.md` : DAG **temporel** pour l'exercice de
**backfill** (7 derniers jours, `max_active_runs=2`), mêmes tâches que `laptop_air_bulletin`
(module partagé `laptop_air_common.py`).

- Jour du bulletin : param `ds` s'il est fourni, sinon la date de `logical_date` (backfill : un run par
  jour), sinon `run_after` (run manuel `{"logical_date": null}`).
- `collect_day` (boto3 + DuckDB local) → `air/bulletin/<ds>.md` ; `render_chart` (`@task.virtualenv`,
  matplotlib) → `air/bulletin/<ds>.png` ; `publish_bulletin` → asset `air_bulletin`.
- Run manuel sans date : `POST /api/v2/dags/laptop_air_bulletin_cron/dagRuns`
  `{"logical_date": null, "conf": {"ds": "2026-10-10"}}`.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag

from laptop_air_common import AIR_PARAMS, AIR_TAGS, LAPTOP_DEFAULT_ARGS, build_bulletin_tasks


@dag(
    dag_id="laptop_air_bulletin_cron",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=2,
    default_args=LAPTOP_DEFAULT_ARGS,
    tags=[*AIR_TAGS, "backfill"],
    params=AIR_PARAMS,
    doc_md=__doc__,
)
def laptop_air_bulletin_cron():
    build_bulletin_tasks()


laptop_air_bulletin_cron()
