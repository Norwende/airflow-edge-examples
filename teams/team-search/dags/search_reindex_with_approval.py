"""
### search_reindex_with_approval — recréation de l'index `orders` après validation humaine

Démontre le **Human-in-the-loop** (Airflow 3.1+, `ApprovalOperator` du provider standard).

**Donner l'approbation dans l'UI** : déclencher le DAG, puis menu **Browse → Required Actions**
(ou onglet *Required Actions* du run / de la tâche `approve_reindex`) → **Approve** ou **Reject**.
Équivalent API : `PATCH /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/approve_reindex/-1/hitlDetails`
avec `{"chosen_options": ["Approve"], "params_input": {}}`.

- En 3.3, la tâche HITL passe à l'état **awaiting_input** : ni trigger ni triggerer, **aucun slot
  de worker occupé** pendant l'attente. La réponse remet la tâche en *scheduled* et le scheduler
  relance `execute_complete` sur **son executor** (ici EdgeExecutor, queue team-search).
- **Approve** → `recreate_index` (delete + create avec mapping) → `reindex_latest` (dernier parquet).
- **Reject** → l'aval direct est *skipped* ; `cancelled` (trigger_rule `all_skipped`) logge « annulé ».
- Sans réponse sous 4 h → option par défaut **Reject**.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import TriggerRule, dag, task

from search_common import (
    TEAM_DEFAULT_ARGS,
    TEAM_TAGS,
    bulk_index,
    ensure_index,
    latest_daily_key,
    read_daily_docs,
)


@dag(
    dag_id="search_reindex_with_approval",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=TEAM_DEFAULT_ARGS,
    tags=[*TEAM_TAGS, "hitl"],
    doc_md=__doc__,
)
def search_reindex_with_approval():
    approve = ApprovalOperator(
        task_id="approve_reindex",
        subject="Recréer l'index orders ?",
        body=(
            "L'index Elasticsearch **orders** va être **supprimé** puis recréé "
            "(mapping `region` keyword, `revenue` float, `ds` date), puis réalimenté "
            "avec le dernier parquet de `orders/daily/`. Run : `{{ run_id }}`."
        ),
        defaults="Reject",
        response_timeout=timedelta(hours=4),
    )

    @task
    def recreate_index() -> None:
        """Supprime et recrée l'index `orders` avec son mapping explicite."""
        ensure_index(recreate=True)

    @task
    def reindex_latest() -> int:
        """Réindexe le dernier parquet quotidien."""
        return bulk_index(read_daily_docs(latest_daily_key()))

    @task(trigger_rule=TriggerRule.ALL_SKIPPED)
    def cancelled(**context) -> None:
        """Branche « Reject » : ne s'exécute que si la recréation a été sautée."""
        answer = context["ti"].xcom_pull(task_ids="approve_reindex") or {}
        print(f"annulé (réponse : {answer.get('chosen_options')}, par {answer.get('responded_by_user')})")

    recreated = recreate_index()
    approve >> recreated >> reindex_latest()
    recreated >> cancelled()


search_reindex_with_approval()
