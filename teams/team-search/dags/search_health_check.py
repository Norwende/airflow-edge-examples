"""
### search_health_check — santé du cluster Elasticsearch vue depuis l'edge worker team-search

Toutes les 30 min (en pause à la création). Appelle `/_cluster/health` et `/` avec le client
`elasticsearch`, et logge version ES, nœud, hostname et version de Python du worker : prouve
**où** la tâche tourne et **quelles libs** y sont disponibles (absentes du central).
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from search_common import TEAM_DEFAULT_ARGS, TEAM_TAGS, es_client


@dag(
    dag_id="search_health_check",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=TEAM_DEFAULT_ARGS,
    tags=TEAM_TAGS,
    doc_md=__doc__,
)
def search_health_check():
    @task
    def cluster_health() -> dict[str, str]:
        """Interroge Elasticsearch et logge le contexte d'exécution du worker."""
        import socket
        import sys

        import elasticsearch

        es = es_client()
        info, health = es.info(), es.cluster.health()
        result = {
            "worker_host": socket.gethostname(),
            "python": sys.version.split()[0],
            "client": elasticsearch.__versionstr__,
            "es_version": info["version"]["number"],
            "node": info["name"],
            "cluster": health["cluster_name"],
            "status": health["status"],
        }
        for k, v in result.items():
            print(f"{k:>12} : {v}")
        return result

    cluster_health()


search_health_check()
