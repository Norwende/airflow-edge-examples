"""
### demo_shorthand — le raccourci `@asset` (et sa limite avec l'Edge Executor)

**Ce que montre ce fichier**

- `@asset` crée en une fois **un asset, un DAG et une tâche** (dag_id = nom de l'asset) :
  la fonction *est* la matérialisation de l'asset. Avant : un DAG + un opérateur + `outlets`.
- **Limite (Airflow 3.3.2)** : `@asset` n'accepte ni `executor` ni `queue` (ni `default_args`).
  Sa tâche tourne donc sur l'**executor par défaut = LocalExecutor, dans le scheduler central**,
  et **pas** sur le worker team-data. Le corps n'utilise que la bibliothèque standard
  (pas de polars/boto3 au centre) et affiche hostname + version de Python pour prouver
  où il s'est exécuté (pod scheduler, Python 3.13).
- C'est pour cette raison que les autres DAGs de l'équipe utilisent `@task(outlets=[...])`
  avec `default_args={"executor": "EdgeExecutor", "queue": "team-data"}`.
"""

from __future__ import annotations

import socket
import sys

from airflow.sdk import asset


@asset(
    schedule=None,
    uri="s3://datalake/demo/asset_shorthand",
    name="demo_shorthand",
    tags=["team-data", "edge", "assets"],
    description="Démo du raccourci @asset : s'exécute au centre (LocalExecutor), pas sur le worker edge.",
)
def demo_shorthand() -> dict:
    """Matérialise l'asset `demo_shorthand` (bibliothèque standard uniquement)."""
    where = {"hostname": socket.gethostname(), "python": sys.version}
    print(f"@asset exécuté sur {where['hostname']} avec Python {where['python']}")
    return where
