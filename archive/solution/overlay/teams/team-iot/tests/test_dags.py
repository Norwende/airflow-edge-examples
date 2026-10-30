"""Tests d'intégrité des DAGs team-iot (à lancer dans l'image worker, cf. README).

Génériques sur dags/*.py : tout nouveau fichier DAG est couvert sans modifier ce test.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

# Avant tout import d'airflow : le DagBag refuse executor="EdgeExecutor" si cet executor
# n'est pas configuré (même valeur qu'au centre). Pas de base de données nécessaire.
os.environ.setdefault(
    "AIRFLOW__CORE__EXECUTOR", "LocalExecutor,airflow.providers.edge3.executors.EdgeExecutor"
)
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

ROOT = Path(__file__).resolve().parents[1]
DAGS = ROOT / "dags"
DAG_FILES = sorted(DAGS.glob("*.py"))
# Libs de l'équipe absentes du centre (règle D3) : interdites au niveau module.
TEAM_LIBS = ("sklearn", "numpy", "boto3", "botocore", "s3fs")
# Le dossier du bundle est dans sys.path au centre comme sur le worker : un DAG peut importer
# un module voisin du bundle (ex. iot_triggers.py).
sys.path.insert(0, str(DAGS))


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert {"iot_where_am_i", "iot_slow_task"} <= set(dagbag.dag_ids)


def test_routing_edge(dagbag):
    """Toutes les tâches de tous les DAGs du bundle vont sur EdgeExecutor / queue team-iot."""
    for dag_id, dag in dagbag.dags.items():
        assert "team-iot" in dag.tags, dag_id
        for t in dag.tasks:
            assert (t.executor, t.queue) == ("EdgeExecutor", "team-iot"), f"{dag_id}.{t.task_id}"


def test_slow_task_params(dagbag):
    dag = dagbag.dags["iot_slow_task"]
    assert dag.params["seconds"] == 90
    (task,) = dag.tasks
    assert task.retries == 1


# Règle D3 : chaque fichier doit se parser sans les libs de l'équipe (comme le dag-processor central).
_BLOCKED_PARSE = f"""
import sys
for mod in {TEAM_LIBS!r}:
    sys.modules[mod] = None  # tout `import sklearn` lève désormais ImportError
from airflow.dag_processing.dagbag import DagBag
bag = DagBag(dag_folder=sys.argv[1])
assert not bag.import_errors, bag.import_errors
print("OK", sorted(bag.dag_ids))
"""


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda p: p.name)
def test_parse_without_team_libs(path: Path):
    env = {**os.environ, "PYTHONPATH": str(DAGS)}
    res = subprocess.run(
        [sys.executable, "-c", _BLOCKED_PARSE, str(path)], capture_output=True, text=True, env=env
    )
    assert res.returncode == 0, res.stdout + res.stderr
