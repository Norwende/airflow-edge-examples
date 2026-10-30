"""Tests d'intégrité des DAGs team-data (à lancer dans l'image worker, cf. README)."""

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
V2 = ROOT / "versions" / "v2"
EXPECTED = {"data_ingest_orders", "data_orders_daily", "demo_shorthand"}
ASSET_DAG = "demo_shorthand"  # @asset : pas d'executor/queue possible (limite 3.3.2)
DAG_FILES = (
    sorted(DAGS.glob("*.py")) + sorted(V2.glob("*.py")) + sorted((ROOT / "versions" / "v1").glob("*.py"))
)


def _dagbag(folder: Path):
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(folder))


@pytest.fixture(scope="module")
def dagbag():
    return _dagbag(DAGS)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dag_ids(dagbag):
    assert set(dagbag.dag_ids) == EXPECTED


def test_routing_edge(dagbag):
    """Toutes les tâches vont sur EdgeExecutor / queue team-data, sauf la tâche @asset."""
    for dag_id, dag in dagbag.dags.items():
        assert "team-data" in dag.tags
        for t in dag.tasks:
            if dag_id == ASSET_DAG:
                # Tâche @asset : executor non défini -> executor par défaut (LocalExecutor, au centre)
                assert t.executor is None, t.task_id
            else:
                assert (t.executor, t.queue) == ("EdgeExecutor", "team-data"), f"{dag_id}.{t.task_id}"


def test_ingest_structure(dagbag):
    dag = dagbag.dags["data_ingest_orders"]
    # dynamic task mapping : generate_orders est un MappedOperator
    assert type(dag.task_dict["generate_orders"]).__name__.endswith("MappedOperator")
    assert [o.name for o in dag.task_dict["publish"].outlets] == ["orders_raw"]


def test_daily_is_asset_scheduled(dagbag):
    dag = dagbag.dags["data_orders_daily"]
    assert type(dag.timetable).__name__ == "AssetTriggeredTimetable"
    assert "orders_raw" in repr(dag.timetable.asset_condition)
    agg = dag.task_dict["aggregate_daily"]
    assert [o.name for o in agg.outlets] == ["orders_daily"]
    assert dag.task_dict["log_summary"].upstream_task_ids == {"aggregate_daily"}


def test_v2_adds_validate():
    bag = _dagbag(V2)
    assert bag.import_errors == {}
    dag = bag.dags["data_ingest_orders"]
    assert dag.task_dict["validate"].downstream_task_ids == {"publish"}
    assert dag.task_dict["validate"].upstream_task_ids == {"generate_orders"}


# Règle D3 : chaque fichier doit se parser sans polars ni boto3 (comme le dag-processor central).
_BLOCKED_PARSE = """
import sys
for mod in ("polars", "boto3", "botocore"):
    sys.modules[mod] = None  # tout `import polars` lève désormais ImportError
from airflow.dag_processing.dagbag import DagBag
bag = DagBag(dag_folder=sys.argv[1])
assert not bag.import_errors, bag.import_errors
assert bag.dag_ids, "aucun DAG"
assert "polars" not in [m for m, v in sys.modules.items() if v is not None]
print("OK", sorted(bag.dag_ids))
"""


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda p: str(p.relative_to(ROOT)))
def test_parse_without_team_libs(path: Path):
    res = subprocess.run([sys.executable, "-c", _BLOCKED_PARSE, str(path)], capture_output=True, text=True)
    assert res.returncode == 0, res.stdout + res.stderr
