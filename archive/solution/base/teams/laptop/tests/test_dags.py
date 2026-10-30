"""Tests d'import des DAGs du worker laptop (exécutés dans l'image du worker)."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"
EXPECTED_DAG_IDS = {
    "laptop_where_am_i",
    "laptop_duckdb_analytics",
}
# matplotlib : lib lourde, jamais importée au parsing
TEAM_LIBS = ("duckdb", "boto3", "botocore", "matplotlib")

os.environ.setdefault(
    "AIRFLOW__CORE__EXECUTOR", "LocalExecutor,airflow.providers.edge3.executors.EdgeExecutor"
)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import BundleDagBag

    # BundleDagBag (comme le dag-processor) : ajoute le bundle au sys.path (imports entre fichiers)
    return BundleDagBag(dag_folder=str(DAGS_DIR), bundle_path=DAGS_DIR)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dag_ids(dagbag):
    assert set(dagbag.dag_ids) == EXPECTED_DAG_IDS


def test_tasks_routed_to_laptop_edge(dagbag):
    for dag in dagbag.dags.values():
        assert "laptop" in dag.tags and "edge" in dag.tags
        for t in dag.tasks:
            assert t.executor == "EdgeExecutor", f"{dag.dag_id}.{t.task_id}"
            assert t.queue == "laptop", f"{dag.dag_id}.{t.task_id}"
            assert t.owner == "laptop", f"{dag.dag_id}.{t.task_id}"


def test_analytics_scheduled_on_orders_daily(dagbag):
    dag = dagbag.dags["laptop_duckdb_analytics"]
    assert "orders_daily" in repr(dag.timetable.asset_condition)


@pytest.mark.parametrize("path", sorted(DAGS_DIR.glob("*.py")), ids=lambda p: p.name)
def test_parseable_without_team_libs(path):
    """Contrat D3 : le fichier doit s'importer au centre, où duckdb/boto3 sont absents."""
    code = textwrap.dedent(
        f"""
        import sys, runpy
        for name in {TEAM_LIBS!r}:
            sys.modules[name] = None  # tout `import duckdb` lèvera ImportError
        sys.path.insert(0, {str(DAGS_DIR)!r})  # bundle dans le sys.path, comme au centre
        runpy.run_path({str(path)!r}, run_name="dag_under_test")
        """
    )
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=os.environ.copy())
    assert res.returncode == 0, res.stderr[-3000:]
