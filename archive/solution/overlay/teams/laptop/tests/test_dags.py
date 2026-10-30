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
    "laptop_air_bulletin",
    "laptop_air_bulletin_cron",
}
# matplotlib : uniquement dans le venv de render_chart, jamais au parsing
TEAM_LIBS = ("duckdb", "boto3", "botocore", "matplotlib")

os.environ.setdefault(
    "AIRFLOW__CORE__EXECUTOR", "LocalExecutor,airflow.providers.edge3.executors.EdgeExecutor"
)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import BundleDagBag

    # BundleDagBag (comme le dag-processor) : ajoute le bundle au sys.path → `import laptop_air_common`
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


def test_air_bulletin_partitioned_schedule(dagbag):
    """Planification par partition : air_hourly (par nom) ramené au jour par StartOfDayMapper."""
    from airflow.sdk import PartitionedAssetTimetable, StartOfDayMapper

    tt = dagbag.dags["laptop_air_bulletin"].timetable
    assert type(tt).__name__ == "PartitionedAssetTimetable" or isinstance(tt, PartitionedAssetTimetable)
    assert "air_hourly" in repr(tt.asset_condition)
    assert isinstance(tt.default_partition_mapper, StartOfDayMapper)


def test_air_bulletin_tasks(dagbag):
    """Mêmes tâches dans les deux DAGs ; render_chart en venv (cache), outlet air_bulletin."""
    cron = dagbag.dags["laptop_air_bulletin_cron"]
    assert cron.max_active_runs == 2 and cron.catchup is False
    for dag_id in ("laptop_air_bulletin", "laptop_air_bulletin_cron"):
        dag = dagbag.dags[dag_id]
        assert set(dag.task_ids) == {"collect_day", "render_chart", "publish_bulletin"}
        chart = dag.get_task("render_chart")
        assert type(chart).__name__ == "_PythonVirtualenvDecoratedOperator"
        assert chart.system_site_packages is False
        assert str(chart.venv_cache_path) == "/opt/airflow/.venvs"
        assert "matplotlib" in chart.requirements
        publish = dag.get_task("publish_bulletin")
        assert [(o.name, o.uri) for o in publish.outlets] == [("air_bulletin", "s3://datalake/air/bulletin")]
