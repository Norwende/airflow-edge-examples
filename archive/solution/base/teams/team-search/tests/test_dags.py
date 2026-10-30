"""Tests d'import des DAGs team-search (à lancer dans l'image du worker)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
EXPECTED_DAG_IDS = {
    "search_index_orders",
    "search_reindex_with_approval",
    "search_health_check",
}
TEAM_LIBS = ("elasticsearch", "pandas", "pyarrow", "boto3", "matplotlib")

# Même configuration d'executors que le central : EdgeExecutor doit être connu pour valider `executor=`
os.environ.setdefault(
    "AIRFLOW__CORE__EXECUTOR", "LocalExecutor,airflow.providers.edge3.executors.EdgeExecutor"
)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")


@pytest.fixture(scope="session")
def dagbag():
    from airflow.dag_processing.dagbag import BundleDagBag

    # BundleDagBag = ce qu'utilisent le dag-processor et le worker (ajoute le bundle au sys.path)
    return BundleDagBag(dag_folder=str(DAGS_DIR), bundle_path=DAGS_DIR)


def test_no_import_errors(dagbag) -> None:
    assert dagbag.import_errors == {}


def test_expected_dag_ids(dagbag) -> None:
    assert set(dagbag.dag_ids) == EXPECTED_DAG_IDS


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_tasks_routed_to_edge(dagbag, dag_id: str) -> None:
    """Toutes les tâches (y compris l'ApprovalOperator HITL) partent sur l'Edge, queue team-search."""
    dag = dagbag.dags[dag_id]
    assert dag.tags and "team-search" in dag.tags
    assert dag.catchup is False
    for t in dag.tasks:
        assert t.executor == "EdgeExecutor", f"{dag_id}.{t.task_id}"
        assert t.queue == "team-search", f"{dag_id}.{t.task_id}"
        assert t.owner == "team-search", f"{dag_id}.{t.task_id}"


def test_asset_ref_schedule(dagbag) -> None:
    """`schedule=[Asset.ref(name="orders_daily")]` est accepté et conservé tel quel (référence par nom)."""
    from airflow.sdk.definitions.asset import AssetNameRef

    dag = dagbag.dags["search_index_orders"]
    refs = list(dag.timetable.asset_condition.objects)  # liste de refs → AssetAll(ref)
    assert [r.name for r in refs if isinstance(r, AssetNameRef)] == ["orders_daily"]
    fetch = dag.get_task("fetch_daily")
    assert any(isinstance(i, AssetNameRef) and i.name == "orders_daily" for i in fetch.inlets)
    publish = dag.get_task("publish")
    # Airflow normalise une URI sans chemin : "es://orders" est stockée "es://orders/"
    assert [(o.name, o.uri.rstrip("/")) for o in publish.outlets] == [("orders_index", "es://orders")]


def test_health_check_paused_by_default(dagbag) -> None:
    assert dagbag.dags["search_health_check"].is_paused_upon_creation is True


def test_import_without_team_libs() -> None:
    """Règle D3 : les fichiers se parsent sans elasticsearch/pandas/pyarrow/boto3/matplotlib (central)."""
    code = f"""
import sys
for name in {TEAM_LIBS!r}:
    sys.modules[name] = None  # tout `import name` lève ImportError
from airflow.dag_processing.dagbag import BundleDagBag
from pathlib import Path
p = Path({str(DAGS_DIR)!r})
bag = BundleDagBag(dag_folder=str(p), bundle_path=p)
assert not bag.import_errors, bag.import_errors
assert len(bag.dag_ids) == {len(EXPECTED_DAG_IDS)}, bag.dag_ids
leaked = [n for n in {TEAM_LIBS!r} if sys.modules.get(n) is not None]
assert not leaked, leaked
print("OK", sorted(bag.dag_ids))
"""
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=os.environ.copy())
    assert res.returncode == 0, res.stdout + res.stderr
    assert "OK" in res.stdout
