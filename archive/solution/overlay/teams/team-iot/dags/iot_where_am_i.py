"""
Où suis-je ? — preuve que les tâches team-iot tournent sur le worker edge ``team-iot-worker-1``.

Le DAG est parsé au centre (dag-processor, sans scikit-learn) ; ses tâches sont routées vers
l'EdgeExecutor, queue ``team-iot`` : elles s'exécutent dans l'image Python 3.10 + numpy +
scikit-learn de l'équipe. ``import sklearn`` est fait DANS la tâche (règle D3).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(
    dag_id="iot_where_am_i",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"executor": "EdgeExecutor", "queue": "team-iot", "owner": "team-iot"},
    tags=["team-iot", "edge"],
    doc_md=__doc__,
)
def iot_where_am_i():
    @task
    def report_environment() -> dict:
        """Affiche hôte, Python, versions Airflow/edge3 et des libs de l'équipe."""
        import importlib.metadata as md
        import platform
        import socket
        import sys

        import airflow
        import numpy
        import sklearn  # absent du centre : n'est importable que sur le worker team-iot
        from airflow.providers.edge3 import __version__ as edge3_version

        packages = {}
        for name in ("numpy", "scikit-learn", "boto3", "s3fs", "polars", "duckdb"):
            try:
                packages[name] = md.version(name)
            except md.PackageNotFoundError:
                packages[name] = None  # absent : attendu pour polars/duckdb sur team-iot

        info = {
            "hostname": socket.gethostname(),
            "platform": platform.platform(),
            "python": sys.version,
            "airflow": airflow.__version__,
            "edge3": edge3_version,
            "sklearn": sklearn.__version__,
            "numpy": numpy.__version__,
            "packages": packages,
        }
        for key, value in info.items():
            print(f"{key:>10}: {value}")
        return info

    bash_hello = BashOperator(
        task_id="bash_hello",
        bash_command='echo "edge worker: $(hostname)"',
    )

    report_environment() >> bash_hello


iot_where_am_i()
