"""
Où suis-je ? — preuve que les tâches tournent sur le worker edge « laptop ».

Le DAG est parsé au centre (dag-processor) mais ses tâches sont routées vers
l'EdgeExecutor, queue ``laptop`` : elles s'exécutent hors du cluster, dans
l'environnement Python minimal du laptop (python 3.11 slim + duckdb, sans pandas/polars).
"""

from __future__ import annotations

from datetime import UTC

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

PROOF_FILE = "/opt/airflow/logs/laptop-proof.txt"


@dag(
    dag_id="laptop_where_am_i",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"executor": "EdgeExecutor", "queue": "laptop", "owner": "laptop"},
    tags=["laptop", "edge"],
    doc_md=__doc__,
)
def laptop_where_am_i():
    @task
    def report_environment() -> dict:
        """Affiche hôte, OS, Python, versions Airflow/edge3 et quelques paquets installés."""
        import importlib.metadata as md
        import platform
        import socket
        import sys

        import airflow
        from airflow.providers.edge3 import __version__ as edge3_version

        packages = {}
        for name in ("duckdb", "boto3", "polars", "pandas", "pyarrow", "elasticsearch"):
            try:
                packages[name] = md.version(name)
            except md.PackageNotFoundError:
                packages[name] = None  # absent : attendu pour polars/pandas sur le laptop

        info = {
            "hostname": socket.gethostname(),
            "platform": platform.platform(),
            "python": sys.version,
            "airflow": airflow.__version__,
            "edge3": edge3_version,
            "packages": packages,
        }
        for key, value in info.items():
            print(f"{key:>10}: {value}")
        return info

    @task
    def write_proof(info: dict) -> str:
        """Écrit un fichier de preuve dans /opt/airflow/logs (visible sur l'hôte via le volume)."""
        from datetime import datetime

        line = (
            f"{datetime.now(UTC).isoformat()} host={info['hostname']} "
            f"python={info['python'].split()[0]} airflow={info['airflow']} edge3={info['edge3']}\n"
        )
        with open(PROOF_FILE, "a", encoding="utf-8") as fh:
            fh.write(line)
        print(f"preuve écrite dans {PROOF_FILE} : {line.strip()}")
        return PROOF_FILE

    bash_hello = BashOperator(
        task_id="bash_hello",
        bash_command='echo "edge worker: $(hostname)"',
    )

    write_proof(report_environment()) >> bash_hello


laptop_where_am_i()
