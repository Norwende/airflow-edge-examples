"""Pipeline ETL "à l'ancienne" (idéologie Airflow 2), exécuté DANS l'instance centrale.

Ce qui est "old-school" ici (volontairement) :
- les opérateurs sont des CLASSES instanciées (PythonOperator, BashOperator...) au lieu
  de fonctions décorées @task ;
- les données passent par XCom de façon EXPLICITE : `ti.xcom_pull(task_ids="extract")`
  dans `**context`, au lieu de valeurs de retour passées d'une tâche à l'autre ;
- l'enchaînement est IMPÉRATIF avec `>>` ;
- le couplage entre DAGs est un appel direct : TriggerDagRunOperator déclenche
  `classic_report` (le producteur doit connaître le consommateur), au lieu d'Assets
  où le consommateur s'abonne à une donnée ;
- l'accès aux systèmes externes passe par les HOOKS/OPÉRATEURS DES PROVIDERS installés
  dans l'IMAGE CENTRALE (ici `S3Hook` du provider Amazon, connexion `minio_s3`) : le DAG
  est lié aux versions de providers du centre, et toute nouvelle dépendance (provider ou
  lib) impose de reconstruire et redéployer l'instance Airflow. Les équipes Edge, elles,
  utilisent directement LEURS libs (boto3, polars, elasticsearch...) dans LEUR image.

Les imports utilisent toutefois les chemins valides en 3.3 (`airflow.sdk`,
`airflow.providers.standard.*`) : `airflow.models.DAG` / `airflow.operators.*` ne sont
plus que des alias dépréciés.
"""

import csv
import io
import random

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG

BUCKET = "datalake"

default_args = {
    # LocalExecutor est de toute façon le défaut (1er executor de la liste
    # `LocalExecutor,...EdgeExecutor`) : on l'écrit pour que ce soit visible.
    # Les tâches tournent donc comme sous-processus du POD SCHEDULER, avec les
    # librairies de l'image centrale.
    "executor": "LocalExecutor",
    "owner": "core-platform",
    "retries": 1,
    "retry_delay": pendulum.duration(seconds=30),
}


def _run_date(context) -> str:
    # `ds` n'existe pas pour un run déclenché sans logical_date (Airflow 3).
    ds = context.get("ds")
    return ds or context["dag_run"].run_after.strftime("%Y-%m-%d")


def extract(**context):
    """Simule une extraction : la valeur de retour est poussée en XCom (clé return_value)."""
    rng = random.Random(_run_date(context))
    customers = ["acme", "globex", "initech", "umbrella"]
    orders = [
        {
            "order_id": i,
            "customer": rng.choice(customers),
            "amount": round(rng.uniform(5, 500), 2),
        }
        for i in range(1, 21)
    ]
    print(f"{len(orders)} commandes extraites")
    return orders


def transform(**context):
    # XCom explicite : on va chercher le résultat de la tâche amont par son task_id.
    ti = context["ti"]
    orders = ti.xcom_pull(task_ids="extract")
    for order in orders:
        order["amount_ttc"] = round(order["amount"] * 1.2, 2)
        order["big"] = order["amount"] >= 250
    print(f"{sum(o['big'] for o in orders)} grosses commandes sur {len(orders)}")
    return orders


def load_to_s3(**context):
    # Hook du provider Amazon (installé dans l'image centrale) : il lit la connexion
    # `minio_s3` (login/password + extra endpoint_url/region_name) et crée le client boto3.
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    orders = context["ti"].xcom_pull(task_ids="transform")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(orders[0].keys()))
    writer.writeheader()
    writer.writerows(orders)

    key = f"classic/orders_{_run_date(context)}.csv"
    S3Hook(aws_conn_id="minio_s3").load_string(buf.getvalue(), key=key, bucket_name=BUCKET, replace=True)
    print(f"Écrit s3://{BUCKET}/{key} ({len(orders)} lignes) via S3Hook")
    return key  # XCom : relu par trigger_report via un template Jinja


with DAG(
    dag_id="classic_etl_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["core", "classic"],
    doc_md=__doc__,
) as dag:
    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_check_env = BashOperator(
        task_id="check_env",
        bash_command=(
            'echo "hostname: $(hostname)"; python --version; '
            "pip list 2>/dev/null | grep -i -E 'boto3|pandas|providers-amazon'"
        ),
    )
    t_load = PythonOperator(task_id="load_to_s3", python_callable=load_to_s3)
    t_trigger = TriggerDagRunOperator(
        task_id="trigger_report",
        trigger_dag_id="classic_report",
        conf={"key": "{{ ti.xcom_pull(task_ids='load_to_s3') }}"},
        wait_for_completion=False,
    )

    t_extract >> t_transform >> t_check_env >> t_load >> t_trigger
