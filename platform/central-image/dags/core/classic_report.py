"""Rapport "à l'ancienne" : déclenché par `classic_etl_pipeline` (TriggerDagRunOperator).

Pas de planning (`schedule=None`) : ce DAG ne sait pas quelle donnée il consomme,
il reçoit la clé S3 dans `dag_run.conf` (couplage par appel, pas par Asset).
Accès S3 par le hook du provider Amazon (`S3Hook`, connexion `minio_s3`) et pandas, tous
deux fournis par l'image centrale : "ancienne idéologie" = hooks/opérateurs de providers liés
à l'image du centre (les équipes Edge utilisent directement leurs propres libs).
Exécution en LocalExecutor (scheduler).
"""

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

BUCKET = "datalake"

default_args = {
    "executor": "LocalExecutor",  # le défaut de toute façon (1er de la liste)
    "owner": "core-platform",
}


def build_report(**context):
    import io

    import pandas as pd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    conf = context["dag_run"].conf or {}
    hook = S3Hook(aws_conn_id="minio_s3")  # endpoint_url / region_name lus dans l'extra

    key = conf.get("key")
    if not key:  # lancement manuel sans conf : on prend le dernier fichier produit
        objects = hook.get_file_metadata(prefix="classic/", bucket_name=BUCKET)  # list_objects_v2 paginé
        if not objects:
            raise ValueError("Aucun fichier classic/*.csv : lancer classic_etl_pipeline d'abord")
        key = max(objects, key=lambda o: o["LastModified"])["Key"]

    body = hook.get_key(key, bucket_name=BUCKET).get()["Body"].read()  # octets bruts pour pandas
    df = pd.read_csv(io.BytesIO(body))
    by_customer = df.groupby("customer")["amount"].agg(["count", "sum"]).round(2)
    print(f"Rapport sur s3://{BUCKET}/{key} : {len(df)} lignes (S3Hook, pandas {pd.__version__})")
    print(by_customer.to_string())
    return {"key": key, "rows": int(len(df)), "total": float(df["amount"].sum().round(2))}


with DAG(
    dag_id="classic_report",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["core", "classic"],
    doc_md=__doc__,
) as dag:
    PythonOperator(task_id="build_report", python_callable=build_report)
