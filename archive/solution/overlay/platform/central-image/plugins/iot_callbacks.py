"""
Callbacks exécutés **au centre** pour team-iot (docs/09, étape 11) — module des plugins centraux.

`notify_deadline` est référencé par chemin (`AsyncCallback("iot_callbacks.notify_deadline", ...)`)
dans `iot_ingest_measurements` : le triggerer l'importe depuis le `plugins_folder`
(`/opt/airflow/plugins`, dans son `sys.path`). Le worker team-iot n'en a pas besoin : d'où le chemin
pointé plutôt que la fonction (qui ferait échouer l'import du DAG sur le worker).

Vérifié dans le code 3.3.2 (`airflow/triggers/callback.py`, `airflow/models/deadline.py`) :

- la fonction doit être **awaitable** (`async def`) : `CallbackTrigger` fait `await callback(**kwargs)` ;
- Airflow ajoute le kwarg réservé `context` = `{"dag_run": <DAGRunResponse en JSON>, "deadline":
  {"id", "deadline_time"}}` (pas le contexte complet d'une tâche) ; il n'est passé que si la
  fonction l'accepte ;
- les autres kwargs sont ceux du `AsyncCallback(..., kwargs={...})` du DAG (JSON).

Pas de lib d'équipe : boto3 (provider Amazon de l'image officielle) et `smtplib` (stdlib). Les
connexions sont lues par `Connection.async_get` (on est dans la boucle asyncio du triggerer).
Redémarrer le triggerer après modification.
"""

from __future__ import annotations

import asyncio
import json
import smtplib
from email.message import EmailMessage
from typing import Any


def _put_alert(conn: Any, bucket: str, key: str, body: bytes) -> None:
    import boto3

    extra = conn.extra_dejson
    boto3.client(
        "s3",
        endpoint_url=extra.get("endpoint_url"),
        region_name=extra.get("region_name", "us-east-1"),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    ).put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def _send_mail(conn: Any, mail_from: str, mail_to: str, subject: str, text: str) -> None:
    msg = EmailMessage()
    msg["From"], msg["To"], msg["Subject"] = mail_from, mail_to, subject
    msg.set_content(text)
    with smtplib.SMTP(conn.host, int(conn.port or 25), timeout=10) as smtp:  # Mailpit : sans TLS ni auth
        if conn.login:
            smtp.login(conn.login, conn.password or "")
        smtp.send_message(msg)


async def notify_deadline(
    *,
    context: dict[str, Any] | None = None,
    bucket: str = "datalake",
    alert_prefix: str = "air/alerts/",
    aws_conn_id: str = "minio_s3",
    smtp_conn_id: str = "smtp_default",
    mail_to: str | None = None,
    mail_from: str = "airflow@airflow-edge.local",
    **kwargs: Any,
) -> str:
    """Deadline dépassée : écrit `air/alerts/deadline-<dag_id>-<run_id>.json` et, si possible, un mail.

    Renvoie une CHAÎNE : le triggerer stocke la valeur de retour dans la colonne `callback.output`
    (texte). Observé en 3.3.2 : un `dict` renvoyé provoque `psycopg2.ProgrammingError: can't adapt
    type 'dict'`, le triggerer plante, redémarre et REJOUE le callback (mails en double)."""
    from airflow.sdk import Connection

    context = context or {}
    dag_run = context.get("dag_run") or {}
    dag_id = dag_run.get("dag_id", "unknown")
    run_id = dag_run.get("dag_run_id") or dag_run.get("run_id") or "unknown"
    payload = {
        "dag_id": dag_id,
        "run_id": run_id,
        "state": dag_run.get("state"),
        "queued_at": dag_run.get("queued_at"),
        "deadline": context.get("deadline"),
        "extra_kwargs": kwargs,
    }
    body = json.dumps(payload, indent=2, default=str).encode()
    key = f"{alert_prefix}deadline-{dag_id}-{run_id}.json"
    result: dict[str, Any] = {"key": key}

    s3_conn = await Connection.async_get(aws_conn_id)
    await asyncio.to_thread(_put_alert, s3_conn, bucket, key, body)
    print(f"deadline dépassée : s3://{bucket}/{key}")

    if mail_to:
        try:
            smtp_conn = await Connection.async_get(smtp_conn_id)
            subject = f"[deadline] {dag_id} / {run_id}"
            await asyncio.to_thread(_send_mail, smtp_conn, mail_from, mail_to, subject, body.decode())
            result["mail"] = mail_to
        except Exception as exc:  # le mail est un bonus : l'objet MinIO fait foi
            print(f"mail de deadline non envoyé : {exc!r}")
            result["mail_error"] = repr(exc)
    return json.dumps(result)
