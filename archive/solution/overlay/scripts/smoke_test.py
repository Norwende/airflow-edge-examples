# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3"]
# ///
"""Vérifie la plateforme centrale de bout en bout (via Traefik : 127.0.0.1 + en-tête Host).

uv run scripts/smoke_test.py          # contrôles en lecture seule
uv run scripts/smoke_test.py --run    # + déclenche classic_etl_pipeline et attend classic_report
"""

import argparse
import sys
import time
from datetime import UTC, datetime

import boto3
from botocore.config import Config

from _common import (
    ES_HOST,
    INGRESS,
    MINIO_HOST,
    NAMESPACE,
    airflow_token,
    fail,
    http,
    info,
    kubectl,
    ok,
    step,
)

CORE_DAGS = ["classic_etl_pipeline", "classic_report"]
BUNDLES = ["core", "team-data", "team-search", "laptop", "team-iot"]  # team-iot : docs/09
results: list[tuple[str, bool]] = []


def check(name: str, passed: bool, detail: str = "") -> bool:
    results.append((name, passed))
    (ok if passed else fail)(f"{name}{' : ' + detail if detail else ''}")
    return passed


def s3_client():
    # MinIO via l'Ingress : l'en-tête Host doit valoir minio.localhost (signature SigV4
    # calculée dessus) => on injecte le Host avant signature.
    client = boto3.client(
        "s3",
        endpoint_url=INGRESS,
        region_name="us-east-1",
        aws_access_key_id="minio",
        aws_secret_access_key="minio12345",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 2}),
    )

    def set_host(request, **_):
        request.headers["Host"] = MINIO_HOST

    client.meta.events.register_first("before-sign.s3.*", set_host)
    return client


def platform_checks(token: str) -> None:
    step("Airflow : UI, santé, DAGs")
    code, _ = http("GET", "/")
    check("UI /", code in (200, 302), f"HTTP {code}")
    code, health = http("GET", "/api/v2/monitor/health")
    if code == 200:
        states = {k: v.get("status") for k, v in health.items()}
        check("/api/v2/monitor/health", all(s == "healthy" for s in states.values()), str(states))
    else:
        check("/api/v2/monitor/health", False, f"HTTP {code}")

    code, data = http("GET", "/api/v2/dags?limit=100", token=token)
    dags = {d["dag_id"]: d for d in data.get("dags", [])} if code == 200 else {}
    for dag_id in CORE_DAGS:
        dag = dags.get(dag_id)
        check(f"DAG {dag_id}", dag is not None, f"bundle={dag.get('bundle_name')}" if dag else "absent")
    by_bundle: dict[str, list[str]] = {}
    for dag in dags.values():
        by_bundle.setdefault(dag.get("bundle_name"), []).append(dag["dag_id"])
    for bundle, ids in sorted(by_bundle.items(), key=lambda x: str(x[0])):
        info(f"bundle {bundle}: {', '.join(sorted(ids))}")

    code, data = http("GET", "/api/v2/importErrors", token=token)
    errors = data.get("import_errors", []) if code == 200 else []
    core_errors = [e for e in errors if e.get("bundle_name") == "core"]
    check(
        "aucune erreur d'import (bundle core)",
        code == 200 and not core_errors,
        f"{len(errors)} erreur(s) au total" if errors else "",
    )
    for err in errors:
        info(
            f"import error [{err.get('bundle_name')}] {err['filename']}: "
            f"{err['stack_trace'].strip().splitlines()[-1]}"
        )

    # Pas d'endpoint REST pour les bundles en 3.3 : table dag_bundle (remplie par le dag-processor).
    out = kubectl(
        "exec",
        "sts/airflow-postgresql",
        "--",
        "env",
        "PGPASSWORD=postgres",
        "psql",
        "-U",
        "postgres",
        "-d",
        "postgres",
        "-At",
        "-c",
        "SELECT name FROM dag_bundle WHERE active ORDER BY name;",
        capture=True,
        check=False,
        quiet=True,
    ).stdout.split()
    check(f"{len(BUNDLES)} bundles actifs (table dag_bundle)", sorted(out) == sorted(BUNDLES), ", ".join(out))

    step("Edge : plugin d'API et tables")
    # NB : /edge_worker/v1/rpcapi n'est PAS une route (404) : c'est un segment "fictif" que
    # le worker remplace (urljoin) par worker/..., jobs/..., logs/... La sonde est /v1/health.
    code, data = http("GET", "/edge_worker/v1/health")
    check("GET /edge_worker/v1/health", code == 200, f"HTTP {code} {data}")
    code, _ = http("POST", "/edge_worker/v1/worker/smoke-probe", body={})
    check("route worker (sans JWT -> 4xx != 404)", code in (401, 403, 422), f"HTTP {code}")
    code, data = http("GET", "/edge_worker/ui/worker", token=token)
    if check("GET /edge_worker/ui/worker", code == 200, f"HTTP {code}"):
        for w in data.get("workers", []):
            info(f"worker {w.get('worker_name')} state={w.get('state')} queues={w.get('queues')}")
        if not data.get("workers"):
            info("aucun edge worker enregistré (normal avant l'étape équipes)")
    out = kubectl(
        "exec",
        "sts/airflow-postgresql",
        "--",
        "env",
        "PGPASSWORD=postgres",
        "psql",
        "-U",
        "postgres",
        "-d",
        "postgres",
        "-At",
        "-c",
        "SELECT tablename FROM pg_tables WHERE tablename LIKE 'edge%' ORDER BY 1;",
        capture=True,
        check=False,
        quiet=True,
    ).stdout.split()
    check(
        "tables edge_job / edge_logs / edge_worker",
        {"edge_job", "edge_logs", "edge_worker"} <= set(out),
        ", ".join(out),
    )

    step("Externes : MinIO, Elasticsearch")
    try:
        buckets = [b["Name"] for b in s3_client().list_buckets()["Buckets"]]
        check("MinIO bucket datalake", "datalake" in buckets, ", ".join(buckets))
    except Exception as err:  # noqa: BLE001
        check("MinIO bucket datalake", False, str(err))
    code, data = http("GET", "/_cluster/health", host=ES_HOST)
    status = data.get("status") if isinstance(data, dict) else None
    check("Elasticsearch /_cluster/health", status in ("green", "yellow"), f"HTTP {code} status={status}")


def wait_run(token: str, dag_id: str, run_id: str, timeout: float = 600) -> str:
    deadline = time.monotonic() + timeout
    state = "?"
    while time.monotonic() < deadline:
        code, data = http("GET", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}", token=token)
        state = data.get("state") if code == 200 else f"HTTP {code}"
        if state in ("success", "failed"):
            break
        time.sleep(5)
    code, data = http("GET", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances", token=token)
    for ti in data.get("task_instances", []) if code == 200 else []:
        info(
            f"  {dag_id}.{ti['task_id']}: {ti['state']} (executor={ti.get('executor')}, "
            f"hostname={ti.get('hostname')})"
        )
    return state


def run_classic(token: str) -> None:
    step("Exécution : classic_etl_pipeline -> classic_report")
    for dag_id in CORE_DAGS:  # un DAG en pause ne lance pas les runs déclenchés
        http("PATCH", f"/api/v2/dags/{dag_id}", body={"is_paused": False}, token=token)
    started = datetime.now(UTC)
    code, data = http(
        "POST",
        "/api/v2/dags/classic_etl_pipeline/dagRuns",
        body={"logical_date": started.isoformat()},
        token=token,
    )
    if not check("trigger classic_etl_pipeline", code in (200, 201), f"HTTP {code}"):
        info(str(data))
        return
    run_id = data["dag_run_id"]
    info(f"run_id={run_id}")
    state = wait_run(token, "classic_etl_pipeline", run_id)
    check("classic_etl_pipeline", state == "success", state)

    # Le run de classic_report a été créé par TriggerDagRunOperator pendant notre run.
    report_run = None
    for _ in range(24):
        code, data = http(
            "GET", "/api/v2/dags/classic_report/dagRuns?order_by=-run_after&limit=5", token=token
        )
        runs = [r for r in data.get("dag_runs", []) if r.get("run_after", "") >= started.isoformat()[:19]]
        if runs:
            report_run = runs[0]
            break
        time.sleep(5)
    if not check("classic_report déclenché", report_run is not None):
        return
    info(f"conf={report_run.get('conf')}")
    state = wait_run(token, "classic_report", report_run["dag_run_id"])
    check("classic_report", state == "success", state)

    key = (report_run.get("conf") or {}).get("key", "")
    try:
        head = s3_client().head_object(Bucket="datalake", Key=key)
        check(f"CSV s3://datalake/{key}", True, f"{head['ContentLength']} octets")
    except Exception as err:  # noqa: BLE001
        check(f"CSV s3://datalake/{key}", False, str(err))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--run", action="store_true", help="déclencher les DAGs classiques et attendre")
    args = parser.parse_args()

    token = airflow_token()
    ok("login admin via POST /auth/token")
    platform_checks(token)
    if args.run:
        run_classic(token)

    step("Résumé")
    failed = [name for name, passed in results if not passed]
    print(f"  {len(results) - len(failed)}/{len(results)} contrôles OK (namespace {NAMESPACE})")
    for name in failed:
        fail(name)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
