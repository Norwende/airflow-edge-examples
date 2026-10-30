# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3"]
# ///
"""Scénarios de démo de bout en bout (API REST v2 + plugin Edge + MinIO + Elasticsearch).

uv run scripts/scenario.py classic      # DAGs "ancienne idéologie" : LocalExecutor dans le scheduler
uv run scripts/scenario.py assets       # chaîne d'assets inter-équipes, 3 edge workers (+ @asset au centre)
uv run scripts/scenario.py versioning   # dags.py apply --version v2 -> nouvelle version de DAG, sans rebuild
uv run scripts/scenario.py hitl         # Human-in-the-loop : approbation par l'API (ou l'UI)
uv run scripts/scenario.py isolation    # environnements des workers, maintenance, worker arrêté
uv run scripts/scenario.py all          # tout, dans l'ordre

Pré-requis : plateforme centrale (up.py), workers déployés (kubectl apply -k teams/<team>/k8s),
DAGs publiés (dags.py apply <team>), worker laptop lancé (docker compose up -d dans teams/laptop ;
facultatif, sinon les contrôles laptop sont sautés avec un avertissement).
Chaque scénario est relançable (il dé-pause les DAGs dont il a besoin).
"""

import argparse
import sys
import time
from datetime import UTC, datetime
from urllib.parse import quote

import boto3
from botocore.config import Config

from _common import (
    ADMIN,
    ES_HOST,
    INGRESS,
    MINIO_HOST,
    ROOT,
    airflow_token,
    color,
    fail,
    http,
    info,
    ok,
    run,
    step,
)

TERMINAL = {"success", "failed"}
WORKERS = {"team-data": "team-data-worker-1", "team-search": "team-search-worker-1"}
SCHEDULER_HOST = "airflow-scheduler-0"
results: list[tuple[str, bool]] = []
_token: str | None = None


class ScenarioError(Exception):
    """Échec bloquant d'un scénario (délai dépassé, run en échec...)."""


# --------------------------------------------------------------------------- utilitaires API


def api(method: str, path: str, body: dict | None = None, *, expect: tuple[int, ...] = (200,)):
    """Appel REST authentifié (jeton admin renouvelé si 401) ; lève ScenarioError si code inattendu."""
    global _token
    if _token is None:
        _token = airflow_token(*ADMIN)
    code, data = http(method, path, body=body, token=_token, timeout=30)
    if code == 401:
        _token = airflow_token(*ADMIN)
        code, data = http(method, path, body=body, token=_token, timeout=30)
    if code not in expect:
        raise ScenarioError(f"{method} {path} -> HTTP {code} : {str(data)[:300]}")
    return data


def story(text: str) -> None:
    """Ligne de narration (le "pourquoi" de chaque étape)."""
    print(f"  {color('>>', '35')}  {text}", flush=True)


def short(host: str | None) -> str | None:
    """Nom court : le scheduler (StatefulSet + service headless) remonte un FQDN."""
    return host.split(".")[0] if host else host


def check(name: str, passed: bool, detail: str = "") -> bool:
    results.append((name, passed))
    (ok if passed else fail)(f"{name}{' : ' + detail if detail else ''}")
    return passed


def poll(predicate, *, timeout: float, what: str, interval: float = 3):
    """Rappelle predicate() jusqu'à une valeur vraie ; ScenarioError explicite à l'expiration."""
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise ScenarioError(f"délai dépassé ({timeout:.0f} s) : {what}")
        time.sleep(interval)


def unpause(*dag_ids: str) -> None:
    for dag_id in dag_ids:
        poll(
            lambda dag_id=dag_id: (
                http("GET", f"/api/v2/dags/{dag_id}", token=_token or airflow_token())[0] == 200
            ),
            timeout=180,
            what=f"DAG {dag_id} visible dans l'API (publié ? dags.py apply)",
        )
        api("PATCH", f"/api/v2/dags/{dag_id}", {"is_paused": False})
    info(f"dé-pausé(s) : {', '.join(dag_ids)}")


def pause(*dag_ids: str) -> None:
    for dag_id in dag_ids:
        api("PATCH", f"/api/v2/dags/{dag_id}", {"is_paused": True})


def trigger(dag_id: str, *, with_logical_date: bool = True, conf: dict | None = None) -> str:
    body: dict = {"logical_date": datetime.now(UTC).isoformat() if with_logical_date else None}
    if conf:
        body["conf"] = conf
    data = api("POST", f"/api/v2/dags/{dag_id}/dagRuns", body)
    run_id = data["dag_run_id"]
    info(f"déclenché {dag_id} run_id={run_id} logical_date={data.get('logical_date')}")
    return run_id


def get_run(dag_id: str, run_id: str) -> dict:
    return api("GET", f"/api/v2/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}")


def wait_run(dag_id: str, run_id: str, timeout: float = 300) -> dict:
    """Attend la fin du run (success/failed) ; affiche les états des tâches."""
    start = time.monotonic()
    last = None

    def done():
        nonlocal last
        r = get_run(dag_id, run_id)
        if r["state"] != last:
            info(f"{dag_id} : {r['state']}")
            last = r["state"]
        return r if r["state"] in TERMINAL else None

    r = poll(done, timeout=timeout, what=f"fin du run {dag_id}/{run_id}")
    info(f"{dag_id} terminé en {time.monotonic() - start:.0f} s ({r['state']})")
    return r


def task_instances(dag_id: str, run_id: str) -> list[dict]:
    data = api("GET", f"/api/v2/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances?limit=100")
    return data["task_instances"]


def show_tis(tis: list[dict]) -> None:
    for ti in sorted(tis, key=lambda t: (t["task_id"], t["map_index"])):
        idx = f"[{ti['map_index']}]" if ti["map_index"] >= 0 else ""
        print(
            f"        {ti['task_id'] + idx:<28} {str(ti['state']):<15} "
            f"hostname={ti.get('hostname') or '-':<22} "
            f"executor={ti.get('executor') or '(défaut)':<13} queue={ti.get('queue')}"
        )


def assert_ran_on(
    dag_id: str,
    run_id: str,
    worker: str,
    *,
    executor: str | None = "EdgeExecutor",
    queue: str | None = None,
    states=("success",),
) -> list[dict]:
    """Vérifie hostname / executor / queue de toutes les task instances exécutées du run."""
    tis = task_instances(dag_id, run_id)
    show_tis(tis)
    ran = [t for t in tis if t["state"] in states]
    hosts = {short(t.get("hostname")) for t in ran}
    check(
        f"{dag_id} : tâches exécutées sur {worker}",
        bool(ran) and hosts == {worker},
        f"hostnames={sorted(map(str, hosts))}",
    )
    if executor is not None:
        execs = {t.get("executor") for t in ran}
        check(f"{dag_id} : executor={executor}", execs == {executor}, f"{sorted(map(str, execs))}")
    if queue is not None:
        queues = {t.get("queue") for t in ran}
        check(f"{dag_id} : queue={queue}", queues == {queue}, f"{sorted(map(str, queues))}")
    return tis


def task_log(dag_id: str, run_id: str, task_id: str, try_number: int = 1, map_index: int = -1) -> str:
    """Log d'une tâche via l'API (pour l'Edge : poussé par le worker vers l'api-server)."""
    path = (
        f"/api/v2/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances/{task_id}/logs/{try_number}"
        f"?map_index={map_index}&full_content=true"
    )
    data = api("GET", path)
    content = data.get("content") if isinstance(data, dict) else data
    if isinstance(content, list):  # 3.x : liste d'entrées structurées {event, timestamp, level...}
        return "\n".join(str(e.get("event", e)) if isinstance(e, dict) else str(e) for e in content)
    return str(content)


def wait_log(
    dag_id: str, run_id: str, task_id: str, needle: str, *, map_index: int = -1, timeout: float = 60
) -> str:
    """Le worker pousse ses logs par lots : on attend que la ligne voulue soit arrivée."""
    start = time.monotonic()

    def fetch():
        text = task_log(dag_id, run_id, task_id, map_index=map_index)
        return text if needle in text else None

    text = poll(fetch, timeout=timeout, what=f"ligne '{needle}' dans le log {dag_id}.{task_id}")
    info(f"log {dag_id}.{task_id} disponible via l'API après {time.monotonic() - start:.0f} s d'attente")
    return text


def grep(text: str, *needles: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if any(n in line for n in needles)]


def asset_events(**filters) -> list[dict]:
    query = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in filters.items())
    return api("GET", f"/api/v2/assets/events?order_by=-timestamp&limit=50&{query}")["asset_events"]


def downstream_runs(
    source_dag: str, source_run: str, asset: str, expected: set[str], timeout: float = 120
) -> dict:
    """Runs créés par l'événement d'asset émis par (source_dag, source_run) -> {dag_id: run_id}."""

    def found():
        events = [
            e for e in asset_events(source_dag_id=source_dag, source_run_id=source_run) if e["name"] == asset
        ]
        if not events:
            return None
        created = {r["dag_id"]: r["run_id"] for e in events for r in e.get("created_dagruns", [])}
        # Constaté en 3.3.2 : un DAG planifié par Asset.ref(name=...) est bien déclenché, mais son run
        # n'est PAS rattaché à l'événement (created_dagruns / upstreamAssetEvents vides, donc
        # triggering_asset_events vide dans la tâche -> lire inlet_events). Repli : 1er run
        # asset_triggered du DAG dont run_after >= horodatage de l'événement.
        ts = min(e["timestamp"] for e in events)[:23] + "Z"
        for dag_id in expected - set(created):
            runs = api(
                "GET",
                f"/api/v2/dags/{dag_id}/dagRuns?order_by=run_after&limit=5"
                f"&run_after_gte={quote(ts, safe='')}",
            )["dag_runs"]
            runs = [r for r in runs if r["run_type"] == "asset_triggered"]
            if runs:
                created[dag_id] = runs[0]["dag_run_id"]
        return created if expected <= set(created) else None

    return poll(
        found, timeout=timeout, what=f"runs {sorted(expected)} créés par l'événement {asset} de {source_dag}"
    )


def edge_workers() -> dict[str, dict]:
    return {w["worker_name"]: w for w in api("GET", "/edge_worker/ui/worker")["workers"]}


def show_workers(title: str) -> dict[str, dict]:
    workers = edge_workers()
    info(title)
    for name, w in sorted(workers.items()):
        sysinfo = w.get("sysinfo") or {}
        print(
            f"        {name:<22} state={w['state']:<18} queues={w.get('queues')} "
            f"python={str(sysinfo.get('python_version', '?')).split()[0]} "
            f"airflow={sysinfo.get('airflow_version')} edge3={sysinfo.get('edge_provider_version')}"
        )
    return workers


def laptop_worker() -> str | None:
    """Nom du worker laptop s'il est en ligne (laptop-<user>), sinon None."""
    for name, w in edge_workers().items():
        if name.startswith("laptop-") and w["state"] in ("idle", "running"):
            return name
    return None


def es_count(index: str = "orders") -> int:
    code, data = http("GET", f"/{index}/_count", host=ES_HOST)
    return int(data.get("count", 0)) if code == 200 and isinstance(data, dict) else -1


def s3_client():
    """MinIO via l'Ingress (Host: minio.localhost injecté avant signature SigV4)."""
    client = boto3.client(
        "s3",
        endpoint_url=INGRESS,
        region_name="us-east-1",
        aws_access_key_id="minio",
        aws_secret_access_key="minio12345",
        config=Config(s3={"addressing_style": "path"}),
    )
    client.meta.events.register_first(
        "before-sign.s3.*", lambda request, **_: request.headers.__setitem__("Host", MINIO_HOST)
    )
    return client


def kube(*args: str, check_: bool = True) -> str:
    return run(["kubectl", *args], capture=True, check=check_).stdout


# --------------------------------------------------------------------------- scénarios


def scenario_classic() -> None:
    step("Scénario 1 — classique : DAG(...) + PythonOperator + TriggerDagRunOperator, au centre")
    story("Ces DAGs n'ont ni executor ni queue : ils prennent l'executor par défaut (LocalExecutor),")
    story("donc s'exécutent en sous-process du scheduler, avec les libs de l'image centrale.")
    unpause("classic_etl_pipeline", "classic_report")
    started = datetime.now(UTC).isoformat()
    run_id = trigger("classic_etl_pipeline")
    r = wait_run("classic_etl_pipeline", run_id)
    check("classic_etl_pipeline success", r["state"] == "success", run_id)
    tis = task_instances("classic_etl_pipeline", run_id)
    show_tis(tis)
    hosts = {short(t.get("hostname")) for t in tis if t["state"] == "success"}
    check("tâches classiques sur le scheduler", hosts == {SCHEDULER_HOST}, str(sorted(map(str, hosts))))
    check(
        "executor par défaut (champ executor vide = LocalExecutor)",
        all(t.get("executor") in (None, "LocalExecutor") for t in tis),
        str({t.get("executor") for t in tis}),
    )

    story(
        "classic_report a été déclenché par TriggerDagRunOperator "
        "(couplage explicite producteur -> consommateur)."
    )
    rep = poll(
        lambda: [
            x
            for x in api("GET", "/api/v2/dags/classic_report/dagRuns?order_by=-run_after&limit=5")["dag_runs"]
            if x["run_after"] >= started[:19]
        ],
        timeout=120,
        what="run de classic_report",
    )[0]
    r2 = wait_run("classic_report", rep["dag_run_id"])
    check(
        "classic_report déclenché et success",
        r2["state"] == "success",
        f"{rep['dag_run_id']} conf={rep.get('conf')}",
    )


def scenario_assets() -> None:
    step("Scénario 2 — assets : chaîne pilotée par la donnée, 3 équipes, 3 workers")
    show_workers("workers edge :")
    laptop = laptop_worker()
    if not laptop:
        print(
            color(
                "  !!  worker laptop absent : les contrôles laptop_duckdb_analytics seront sautés "
                "(cd teams/laptop && docker compose up -d)",
                "33",
            )
        )
    consumers = ["data_orders_daily", "search_index_orders"] + (["laptop_duckdb_analytics"] if laptop else [])
    unpause("data_ingest_orders", *consumers, "demo_shorthand")

    story("data_ingest_orders (team-data) : expand() sur 3 régions, parquet polars -> MinIO, puis")
    story("publish émet un événement sur l'asset orders_raw (extra = clés S3). Aucun TriggerDagRun.")
    t0 = time.monotonic()
    run_id = trigger("data_ingest_orders", with_logical_date=True)
    r = wait_run("data_ingest_orders", run_id)
    check("data_ingest_orders success (avec logical_date)", r["state"] == "success", run_id)
    assert_ran_on("data_ingest_orders", run_id, WORKERS["team-data"], queue="team-data")
    log = wait_log("data_ingest_orders", run_id, "publish", "worker=")
    lines = grep(log, "worker=")
    check(
        "log publish : Python 3.12 + polars sur team-data-worker-1",
        any("worker=team-data-worker-1" in ln and "python=3.12" in ln for ln in lines),
        lines[0] if lines else "absent",
    )
    keys = [
        o["Key"]
        for o in s3_client().list_objects_v2(Bucket="datalake", Prefix="orders/raw/").get("Contents", [])
    ]
    check("MinIO : parquets sous orders/raw/", len(keys) >= 3, f"{len(keys)} objet(s)")

    ev = poll(
        lambda: asset_events(source_dag_id="data_ingest_orders", source_run_id=run_id),
        timeout=60,
        what="événement orders_raw",
    )
    check(
        "événement d'asset orders_raw créé",
        ev[0]["name"] == "orders_raw",
        f"id={ev[0]['id']} extra.rows={ev[0]['extra'].get('rows')} ds={ev[0]['extra'].get('ds')}",
    )

    story("L'événement orders_raw déclenche data_orders_daily (même équipe), qui émet orders_daily ;")
    story("orders_daily déclenche search_index_orders (team-search) et laptop_duckdb_analytics (laptop),")
    story("qui ne connaissent l'asset que par son NOM (Asset.ref) : aucun import de code entre équipes.")
    daily_run = downstream_runs("data_ingest_orders", run_id, "orders_raw", {"data_orders_daily"})[
        "data_orders_daily"
    ]
    r = wait_run("data_orders_daily", daily_run)
    check(
        "data_orders_daily auto-déclenché et success",
        r["state"] == "success",
        f"{daily_run} ({r['run_type']})",
    )
    assert_ran_on("data_orders_daily", daily_run, WORKERS["team-data"], queue="team-data")

    expected = set(consumers) - {"data_orders_daily"}
    runs = downstream_runs("data_orders_daily", daily_run, "orders_daily", expected)
    r = wait_run("search_index_orders", runs["search_index_orders"])
    check(
        "search_index_orders auto-déclenché et success", r["state"] == "success", runs["search_index_orders"]
    )
    assert_ran_on(
        "search_index_orders", runs["search_index_orders"], WORKERS["team-search"], queue="team-search"
    )
    n = es_count()
    check("Elasticsearch : index orders alimenté", n > 0, f"{n} document(s)")
    if laptop:
        lrun = runs["laptop_duckdb_analytics"]
        r = wait_run("laptop_duckdb_analytics", lrun)
        check("laptop_duckdb_analytics auto-déclenché et success", r["state"] == "success", lrun)
        assert_ran_on("laptop_duckdb_analytics", lrun, laptop, queue="laptop")
        log = wait_log("laptop_duckdb_analytics", lrun, "revenue_by_region", "lecture duckdb")
        for line in grep(log, "endpoint S3", "lecture duckdb"):
            print(f"        | {line}")
    info(f"chaîne complète en {time.monotonic() - t0:.0f} s")

    story("Run manuel SANS logical_date (Airflow 3) : `ds` est absent, le DAG retombe sur dag_run.run_after.")
    run_id2 = trigger("data_ingest_orders", with_logical_date=False)
    r = wait_run("data_ingest_orders", run_id2)
    check(
        "data_ingest_orders success (logical_date=None)",
        r["state"] == "success" and r["logical_date"] is None,
        f"{run_id2} logical_date={r['logical_date']} run_after={r['run_after']}",
    )
    # On laisse la chaîne aval de ce 2e run se terminer, pour ne pas polluer les scénarios suivants.
    daily2 = downstream_runs("data_ingest_orders", run_id2, "orders_raw", {"data_orders_daily"})[
        "data_orders_daily"
    ]
    wait_run("data_orders_daily", daily2)
    for dag_id, rid in downstream_runs("data_orders_daily", daily2, "orders_daily", expected).items():
        wait_run(dag_id, rid)

    story("demo_shorthand = @asset : pas de paramètre executor/queue -> executor par défaut, au centre.")
    run_id3 = trigger("demo_shorthand")
    r = wait_run("demo_shorthand", run_id3)
    check("demo_shorthand success", r["state"] == "success", run_id3)
    tis = task_instances("demo_shorthand", run_id3)
    show_tis(tis)
    check(
        "@asset exécuté sur le scheduler central (limite documentée)",
        {short(t.get("hostname")) for t in tis} == {SCHEDULER_HOST}
        and all(t.get("executor") in (None, "LocalExecutor") for t in tis),
    )

    step("Graphe des assets (GET /api/v2/assets + asset_expression de GET /api/v2/dags/{id}/details)")
    # scheduled_dags ne liste que les DAGs planifiés par Asset(...) ; ceux planifiés par Asset.ref(name=...)
    # (dépendance inter-équipes) n'apparaissent que dans l'asset_expression du DAG consommateur.
    refs: dict[str, set[str]] = {}

    def names(expr) -> list[str]:
        if isinstance(expr, dict):
            for kind in ("asset", "asset_ref"):
                if kind in expr and isinstance(expr[kind], dict) and "name" in expr[kind]:
                    return [expr[kind]["name"]]
            return [n for v in expr.values() for n in names(v)]
        return [n for v in expr for n in names(v)] if isinstance(expr, list) else []

    for d in api("GET", "/api/v2/dags?limit=100")["dags"]:
        expr = api("GET", f"/api/v2/dags/{d['dag_id']}/details").get("asset_expression")
        for n in names(expr or {}):
            refs.setdefault(n, set()).add(d["dag_id"])
    for a in sorted(api("GET", "/api/v2/assets?limit=100")["assets"], key=lambda a: a["name"]):
        prod = ", ".join(f"{p['dag_id']}.{p['task_id']}" for p in a.get("producing_tasks", [])) or "-"
        cons = (
            ", ".join(sorted(refs.get(a["name"], set()) | {d["dag_id"] for d in a.get("scheduled_dags", [])}))
            or "-"
        )
        print(f"        {a['name']:<14} {a['uri']:<34} produit par {prod:<42} -> déclenche {cons}")


def dag_versions(dag_id: str) -> list[dict]:
    return api("GET", f"/api/v2/dags/{dag_id}/dagVersions?order_by=-version_number&limit=50")["dag_versions"]


def dag_tasks(dag_id: str) -> set[str]:
    return {t["task_id"] for t in api("GET", f"/api/v2/dags/{dag_id}/tasks")["tasks"]}


def worker_has_validate() -> bool:
    out = run(
        [
            "kubectl",
            "-n",
            "team-data",
            "exec",
            "deploy/team-data-worker",
            "--",
            "grep",
            "-c",
            "def validate",
            "/opt/airflow/dags/team-data/..data/data_ingest_orders.py",
        ],
        capture=True,
        check=False,
        quiet=True,
    )
    return out.stdout.strip() not in ("", "0")


def publish(version: str | None) -> None:
    args = [sys.executable, str(ROOT / "scripts" / "dags.py"), "apply", "team-data"]
    run(args + (["--version", version] if version else []))


def wait_version(dag_id: str, after: int, want_validate: bool) -> dict:
    """Nouvelle version (> after) au centre ET fichier à jour dans le pod worker."""
    t0 = time.monotonic()

    def ready():
        versions = dag_versions(dag_id)
        if (
            versions
            and versions[0]["version_number"] > after
            and (("validate" in dag_tasks(dag_id)) == want_validate)
        ):
            return versions[0]
        return None

    v = poll(
        ready,
        timeout=240,
        interval=5,
        what=f"nouvelle version de {dag_id} (propagation ConfigMap ~60 s + refresh 30 s)",
    )
    info(f"version {v['version_number']} visible au centre après {time.monotonic() - t0:.0f} s")
    poll(
        lambda: worker_has_validate() == want_validate,
        timeout=120,
        interval=5,
        what="fichier mis à jour dans le pod team-data-worker (kubelet)",
    )
    info(
        f"fichier à jour dans le pod worker après {time.monotonic() - t0:.0f} s (sans rebuild ni redémarrage)"
    )
    return v


def scenario_versioning() -> None:
    dag_id = "data_ingest_orders"
    step("Scénario 3 — versioning : publier v2 (tâche validate) sans reconstruire l'image du worker")
    unpause(dag_id)
    before = dag_versions(dag_id)
    current = before[0]["version_number"] if before else 0
    info(f"version actuelle : {current} ; tâches : {sorted(dag_tasks(dag_id))}")
    if "validate" in dag_tasks(dag_id):
        story("v2 déjà active (scénario interrompu ?) : on republie v1 d'abord.")
        publish(None)
        current = wait_version(dag_id, current, want_validate=False)["version_number"]

    story(
        "UNE commande met à jour la ConfigMap centrale (parsing) ET celle du namespace team-data (exécution)."
    )
    publish("v2")
    v2 = wait_version(dag_id, current, want_validate=True)
    check(
        "nouvelle version de DAG avec la tâche validate",
        "validate" in dag_tasks(dag_id),
        f"v{v2['version_number']}",
    )
    run_v2 = trigger(dag_id)
    r = wait_run(dag_id, run_v2)
    check("run v2 success", r["state"] == "success", run_v2)
    tis = assert_ran_on(dag_id, run_v2, WORKERS["team-data"], queue="team-data")
    val = [t for t in tis if t["task_id"] == "validate"]
    check(
        "validate exécutée sur team-data-worker-1 (code lu depuis la ConfigMap)",
        bool(val) and val[0]["state"] == "success" and val[0]["hostname"] == WORKERS["team-data"],
    )
    log = wait_log(dag_id, run_v2, "validate", "OK orders/raw/")
    for line in grep(log, "OK orders/raw/")[:3]:
        print(f"        | {line}")

    story(
        "Retour en v1 : même commande sans --version -> encore une NOUVELLE version "
        "(l'historique est conservé)."
    )
    publish(None)
    v3 = wait_version(dag_id, v2["version_number"], want_validate=False)
    check(
        "retour v1 = nouvelle version sans validate",
        "validate" not in dag_tasks(dag_id),
        f"v{v3['version_number']}",
    )

    step(f"Versions de {dag_id} (GET /api/v2/dags/{dag_id}/dagVersions)")
    for v in dag_versions(dag_id)[:6]:
        print(
            f"        v{v['version_number']:<3} created_at={v['created_at'][:19]} bundle={v['bundle_name']} "
            f"bundle_version={v['bundle_version']}"
        )
    story(
        "bundle_version=None : un LocalDagBundle n'est pas versionné. "
        "La version de DAG (structure sérialisée)"
    )
    story("est historisée — la grille d'un ancien run garde SA structure — mais le CODE n'est pas épinglé :")
    story("relancer une tâche d'un ancien run exécute le fichier actuel. Épinglage du code = GitDagBundle.")
    old = get_run(dag_id, run_v2)
    old_versions = [v["version_number"] for v in old.get("dag_versions", [])]
    check(
        "le run v2 référence toujours sa version",
        old_versions == [v2["version_number"]],
        f"{run_v2} dag_versions={old_versions} (version courante v{v3['version_number']})",
    )
    old_tasks = {t["task_id"] for t in task_instances(dag_id, run_v2)}
    check("la grille du run v2 contient toujours validate", "validate" in old_tasks)


def scenario_hitl() -> None:
    dag_id, task_id = "search_reindex_with_approval", "approve_reindex"
    step("Scénario 4 — Human-in-the-loop : approbation avant de recréer l'index Elasticsearch")
    unpause(dag_id)
    run_id = trigger(dag_id)
    story(
        "ApprovalOperator met la tâche en état awaiting_input : "
        "aucun slot de worker n'est occupé pendant l'attente."
    )

    def awaiting():
        tis = {t["task_id"]: t for t in task_instances(dag_id, run_id)}
        state = tis.get(task_id, {}).get("state")
        if state in ("failed", "upstream_failed"):
            raise ScenarioError(f"{task_id} en état {state}")
        return tis[task_id] if state == "awaiting_input" else None

    ti = poll(awaiting, timeout=180, what=f"{task_id} en awaiting_input")
    check(f"{task_id} en awaiting_input", True, f"hostname={ti['hostname']} executor={ti['executor']}")
    # jobs_active est remonté au heartbeat du worker (10 s) : on laisse le temps de rafraîchir.
    try:
        poll(
            lambda: edge_workers()[WORKERS["team-search"]]["jobs_active"] == 0, timeout=30, what="slot libéré"
        )
        check("pendant l'attente : 0 job actif sur team-search-worker-1 (slot libéré)", True)
    except ScenarioError as err:
        check("pendant l'attente : 0 job actif sur team-search-worker-1 (slot libéré)", False, str(err))

    pending = api("GET", "/api/v2/dags/~/dagRuns/~/hitlDetails?response_received=false")
    mine = [h for h in pending["hitl_details"] if h["task_instance"]["dag_run_id"] == run_id]
    check(
        "action requise listée (GET /api/v2/dags/~/dagRuns/~/hitlDetails)",
        bool(mine),
        f"{pending['total_entries']} en attente au total",
    )
    if mine:
        h = mine[0]
        info(f"sujet : {h['subject']} | options : {h['options']} | défaut : {h['defaults']}")
    story("Dans l'UI : Browse -> Required Actions (ou onglet Required Actions du run), bouton Approve.")
    story(f"Ici via l'API : PATCH /api/v2/dags/{dag_id}/dagRuns/<run>/taskInstances/{task_id}/-1/hitlDetails")
    resp = api(
        "PATCH",
        f"/api/v2/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances/{task_id}/-1/hitlDetails",
        {"chosen_options": ["Approve"], "params_input": {}},
    )
    info(
        f"réponse enregistrée : {resp.get('chosen_options')} "
        f"par {resp.get('responded_by', resp.get('responded_by_user'))}"
    )

    r = wait_run(dag_id, run_id)
    check("run HITL success après approbation", r["state"] == "success", run_id)
    tis = {t["task_id"]: t for t in task_instances(dag_id, run_id)}
    show_tis(list(tis.values()))
    check(
        "recreate_index + reindex_latest sur team-search-worker-1",
        all(
            tis[t]["state"] == "success" and tis[t]["hostname"] == WORKERS["team-search"]
            for t in ("recreate_index", "reindex_latest")
        ),
    )
    check("branche cancelled sautée", tis["cancelled"]["state"] == "skipped")
    n = es_count()
    check("index orders recréé et réalimenté", n > 0, f"{n} document(s)")


def wait_worker_state(name: str, states: set[str], timeout: float = 90) -> dict:
    return poll(
        lambda: (w := edge_workers().get(name)) and w["state"] in states and w,
        timeout=timeout,
        what=f"worker {name} dans l'état {sorted(states)}",
    )


def assert_stays_queued(dag_id: str, run_id: str, seconds: int = 30) -> None:
    info(f"observation pendant {seconds} s...")
    time.sleep(seconds)
    tis = task_instances(dag_id, run_id)
    show_tis(tis)
    started = [t for t in tis if t["state"] not in ("queued", "scheduled", None)]
    check(
        f"{dag_id} : aucune tâche démarrée (en attente d'un worker)",
        not started and any(t["state"] == "queued" for t in tis),
        ", ".join(f"{t['task_id']}={t['state']}" for t in tis),
    )


def scenario_isolation() -> None:
    step("Scénario 5a — chaque worker a son environnement (hostname, Python, libs)")
    laptop = laptop_worker()
    unpause("search_health_check", *(["laptop_where_am_i"] if laptop else []))
    runs = {"search_health_check": trigger("search_health_check")}
    if laptop:
        runs["laptop_where_am_i"] = trigger("laptop_where_am_i")
    else:
        print(color("  !!  worker laptop absent : laptop_where_am_i sauté", "33"))
    for dag_id, run_id in runs.items():
        r = wait_run(dag_id, run_id)
        check(f"{dag_id} success", r["state"] == "success", run_id)
    log = wait_log("search_health_check", runs["search_health_check"], "cluster_health", "worker_host")
    for line in grep(log, "worker_host", "python", "client", "es_version"):
        print(f"        | {line}")
    check(
        "search_health_check sur team-search-worker-1 (Python 3.13)",
        "team-search-worker-1" in log and "3.13" in log,
    )
    if laptop:
        log = wait_log("laptop_where_am_i", runs["laptop_where_am_i"], "report_environment", "hostname")
        for line in grep(log, "hostname:", "python:", "packages:"):
            print(f"        | {line}")
        check(
            f"laptop_where_am_i sur {laptop} (Python 3.11, duckdb, sans polars)",
            laptop in log and "3.11" in log and "'polars': None" in log,
        )
        needle = f"edge worker: {laptop}"  # sortie de `echo "edge worker: $(hostname)"` (BashOperator)
        blog = wait_log("laptop_where_am_i", runs["laptop_where_am_i"], "bash_hello", needle)
        print(f"        | {grep(blog, needle)[0]}")
        check("BashOperator classique exécuté sur le laptop", True, needle)
    pause("search_health_check")  # planifié toutes les 30 min : on le remet en pause

    step("Scénario 5c — worker team-search arrêté : la tâche attend dans la file, rien ne tourne ailleurs")
    story("Aucun autre worker n'écoute la queue team-search : ni le centre ni team-data ne la prennent.")
    kube("-n", "team-search", "scale", "deploy/team-search-worker", "--replicas=0")
    run(
        [
            "kubectl",
            "-n",
            "team-search",
            "wait",
            "--for=delete",
            "pod",
            "-l",
            "app=team-search-worker",
            "--timeout=120s",
        ],
        check=False,
    )
    show_workers("workers après scale 0 :")
    unpause("search_health_check")
    run_id = trigger("search_health_check")
    try:
        assert_stays_queued("search_health_check", run_id)
    finally:
        kube("-n", "team-search", "scale", "deploy/team-search-worker", "--replicas=1")
    wait_worker_state(WORKERS["team-search"], {"idle", "running"}, timeout=180)
    show_workers("workers après scale 1 :")
    r = wait_run("search_health_check", run_id)
    check("search_health_check exécuté dès le retour du worker", r["state"] == "success", run_id)
    pause("search_health_check")

    step("Scénario 5b — team-data-worker-1 en maintenance : il finit ses tâches mais n'en prend plus")
    w = WORKERS["team-data"]
    api("POST", f"/edge_worker/ui/worker/{w}/maintenance", {"maintenance_comment": "demo scenario isolation"})
    story(
        f"équivalent CLI : kubectl -n airflow-edge exec sts/airflow-scheduler -- airflow edge "
        f"remote-edge-worker-request-maintenance -H {w} -c 'demo'"
    )
    wait_worker_state(w, {"maintenance mode"})
    show_workers("workers (maintenance) :")
    unpause("data_ingest_orders")
    run_id = trigger("data_ingest_orders")
    try:
        assert_stays_queued("data_ingest_orders", run_id)
    finally:
        api("DELETE", f"/edge_worker/ui/worker/{w}/maintenance")
    wait_worker_state(w, {"idle", "running"})
    show_workers("workers (fin de maintenance) :")
    r = wait_run("data_ingest_orders", run_id)
    check("data_ingest_orders exécuté après la fin de maintenance", r["state"] == "success", run_id)
    assert_ran_on("data_ingest_orders", run_id, w, queue="team-data")


SCENARIOS = {
    "classic": scenario_classic,
    "assets": scenario_assets,
    "versioning": scenario_versioning,
    "hitl": scenario_hitl,
    "isolation": scenario_isolation,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("scenario", choices=[*SCENARIOS, "all"])
    args = parser.parse_args()
    if not sys.stdout.isatty():  # sortie redirigée (tee, CI) : UTF-8 plutôt que la page de code Windows
        sys.stdout.reconfigure(encoding="utf-8")
    names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    durations: dict[str, tuple[float, str]] = {}
    for name in names:
        t0, before = time.monotonic(), len(results)
        try:
            SCENARIOS[name]()
            status = "OK" if all(p for _, p in results[before:]) else "KO"
        except ScenarioError as err:
            check(f"{name} : {err}", False)
            status = "KO"
        durations[name] = (time.monotonic() - t0, status)

    step("Résumé")
    for name, (secs, status) in durations.items():
        (ok if status == "OK" else fail)(f"{name:<11} {secs:5.0f} s")
    failed = [n for n, p in results if not p]
    print(f"  {len(results) - len(failed)}/{len(results)} contrôles OK")
    for n in failed:
        fail(n)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
