# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3"]
# ///
"""Vérification automatisée de la solution de l'exercice docs/09 (« Observatoire de la qualité de l'air »).

uv run archive/solution/verify.py 3             # une étape (1..15, 13.1-13.4, 13.6 ; 13 = 13.1-13.4)
uv run archive/solution/verify.py all           # étapes 1-10, 12, 14, 15 (non destructives, ~15 min)
uv run archive/solution/verify.py all --edge    # + 13.1-13.4 : maintenance, queues à chaud, worker-2, drain
uv run archive/solution/verify.py all --deadline  # + 11 (Deadline Alert, ~12 min) et 13.6 (queued_timeout)
uv run archive/solution/verify.py 1 --read-only   # contrôles en lecture seule (aucun déclenchement)

Lancer depuis la racine du dépôt. Les dépôts du simulateur utilisent des heures « fraîches » du jour UTC
(--hour-base, défaut : heure courante) : chaque étape est relançable (le DAG d'ingestion liste aussi
air/inbox/, un fichier resté en attente est traité au run suivant). 13.5 (kill -9) et 13.7 (version
mismatch) sont manuelles : la commande est affichée.
"""

import argparse
import os
import re
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from _common import (  # noqa: E402
    ADMIN,
    ES_HOST,
    INGRESS,
    MINIO_HOST,
    NAMESPACE,
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

os.environ["MSYS_NO_PATHCONV"] = "1"  # Git Bash : ne pas réécrire les chemins absolus de kubectl exec
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

INGEST = "iot_ingest_measurements"
SLOW = "iot_slow_task"
W1, W2 = "team-iot-worker-1", "team-iot-worker-2"
SEARCH_WORKER = "team-search-worker-1"
BUCKET = "datalake"
MAIL_HOST = "mailpit.localhost"
IOT_NS = "team-iot"
IOT_DAGS = {
    "iot_where_am_i",
    INGEST,
    "iot_hourly_stats",
    "iot_hourly_stats_runtime",
    "iot_manual_reconcile",
    "iot_reconcile",
    SLOW,
    "iot_station_report",
    "iot_weekly_recap",
}
TERMINAL = {"success", "failed"}
KEY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:00:00$")

results: list[tuple[str, bool]] = []
STATE: dict = {}  # passé d'une étape à l'autre (run de l'étape 3, ...)
ARGS: argparse.Namespace
_token: str | None = None
_s3 = None


class VerifyError(Exception):
    """Échec bloquant d'une étape (délai dépassé, code HTTP inattendu...)."""


# --------------------------------------------------------------------------- utilitaires


def api(method: str, path: str, body: dict | None = None, *, expect: tuple[int, ...] = (200,)):
    """Appel REST authentifié (jeton renouvelé si 401) ; VerifyError si code inattendu."""
    global _token
    if _token is None:
        _token = airflow_token(*ADMIN)
    code, data = http(method, path, body=body, token=_token, timeout=30)
    if code == 401:
        _token = airflow_token(*ADMIN)
        code, data = http(method, path, body=body, token=_token, timeout=30)
    if code not in expect:
        raise VerifyError(f"{method} {path} -> HTTP {code} : {str(data)[:300]}")
    return data


def api_code(method: str, path: str, body: dict | None = None) -> tuple[int, object]:
    """Comme api() mais renvoie (code, données) sans lever (contrôles négatifs)."""
    api("GET", "/api/v2/version")  # rafraîchit le jeton si besoin
    return http(method, path, body=body, token=_token, timeout=30)


def check(name: str, passed: bool, detail: str = "") -> bool:
    results.append((name, bool(passed)))
    (ok if passed else fail)(f"{name}{' : ' + detail if detail else ''}")
    return bool(passed)


def poll(predicate, *, timeout: float, what: str, interval: float = 3):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise VerifyError(f"délai dépassé ({timeout:.0f} s) : {what}")
        time.sleep(interval)


def manual(text: str) -> None:
    print(f"  {color('>>', '35')}  {text}", flush=True)


def q(run_id: str) -> str:
    return quote(run_id, safe="")


def short(host: str | None) -> str | None:
    return host.split(".")[0] if host else host


def today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def fresh_hour(offset: int) -> str:
    """Heure du jour UTC (base --hour-base + offset, modulo 24) au format --start du simulateur."""
    day = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    return (day + timedelta(hours=(ARGS.hour_base + offset) % 24)).strftime("%Y-%m-%dT%H:00")


# --------------------------------------------------------------------------- Airflow


def dag_runs(dag_id: str, query: str = "") -> list[dict]:
    return api("GET", f"/api/v2/dags/{dag_id}/dagRuns?order_by=-run_after&limit=100{query}")["dag_runs"]


def run_ids(dag_id: str) -> set[str]:
    return {r["dag_run_id"] for r in dag_runs(dag_id)}


def get_run(dag_id: str, run_id: str) -> dict:
    return api("GET", f"/api/v2/dags/{dag_id}/dagRuns/{q(run_id)}")


def unpause(*dag_ids: str) -> None:
    for dag_id in dag_ids:
        api("PATCH", f"/api/v2/dags/{dag_id}", {"is_paused": False})


def trigger(dag_id: str, conf: dict | None = None, note: str | None = None) -> str:
    body: dict = {"logical_date": None, "conf": conf or {}}
    if note:
        body["note"] = note
    rid = api("POST", f"/api/v2/dags/{dag_id}/dagRuns", body)["dag_run_id"]
    info(f"déclenché {dag_id} run_id={rid} conf={conf or {}}")
    return rid


def tis(dag_id: str, run_id: str) -> list[dict]:
    return api("GET", f"/api/v2/dags/{dag_id}/dagRuns/{q(run_id)}/taskInstances?limit=100")["task_instances"]


def ti_map(dag_id: str, run_id: str) -> dict[str, dict]:
    """task_id -> task instance (non mappées seulement)."""
    return {t["task_id"]: t for t in tis(dag_id, run_id) if t["map_index"] == -1}


def ti_state(dag_id: str, run_id: str, task_id: str) -> str | None:
    return (ti_map(dag_id, run_id).get(task_id) or {}).get("state")


def show_tis(dag_id: str, run_id: str) -> None:
    for t in sorted(tis(dag_id, run_id), key=lambda t: (t.get("start_date") or "9", t["task_id"])):
        idx = f"[{t['map_index']}]" if t["map_index"] >= 0 else ""
        print(
            f"        {t['task_id'] + idx:<24} {str(t['state']):<16} {t.get('hostname') or '-':<22} "
            f"{t.get('rendered_map_index') or ''}"
        )


def wait_ti(dag_id: str, run_id: str, task_id: str, states: set[str], timeout: float = 180) -> str:
    def reached():
        s = ti_state(dag_id, run_id, task_id)
        return s if s in states else None

    return poll(reached, timeout=timeout, what=f"{dag_id}.{task_id} dans l'état {sorted(states)}")


def wait_run(dag_id: str, run_id: str, timeout: float = 300, auto_hitl: bool = False) -> dict:
    """Attend success/failed ; auto_hitl : répond « ignore » à une décision HITL inattendue."""
    t0, answered = time.monotonic(), False

    def done():
        nonlocal answered
        r = get_run(dag_id, run_id)
        if auto_hitl and not answered and ti_state(dag_id, run_id, "hitl_decide") == "awaiting_input":
            info("décision HITL inattendue (fichier en dérive resté dans air/inbox/ ?) : réponse « ignore »")
            hitl_respond(run_id, "hitl_decide", ["ignore"])
            answered = True
        return r if r["state"] in TERMINAL else None

    r = poll(done, timeout=timeout, what=f"fin du run {dag_id}/{run_id}", interval=4)
    info(f"{dag_id} : {r['state']} en {time.monotonic() - t0:.0f} s")
    return r


def wait_new_runs(dag_id: str, before: set[str], timeout: float = 90) -> list[str]:
    def new():
        return sorted(run_ids(dag_id) - before)

    return poll(new, timeout=timeout, what=f"nouveau run de {dag_id}")


def task_log(
    dag_id: str, run_id: str, task_id: str, try_number: int | None = None, map_index: int = -1
) -> str:
    """Log via l'API (événements structurés 3.x + error_detail aplatis en texte)."""
    if try_number is None:
        tries = [t["try_number"] for t in tis(dag_id, run_id) if t["task_id"] == task_id]
        try_number = max(tries or [1]) or 1
    path = (
        f"/api/v2/dags/{dag_id}/dagRuns/{q(run_id)}/taskInstances/{task_id}/logs/{try_number}"
        f"?map_index={map_index}&full_content=true"
    )
    data = api("GET", path)
    content = data.get("content") if isinstance(data, dict) else data
    if not isinstance(content, list):
        return str(content)
    lines = []
    for e in content:
        if not isinstance(e, dict):
            lines.append(str(e))
            continue
        lines.append(str(e.get("event", "")))
        lines += [f"{d.get('exc_type')}: {d.get('exc_value')}" for d in e.get("error_detail") or []]
    return "\n".join(lines)


def wait_log(dag_id: str, run_id: str, task_id: str, needle: str, timeout: float = 60) -> str | None:
    """Le worker pousse ses logs par lots : attend la ligne voulue ; None si absente."""
    try:
        return poll(
            lambda: (t := task_log(dag_id, run_id, task_id)) and needle in t and t,
            timeout=timeout,
            what=f"'{needle}' dans le log {dag_id}.{task_id}",
            interval=5,
        )
    except VerifyError:
        return None


def grep(text: str | None, *needles: str) -> list[str]:
    return [ln.strip() for ln in (text or "").splitlines() if any(n in ln for n in needles)]


def asset_events(name: str, **filters) -> list[dict]:
    extra = "".join(f"&{k}={quote(str(v), safe='')}" for k, v in filters.items())
    path = f"/api/v2/assets/events?name_pattern={name}&order_by=-timestamp&limit=20{extra}"
    return [e for e in api("GET", path)["asset_events"] if e["name"] == name]


def hitl_respond(run_id: str, task_id: str, options: list[str], params: dict | None = None) -> dict:
    path = f"/api/v2/dags/{INGEST}/dagRuns/{q(run_id)}/taskInstances/{task_id}/-1/hitlDetails"
    return api("PATCH", path, {"chosen_options": options, "params_input": params or {}})


def workers() -> dict[str, dict]:
    return {w["worker_name"]: w for w in api("GET", "/edge_worker/ui/worker")["workers"]}


def wstate(name: str) -> tuple:
    w = workers().get(name) or {}
    return (
        w.get("state"),
        w.get("jobs_active"),
        w.get("queues"),
        (w.get("sysinfo") or {}).get("concurrency"),
    )


def edge_jobs() -> list[dict]:
    data = api("GET", "/edge_worker/ui/jobs")
    return data if isinstance(data, list) else data.get("jobs", [])


def watch_worker(name: str, secs: float, stop=None, interval: float = 3) -> list[tuple[int, tuple]]:
    """Trace les changements d'état d'un worker ; [(t, état)]."""
    seen: list[tuple[int, tuple]] = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < secs:
        s = wstate(name)
        if not seen or seen[-1][1] != s:
            seen.append((round(time.monotonic() - t0), s))
            info(f"[{name}] t+{seen[-1][0]}s state={s[0]} jobs={s[1]} queues={s[2]} concurrence={s[3]}")
        if stop and stop(s):
            break
        time.sleep(interval)
    return seen


def laptop_worker() -> str | None:
    for name, w in workers().items():
        if name.startswith("laptop-") and w["state"] in ("idle", "running"):
            return name
    return None


def edge_cli(*args: str) -> tuple[int, str]:
    """`airflow edge ...` au centre (pod scheduler)."""
    cmd = ["kubectl", "-n", NAMESPACE, "exec", "sts/airflow-scheduler", "-c", "scheduler", "--"]
    out = run([*cmd, "airflow", "edge", *args], capture=True, check=False)
    return out.returncode, (out.stdout + out.stderr).strip()


def kube(*args: str) -> tuple[int, str]:
    out = run(["kubectl", *args], capture=True, check=False)
    return out.returncode, (out.stdout + out.stderr).strip()


# --------------------------------------------------------------------------- MinIO, Mailpit, simulateur


def s3():
    """MinIO via l'Ingress (Host: minio.localhost injecté avant signature SigV4)."""
    global _s3
    if _s3 is None:
        _s3 = boto3.client(
            "s3",
            endpoint_url=INGRESS,
            region_name="us-east-1",
            aws_access_key_id="minio",
            aws_secret_access_key="minio12345",
            config=Config(s3={"addressing_style": "path"}),
        )
        _s3.meta.events.register_first(
            "before-sign.s3.*", lambda request, **_: request.headers.__setitem__("Host", MINIO_HOST)
        )
    return _s3


def s3_objects(prefix: str) -> dict[str, str]:
    """{clé: LastModified} sous prefix."""
    pages = s3().get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
    return {o["Key"]: str(o["LastModified"]) for p in pages for o in p.get("Contents", [])}


def mail_total() -> int:
    code, data = http("GET", "/api/v1/messages?limit=1", host=MAIL_HOST)
    return int(data.get("total", -1)) if code == 200 and isinstance(data, dict) else -1


def mail_subjects(n: int = 10) -> list[str]:
    code, data = http("GET", f"/api/v1/messages?limit={n}", host=MAIL_HOST)
    return [m.get("Subject", "") for m in data.get("messages", [])] if code == 200 else []


def simulator(*args: str) -> str:
    """`uv run scripts/air_stations.py ...` ; renvoie la sortie."""
    out = run(["uv", "run", str(ROOT / "scripts" / "air_stations.py"), *args], capture=True, check=False)
    if out.returncode != 0:
        raise VerifyError(f"air_stations.py en échec ({out.returncode}) : {out.stderr[-300:]}")
    return out.stdout


def deposit(
    offset: int, *, stations: int = 3, drift: str | None = None, samples: int | None = None
) -> list[str]:
    """Dépose des CSV pour une heure fraîche ; renvoie les clés écrites."""
    args = [
        "--stations",
        str(stations),
        "--hours",
        "1",
        "--start",
        fresh_hour(offset),
        "--seed",
        str(offset + 1),
    ]
    if drift:
        args += ["--drift", drift]
    if samples:
        args += ["--samples", str(samples)]
    out = simulator(*args)
    keys = re.findall(rf"s3://{BUCKET}/(\S+\.csv)", out)
    for line in out.strip().splitlines():
        info(f"dépôt : {line}")
    return keys


def deposit_and_wait_run(offset: int, **kwargs) -> tuple[list[str], str]:
    before = run_ids(INGEST)
    keys = deposit(offset, **kwargs)
    rid = wait_new_runs(INGEST, before)[0]
    info(f"run déclenché par le dépôt : {rid}")
    return keys, rid


def clean_processed_prefix() -> str | None:
    """Préfixe relatif (sous air/processed/) d'UN csv sans dérive (moyenne pm25 <= 45)."""
    for key in sorted(s3_objects("air/processed/"), reverse=True):
        if not key.endswith(".csv"):
            continue
        body = s3().get_object(Bucket=BUCKET, Key=key)["Body"].read().decode().splitlines()
        header = body[0].split(",")
        pm = [float(ln.split(",")[header.index("pm25")]) for ln in body[1:] if ln]
        if pm and sum(pm) / len(pm) <= 45 and min(pm) >= 0:
            return key[len("air/processed/") : -len(".csv")]
    return None


# --------------------------------------------------------------------------- étapes


def step1() -> None:
    step("Étape 1 — équipe team-iot : worker, bundle, DAGs, pool")
    w = workers().get(W1)
    check(f"{W1} enregistré", w is not None)
    if w:
        sysinfo = w.get("sysinfo") or {}
        check(f"{W1} en ligne", w["state"] in ("idle", "running"), w["state"])
        check(f"{W1} queues == ['team-iot']", w.get("queues") == ["team-iot"], str(w.get("queues")))
        py = str(sysinfo.get("python_version", ""))
        check("python 3.10 sur le worker", py.startswith("3.10"), py.split()[0] if py else "?")
        check(
            "edge3 4.3.2",
            sysinfo.get("edge_provider_version") == "4.3.2",
            str(sysinfo.get("edge_provider_version")),
        )
    dags = {d["dag_id"]: d for d in api("GET", "/api/v2/dags?limit=100")["dags"]}
    iot = {d for d, v in dags.items() if v.get("bundle_name") == "team-iot"}
    check("9 DAGs dans le bundle team-iot", IOT_DAGS <= iot, f"manquants={sorted(IOT_DAGS - iot)}")
    errors = api("GET", "/api/v2/importErrors?limit=100")["import_errors"]
    iot_err = [
        e for e in errors if e.get("bundle_name") == "team-iot" or "team-iot" in str(e.get("filename"))
    ]
    check("aucune erreur d'import team-iot", not iot_err, f"{len(errors)} erreur(s) au total")
    code, pool = api_code("GET", "/api/v2/pools/team-iot")
    check("pool team-iot", code == 200, f"slots={pool.get('slots') if isinstance(pool, dict) else '?'}")
    if ARGS.read_only:
        return
    unpause("iot_where_am_i")
    rid = trigger("iot_where_am_i")
    r = wait_run("iot_where_am_i", rid, 240)
    check("iot_where_am_i success", r["state"] == "success", rid)
    hosts = {short(t.get("hostname")) for t in tis("iot_where_am_i", rid)}
    check(f"tâches sur {W1}", hosts == {W1}, str(sorted(map(str, hosts))))
    log = wait_log("iot_where_am_i", rid, "report_environment", "sklearn")
    check("import sklearn réussi dans la tâche", log is not None, (grep(log, "sklearn") or ["absent"])[0])


def step2() -> None:
    step("Étape 2 — simulateur de stations (dry-run)")
    out = simulator("--stations", "1", "--samples", "3", "--dry-run")
    check("sortie marquée (dry-run)", "(dry-run)" in out)
    check("en-tête CSV", "ts,station,lat,lon,pm25,pm10,no2,o3,temp" in out)
    out = simulator("--stations", "1", "--drift", "st-01", "--dry-run", "--seed", "1")
    m = re.search(r"pm25 moyen=([\d.]+)", out)
    mean = float(m.group(1)) if m else -1
    check("--drift : moyenne pm25 > 45 (seuil de dérive du DAG)", mean > 45, f"{mean}")


def step3() -> None:
    step("Étape 3 — ingestion événementielle (AssetWatcher + S3NewObjectTrigger)")
    unpause(INGEST)
    pending = [k for k in s3_objects("air/inbox/") if k.endswith(".csv")]
    if pending:
        info(f"{len(pending)} fichier(s) déjà en attente dans air/inbox/ (seront traités avec le dépôt)")
    t0 = time.monotonic()
    keys, rid = deposit_and_wait_run(0)
    info(f"run créé {time.monotonic() - t0:.0f} s après le dépôt (poll du trigger : 15 s)")
    STATE["ingest_run"], STATE["ingest_keys"] = rid, keys
    r = get_run(INGEST, rid)
    check("run_type asset_triggered", r["run_type"] == "asset_triggered", rid)
    ev = asset_events("air_inbox")
    extra = ev[0]["extra"] if ev else {}
    payload = extra.get("payload") or {}
    check("événement air_inbox : from_trigger", extra.get("from_trigger") is True)
    check(
        "payload.keys contient les 3 clés déposées",
        isinstance(payload.get("keys"), list) and set(keys) <= set(payload["keys"]),
        f"{len(payload.get('keys') or [])} clé(s)",
    )
    r = wait_run(INGEST, rid, 180, auto_hitl=True)
    check("run d'ingestion success", r["state"] == "success")
    up = api("GET", f"/api/v2/dags/{INGEST}/dagRuns/{q(rid)}/upstreamAssetEvents")["asset_events"]
    check("upstreamAssetEvents du run", len(up) >= 1, f"{len(up)} événement(s)")
    inbox, processed = s3_objects("air/inbox/"), s3_objects("air/processed/")
    check("fichiers retirés de air/inbox/", not set(keys) & set(inbox))
    moved = {"air/processed/" + k[len("air/inbox/") :] for k in keys}
    check("fichiers déplacés sous air/processed/", moved <= set(processed))
    meas = s3_objects(f"air/measurements/ds={today()}/")
    expected = {f"air/measurements/ds={today()}/{k.split('/')[2]}-{k[-8:-4]}.parquet" for k in keys}
    check(f"parquets sous air/measurements/ds={today()}/", expected <= set(meas), f"{len(meas)} objet(s)")

    info("redépôt des MÊMES fichiers : un seul nouveau run attendu, pas de boucle")
    before = run_ids(INGEST)
    deposit(0)
    new = wait_new_runs(INGEST, before)
    for nrid in new:
        wait_run(INGEST, nrid, 180, auto_hitl=True)
    time.sleep(40)
    new = sorted(run_ids(INGEST) - before)
    check(
        "redépôt : 1 run (2 si le dépôt chevauche un poll), pas de boucle", 1 <= len(new) <= 2, f"{len(new)}"
    )


def last_ingest_run() -> str:
    if "ingest_run" in STATE:
        return STATE["ingest_run"]
    runs = [r for r in dag_runs(INGEST) if r["run_type"] == "asset_triggered" and r["state"] == "success"]
    if not runs:
        raise VerifyError("aucun run asset_triggered réussi de iot_ingest_measurements (étape 3 d'abord)")
    return runs[0]["dag_run_id"]


def step4() -> None:
    step("Étape 4 — mapping nommé, params, dry_run")
    rid = last_ingest_run()
    mapped = api(
        "GET", f"/api/v2/dags/{INGEST}/dagRuns/{q(rid)}/taskInstances/parse_file/listMapped?limit=100"
    )
    names = [t.get("rendered_map_index") for t in mapped["task_instances"]]
    check(
        "parse_file : rendered_map_index = clé du fichier",
        bool(names) and all(str(n).startswith("air/") and str(n).endswith(".csv") for n in names),
        f"{names[:3]}",
    )
    if "ingest_keys" in STATE:
        check("les clés déposées sont des index mappés", set(STATE["ingest_keys"]) <= set(names))
    params = api("GET", f"/api/v2/dags/{INGEST}/details").get("params") or {}
    check(
        "params stations / reprocess_prefix / dry_run",
        {"stations", "reprocess_prefix", "dry_run"} <= set(params),
    )
    if ARGS.read_only:
        return
    prefix = clean_processed_prefix()
    if not check("csv sans dérive disponible sous air/processed/", prefix is not None):
        return
    before = s3_objects("air/measurements/")
    rid = trigger(INGEST, {"dry_run": True, "reprocess_prefix": prefix})
    r = wait_run(INGEST, rid, 240)
    check("run dry_run success", r["state"] == "success", f"reprocess_prefix={prefix}")
    check("publish skipped (dry_run)", ti_state(INGEST, rid, "publish") == "skipped")
    check("dry_run : air/measurements/ inchangé", s3_objects("air/measurements/") == before)
    check("dry_run : pas de marqueur air/tmp/<run_id>/", not s3_objects(f"air/tmp/{rid}/"))


def step5() -> None:
    step("Étape 5 — échec volontaire (notifiers, teardown) et court-circuit")
    if ARGS.read_only:
        return
    prefix = clean_processed_prefix()
    if not check("csv sans dérive disponible sous air/processed/", prefix is not None):
        return
    mails = mail_total()
    rid = trigger(INGEST, {"fail_publish": True, "reprocess_prefix": prefix})
    r = wait_run(INGEST, rid, 240)
    check("run fail_publish en échec", r["state"] == "failed", rid)
    states = {k: v["state"] for k, v in ti_map(INGEST, rid).items()}
    check("publish failed", states.get("publish") == "failed", str(states.get("publish")))
    check(
        "report failed/upstream_failed",
        states.get("report") in ("failed", "upstream_failed"),
        str(states.get("report")),
    )
    check("drop_tmp_prefix (teardown) success", states.get("drop_tmp_prefix") == "success")
    try:
        poll(lambda: mail_total() > mails, timeout=90, what="mail d'échec dans Mailpit", interval=5)
        check("mail d'échec reçu dans Mailpit", True, f"total {mails} -> {mail_total()}")
    except VerifyError as err:
        check("mail d'échec reçu dans Mailpit", False, str(err))
    alert = f"air/alerts/{INGEST}-{rid}.json"
    try:
        poll(lambda: alert in s3_objects("air/alerts/"), timeout=90, what=alert, interval=5)
        check("MinioAlertNotifier : objet air/alerts/", True, alert)
    except VerifyError as err:
        check("MinioAlertNotifier : objet air/alerts/", False, str(err))

    rid = trigger(INGEST, {"reprocess_prefix": "none/"})
    r = wait_run(INGEST, rid, 180)
    states = {k: v["state"] for k, v in ti_map(INGEST, rid).items()}
    check("run sans fichier success", r["state"] == "success", rid)
    check(
        "has_work success (court-circuit)", states.get("has_work") == "success", str(states.get("has_work"))
    )
    downstream = {t: states.get(t) for t in ("quality_gate", "publish")}
    parse = {t["state"] for t in tis(INGEST, rid) if t["task_id"] == "parse_file"}
    check(
        "aval skipped",
        set(downstream.values()) == {"skipped"} and parse <= {"skipped"},
        f"{downstream} parse_file={sorted(map(str, parse))}",
    )
    check("drop_tmp_prefix success malgré tout", states.get("drop_tmp_prefix") == "success")


def wait_hitl(rid: str, task_id: str = "hitl_decide") -> None:
    t0 = time.monotonic()
    wait_ti(INGEST, rid, task_id, {"awaiting_input"}, timeout=180)
    check(f"{task_id} en awaiting_input", True, f"{time.monotonic() - t0:.0f} s")


def step6() -> None:
    step("Étape 6 — HITL : calibrate / quarantine + incident / ignore")
    if ARGS.read_only:
        return
    unpause(INGEST)
    mails = mail_total()
    # --- calibrate
    _, rid = deposit_and_wait_run(1, drift="st-02")
    wait_hitl(rid)
    try:
        poll(lambda: wstate(W1)[1] == 0, timeout=40, what="0 job actif")
        check(f"pendant l'attente : 0 job actif sur {W1}", True)
    except VerifyError as err:
        check(f"pendant l'attente : 0 job actif sur {W1}", False, str(err))
    pending = api("GET", "/api/v2/dags/~/dagRuns/~/hitlDetails?response_received=false&limit=100")
    mine = [h for h in pending["hitl_details"] if h["task_instance"]["dag_run_id"] == rid]
    check("hitlDetails en attente listé", bool(mine), f"{pending['total_entries']} en attente au total")
    if mine:
        h = mine[0]
        check(
            "options calibrate/quarantine/ignore", h.get("options") == ["calibrate", "quarantine", "ignore"]
        )
        check("param offset", "offset" in (h.get("params") or {}))
    try:
        poll(lambda: mail_total() > mails, timeout=60, what="mail HITL", interval=5)
        subjects = mail_subjects(5)
        check(
            "mail « décision attendue »",
            any("décision attendue" in s for s in subjects),
            (subjects or [""])[0],
        )
    except VerifyError as err:
        check("mail « décision attendue »", False, str(err))
    resp = hitl_respond(rid, "hitl_decide", ["calibrate"], {"offset": -40})
    check(
        "réponse API enregistrée par admin (id 1)",
        (resp.get("responded_by") or {}).get("id") == "1",
        str(resp.get("responded_by")),
    )
    r = wait_run(INGEST, rid, 300)
    check("run calibrate success", r["state"] == "success", rid)
    cal = ti_map(INGEST, rid).get("apply_calibration") or {}
    check(
        f"apply_calibration success sur {W1}",
        cal.get("state") == "success" and short(cal.get("hostname")) == W1,
    )
    ev = asset_events("air_measurements", source_dag_id=INGEST, source_run_id=rid)
    offset = ev[0]["extra"].get("calibration_offset") if ev else None
    check("air_measurements : calibration_offset == -40", offset == -40, str(offset))

    # --- quarantine + incident
    incident = f"capteur st-03 encrassé (verify {datetime.now(UTC):%H:%M})"
    keys, rid = deposit_and_wait_run(2, drift="st-03")
    wait_hitl(rid)
    hitl_respond(rid, "hitl_decide", ["quarantine"])
    wait_hitl(rid, "incident_note")
    hitl_respond(rid, "incident_note", ["OK"], {"incident": incident})
    r = wait_run(INGEST, rid, 300)
    check("run quarantine success", r["state"] == "success", rid)
    ev = asset_events("air_quarantine", source_dag_id=INGEST, source_run_id=rid)
    extra = ev[0]["extra"] if ev else {}
    check(
        "air_quarantine : stations ['st-03']", extra.get("stations") == ["st-03"], str(extra.get("stations"))
    )
    check("air_quarantine : texte de l'incident", extra.get("incident") == incident)
    q_key = next(("air/quarantine/" + k.split("/", 2)[2] for k in keys if "/st-03/" in k), "")
    check("objet sous air/quarantine/st-03/", q_key in s3_objects("air/quarantine/st-03/"), q_key)

    # --- ignore (+ contrôle négatif)
    _, rid = deposit_and_wait_run(3, drift="st-01")
    wait_hitl(rid)
    code, _ = api_code(
        "PATCH",
        f"/api/v2/dags/{INGEST}/dagRuns/{q(rid)}/taskInstances/hitl_decide/-1/hitlDetails",
        {"chosen_options": ["bogus"], "params_input": {}},
    )
    check("option inconnue refusée (4xx)", 400 <= code < 500, f"HTTP {code}")
    hitl_respond(rid, "hitl_decide", ["ignore"])
    r = wait_run(INGEST, rid, 300)
    states = {k: v["state"] for k, v in ti_map(INGEST, rid).items()}
    check("run ignore success", r["state"] == "success", rid)
    check("publish success", states.get("publish") == "success")
    check(
        "apply_calibration / incident_note skipped",
        states.get("apply_calibration") == "skipped" and states.get("incident_note") == "skipped",
    )
    x = api(
        "GET", f"/api/v2/dags/{INGEST}/dagRuns/{q(rid)}/taskInstances/hitl_decide/xcomEntries/hitl_response"
    )
    value = x.get("value") if isinstance(x, dict) else None
    chosen = value.get("chosen_options") if isinstance(value, dict) else None
    check("XCom hitl_response : chosen_options ['ignore']", chosen == ["ignore"], str(value)[:120])
    manual("UI : Browse → Required Actions ; utilisateur non assigné → 403 (à vérifier à la main)")


def latest_finished(dag_id: str, timeout: float = 90) -> dict:
    """Dernier run du DAG, attendu terminé (il vient peut-être d'être déclenché par l'étape 6)."""

    def finished():
        runs = dag_runs(dag_id, "&limit=1")
        return runs and runs[0]["state"] in TERMINAL and runs[0]

    try:
        return poll(finished, timeout=timeout, what=f"fin du dernier run de {dag_id}", interval=5)
    except VerifyError as err:
        info(str(err))
        runs = dag_runs(dag_id, "&limit=1")
        return runs[0] if runs else {}


def step7() -> None:
    step("Étape 7 — alias, expressions d'assets, temps + assets")
    names = {a["name"] for a in api("GET", "/api/v2/assets?name_pattern=air_station&limit=100")["assets"]}
    want = {"air_station_st-01", "air_station_st-02", "air_station_st-03"}
    check("assets air_station_st-01..03 (alias air_stations)", want <= names, str(sorted(names)))
    r = latest_finished("iot_station_report")
    check(
        "iot_station_report : dernier run asset_triggered success",
        r.get("run_type") == "asset_triggered" and r.get("state") == "success",
        f"{r.get('dag_run_id')} {r.get('state')}",
    )
    if r:
        log = task_log("iot_station_report", r["dag_run_id"], "show_events")
        check("alias : triggering_asset_events renseigné", "triggering_asset_events renseigné" in log)
    runs = [x for x in dag_runs("search_index_air") if x["state"] == "success"]
    if check("search_index_air : un run success", bool(runs)):
        log = task_log("search_index_air", runs[0]["dag_run_id"], "fetch_measurements")
        check("Asset.ref : triggering_asset_events vide", "triggering_asset_events : vide" in log)
    latest_finished("iot_reconcile")
    rec = [
        x for x in dag_runs("iot_reconcile") if x["run_type"] == "asset_triggered" and x["state"] == "success"
    ]
    check("iot_reconcile : run asset_triggered success", bool(rec), f"{len(rec)} run(s)")
    expr = api("GET", "/api/v2/dags/iot_reconcile/details").get("asset_expression") or {}
    has_all = any(isinstance(e, dict) and "all" in e for e in expr.get("any", []))
    check("asset_expression : any[ all[...], asset_ref ]", "any" in expr and has_all, str(expr)[:120])
    latest_finished("iot_weekly_recap")
    wk = [x for x in dag_runs("iot_weekly_recap") if x["run_type"] == "asset_triggered"]
    check("iot_weekly_recap : run asset_triggered", bool(wk), f"{len(wk)} run(s)")


def step8() -> None:
    step("Étape 8 — partitions d'assets")
    hourly = [r for r in dag_runs("iot_hourly_stats") if r["run_type"] == "scheduled"][:10]
    check("iot_hourly_stats : runs planifiés", bool(hourly), f"{len(hourly)} run(s)")
    bad = [r for r in hourly if not KEY_RE.match(str(r.get("partition_key"))) or r.get("logical_date")]
    check(
        "partition_key %Y-%m-%dT%H:00:00 et logical_date None",
        bool(hourly) and not bad,
        f"ex. {hourly[0].get('partition_key') if hourly else '-'}",
    )
    if ARGS.read_only:
        return
    key = f"{today()}T12:00:00"
    unpause("iot_hourly_stats_runtime", "laptop_air_bulletin")
    rid = trigger("iot_hourly_stats_runtime", {"hours": [key]})
    r = wait_run("iot_hourly_stats_runtime", rid, 180)
    check("iot_hourly_stats_runtime success", r["state"] == "success", rid)

    def event():
        ev = asset_events("air_hourly", source_dag_id="iot_hourly_stats_runtime", source_run_id=rid)
        return ev and ev[0].get("created_dagruns") and ev[0]

    try:
        ev = poll(event, timeout=60, what="événement air_hourly avec created_dagruns")
    except VerifyError as err:
        check("événement air_hourly partitionné", False, str(err))
        return
    check(
        "air_hourly : partition_key de l'événement",
        ev.get("partition_key") == key,
        str(ev.get("partition_key")),
    )
    bulletins = [c for c in ev["created_dagruns"] if c["dag_id"] == "laptop_air_bulletin"]
    check(
        "laptop_air_bulletin créé pour la partition jour",
        bool(bulletins) and bulletins[0].get("partition_key") == today(),
        str([(c["dag_id"], c.get("partition_key")) for c in ev["created_dagruns"]]),
    )
    if not bulletins:
        return
    if not check(
        "worker laptop en ligne", laptop_worker() is not None, "cd teams/laptop && docker compose up -d"
    ):
        return
    r = wait_run("laptop_air_bulletin", bulletins[0]["run_id"], 180)
    check(
        "laptop_air_bulletin success (partition jour)",
        r["state"] == "success" and r.get("partition_key") == today(),
        f"{r['dag_run_id']} {r['state']} partition_key={r.get('partition_key')}",
    )


def step9() -> None:
    step("Étape 9 — team-search : Asset.ref, sensors, Elasticsearch")
    runs = dag_runs("search_index_air", "&limit=5")
    last = next((x for x in runs if x["state"] in TERMINAL), None)
    check(
        "search_index_air : dernier run terminé success",
        bool(last) and last["state"] == "success",
        str(last and last["dag_run_id"]),
    )
    if last:
        t = ti_map("search_index_air", last["dag_run_id"])
        tasks = ("wait_es", "wait_es_poke", "fetch_measurements", "index_air")
        hosts = {k: short(t.get(k, {}).get("hostname")) for k in tasks}
        check(f"tâches sur {SEARCH_WORKER}", set(hosts.values()) == {SEARCH_WORKER}, str(hosts))
    code, data = http("GET", "/air/_count", host=ES_HOST)
    n = data.get("count", 0) if code == 200 and isinstance(data, dict) else -1
    check("ES : index air alimenté", n > 0, f"{n} document(s)")
    code, data = http("GET", "/air/_mapping", host=ES_HOST)
    loc = (
        data.get("air", {}).get("mappings", {}).get("properties", {}).get("location", {})
        if code == 200
        else {}
    )
    check("ES : location en geo_point", loc.get("type") == "geo_point")
    if ARGS.read_only:
        return
    unpause("search_index_air")
    rid = trigger("search_index_air", {"es_wait_for_status": "green"})
    try:
        wait_ti("search_index_air", rid, "wait_es", {"deferred"}, timeout=90)
        check("wait_es deferred (statut green jamais atteint)", True)
        try:
            poll(lambda: wstate(SEARCH_WORKER)[1] == 0, timeout=40, what="0 job actif", interval=2)
            check(f"pendant la deferral : 0 job actif sur {SEARCH_WORKER}", True)
        except VerifyError as err:
            check(f"pendant la deferral : 0 job actif sur {SEARCH_WORKER}", False, str(err))
        t = api("GET", f"/api/v2/dags/search_index_air/dagRuns/{q(rid)}/taskInstances/wait_es")
        check("GET taskInstances/wait_es : deferred", t.get("state") == "deferred")
    except VerifyError as err:
        check("wait_es deferred", False, str(err))
    finally:
        api("PATCH", f"/api/v2/dags/search_index_air/dagRuns/{q(rid)}", {"state": "failed"})
        info(f"run {rid} marqué failed (fin de l'observation)")


def step10() -> None:
    step("Étape 10 — laptop : backfill 7 jours, run sans logical_date, venv en cache")
    if ARGS.read_only:
        return
    if not check(
        "worker laptop en ligne", laptop_worker() is not None, "cd teams/laptop && docker compose up -d"
    ):
        return
    dag = "laptop_air_bulletin_cron"
    d0 = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    days = [(d0 - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7, 0, -1)]
    body = {
        "dag_id": dag,
        "from_date": f"{days[0]}T00:00:00Z",
        "to_date": f"{d0:%Y-%m-%d}T00:00:00Z",
        "reprocess_behavior": "completed",
        "max_active_runs": 2,
    }
    unpause(dag)  # un backfill d'un DAG en pause reste queued
    try:
        dry = api("POST", "/api/v2/backfills/dry_run", body)
        dates = sorted(str(b.get("logical_date"))[:10] for b in dry.get("backfills", []))
        check("dry_run : 7 dates", dates == days, str(dates))
        created = datetime.now(UTC).isoformat()
        bf = api("POST", "/api/v2/backfills", {**body, "dag_run_conf": {"source": "verify docs/09"}})
        info(f"backfill id={bf.get('id')} max_active_runs={bf.get('max_active_runs')}")
        peak, t0 = 0, time.monotonic()
        latest: dict[str, dict] = {}
        while time.monotonic() - t0 < 600:
            latest = {}
            for x in dag_runs(dag, "&run_type=backfill"):
                ds = str(x.get("logical_date"))[:10]
                if ds in days and (
                    ds not in latest or str(x.get("queued_at")) > str(latest[ds].get("queued_at"))
                ):
                    latest[ds] = x
            peak = max(peak, sum(1 for x in latest.values() if x["state"] == "running"))
            fresh = [x for x in latest.values() if str(x.get("end_date") or "") >= created[:19]]
            if len(fresh) == 7 and all(x["state"] in TERMINAL for x in fresh):
                break
            time.sleep(10)
        states = sorted(str(x["state"]) for x in latest.values())
        check(
            "7 runs backfill success", states == ["success"] * 7, f"{states} en {time.monotonic() - t0:.0f} s"
        )
        check("au plus 2 runs simultanés", peak <= 2, f"max observé {peak}")

        ds8 = (d0 - timedelta(days=8)).strftime("%Y-%m-%d")
        rid = trigger(dag, {"ds": ds8})
        r = get_run(dag, rid)
        check(
            "run manuel : logical_date None, run_after renseigné",
            r["logical_date"] is None and bool(r["run_after"]),
        )
        r = wait_run(dag, rid, 180)
        check("run manuel success", r["state"] == "success", f"ds={ds8}")
        t = ti_map(dag, rid).get("render_chart") or {}
        if t.get("start_date") and t.get("end_date"):
            dur = (
                datetime.fromisoformat(t["end_date"]) - datetime.fromisoformat(t["start_date"])
            ).total_seconds()
            check("render_chart < 4 s (venv en cache)", dur < 4, f"{dur:.1f} s")
        objs = s3_objects("air/bulletin/")
        missing = [
            f"{d}{ext}"
            for d in [*days, ds8]
            for ext in (".md", ".png")
            if f"air/bulletin/{d}{ext}" not in objs
        ]
        check("air/bulletin/<ds>.md et .png pour les 8 jours", not missing, f"manquants={missing}")
    finally:
        api("PATCH", f"/api/v2/dags/{dag}", {"is_paused": True})
        info(f"{dag} remis en pause")
    manual(
        f"CLI : kubectl -n {NAMESPACE} exec sts/airflow-scheduler -c scheduler -- airflow backfill create "
        f"--dag-id {dag} --from-date {days[-2]} --to-date {days[-1]} --dry-run"
    )


def step11() -> None:
    step("Étape 11 — Deadline Alert (worker en maintenance, ~12 min)")
    unpause(INGEST)
    code, alerts = api_code("GET", f"/ui/dags/{INGEST}/deadlineAlerts")
    total = alerts.get("total_entries") if isinstance(alerts, dict) else None
    check(
        "deadline alert déclarée (GET /ui/dags/<dag>/deadlineAlerts)",
        code == 200 and bool(total),
        str(alerts)[:150],
    )
    api("POST", f"/edge_worker/ui/worker/{W1}/maintenance", {"maintenance_comment": "verify étape 11"})
    rid = None
    try:
        poll(lambda: wstate(W1)[0] == "maintenance mode", timeout=180, what=f"{W1} en maintenance")
        mails = mail_total()
        _, rid = deposit_and_wait_run(4, stations=1)
        time.sleep(30)
        check(
            "run en file (worker en maintenance)",
            ti_state(INGEST, rid, "new_keys") in ("queued", "scheduled"),
        )
        dl_path = f"/ui/dags/{INGEST}/dagRuns/{q(rid)}/deadlines"
        code, dl = api_code("GET", dl_path)
        listed = isinstance(dl, dict) and dl.get("total_entries", 0) >= 1
        check("échéance listée pour le run (page Deadlines)", code == 200 and listed, str(dl)[:200])
        alert = f"air/alerts/deadline-{INGEST}-{rid}.json"
        t0 = time.monotonic()

        def fired():
            code, d = api_code("GET", dl_path)
            missed = any(x.get("missed") for x in (d.get("deadlines", []) if isinstance(d, dict) else []))
            return missed or alert in s3_objects("air/alerts/") or mail_total() > mails

        try:
            poll(fired, timeout=720, what="dépassement de l'échéance", interval=15)
            info(f"échéance dépassée après {time.monotonic() - t0 + 30:.0f} s d'attente")
        except VerifyError as err:
            info(str(err))
        try:
            poll(lambda: alert in s3_objects("air/alerts/"), timeout=60, what=alert, interval=5)
        except VerifyError:
            pass
        check("callback : objet air/alerts/deadline-...", alert in s3_objects("air/alerts/"), alert)
        info(f"Mailpit : {mails} -> {mail_total()} message(s)")
    except VerifyError as err:
        check("étape 11", False, str(err))
    finally:
        api("DELETE", f"/edge_worker/ui/worker/{W1}/maintenance")
        info(f"{W1} sorti de maintenance")
    if rid:
        r = wait_run(INGEST, rid, 300, auto_hitl=True)
        check("le run finit quand même (l'alerte n'annule rien)", r["state"] == "success", rid)


def step12() -> None:
    step("Étape 12 — XCom sur object storage, logs distants S3")
    if ARGS.read_only:
        return
    unpause(INGEST)
    keys, rid = deposit_and_wait_run(5, stations=1, samples=3600)
    r = wait_run(INGEST, rid, 300, auto_hitl=True)
    check("run (3600 lignes) success", r["state"] == "success", rid)
    mapped = api(
        "GET", f"/api/v2/dags/{INGEST}/dagRuns/{q(rid)}/taskInstances/parse_file/listMapped?limit=100"
    )
    idx = next(
        (t["map_index"] for t in mapped["task_instances"] if t.get("rendered_map_index") == keys[0]), 0
    )
    x = api(
        "GET",
        f"/api/v2/dags/{INGEST}/dagRuns/{q(rid)}/taskInstances/parse_file/xcomEntries/return_value?map_index={idx}",
    )
    value = str(x.get("value"))
    check(
        "XCom parse_file = référence s3://minio_s3@datalake/xcom/",
        value.startswith("s3://minio_s3@datalake/xcom/"),
        value[:110],
    )
    xcom = s3_objects(f"xcom/{INGEST}/{rid}/")
    check("objet sous datalake/xcom/", bool(xcom), f"{len(xcom)} objet(s)")
    logs = s3_objects(f"logs/dag_id={INGEST}/run_id={rid}/")
    check("logs sous datalake/logs/dag_id=.../run_id=.../", bool(logs), f"{len(logs)} objet(s)")
    data = api(
        "GET", f"/api/v2/dags/{INGEST}/dagRuns/{q(rid)}/taskInstances/new_keys/logs/1?full_content=true"
    )
    check("log new_keys lisible via l'API", bool(data.get("content")))
    manual(
        f"kubectl -n {NAMESPACE} delete pod -l component=api-server → l'ancien log reste lisible (à la main)"
    )


def step13_1() -> None:
    step("Étape 13.1 — maintenance : API, redémarrage en maintenance, CLI locale")
    info("observé le 2026-10-17 : request → pending 12 s, pending → mode 79 s (fin de la tâche en cours) ;")
    info("après delete pod : offline maintenance 3 s, maintenance request 9 s, maintenance mode 12 s")
    unpause(SLOW)
    check(f"{W1} idle au départ", wstate(W1)[0] in ("idle", "running"), str(wstate(W1)[0]))
    rid1 = trigger(SLOW, {"seconds": 90}, note="verify 13.1")
    poll(lambda: wstate(W1)[0] == "running", timeout=90, what=f"{W1} running")
    api("POST", f"/edge_worker/ui/worker/{W1}/maintenance", {"maintenance_comment": "verify 13.1"})
    seen = watch_worker(W1, 180, stop=lambda s: s[0] == "maintenance mode")
    states = [s[0] for _, s in seen]
    check(
        "maintenance request/pending avant maintenance mode",
        {"maintenance request", "maintenance pending"} & set(states[:-1])
        and states[-1] == "maintenance mode",
        " → ".join(states),
    )
    check("la tâche en cours a fini avant le mode maintenance", get_run(SLOW, rid1)["state"] == "success")
    rid2 = trigger(SLOW, {"seconds": 20}, note="verify 13.1 pendant la maintenance")
    time.sleep(15)
    job = [j for j in edge_jobs() if j.get("run_id") == rid2]
    check(
        "nouveau job en file, non pris",
        bool(job) and job[0]["state"] == "queued" and not job[0].get("edge_worker"),
        str(job[:1]),
    )
    check("commentaire de maintenance", "verify 13.1" in str(workers()[W1].get("maintenance_comments")))
    kube("-n", IOT_NS, "delete", "pod", "-l", "app=team-iot-worker", "--wait=false")
    went_offline: list[str] = []

    def back_in_maintenance(s: tuple) -> bool:
        if s[0] and s[0].startswith(("offline", "unknown")):
            went_offline.append(s[0])
        return bool(went_offline) and s[0] == "maintenance mode"

    seen = watch_worker(W1, 150, stop=back_in_maintenance, interval=2)
    states = [s[0] for _, s in seen]
    check(
        "redémarrage : offline maintenance puis retour en maintenance",
        "offline maintenance" in states and states[-1] == "maintenance mode",
        " → ".join(states),
    )
    check(
        "le run soumis pendant la maintenance attend toujours",
        get_run(SLOW, rid2)["state"] in ("queued", "running"),
    )
    code, out = 1, ""
    for _ in range(5):
        code, out = kube(
            "-n", IOT_NS, "exec", "deploy/team-iot-worker", "--", "airflow", "edge", "maintenance", "off"
        )
        if code == 0:
            break
        time.sleep(10)
    check("CLI locale : airflow edge maintenance off", code == 0, out[-80:].replace("\n", " "))
    watch_worker(W1, 90, stop=lambda s: s[0] in ("idle", "running"))
    r = wait_run(SLOW, rid2, 180)
    check("le run en file s'exécute après la maintenance", r["state"] == "success", rid2)


def step13_2() -> None:
    step("Étape 13.2 — queues et concurrence à chaud")
    info("observé le 2026-10-17 : queue ajoutée visible tout de suite ; échec en 3 s : le worker team-iot")
    info(
        "n'a pas le bundle team-data (« Requested bundle 'team-data' is not configured »), avant même polars"
    )
    api(
        "POST",
        "/edge_worker/ui/worker/team-data-worker-1/maintenance",
        {"maintenance_comment": "verify 13.2"},
    )
    rid = None
    try:
        code, out = edge_cli("add-worker-queues", "-H", W1, "-q", "team-data")
        check("add-worker-queues team-data", code == 0, out.splitlines()[-1][-100:] if out else "")
        code, out = edge_cli("set-worker-concurrency", "-H", W1, "-c", "3")
        check("set-worker-concurrency 3", code == 0)
        try:
            poll(
                lambda: "team-data" in (wstate(W1)[2] or []) and wstate(W1)[3] == 3,
                timeout=60,
                what="queue + concurrence",
            )
            check("queues + concurrence 3 visibles", True, str(wstate(W1)))
        except VerifyError as err:
            check("queues + concurrence 3 visibles", False, str(err))
        unpause("data_ingest_orders")
        rid = trigger("data_ingest_orders", note="verify 13.2 : queue team-data servie par team-iot")
        state = wait_ti("data_ingest_orders", rid, "list_regions", {"failed", "up_for_retry", "success"}, 240)
        t = ti_map("data_ingest_orders", rid)["list_regions"]
        check(
            f"list_regions prise par {W1} et en échec",
            state in ("failed", "up_for_retry") and short(t.get("hostname")) == W1,
            f"{state} sur {t.get('hostname')}",
        )
        log = task_log("data_ingest_orders", rid, "list_regions", t["try_number"])
        reason = grep(log, "bundle 'team-data'", "ModuleNotFoundError", "polars")
        check(
            "cause : environnement team-data absent (bundle ou polars)",
            bool(reason),
            reason[0][:120] if reason else "?",
        )
    finally:
        if rid:
            api_code("PATCH", f"/api/v2/dags/data_ingest_orders/dagRuns/{q(rid)}", {"state": "failed"})
        edge_cli("remove-worker-queues", "-H", W1, "-q", "team-data")
        api("DELETE", "/edge_worker/ui/worker/team-data-worker-1/maintenance")
    try:
        poll(lambda: wstate(W1)[2] == ["team-iot"], timeout=60, what="queues == ['team-iot']")
        check("queue team-data retirée", True)
    except VerifyError as err:
        check("queue team-data retirée", False, str(err))


def step13_3() -> None:
    step("Étape 13.3 — deux workers sur la même queue")
    info("observé le 2026-10-17 : 3 jobs / 3 jobs, 67 s pour 6 runs de 30 s (premier poll gagnant)")
    unpause(SLOW)
    code, out = kube("apply", "-f", str(ROOT / "teams" / "team-iot" / "k8s" / "worker-2.yaml"))
    check("kubectl apply worker-2.yaml", code == 0, out[-80:])
    try:
        poll(lambda: wstate(W2)[0] == "idle", timeout=180, what=f"{W2} idle")
        rids = [trigger(SLOW, {"seconds": 30}, note=f"verify 13.3 run {i}") for i in range(6)]
        t0, seen = time.monotonic(), {}
        while time.monotonic() - t0 < 300:
            for j in edge_jobs():
                if j.get("run_id") in rids and j.get("edge_worker"):
                    seen[j["run_id"]] = j["edge_worker"]
            if all(get_run(SLOW, r)["state"] in TERMINAL for r in rids):
                break
            time.sleep(3)
        split = {w: list(seen.values()).count(w) for w in (W1, W2)}
        states = [get_run(SLOW, r)["state"] for r in rids]
        check("6 runs success", states == ["success"] * 6, str(states))
        check(
            "les deux workers ont pris des jobs",
            split[W1] > 0 and split[W2] > 0,
            f"{split} en {time.monotonic() - t0:.0f} s",
        )
    except VerifyError as err:
        check("étape 13.3", False, str(err))
    finally:
        kube("-n", IOT_NS, "delete", "deploy", "team-iot-worker-2", "--ignore-not-found")
    try:
        poll(lambda: wstate(W2)[0] in ("offline", None), timeout=120, what=f"{W2} offline")
        check(f"{W2} offline après suppression", True)
    except VerifyError as err:
        check(f"{W2} offline après suppression", False, str(err))


def step13_4() -> None:
    step("Étape 13.4 — delete pod pendant une tâche : preStop → SIGINT → drain")
    unpause(SLOW)
    concurrency = wstate(W1)[3]
    info(f"concurrence avant redémarrage : {concurrency} (13.2 la passe à 3 par set-worker-concurrency)")
    rid = trigger(SLOW, {"seconds": 60}, note="verify 13.4 drain")
    wait_ti(SLOW, rid, "sleep_a_while", {"running"}, 120)
    time.sleep(5)
    t0 = time.monotonic()
    kube("-n", IOT_NS, "delete", "pod", "-l", "app=team-iot-worker", "--wait=false")
    watch_worker(W1, 180, stop=lambda s: s[0] == "idle" and time.monotonic() - t0 > 30)
    r = wait_run(SLOW, rid, 180)
    t = ti_map(SLOW, rid)["sleep_a_while"]
    check(
        "tâche terminée pendant le drain (sans retry)",
        r["state"] == "success" and t["try_number"] == 1,
        f"try={t['try_number']}",
    )
    s = wstate(W1)
    check("queues après redémarrage == ['team-iot'] (-q fait foi)", s[2] == ["team-iot"], str(s[2]))
    try:
        s = poll(lambda: (x := wstate(W1))[3] == 2 and x, timeout=40, what="concurrence == 2", interval=5)
    except VerifyError:
        pass
    check("concurrence après redémarrage == 2 (valeur -c du worker)", s[3] == 2, f"{concurrency} -> {s[3]}")
    info("la concurrence revient à la valeur -c du worker (set-worker-concurrency perdu au redémarrage)")


def step13_6() -> None:
    step("Étape 13.6 — queue sans worker : révocation après task_queued_timeout (600 s)")
    info("observé le 2026-10-17 : révocation + « Task stuck in queued; will try to requeue » à 661 s,")
    info("même try_number, pas d'échec : la tâche est remise en file")
    unpause(SLOW)
    kube("-n", IOT_NS, "scale", "deploy/team-iot-worker", "--replicas=0")
    rid = None
    try:
        poll(lambda: wstate(W1)[0] in ("offline", "unknown"), timeout=150, what=f"{W1} offline")
        rid = trigger(SLOW, {"seconds": 5}, note="verify 13.6 queue sans worker")
        wait_ti(SLOW, rid, "sleep_a_while", {"queued"}, 60)
        t0 = time.monotonic()

        def revoked():
            jobs = [j for j in edge_jobs() if j.get("run_id") == rid]
            return not jobs or ti_state(SLOW, rid, "sleep_a_while") not in ("queued",)

        time.sleep(30)
        check("tâche en file, job edge présent", any(j.get("run_id") == rid for j in edge_jobs()))
        poll(revoked, timeout=780, what="révocation du job", interval=15)
        check(
            "job révoqué / tâche sortie de la file",
            True,
            f"après {time.monotonic() - t0 + 30:.0f} s ; état {ti_state(SLOW, rid, 'sleep_a_while')}",
        )
    except VerifyError as err:
        check("étape 13.6", False, str(err))
    finally:
        kube("-n", IOT_NS, "scale", "deploy/team-iot-worker", "--replicas=1")
    poll(lambda: wstate(W1)[0] in ("idle", "running"), timeout=180, what=f"{W1} de retour")
    if rid:
        r = wait_run(SLOW, rid, 300)
        info(f"run final : {r['state']} (retries=1 : relancé au retour du worker ?)")


def step13_manual() -> None:
    step("Étapes 13.5 / 13.7 — manuelles")
    manual(
        f"13.5 : kubectl -n {IOT_NS} exec deploy/team-iot-worker -- "
        "sh -c 'kill -9 $(cat /opt/airflow/airflow-edge-worker.pid)'"
    )
    manual("       pendant un run de iot_slow_task (seconds=150)")
    info("observé le 2026-10-17 (13.5) : unknown à t+145 s (5 × heartbeat_interval du scheduler = 30 s,")
    info("pas celui du worker = 10 s) ; pod en CrashLoopBackOff « A worker with the name 'team-iot-worker-1'")
    info("is already active » ; idle à t+205 s (5 redémarrages) ; up_for_retry à t+301 s, retry success")
    manual("13.7 : image team-iot avec apache-airflow-providers-edge3==4.3.1 → HTTP 400 ;")
    manual(
        f"       kubectl -n {NAMESPACE} set env deploy/airflow-api-server "
        "AIRFLOW__EDGE__MINIMUM_ACCEPTABLE_EDGE_VERSION_FOR_WORKERS=4.3.1"
    )
    info("observé le 2026-10-17 (13.7) : HTTP 400 « Version mismatch of Edge worker and Core. Shutting down")
    info("worker. » ; avec la version minimale : status_text « Healthy but version mismatch »")


def step14() -> None:
    step("Étape 14 — plugin UI air_views")
    plugins = {p["name"]: p for p in api("GET", "/api/v2/plugins?limit=100")["plugins"]}
    p = plugins.get("air_views")
    check("plugin air_views chargé", p is not None, str(p and p.get("source")))
    names = [v["name"] for v in (p or {}).get("external_views", [])]
    check("external_views", names == ["Mailpit", "MinIO Console", "Fichiers temporaires du run"], str(names))
    views = {v["name"]: v for v in (p or {}).get("external_views", [])}
    check("Mailpit en iframe (url_route)", (views.get("Mailpit") or {}).get("url_route") == "mailpit")
    check(
        "vue de run avec {RUN_ID}",
        "{RUN_ID}" in str((views.get("Fichiers temporaires du run") or {}).get("href")),
    )
    code, _ = http("GET", "/", host=MAIL_HOST)
    check("UI Mailpit joignable", code == 200, f"HTTP {code}")


def step15() -> None:
    step("Étape 15 — bonus : GitDagBundle")
    runs = dag_runs("iot_where_am_i", "&limit=1")
    version = runs[0].get("bundle_version") if runs else None
    if version:
        check("GitDagBundle actif : bundle_version du run", True, version)
    else:
        info("bundle local : étape 15.a non appliquée (bundle_version nul)")
    code, data = http("GET", "/api/v1/version", host="gitea.localhost")
    info(f"Gitea {'déployé (' + str(data.get('version')) + ')' if code == 200 else 'absent'} (HTTP {code})")


STEPS = {
    "1": step1,
    "2": step2,
    "3": step3,
    "4": step4,
    "5": step5,
    "6": step6,
    "7": step7,
    "8": step8,
    "9": step9,
    "10": step10,
    "11": step11,
    "12": step12,
    "13.1": step13_1,
    "13.2": step13_2,
    "13.3": step13_3,
    "13.4": step13_4,
    "13.6": step13_6,
    "13.m": step13_manual,
    "14": step14,
    "15": step15,
}
EDGE = ["13.1", "13.2", "13.3", "13.4", "13.m"]


def plan(target: str, edge: bool, deadline: bool) -> list[str]:
    if target == "13":
        return EDGE
    if target != "all":
        return [target]
    names = [str(i) for i in range(1, 11)] + (["11"] if deadline else []) + ["12"]
    names += (EDGE if edge else []) + (["13.6"] if deadline else []) + ["14", "15"]
    return names


def main() -> None:
    global ARGS
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("etape", choices=[*STEPS, "13", "all"], help="étape à vérifier")
    parser.add_argument("--edge", action="store_true", help="avec all : ajoute 13.1-13.4 (~10 min)")
    parser.add_argument("--deadline", action="store_true", help="avec all : ajoute 11 et 13.6 (~25 min)")
    parser.add_argument(
        "--hour-base", type=int, default=datetime.now(UTC).hour, help="heure UTC de base des dépôts"
    )
    parser.add_argument("--read-only", action="store_true", help="aucun déclenchement ni dépôt")
    ARGS = parser.parse_args()
    if not sys.stdout.isatty():
        sys.stdout.reconfigure(encoding="utf-8")
    durations: dict[str, tuple[float, str]] = {}
    for name in plan(ARGS.etape, ARGS.edge, ARGS.deadline):
        t0, before = time.monotonic(), len(results)
        try:
            STEPS[name]()
            status = "OK" if all(p for _, p in results[before:]) else "KO"
        except VerifyError as err:
            check(f"étape {name} : {err}", False)
            status = "KO"
        durations[name] = (time.monotonic() - t0, status)

    step("Résumé")
    for name, (secs, status) in durations.items():
        (ok if status == "OK" else fail)(f"étape {name:<5} {secs:5.0f} s")
    failed = [n for n, p in results if not p]
    print(f"  {len(results) - len(failed)}/{len(results)} contrôles OK")
    for n in failed:
        fail(n)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
