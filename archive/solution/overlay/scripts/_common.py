"""Fonctions partagées par les scripts (bibliothèque standard uniquement).

Principe : chaque script AFFICHE les commandes docker / kubectl / helm qu'il lance,
pour pouvoir les recopier à la main (voir platform/README.md).
Depuis Windows, `*.localhost` ne se résout pas hors navigateur : on appelle
http://127.0.0.1 (Traefik) avec l'en-tête Host.
"""

import json
import shlex
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NAMESPACE = "airflow-edge"
RELEASE = "airflow"
CHART = "apache-airflow/airflow"
CHART_VERSION = "1.22.0"
CENTRAL_IMAGE = "airflow-edge/airflow-central:3.3.2"
TEAMS = ["team-data", "team-search", "laptop", "team-iot"]  # team-iot : exercice docs/09

INGRESS = "http://127.0.0.1"
AIRFLOW_HOST = "airflow-edge.localhost"
MINIO_HOST = "minio.localhost"
ES_HOST = "es.localhost"
ADMIN = ("admin", "admin")

_COLORS = sys.stdout.isatty()


def color(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _COLORS else text


def step(message: str) -> None:
    print(f"\n{color('==>', '1;36')} {color(message, '1')}", flush=True)


def ok(message: str) -> None:
    print(f"  {color('OK', '32')}  {message}", flush=True)


def fail(message: str) -> None:
    print(f"  {color('KO', '31')}  {message}", flush=True)


def info(message: str) -> None:
    print(f"  {color('..', '33')}  {message}", flush=True)


def require(*tools: str) -> None:
    missing = [t for t in tools if shutil.which(t) is None]
    if missing:
        sys.exit(f"Outils introuvables dans le PATH : {', '.join(missing)}")


def run(
    cmd: list[str],
    *,
    check: bool = True,
    capture: bool = False,
    input_text: str | None = None,
    quiet: bool = False,
) -> subprocess.CompletedProcess:
    """Exécute une commande (sans shell) en l'affichant sous forme copiable."""
    if not quiet:
        print(color(f"  $ {shlex.join(cmd)}", "2"), flush=True)
    result = subprocess.run(
        cmd,
        text=True,
        encoding="utf-8",
        errors="replace",
        input=input_text,
        capture_output=capture,
        check=False,
    )
    if check and result.returncode != 0:
        if capture:
            sys.stderr.write(result.stdout + result.stderr)
        sys.exit(f"Échec (code {result.returncode}) : {shlex.join(cmd)}")
    return result


def kubectl(*args: str, **kwargs) -> subprocess.CompletedProcess:
    return run(["kubectl", "-n", NAMESPACE, *args], **kwargs)


def airflow_cli(*args: str, **kwargs) -> subprocess.CompletedProcess:
    """`airflow ...` dans le pod scheduler (accès DB + config du release).

    En LocalExecutor le chart déploie le scheduler en StatefulSet (PVC de logs).
    """
    return kubectl("exec", "sts/airflow-scheduler", "-c", "scheduler", "--", "airflow", *args, **kwargs)


def http(
    method: str,
    path: str,
    *,
    host: str = AIRFLOW_HOST,
    body: dict | None = None,
    token: str | None = None,
    timeout: float = 15,
) -> tuple[int, dict | str]:
    """Requête HTTP via l'Ingress Traefik. Renvoie (code, json ou texte)."""
    headers = {"Host": host, "Accept": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{INGRESS}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code, raw = resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as err:
        code, raw = err.code, err.read().decode("utf-8", "replace")
    except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
        return 0, str(err)
    try:
        return code, json.loads(raw)
    except json.JSONDecodeError:
        return code, raw


def airflow_token(username: str = ADMIN[0], password: str = ADMIN[1]) -> str:
    """Jeton JWT de l'API publique (FAB auth manager : POST /auth/token)."""
    code, data = http("POST", "/auth/token", body={"username": username, "password": password})
    if code not in (200, 201) or not isinstance(data, dict):
        sys.exit(f"Login {username} impossible ({code}) : {data}")
    return data["access_token"]


def wait_for(predicate, *, timeout: float, interval: float = 5, what: str = "condition"):
    """Rappelle `predicate()` jusqu'à une valeur vraie (renvoyée) ou expiration."""
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            sys.exit(f"Délai dépassé ({timeout:.0f}s) : {what}")
        time.sleep(interval)
