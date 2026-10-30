# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Publie les DAGs d'une équipe : UNE commande, DEUX ConfigMaps `dags-<team>` (même contenu).

- ns `airflow-edge` : montée (sans subPath) dans le dag-processor et le scheduler sous
  /opt/airflow/dags/<team> -> parsing / planification (bundle <team> = <montage>/..data) ;
- ns `<team>` (si ce namespace existe : team-data, team-search) : montée dans le pod du worker
  edge au même chemin -> exécution. Le laptop (docker compose) monte ./dags directement.

Kubelet propage la mise à jour en ~1 min, puis le dag-processor la voit au rafraîchissement
suivant (30 s) ; le worker, lui, relit le fichier à chaque tâche : pas de rebuild d'image.

Fichiers publiés : tous les `*.py` de teams/<team>/dags/ (modules utilitaires compris, ex.
search_common.py ; ni __pycache__ ni tests). `--version vN` superpose teams/<team>/versions/vN/*.py
(les fichiers de même nom remplacent ceux de dags/, les autres DAGs de l'équipe restent publiés).

uv run scripts/dags.py apply team-data                 # teams/team-data/dags/*.py
uv run scripts/dags.py apply team-data --version v2    # dags/*.py + versions/v2/*.py par-dessus
uv run scripts/dags.py delete team-data
uv run scripts/dags.py status
"""

import argparse
import sys
from collections import Counter
from pathlib import Path

from _common import NAMESPACE, ROOT, TEAMS, airflow_token, fail, http, info, ok, require, run, step

LABEL = "app.kubernetes.io/part-of=airflow-edge-demo"


def dag_files(src: Path) -> dict[str, Path]:
    """`*.py` d'un dossier (niveau 1 : une clé de ConfigMap ne contient pas de '/'), hors tests."""
    return {
        f.name: f
        for f in sorted(src.glob("*.py"))  # niveau 1 seulement : jamais __pycache__/*.pyc
        if f.is_file()
        and not (f.name.startswith("test_") or f.name.endswith("_test.py") or f.name == "conftest.py")
    }


def collect(team: str, version: str | None) -> dict[str, Path]:
    base = ROOT / "teams" / team
    files = dag_files(base / "dags")
    if version:
        vdir = base / "versions" / version
        overlay = dag_files(vdir)
        if not overlay:
            sys.exit(f"Aucun fichier .py dans {vdir}")
        files.update(overlay)
    if not files:
        sys.exit(f"Aucun fichier .py dans {base / 'dags'}")
    return files


def target_namespaces(team: str) -> list[str]:
    """airflow-edge toujours ; + le namespace de l'équipe s'il existe (worker k8s)."""
    exists = run(["kubectl", "get", "namespace", team, "-o", "name"], check=False, capture=True, quiet=True)
    return [NAMESPACE, team] if exists.returncode == 0 else [NAMESPACE]


def apply(team: str, version: str | None) -> None:
    files = collect(team, version)
    namespaces = target_namespaces(team)
    origin = f"dags/ + versions/{version}/" if version else "dags/"
    step(
        f"ConfigMap dags-{team} <- teams/{team}/{origin} ({len(files)} fichier(s)) "
        f"-> ns {', '.join(namespaces)}"
    )
    for ns in namespaces:
        # Équivalent : kubectl create configmap ... --dry-run=client -o yaml | kubectl apply -f -
        manifest = run(
            [
                "kubectl",
                "create",
                "configmap",
                f"dags-{team}",
                "-n",
                ns,
                *[f"--from-file={name}={path}" for name, path in files.items()],
                "--dry-run=client",
                "-o",
                "yaml",
            ],
            capture=True,
        ).stdout
        run(["kubectl", "apply", "-f", "-"], input_text=manifest)
        run(
            [
                "kubectl",
                "-n",
                ns,
                "label",
                "configmap",
                f"dags-{team}",
                "--overwrite",
                LABEL,
                f"airflow-edge/dag-version={version or 'current'}",
            ]
        )
        ok(f"{ns}/dags-{team} : {', '.join(files)}")
    if len(namespaces) == 1 and team != "laptop":
        info(
            f"namespace {team} absent : seul le centre est à jour "
            f"(kubectl apply -k teams/{team}/k8s puis relancer)"
        )
    info("propagation kubelet (~60 s) + refresh du bundle (30 s) avant apparition dans l'UI")


def delete(team: str) -> None:
    for ns in target_namespaces(team):
        step(f"Suppression de la ConfigMap {ns}/dags-{team}")
        run(["kubectl", "-n", ns, "delete", "configmap", f"dags-{team}", "--ignore-not-found"])
    ok("supprimée(s) : le répertoire du bundle redevient vide, ses DAGs disparaissent au refresh suivant")


def status() -> None:
    step("ConfigMaps de DAGs d'équipe (centre + namespaces d'équipe)")
    run(["kubectl", "get", "configmap", "-A", "-l", LABEL, "-L", "airflow-edge/dag-version"], check=False)

    step("Bundles connus de la base (table dag_bundle)")
    # Pas d'endpoint REST "dagBundles" en 3.3 : on lit la table remplie par le dag-processor.
    run(
        [
            "kubectl",
            "-n",
            NAMESPACE,
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
            "-c",
            "SELECT name, active, last_refreshed FROM dag_bundle ORDER BY name;",
        ],
        check=False,
    )

    step("DAGs par bundle (GET /api/v2/dags)")
    token = airflow_token()
    code, data = http("GET", "/api/v2/dags?limit=100", token=token)
    if code != 200:
        fail(f"/api/v2/dags -> {code}")
        return
    dags = data["dags"]
    for bundle, n in sorted(Counter(d.get("bundle_name") for d in dags).items(), key=lambda x: str(x[0])):
        names = ", ".join(sorted(d["dag_id"] for d in dags if d.get("bundle_name") == bundle))
        ok(f"{bundle}: {n} DAG(s) -> {names}")
    code, data = http("GET", "/api/v2/importErrors", token=token)
    if code == 200 and data.get("total_entries"):
        for err in data["import_errors"]:
            last = err["stack_trace"].strip().splitlines()[-1]
            fail(f"[{err.get('bundle_name')}] {err['filename']}: {last}")
    else:
        ok("aucune erreur d'import")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="action", required=True)
    p_apply = sub.add_parser("apply")
    p_apply.add_argument("team", choices=TEAMS)
    p_apply.add_argument("--version", help="ex. v2 : superpose teams/<team>/versions/<v>/*.py à dags/")
    p_delete = sub.add_parser("delete")
    p_delete.add_argument("team", choices=TEAMS)
    sub.add_parser("status")
    args = parser.parse_args()
    require("kubectl")

    if args.action == "apply":
        apply(args.team, args.version)
    elif args.action == "delete":
        delete(args.team)
    else:
        status()


if __name__ == "__main__":
    main()
