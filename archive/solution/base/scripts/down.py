# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Démonte la plateforme : release Helm `airflow` puis namespace airflow-edge.

uv run scripts/down.py               # helm uninstall + suppression du namespace (et donc de ses PVC)
uv run scripts/down.py --keep-ns     # helm uninstall seulement (MinIO, ES, ConfigMaps et PVC conservés)
uv run scripts/down.py --keep-ns --volumes   # ... et suppression des PVC du namespace
uv run scripts/down.py --teams       # + namespaces team-* (data, search) et worker laptop (compose down)
"""

import argparse
import shutil

from _common import NAMESPACE, RELEASE, ROOT, ok, require, run, step


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--keep-ns", action="store_true", help="ne pas supprimer le namespace airflow-edge")
    parser.add_argument(
        "--volumes", action="store_true", help="supprimer les PVC (MinIO, ES, Postgres, logs)"
    )
    parser.add_argument("--teams", action="store_true", help="supprimer les namespaces des équipes")
    args = parser.parse_args()
    require("kubectl", "helm")

    step(f"helm uninstall {RELEASE}")
    run(["helm", "uninstall", RELEASE, "-n", NAMESPACE, "--wait"], check=False)

    if args.volumes:
        # Le StatefulSet postgres et les volumeClaimTemplates laissent leurs PVC après uninstall.
        step("Suppression des PVC")
        run(["kubectl", "delete", "pvc", "--all", "-n", NAMESPACE, "--wait=false"], check=False)

    if not args.keep_ns:
        step(f"Suppression du namespace {NAMESPACE} (PVC compris)")
        run(["kubectl", "delete", "namespace", NAMESPACE, "--ignore-not-found", "--wait=true"], check=False)

    if args.teams:
        step("Suppression des workers d'équipe (namespaces k8s + docker compose du laptop)")
        run(
            ["kubectl", "delete", "namespace", "team-data", "team-search"]
            + ["--ignore-not-found", "--wait=true"],
            check=False,
        )
        compose = ROOT / "teams" / "laptop" / "docker-compose.yml"
        if shutil.which("docker"):
            run(["docker", "compose", "-f", str(compose), "down"], check=False)

    ok("terminé")


if __name__ == "__main__":
    main()
