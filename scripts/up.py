# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Monte la plateforme centrale (relançable sans risque).

uv run scripts/up.py              # image centrale si absente, MinIO, ES, Airflow, connexions, DAGs d'équipe
uv run scripts/up.py --build      # reconstruit l'image centrale d'abord
uv run scripts/up.py --skip-helm  # seulement externes + connexions/variables + DAGs d'équipe
uv run scripts/up.py --teams      # + raccourci "tout-en-un" : workers des équipes (voir ci-dessous)

Étapes (= commandes manuelles de platform/README.md) :
 1. docker build de l'image centrale (si absente ou --build)
 2. kubectl apply namespace + MinIO + Elasticsearch, attente
 3. helm repo add/update apache-airflow, helm upgrade --install airflow (chart 1.22.0), attente des pods
 4. connexions minio_s3 / elasticsearch_default + variable datalake_bucket (CLI dans le scheduler)
 5. ConfigMaps dags-<team> pour chaque teams/<team>/dags/*.py existant

--teams (raccourci de démo ; la voie "normale" reste celle des équipes, cf. teams/<team>/README.md) :
 a. images worker absentes construites (docker build teams/<team>)
 b. kubectl apply -k teams/{team-data,team-search}/k8s (avant l'étape 5, pour que
    dags.py écrive aussi la ConfigMap dans le namespace de l'équipe), puis attente des workers
 c. docker compose -f teams/laptop/docker-compose.yml up -d (worker hors cluster)
"""

import argparse
import json

import build
import dags
from _common import (
    CENTRAL_IMAGE,
    CHART,
    CHART_VERSION,
    NAMESPACE,
    RELEASE,
    ROOT,
    TEAMS,
    airflow_cli,
    info,
    kubectl,
    ok,
    require,
    run,
    step,
)

CONNECTIONS = {
    "minio_s3": [
        "--conn-type",
        "aws",
        "--conn-login",
        "minio",
        "--conn-password",
        "minio12345",
        "--conn-extra",
        json.dumps(
            {
                "endpoint_url": "http://minio.airflow-edge.svc.cluster.local:9000",
                "region_name": "us-east-1",
            }
        ),
    ],
    "elasticsearch_default": [
        "--conn-type",
        "elasticsearch",
        "--conn-host",
        "elasticsearch.airflow-edge.svc.cluster.local",
        "--conn-port",
        "9200",
        "--conn-schema",
        "http",
    ],
}
VARIABLES = {"datalake_bucket": "datalake"}


def externals() -> None:
    step("Namespace, MinIO, Elasticsearch")
    for manifest in ["namespace.yaml", "minio/minio.yaml", "elasticsearch/elasticsearch.yaml"]:
        run(["kubectl", "apply", "-f", str(ROOT / "platform" / manifest)])
    kubectl("rollout", "status", "deploy/minio", "--timeout=180s")
    kubectl("wait", "--for=condition=complete", "job/minio-init", "--timeout=180s")
    kubectl("rollout", "status", "deploy/elasticsearch", "--timeout=300s")
    ok("MinIO (bucket datalake) et Elasticsearch prêts")


def helm_repo() -> None:
    step("Dépôt Helm apache-airflow")
    # --force-update : idempotent (pas d'erreur "already exists" si le dépôt est déjà déclaré).
    run(["helm", "repo", "add", "apache-airflow", "https://airflow.apache.org", "--force-update"])
    run(["helm", "repo", "update", "apache-airflow"])


def airflow() -> None:
    helm_repo()
    step(f"helm upgrade --install {RELEASE} ({CHART} {CHART_VERSION})")
    run(
        [
            "helm",
            "upgrade",
            "--install",
            RELEASE,
            CHART,
            "-n",
            NAMESPACE,
            "--version",
            CHART_VERSION,
            "-f",
            str(ROOT / "platform" / "helm-values" / "airflow.yaml"),
            "--timeout",
            "15m",
        ]
    )
    # Jobs "normaux" (pas de hooks) : la migration conditionne le démarrage des autres pods.
    # (ttlSecondsAfterFinished=300 : un Job déjà terminé peut avoir disparu => check=False)
    kubectl(
        "wait",
        "--for=condition=complete",
        "job/airflow-run-airflow-migrations",
        "--timeout=600s",
        check=False,
    )
    for workload in [
        "deploy/airflow-api-server",
        "sts/airflow-scheduler",
        "deploy/airflow-dag-processor",
        "deploy/airflow-triggerer",
    ]:
        kubectl("rollout", "status", workload, "--timeout=600s")
    kubectl("wait", "--for=condition=complete", "job/airflow-create-user", "--timeout=300s", check=False)
    ok("Airflow déployé : http://airflow-edge.localhost (admin / admin)")


def connections() -> None:
    step("Connexions et variables (CLI dans le pod scheduler)")
    for conn_id, args in CONNECTIONS.items():
        # Idempotent : delete (échec ignoré si absente) puis add.
        airflow_cli("connections", "delete", conn_id, check=False, capture=True)
        # (sortie capturée : la CLI charge tous les providers et émet beaucoup de warnings)
        out = airflow_cli("connections", "add", conn_id, *args, capture=True).stdout
        ok(next((line for line in out.splitlines() if "Successfully" in line), conn_id))
    for key, value in VARIABLES.items():
        out = airflow_cli("variables", "set", key, value, capture=True).stdout
        ok(next((line for line in out.splitlines() if "Variable" in line), key))
    ok(f"connexions {', '.join(CONNECTIONS)} ; variables {', '.join(VARIABLES)}")


def team_dags() -> None:
    step("DAGs d'équipe (ConfigMaps)")
    found = False
    for team in TEAMS:
        if list((ROOT / "teams" / team / "dags").glob("*.py")):
            found = True
            dags.apply(team, None)
    if not found:
        info("aucun teams/<team>/dags/*.py : rien à publier")


K8S_TEAMS = ["team-data", "team-search"]


def team_image(team: str) -> str:
    return f"airflow-edge/worker-{team}:dev"


def ensure_image(image: str, build_fn) -> None:
    present = run(["docker", "image", "inspect", image], check=False, capture=True, quiet=True)
    if present.returncode != 0:
        build_fn()
    else:
        info(f"{image} déjà construite (uv run scripts/build.py pour reconstruire)")


def team_workers_k8s() -> None:
    step("Workers d'équipe dans k3s (kustomize, normalement lancé par chaque équipe)")
    for team in K8S_TEAMS:
        ensure_image(team_image(team), lambda team=team: build.build_team(team))
        run(["kubectl", "apply", "-k", str(ROOT / "teams" / team / "k8s")])


def wait_team_workers() -> None:
    step("Attente des workers d'équipe")
    for team in K8S_TEAMS:
        run(["kubectl", "-n", team, "rollout", "status", f"deploy/{team}-worker", "--timeout=300s"])
    ok(f"workers prêts : {', '.join(K8S_TEAMS)}")


def laptop_worker() -> None:
    step("Worker laptop (docker compose, hors cluster)")
    ensure_image(team_image("laptop"), lambda: build.build_team("laptop"))
    run(["docker", "compose", "-f", str(ROOT / "teams" / "laptop" / "docker-compose.yml"), "up", "-d"])
    ok("laptop-worker démarré (docker compose -f teams/laptop/docker-compose.yml logs -f)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--build", action="store_true", help="reconstruire l'image centrale")
    parser.add_argument("--skip-helm", action="store_true", help="ne pas (re)déployer le chart")
    parser.add_argument(
        "--teams", action="store_true", help="déployer aussi les workers d'équipe (raccourci de démo)"
    )
    args = parser.parse_args()
    require("docker", "kubectl", "helm")

    if args.build:
        build.build_central()
    else:
        ensure_image(CENTRAL_IMAGE, build.build_central)

    externals()
    if not args.skip_helm:
        airflow()
    connections()
    if args.teams:
        team_workers_k8s()
    team_dags()
    if args.teams:
        wait_team_workers()
        laptop_worker()

    step("Terminé")
    ok("UI      : http://airflow-edge.localhost  (admin / admin)")
    ok("MinIO   : http://minio-console.localhost (minio / minio12345)")
    ok("ES      : http://es.localhost")
    if not args.teams:
        info("workers : kubectl apply -k teams/<team>/k8s + dags.py apply <team> (ou up.py --teams)")
    info("vérification : uv run scripts/smoke_test.py [--run] puis uv run scripts/scenario.py all")


if __name__ == "__main__":
    main()
