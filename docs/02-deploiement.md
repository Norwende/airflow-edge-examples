# 02 — Déploiement pas à pas

Chaque script **affiche** les commandes `docker` / `kubectl` / `helm` qu'il lance : on peut toujours
les recopier à la main. Ce document donne les deux colonnes côte à côte.

Toutes les commandes se lancent **depuis la racine du repo**, en PowerShell ou en Git Bash.
En Git Bash, préfixer par `MSYS_NO_PATHCONV=1` toute commande qui passe un chemin absolu Linux
(`kubectl exec ... /opt/...`), sinon il devient `C:/Program Files/Git/opt/...`.

## 0. Prérequis

| Outil | Version utilisée | Remarque |
|---|---|---|
| Rancher Desktop | k3s, moteur **dockerd (moby)** | les images construites par `docker build` sont visibles par k3s (`imagePullPolicy: IfNotPresent`), pas de registry |
| kubectl | 1.35 | contexte `rancher-desktop` |
| helm | 4.2 | dépôt `apache-airflow` (ajouté et mis à jour par `up.py`, ou à la main ci-dessous) |
| uv | 0.10 | lance les scripts PEP 723 (`uv run scripts/x.py`), installe Python si besoin |
| Docker Compose | v2 (fourni par Rancher Desktop) | worker laptop |

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
kubectl config use-context rancher-desktop
```

## 1. Images

| Script | Équivalent manuel |
|---|---|
| `uv run scripts/build.py --central` | `docker build -t airflow-edge/airflow-central:3.3.2 platform/central-image` |
| `uv run scripts/build.py --team team-data` | `docker build -t airflow-edge/worker-team-data:dev teams/team-data` |
| `uv run scripts/build.py --team team-search` | `docker build -t airflow-edge/worker-team-search:dev teams/team-search` |
| `uv run scripts/build.py --team laptop` | `docker build -t airflow-edge/worker-laptop:dev teams/laptop` (ou `docker compose build` dans `teams/laptop`) |
| `uv run scripts/build.py` | les quatre |

Vérifier les versions embarquées (elles doivent être **identiques** au centre pour Airflow et edge3) :

```bash
docker run --rm airflow-edge/worker-team-data:dev python -c "import airflow, sys; from airflow.providers.edge3 import __version__ as v; print(airflow.__version__, v, sys.version)"
```

## 2. Plateforme centrale

`uv run scripts/up.py` enchaîne les étapes 2.1 à 2.4 (relançable ; `--build` reconstruit l'image centrale,
`--skip-helm` saute le chart). `uv run scripts/up.py --teams` fait en plus l'étape 3 : images worker
absentes, `kubectl apply -k` des deux équipes **avant** la publication des ConfigMaps, puis
`docker compose up -d` du laptop.

### 2.1 Namespace et services externes

```bash
kubectl apply -f platform/namespace.yaml
kubectl apply -f platform/minio/minio.yaml
kubectl apply -f platform/elasticsearch/elasticsearch.yaml
kubectl -n airflow-edge rollout status deploy/minio
kubectl -n airflow-edge wait --for=condition=complete job/minio-init --timeout=180s
kubectl -n airflow-edge rollout status deploy/elasticsearch --timeout=300s
```

### 2.2 Airflow (chart officiel)

```bash
helm upgrade --install airflow apache-airflow/airflow -n airflow-edge --version 1.22.0 -f platform/helm-values/airflow.yaml --timeout 15m
kubectl -n airflow-edge wait --for=condition=complete job/airflow-run-airflow-migrations --timeout=600s
kubectl -n airflow-edge rollout status deploy/airflow-api-server --timeout=600s
kubectl -n airflow-edge rollout status sts/airflow-scheduler --timeout=600s
kubectl -n airflow-edge rollout status deploy/airflow-dag-processor --timeout=600s
kubectl -n airflow-edge rollout status deploy/airflow-triggerer --timeout=600s
```

Les Jobs du chart (migration, création de l'utilisateur `admin`) ne sont pas des hooks Helm
(`useHelmHooks: false`) et ont un `ttlSecondsAfterFinished: 300` : passé 5 min, `kubectl wait job/...`
échoue parce que le Job a disparu, ce n'est pas une erreur.

### 2.3 Connexions et variable (CLI dans le scheduler)

```bash
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow connections add minio_s3 --conn-type aws --conn-login minio --conn-password minio12345 --conn-extra '{"endpoint_url": "http://minio.airflow-edge.svc.cluster.local:9000", "region_name": "us-east-1"}'
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow connections add elasticsearch_default --conn-type elasticsearch --conn-host elasticsearch.airflow-edge.svc.cluster.local --conn-port 9200 --conn-schema http
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow variables set datalake_bucket datalake
```

> En PowerShell 5.1, les guillemets doubles à l'intérieur de `'...'` passés à un exécutable natif sont
> mal transmis : utiliser `up.py` (qui passe les arguments sans shell) ou Git Bash pour `--conn-extra`.
> `up.py` fait `connections delete` puis `add` : relançable.

### 2.4 DAGs des équipes (ConfigMaps)

| Script | Équivalent manuel |
|---|---|
| `uv run scripts/dags.py apply team-data` | `kubectl create configmap dags-team-data -n airflow-edge --from-file=teams/team-data/dags/ --dry-run=client -o yaml \| kubectl apply -f -` puis la même chose avec `-n team-data` |
| `uv run scripts/dags.py apply team-data --version v2` | idem avec `--from-file=data_ingest_orders.py=teams/team-data/versions/v2/data_ingest_orders.py` qui remplace la clé du même nom |
| `uv run scripts/dags.py status` | `kubectl get configmap -A -l app.kubernetes.io/part-of=airflow-edge-demo -L airflow-edge/dag-version` |
| `uv run scripts/dags.py delete team-data` | `kubectl -n airflow-edge delete configmap dags-team-data` (+ ns `team-data`) |

`dags.py` ne publie que les `*.py` de premier niveau (pas les tests) et écrit dans le namespace de
l'équipe **seulement s'il existe**. `up.py` publie les trois équipes au centre ; il faut relancer
`dags.py apply <team>` **après** avoir créé le namespace de l'équipe (étape 3).

## 3. Workers des équipes

### 3.1 team-data et team-search (Kubernetes)

```bash
kubectl apply -k teams/team-data/k8s
kubectl apply -k teams/team-search/k8s
uv run scripts/dags.py apply team-data
uv run scripts/dags.py apply team-search
kubectl -n team-data rollout status deploy/team-data-worker
kubectl -n team-search rollout status deploy/team-search-worker
```

Chaque `k8s/` contient un Namespace, le Secret `airflow-edge-worker` (clé `jwt-secret`) et le Deployment.
Tant que la ConfigMap `dags-<team>` n'existe pas dans le namespace de l'équipe, le pod reste en
`ContainerCreating` (volume non optionnel, voulu) puis démarre seul.

### 3.2 laptop (docker compose, hors cluster)

```bash
cd teams/laptop
docker compose up -d --build
docker compose logs -f
```

Le worker s'appelle `laptop-<USERNAME>` (variable Windows `USERNAME`, `dev` sinon). Il rejoint
l'api-server par Traefik : `extra_hosts: airflow-edge.localhost:host-gateway` (idem `minio.localhost`).
Les DAGs `./dags` sont montés directement (pas de `..data`). Surcharges : `cp .env.example .env`.

## 4. Vérifier

```bash
uv run scripts/smoke_test.py          # santé, DAGs, bundles, plugin Edge, tables edge_*, MinIO, ES
uv run scripts/smoke_test.py --run    # + exécute classic_etl_pipeline -> classic_report
uv run scripts/scenario.py all        # les 5 scénarios de démo, 55 contrôles
uv run scripts/dags.py status         # ConfigMaps, table dag_bundle, DAGs par bundle, erreurs d'import
```

À la main :

```bash
kubectl get pods -n airflow-edge
kubectl get pods -n team-data
kubectl get pods -n team-search
curl -s -H "Host: airflow-edge.localhost" http://127.0.0.1/api/v2/monitor/health
curl -s -H "Host: airflow-edge.localhost" http://127.0.0.1/edge_worker/v1/health
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow edge list-workers
```

Sortie attendue de `list-workers` (relevée sur la démo) :

```
worker_name          | state | queues      | jobs_active | ...
laptop-chris         | idle  | laptop      | 0           |
team-data-worker-1   | idle  | team-data   | 0           |
team-search-worker-1 | idle  | team-search | 0           |
```

## 5. Mettre à jour

| Changement | Commande | Effet |
|---|---|---|
| DAG d'une équipe k8s | `uv run scripts/dags.py apply <team> [--version vN]` | ConfigMaps centre + équipe ; visible en ~50-70 s au centre, ~60-80 s dans le pod worker ; pas de rebuild ni de redémarrage |
| DAG du laptop | éditer `teams/laptop/dags/` **et** `uv run scripts/dags.py apply laptop` | le worker lit le bind mount, le centre parse la ConfigMap : les deux doivent être à jour |
| DAG `core` | `uv run scripts/build.py --central` puis `kubectl -n airflow-edge rollout restart deploy/airflow-dag-processor sts/airflow-scheduler` | cuit dans l'image : rebuild obligatoire (c'est le point de l'"ancienne" idéologie) |
| libs d'une équipe | modifier `teams/<team>/Dockerfile`, `uv run scripts/build.py --team <team>`, `kubectl -n <team> rollout restart deploy/<team>-worker` | n'affecte ni le centre ni les autres équipes |
| libs du laptop | `docker compose up -d --build` dans `teams/laptop` | idem |
| values Helm | `helm upgrade --install airflow apache-airflow/airflow -n airflow-edge --version 1.22.0 -f platform/helm-values/airflow.yaml` | `jwtSecret` fixé : les workers restent authentifiés |
| version d'Airflow / edge3 | centre **et** tous les workers en même temps | sinon le serveur refuse les workers (voir [03](03-edge-executor.md#4-règle-des-versions)) |

## 6. Démonter

| Script | Équivalent manuel |
|---|---|
| `uv run scripts/down.py` | `helm uninstall airflow -n airflow-edge --wait` puis `kubectl delete namespace airflow-edge` (PVC compris) |
| `uv run scripts/down.py --keep-ns` | `helm uninstall` seulement (MinIO, ES, ConfigMaps, PVC conservés) |
| `uv run scripts/down.py --keep-ns --volumes` | + `kubectl delete pvc --all -n airflow-edge` |
| `uv run scripts/down.py --teams` | + `kubectl delete namespace team-data team-search` et `docker compose -f teams/laptop/docker-compose.yml down` (SIGINT : drain de 30 s max) |

## 7. Dimensionnement

Mesures `kubectl top` / `docker stats` en régime établi, après `scenario.py all` :

| Pod | Mémoire | requests / limits (values) |
|---|---|---|
| elasticsearch | ~800 Mi | 768Mi / 1Gi (heap 256m) |
| airflow-scheduler-0 | ~570 Mi | 512Mi / 1536Mi |
| airflow-api-server | ~375 Mi | 512Mi / 1536Mi |
| airflow-triggerer | ~350 Mi | 256Mi / 768Mi |
| airflow-dag-processor | ~250 Mi | 384Mi / 1Gi |
| team-data-worker / team-search-worker | ~240 / ~210 Mi | 256Mi / 1Gi |
| airflow-postgresql, minio | ~100 Mi chacun | 128Mi / 512Mi |
| conteneur laptop | ~150 Mi | — |

Total de la démo ≈ 3 Gi. Sur le poste de test, le nœud était à **76 %** de RAM avec tout lancé,
mais il hébergeait aussi d'autres projets (namespaces `mlops`, `monitoring`, `airflow`...).
`[core] parallelism: 8` (au lieu de 32) évite ~1 Go de process LocalExecutor pré-forkés dans le scheduler.

## 8. Spécificités Windows

- `*.localhost` est résolu par les navigateurs et `curl`, **pas** par PowerShell/Python : utiliser
  `http://127.0.0.1` + en-tête `Host:` (c'est ce que font les scripts, voir `scripts/_common.py`).
- `python` n'est pas forcément dans le PATH : `uv run` fournit l'interpréteur.
- Git Bash : `MSYS_NO_PATHCONV=1` pour les chemins `/opt/...` ; `$(pwd -W)` pour un volume Docker.
- Airflow ne tourne **pas** nativement sous Windows (`fork`, signaux POSIX) : un worker "hors cluster"
  se lance en Docker (ce que fait le laptop) ou sous WSL.
