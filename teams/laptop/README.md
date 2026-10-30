# Worker edge « laptop » — hors cluster

Ce worker montre qu'un **edge worker peut tourner n'importe où** (poste de dev, serveur on-prem,
machine GPU…), du moment qu'il joint l'api-server central en HTTP. Il n'a **ni base de données, ni
fernet, ni accès au cluster** : il interroge `/edge_worker/v1/rpcapi`, exécute les tâches de la
queue `laptop` et renvoie états et logs à l'api-server.

Environnement volontairement **différent** de l'instance centrale et des autres équipes :

| | central | team-data | team-search | **laptop** |
|---|---|---|---|---|
| base | `apache/airflow:3.3.2` | `apache/airflow:3.3.2-python3.12` | `apache/airflow:3.3.2` | **`python:3.11-slim` + `uv`** |
| Python | 3.13 | 3.12 | 3.13 | **3.11** |
| libs | — | polars, boto3 | elasticsearch, pandas | **duckdb, boto3** (pas de pandas/polars) |
| où | k8s | k8s | k8s | **docker compose sur l'hôte** |

Airflow est installé **depuis pip** (`requirements.txt` + contraintes officielles 3.3.2/py3.11),
sans les extras `postgres`/`celery` : image ≈ 164 Mo compressée (674 Mo sur disque, httpfs inclus), build à froid ≈ 40 s.

## Contenu

```
Dockerfile          python:3.11-slim + uv, user airflow (uid 50000), AIRFLOW_HOME=/opt/airflow
requirements.txt    apache-airflow==3.3.2, apache-airflow-providers-edge3==4.3.2, duckdb, boto3, pytest
entrypoint.sh       airflow edge worker -q laptop -c 2 -H ${EDGE_HOSTNAME:-laptop-dev}
docker-compose.yml  service laptop-worker ; .env.example pour surcharger
dags/               laptop_where_am_i.py, laptop_duckdb_analytics.py  (bundle « laptop »)
tests/test_dags.py  import DagBag, routage EdgeExecutor/queue laptop, parse sans duckdb/boto3
logs/               monté dans le conteneur (logs de tâches + laptop-proof.txt)
```

## DAGs

- **`laptop_where_am_i`** (manuel) : affiche hostname, OS, Python, versions Airflow/edge3 et les
  paquets présents/absents → preuve que la tâche a tourné sur le laptop, dans son propre
  environnement ; écrit `logs/laptop-proof.txt` (visible sur l'hôte) ; un `BashOperator`
  `echo "edge worker: $(hostname)"` montre que les opérateurs classiques marchent aussi.
- **`laptop_duckdb_analytics`** (planifié sur l'asset `orders_daily` de team-data, consommé par
  `Asset.ref(name="orders_daily")`) : lit l'`extra` du dernier événement (`inlet_events`) pour trouver
  la clé du parquet (`key`, `s3_key`, `path` ou `uri s3://…`), sinon (déclenchement manuel) le dernier
  `.parquet` sous `orders/daily/` ; agrège avec **duckdb** par région en sommant toutes les colonnes
  numériques (schéma lu par `DESCRIBE`). Lecture par défaut : boto3 télécharge puis duckdb lit en
  local ; param `use_httpfs=true` : duckdb lit `s3://` directement (extension `httpfs`
  **pré-installée dans l'image** au build, donc pas d'internet requis au runtime).

Règle D3 : `duckdb`/`boto3` sont importés **dans** les tâches → le fichier se parse au centre, où ces
libs n'existent pas (vérifié par les tests).

### Endpoint « spécifique à l'environnement »

La connexion `minio_s3` (définie au centre) porte l'endpoint **interne au cluster**
`http://minio.airflow-edge.svc.cluster.local:9000`, injoignable depuis le laptop. Plutôt que de
dupliquer la connexion, la tâche prend les identifiants dans la connexion mais **l'endpoint dans la
variable d'environnement `MINIO_ENDPOINT_URL`** si elle est définie (`http://minio.localhost` ici),
sinon l'extra `endpoint_url`. Même code, adresse adaptée au lieu d'exécution. (boto3 est forcé en
adressage *path-style* : l'ingress route sur `Host: minio.localhost`, pas `datalake.minio.localhost`.)

## Utilisation

Pré-requis : la plateforme centrale tourne et les DAGs sont publiés au centre
(`uv run scripts/dags.py apply laptop`) ; le **même** dossier `dags/` est monté dans le worker.

```bash
cd teams/laptop
cp .env.example .env            # optionnel (valeurs par défaut = contrat)
docker compose up -d --build
docker compose logs -f          # bannière "Edge Worker" puis heartbeats
docker compose exec laptop-worker airflow edge status
docker compose exec laptop-worker airflow edge maintenance on -c "pause café"
docker compose exec laptop-worker airflow edge maintenance off
docker compose down             # SIGINT => drain : finit les tâches en cours (30 s max)
```

Le worker apparaît dans l'UI (*Admin → Edge Executor*, `/plugin/edge_executor`) sous `laptop-<USERNAME>`.
Les DAGs sont montés en lecture seule (`./dags` → `/opt/airflow/dags/laptop`, répertoire ordinaire : le
bundle du worker pointe dessus directement, **sans** `..data` — ce suffixe ne concerne que les montages de
ConfigMap, comme au centre). Une modification locale est prise en compte par le worker sans rebuild,
mais doit aussi être republiée au centre pour le parsing (`uv run scripts/dags.py apply laptop`).

Tests (dans l'image) :

```bash
docker run --rm -v "$PWD:/work" -w /work airflow-edge/worker-laptop:dev python -m pytest -q tests
```

## Réseau

```
conteneur ──(extra_hosts: airflow-edge.localhost → host-gateway)──▶ hôte :80 ──▶ Traefik ──▶ Ingress ──▶ svc airflow-api-server:8080
conteneur ──(extra_hosts: minio.localhost        → host-gateway)──▶ hôte :80 ──▶ Traefik ──▶ Ingress ──▶ svc minio:9000
```

`host-gateway` = IP de l'hôte vue depuis le conteneur (172.17.0.1 sous Rancher Desktop/dockerd).
**Validé de bout en bout le 2026-10-17** (route par défaut, sans port-forward) : le worker s'enregistre
(`laptop-<USERNAME>`, état `idle`, Python 3.11.17), exécute `laptop_where_am_i` et
`laptop_duckdb_analytics` (déclenché par l'asset `orders_daily`, lecture MinIO via `http://minio.localhost`),
et ses logs sont visibles dans l'UI / l'API dès la fin de la tâche.
Le conteneur a `hostname: laptop-<USERNAME>` : c'est ce nom qui apparaît dans le champ `hostname` des tâches
(sinon l'id du conteneur).
Attention : `curl` résout lui-même `*.localhost` en 127.0.0.1 et **ignore `/etc/hosts`** — pour tester à
la main, utiliser `curl -H "Host: airflow-edge.localhost" http://host.docker.internal/...` (Python, lui,
respecte `/etc/hosts`).

**Repli** si `host-gateway` n'atteint pas Traefik dans votre environnement (non nécessaire ici) :

```bash
kubectl port-forward -n airflow-edge svc/airflow-api-server 8080:8080 --address 0.0.0.0
# dans .env :
EDGE_API_URL=http://host.docker.internal:8080/edge_worker/v1/rpcapi
```

## Alternative sans Docker (Linux / macOS / WSL)

```bash
uv venv -p 3.11 .venv && source .venv/bin/activate
uv pip install -r requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt
kubectl port-forward -n airflow-edge svc/airflow-api-server 8080:8080 &
export AIRFLOW_HOME=$PWD/.airflow
export AIRFLOW__CORE__EXECUTOR=airflow.providers.edge3.executors.EdgeExecutor
export AIRFLOW__EDGE__API_URL=http://localhost:8080/edge_worker/v1/rpcapi
export AIRFLOW__API_AUTH__JWT_SECRET=airflow-edge-demo-jwt-secret-please-change-0123456789
export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[{"name":"laptop","classpath":"airflow.dag_processing.bundles.local.LocalDagBundle","kwargs":{"path":"'$PWD'/dags"}}]'
export AIRFLOW__CORE__LOAD_EXAMPLES=False MINIO_ENDPOINT_URL=http://minio.localhost
airflow edge worker -q laptop -c 2 -H laptop-$USER
```

Le bundle s'appelle `laptop` partout ; seul le chemin local change. `laptop_where_am_i` écrit sa
preuve dans `/opt/airflow/logs` : hors Docker, créer ce répertoire ou adapter `PROOF_FILE`.

## Limites

- **Windows natif non supporté** par Airflow (le worker utilise `fork`, signaux POSIX…) : Docker ou WSL.
- Versions **Airflow 3.3.2 et edge3 4.3.2 strictement identiques** au serveur, sinon le worker refuse
  de démarrer / s'arrête. La version de Python, elle, peut différer (3.11 ici, 3.13 au centre).
- `AIRFLOW__API_AUTH__JWT_SECRET` doit être le `jwtSecret` du release Helm (sinon 403).
- Logs de tâches poussés à l'api-server (UI) ; une copie locale reste dans `logs/`.
- `inlet_events` résout `Asset.ref(name="orders_daily")` au démarrage de la tâche : l'asset doit déjà
  exister au centre (DAG de team-data déployé), même pour un déclenchement manuel.
