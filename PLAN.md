# Plan d'orchestration — démo Airflow 3 + Edge Executor

> Document de pilotage : décisions, **contrat entre composants**, découpage du travail.
> Toute équipe/agent qui travaille dans ce repo lit ce fichier d'abord. Les docs finales sont dans `docs/`.

## Objectif

Sur un cluster k3s local (Rancher Desktop, Windows), une instance **Airflow 3.3.2** centrale déployée
par le chart Helm officiel (1.22.0) et des **edge workers** (`apache-airflow-providers-edge3` 4.3.2)
portés par des équipes différentes : libs Python, versions de Python, lieux d'exécution différents.
Deux idéologies de DAG côte à côte :
- **"ancienne"** (style Airflow 2 : `DAG(...)` + `PythonOperator` + XCom explicite + `TriggerDagRunOperator`),
  exécutée **dans** l'instance centrale (LocalExecutor) ;
- **"nouvelle"** (Task SDK `airflow.sdk`, `@dag/@task`, Assets, `inlet_events`, dynamic mapping,
  versioning, HITL) exécutée **sur les edge workers**.

## Faits vérifiés (2026-10-17, docs officielles + sources)

- Airflow **3.3.2** = dernière stable ; chart Helm **1.22.0** = dernier (appVersion 3.2.2, mais déploie 3.3.2 via
  `airflowVersion` + image custom). Provider edge3 : **4.3.2** = version épinglée par `constraints-3.3.2`
  (5.0.0 existe depuis 4 jours, hors contraintes : on reste sur 4.3.2).
- L'image officielle `apache/airflow:3.3.2` (Python 3.13) **n'embarque pas** edge3 → image custom obligatoire
  pour api-server + scheduler (l'executor tourne dans le scheduler ; le plugin API edge dans l'api-server).
- Chart : utiliser le **chemin complet** `airflow.providers.edge3.executors.EdgeExecutor` dans `executor`
  (les templates testent cette chaîne). Le chart ne crée **aucun** worker edge ; pas de clé values dédiée →
  `config.edge.*`. `jwtSecret` doit être **fixé** (sinon régénéré à chaque `helm upgrade` → 403 des workers).
- Le plugin monte `/edge_worker/v1/rpcapi` sur l'api-server **ssi** `AIRFLOW__EDGE__API_ENABLED=True` (sinon 405).
- Executors multiples : `executor = LocalExecutor,airflow.providers.edge3.executors.EdgeExecutor` ; le **premier**
  est le défaut. `queue=` seul **ne route pas** vers Edge : il faut `executor="EdgeExecutor"` sur la tâche
  (ou `default_args`). `@asset` **n'accepte ni executor ni queue** (vérifié dans `airflow/sdk/definitions/asset/decorators.py`)
  → une tâche `@asset` tourne toujours sur l'executor par défaut (ici LocalExecutor, au centre).
- Worker : a besoin de `AIRFLOW__CORE__EXECUTOR`, `AIRFLOW__EDGE__API_URL`, `AIRFLOW__API_AUTH__JWT_SECRET`
  (même valeur que le serveur), **pas de DB**, pas de fernet. Il résout le fichier DAG par
  **nom de bundle** (`DagBundlesManager().get_bundle(name)`) → même `dag_bundle_config_list` (noms identiques,
  chemins locaux) sur le worker. Versions Airflow **et** edge3 du worker = exactement celles du serveur
  (sinon le worker s'arrête) ; la version de Python est seulement rapportée.
- Logs : le worker pousse les logs vers l'api-server (table `edge_logs` + fichier local de l'api-server) →
  **1 seul replica** d'api-server.
- Bundle local : les changements de structure créent des **versions de DAG** visibles dans l'UI, mais le code
  n'est pas épinglé par run (ça, c'est `GitDagBundle`). Documenter la différence.
- Identité worker = hostname → fixer `-H <nom stable>` (sinon un pod k8s redémarré laisse une entrée fantôme).
- Arrêt worker : SIGINT = drain, SIGTERM = arrêt dur (jobs tués). En k8s, PID 1 = `dumb-init` (`DUMB_INIT_SETSID=1`) →
  `preStop` : `kill -INT $(cat /opt/airflow/airflow-edge-worker.pid)` + attente ; drain borné à 45 s, grâce 90 s (validé).
- `schedule=[Asset.ref(name=...)]` (3.3.2, observé) : le DAG est bien déclenché mais son run n'est **pas lié** à
  l'événement (`created_dagruns`, `upstreamAssetEvents`, `triggering_asset_events` vides ; `inlet_events` fonctionne).
  Recherche GitHub apache/airflow (2026-10-17) : **aucune issue trouvée** — comportement observé seulement (voisins :
  #47970 `consuming_dags` et `Asset.ref`, fermée not planned ; #50182/#55534 même correctif pour `AssetAlias`).

## Décisions

| # | Décision | Alternative écartée | Pourquoi |
|---|---|---|---|
| D1 | Executors hybrides `LocalExecutor,EdgeExecutor` (Local par défaut) | tout Edge | garder les DAGs "classiques" exécutés dans l'instance centrale ; `@asset` tombe sur le défaut |
| D2 | DAGs `core` cuits dans l'image centrale ; DAGs d'équipe livrés par **ConfigMap** montée dans le dag-processor, **1 DAG bundle par équipe** | git-sync / GitDagBundle | pas de serveur git local ; une équipe déploie ses DAGs sans toucher au release Helm ; GitDagBundle = voie prod documentée |
| D3 | Un fichier DAG d'équipe doit être **parseable au centre sans les libs de l'équipe** : imports spécifiques **dans** les fonctions de tâche | installer toutes les libs au centre | c'est le problème que l'Edge Executor résout |
| D4 | Externes : **MinIO** (S3) + **Elasticsearch** mono-nœud (heap 256m), namespace `airflow-edge` | OpenSearch / Postgres | demandés ; ES = lib spécifique parlante ; RAM du cluster limitée |
| D5 | 3 workers : `team-data` (k8s, py3.12, polars+boto3), `team-search` (k8s, py3.13, elasticsearch+pandas/pyarrow), `laptop` (docker compose hors cluster, py3.11 slim + duckdb) | 1 worker | isolation libs / Python / lieu d'exécution |
| D6 | Secrets fixes et connus (JWT, fernet, api key) | générés | workers hors cluster ; reproductible ; démo |
| D7 | Scripts Python PEP 723 lancés par `uv run` + commandes brutes documentées | Makefile / bash | Windows ; cohérent avec le projet voisin |
| D8 | Nouveau namespace `airflow-edge` + Ingress Traefik `*.localhost` | réutiliser le release `airflow` existant / Gateway du projet voisin | auto-suffisant, n'abîme pas l'autre projet |

## CONTRAT (valeurs figées — ne pas inventer d'autres noms)

### Versions / images
- Airflow `3.3.2`, edge3 `4.3.2`, contraintes `https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-<py>.txt`.
- Image centrale : `airflow-edge/airflow-central:3.3.2` (FROM `apache/airflow:3.3.2`, Python 3.13). Workers :
  `airflow-edge/worker-team-data:dev` (FROM `apache/airflow:3.3.2-python3.12`), `airflow-edge/worker-team-search:dev`
  (FROM `apache/airflow:3.3.2`), `airflow-edge/worker-laptop:dev` (FROM `python:3.11-slim` + uv).
  Rancher Desktop (dockerd) : les images locales sont vues par k3s → `imagePullPolicy: IfNotPresent`.

### Instance centrale
- Namespace `airflow-edge`, release Helm `airflow`. Service API : `airflow-api-server` port 8080.
- UI/API : `http://airflow-edge.localhost` (Ingress classe `traefik`), **admin / admin**.
  Depuis PowerShell/Python Windows, `*.localhost` ne se résout pas : utiliser `http://127.0.0.1` + en-tête `Host`.
- Executor : `LocalExecutor,airflow.providers.edge3.executors.EdgeExecutor`.
- Secrets (values Helm) : `jwtSecret: airflow-edge-demo-jwt-secret-please-change-0123456789`,
  `fernetKey: YWlyZmxvdy1lZGdlLWRlbW8tZmVybmV0LWtleS0zMmI=`, `apiSecretKey: airflow-edge-demo-api-secret-key`.
- Edge API (interne cluster) : `http://airflow-api-server.airflow-edge.svc.cluster.local:8080/edge_worker/v1/rpcapi`.
  Hors cluster (docker compose) : `http://airflow-edge.localhost/edge_worker/v1/rpcapi` avec
  `extra_hosts: ["airflow-edge.localhost:host-gateway"]` (à valider ; repli : `kubectl port-forward` 8080).
- Bundles (`dagProcessor.dagBundleConfigList`), **noms identiques partout**, `LocalDagBundle` :
  `core` → `/opt/airflow/dags/core` (dans l'image centrale) ; `team-data`, `team-search`, `laptop` →
  `/opt/airflow/dags/<nom>` montés depuis les ConfigMaps **`dags-team-data`, `dags-team-search`, `dags-laptop`**
  (namespace `airflow-edge`, volumes `optional: true`, montage de répertoire sans `subPath`).
  Pas de bundle `dags-folder` par défaut.
- Connexions/Variables créées au centre (CLI dans le pod scheduler) : connexion **`minio_s3`** (type `aws`,
  login `minio`, password `minio12345`, extra `{"endpoint_url": "http://minio.airflow-edge.svc.cluster.local:9000", "region_name": "us-east-1"}`),
  connexion **`elasticsearch_default`** (type `elasticsearch`, host `elasticsearch.airflow-edge.svc.cluster.local`, port 9200, schema `http`),
  variable **`datalake_bucket`** = `datalake`.
  Les tâches y accèdent via `from airflow.sdk import Connection, Variable` (Execution API, pas de DB).

### Externes
- MinIO : service `minio` ns `airflow-edge`, API 9000, console 9001 ; Ingress `http://minio.localhost` (API) et
  `http://minio-console.localhost` ; creds `minio / minio12345` ; bucket `datalake` créé par un Job (`minio/mc`).
- Elasticsearch 8.x mono-nœud, sécurité désactivée, `ES_JAVA_OPTS=-Xms256m -Xmx256m`, service `elasticsearch` 9200 ;
  Ingress `http://es.localhost`.

### Équipes
- Namespaces k8s `team-data`, `team-search` ; worker = Deployment 1 replica, `imagePullPolicy: IfNotPresent`,
  Secret local `airflow-edge-worker` (clé `jwt-secret`, même valeur que ci-dessus).
- Queues = nom d'équipe : `team-data`, `team-search`, `laptop`. Hostnames stables : `team-data-worker-1`,
  `team-search-worker-1`, `laptop-<user>`.
- Commande worker : `airflow edge worker -q <queue> -c 2 -H <hostname>`.
- Env minimal worker : `AIRFLOW__CORE__EXECUTOR=airflow.providers.edge3.executors.EdgeExecutor`,
  `AIRFLOW__EDGE__API_URL=...`, `AIRFLOW__API_AUTH__JWT_SECRET=...`,
  `AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[{"name":"<bundle>","classpath":"airflow.dag_processing.bundles.local.LocalDagBundle","kwargs":{"path":"/opt/airflow/dags/<bundle>"}}]'`,
  `AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs` (répertoire inscriptible), `AIRFLOW__CORE__LOAD_EXAMPLES=False`.
- DAGs d'équipe : `teams/<team>/dags/*.py` ; chaque DAG fixe `default_args={"executor": "EdgeExecutor", "queue": "<team>"}`
  (ou sur chaque `@task`). Les DAGs sont publiés au centre par `uv run scripts/dags.py apply <team>` (ConfigMap `dags-<team>`).
- Assets partagés (noms stables, URIs) : `orders_raw` = `s3://datalake/orders/raw`, `orders_daily` = `s3://datalake/orders/daily`,
  `orders_index` = `es://orders`. Une équipe consomme l'asset d'une autre par **`Asset.ref(name=...)`**, sans importer son code.

### Arborescence
```
PLAN.md  README.md  docs/
platform/   namespace.yaml  minio/  elasticsearch/  helm-values/airflow.yaml  central-image/{Dockerfile,dags/core/*}
scripts/    build.py  up.py  down.py  dags.py  smoke_test.py  (PEP 723, `uv run scripts/x.py`)
teams/team-data/    Dockerfile  requirements.txt  dags/  versions/v2/  k8s/  tests/
teams/team-search/  Dockerfile  requirements.txt  dags/  k8s/  tests/
teams/laptop/       Dockerfile  requirements.txt  dags/  docker-compose.yml  tests/
```

## Découpage et statut
1. Recherche doc ✅
2. Plateforme (agent Opus) : namespace, MinIO, ES, image centrale, values, Ingress, DAGs `core`, scripts, déploiement réel.
3. Équipes (3 agents Opus en parallèle) : images, DAGs, manifests/compose, tests d'import. Pas de déploiement cluster.
4. Intégration : déployer les workers, dérouler les scénarios de bout en bout ✅ (`scripts/scenario.py all` : 55/55).
   Changement de contrat : les workers k8s montent la ConfigMap `dags-<team>` de leur namespace (publiée par
   le même `dags.py apply`) sur `/opt/airflow/dags/<team>`, bundle `/opt/airflow/dags/<team>/..data`.
5. Documentation + audit.
6. Finition : DAGs `core` via `S3Hook` (provider Amazon de l'image centrale) ; `ruff.toml` (ruff check/format
   propres) ; `up.py --teams` (raccourci : workers + compose laptop) et `down.py --teams` (attend la fin des
   namespaces + `compose down`) ; rejeu depuis zéro (`down.py --teams --volumes` → `build.py` → `up.py` →
   voie équipes → `smoke_test.py --run` → `scenario.py all`) ✅.
