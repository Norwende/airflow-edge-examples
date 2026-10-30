# Plateforme centrale (namespace `airflow-edge`)

Instance Airflow **3.3.2** (chart Helm officiel 1.22.0) + MinIO + Elasticsearch, sur k3s
(Rancher Desktop). Executors hybrides `LocalExecutor` (défaut) + `EdgeExecutor`.

| Fichier | Rôle |
|---|---|
| `namespace.yaml` | namespace `airflow-edge` + LimitRange (défauts pour les conteneurs sans resources) |
| `minio/minio.yaml` | MinIO (Secret `minio/minio12345`, PVC 2Gi, Service 9000/9001, Ingress `minio.localhost` / `minio-console.localhost`) + Job `minio-init` (bucket `datalake`) |
| `elasticsearch/elasticsearch.yaml` | ES 8.19.6 mono-nœud, sécurité off, heap 256m, PVC 1Gi, Ingress `es.localhost` |
| `mailpit/mailpit.yaml` | Mailpit v1.31.6 (exercice [docs/09](../docs/09-exercice-qualite-air.md)) : SMTP `mailpit:1025` sans TLS, UI/API `mailpit.localhost` ; non appliqué par `up.py` (`kubectl apply -f`) |
| `gitea/values.yaml` | values du chart `gitea-charts/gitea` 12.7.0 (SQLite, sans PostgreSQL/Valkey, UI `gitea.localhost`) pour l'étape 15.a de docs/09 (GitDagBundle) ; non appliqué par `up.py` (`helm upgrade --install gitea ...`) |
| `central-image/Dockerfile` | `apache/airflow:3.3.2` + `apache-airflow-providers-edge3==4.3.2` (contraintes 3.3.2/py3.13) + DAGs `core` |
| `central-image/dags/core/` | DAGs "ancienne idéologie" : `classic_etl_pipeline` -> `classic_report` (LocalExecutor, `S3Hook` du provider Amazon + pandas de l'image) |
| `helm-values/airflow.yaml` | values du chart : image, executors, secrets fixes, plugin Edge, 4 bundles, montages ConfigMap, Ingress, ressources (la solution de [docs/09](../docs/09-exercice-qualite-air.md) y ajoute un bundle, XCom object storage et logs distants S3 : `archive/solution/`) |
| `compose/docker-compose.yml` + `.env.example` | **la même instance centrale sans Kubernetes** : api-server, scheduler, dag-processor, triggerer, postgres, MinIO, ES ; même image, mêmes executors, mêmes secrets, connexions par `AIRFLOW_CONN_*` ; DAGs d'équipe en bind mount. Mode d'emploi : [docs/10 §3.B](../docs/10-reproduire.md#3b-sans-kubernetes--docker-compose) |

URLs (navigateur) : <http://airflow-edge.localhost> (admin / admin), <http://minio-console.localhost>
(minio / minio12345), <http://es.localhost>. Depuis PowerShell / Python : `http://127.0.0.1` + en-tête `Host:`.

## Scripts (`uv run scripts/<x>.py`, PowerShell ou bash)

| Script | Effet |
|---|---|
| `build.py [--central] [--team X]` | `docker build` image centrale / images worker `teams/<X>/Dockerfile` |
| `up.py [--build] [--skip-helm] [--teams]` | tout le déploiement ci-dessous, idempotent (relançable) ; construit l'image centrale si absente ; `--teams` = raccourci de démo qui déploie aussi les 3 workers (images absentes construites, `kubectl apply -k`, `docker compose up -d`) |
| `dags.py apply <team> [--version vN]` / `delete <team>` / `status` | ConfigMap `dags-<team>` au centre **et** dans le namespace de l'équipe s'il existe (source unique parsing + exécution) ; état des bundles et DAGs |
| `scenario.py classic\|assets\|versioning\|hitl\|isolation\|all` | scénarios de démo de bout en bout, avec assertions (API v2, plugin Edge, MinIO, ES) |
| `smoke_test.py [--run]` | santé, DAGs, bundles, plugin Edge, tables edge, MinIO, ES ; `--run` exécute les DAGs classiques |
| `down.py [--keep-ns] [--volumes] [--teams]` | `helm uninstall` + suppression du namespace (attend sa fin) ; `--teams` : namespaces `team-data`/`team-search` + `docker compose down` du laptop |

## Depuis un clone neuf

```bash
uv run scripts/build.py                 # 4 images (centrale + 3 workers) ; up.py ne construit que la centrale
uv run scripts/up.py                    # plateforme : MinIO, ES, Airflow, connexions, ConfigMaps centrales
# voie "équipes" (chacune déploie son worker ; namespace AVANT dags.py apply)
kubectl apply -k teams/team-data/k8s && kubectl apply -k teams/team-search/k8s
uv run scripts/dags.py apply team-data && uv run scripts/dags.py apply team-search && uv run scripts/dags.py apply laptop
cd teams/laptop && docker compose up -d && cd ../..
uv run scripts/smoke_test.py --run && uv run scripts/scenario.py all
```

Raccourci équivalent des étapes 2-5 : `uv run scripts/up.py --teams` (construit aussi les images worker
absentes). Démontage complet : `uv run scripts/down.py --teams --volumes`.

## Commandes manuelles équivalentes

```bash
# 1. Image centrale (dockerd de Rancher Desktop => visible par k3s, pullPolicy IfNotPresent)
docker build -t airflow-edge/airflow-central:3.3.2 platform/central-image

# 2. Externes
kubectl apply -f platform/namespace.yaml
kubectl apply -f platform/minio/minio.yaml
kubectl apply -f platform/elasticsearch/elasticsearch.yaml
kubectl -n airflow-edge rollout status deploy/minio
kubectl -n airflow-edge wait --for=condition=complete job/minio-init --timeout=180s
kubectl -n airflow-edge rollout status deploy/elasticsearch --timeout=300s

# 3. Airflow (up.py ajoute/met à jour le dépôt lui-même)
helm repo add apache-airflow https://airflow.apache.org --force-update && helm repo update apache-airflow
helm upgrade --install airflow apache-airflow/airflow -n airflow-edge \
  --version 1.22.0 -f platform/helm-values/airflow.yaml --timeout 15m
kubectl -n airflow-edge rollout status sts/airflow-scheduler --timeout=600s   # StatefulSet en LocalExecutor
kubectl -n airflow-edge rollout status deploy/airflow-api-server --timeout=600s

# 4. Connexions / variables (CLI dans le scheduler ; delete puis add = idempotent)
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow connections add minio_s3 \
  --conn-type aws --conn-login minio --conn-password minio12345 \
  --conn-extra '{"endpoint_url": "http://minio.airflow-edge.svc.cluster.local:9000", "region_name": "us-east-1"}'
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow connections add elasticsearch_default \
  --conn-type elasticsearch --conn-host elasticsearch.airflow-edge.svc.cluster.local --conn-port 9200 --conn-schema http
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow variables set datalake_bucket datalake

# 5. DAGs d'une équipe (bundle <team>)
kubectl create configmap dags-team-data -n airflow-edge --from-file=teams/team-data/dags/ \
  --dry-run=client -o yaml | kubectl apply -f -

# Vérifications
curl -s http://127.0.0.1/api/v2/monitor/health -H 'Host: airflow-edge.localhost'
curl -s http://127.0.0.1/edge_worker/v1/health -H 'Host: airflow-edge.localhost'   # {"status":"healthy"}
kubectl -n airflow-edge exec sts/airflow-postgresql -- env PGPASSWORD=postgres psql -U postgres -c '\dt edge*'

# Démontage
helm uninstall airflow -n airflow-edge && kubectl delete namespace airflow-edge
```

En Git Bash, préfixer les `kubectl exec ... /chemin` par `MSYS_NO_PATHCONV=1` (sinon `/opt/...`
devient `C:/Program Files/Git/opt/...`).

## Pièges rencontrés

- **ConfigMap montée = symlinks** (`..data -> ..<horodatage>/`). Le dag-processor suit les liens et
  plante (`Detected recursive loop when walking DAG directory`), y compris pour une ConfigMap absente
  (`optional: true` crée quand même `..data`). Les bundles d'équipe pointent donc sur
  `/opt/airflow/dags/<team>/..data` (montage toujours en `/opt/airflow/dags/<team>`, sans subPath).
  Le worker k8s monte sa propre copie de la ConfigMap (namespace de l'équipe) et pointe aussi sur
  `..data` ; il résout par **nom** de bundle + chemin **relatif** du fichier (le laptop monte un
  répertoire ordinaire, sans `..data`).
- Versioning : une nouvelle **version de DAG** n'est créée que si la **structure** sérialisée change
  (tâche ajoutée...) ; modifier seulement le corps d'une fonction n'en crée pas. Délai observé après
  `dags.py apply` : 50-70 s au centre, 60-80 s dans le pod worker → attendre les deux avant de
  déclencher (sinon le worker peut exécuter l'ancien fichier, ou ne pas trouver la nouvelle tâche).
- Un DAG planifié par **`Asset.ref(name=...)`** est bien déclenché, mais en 3.3.2 son run n'est **pas
  rattaché** à l'événement (`created_dagruns` de l'événement et `upstreamAssetEvents` du run vides →
  `triggering_asset_events` vide dans la tâche : lire `inlet_events`). `scheduled_dags` de
  `GET /api/v2/assets` ne le liste pas non plus : le lien n'apparaît que dans l'`asset_expression`
  du DAG (`GET /api/v2/dags/{id}/details`).
- Le champ `hostname` des task instances = `socket.getfqdn()` du worker : on fixe `hostname:` du pod
  (et du conteneur compose) au nom du worker edge ; le scheduler (StatefulSet) remonte un FQDN
  `airflow-scheduler-0.airflow-scheduler.airflow-edge.svc.cluster.local`.
- `/edge_worker/v1/rpcapi` **n'est pas une route** (404 même plugin actif) : le worker fait
  `urljoin(api_url, "worker/<nom>")`, le dernier segment est remplacé. Sonde : `/edge_worker/v1/health`,
  doc OpenAPI : `/edge_worker/docs`.
- `[core] parallelism` = nombre de process LocalExecutor **pré-forkés** dans le scheduler (32 par défaut
  ≈ 1 Go) ; fixé à 8.
- En LocalExecutor, le chart fait du scheduler un **StatefulSet** avec un PVC de logs (100Gi par défaut,
  réduit à 1Gi). Les logs des tâches Edge, eux, sont poussés à l'api-server (emptyDir, 1 replica) :
  perdus si l'api-server redémarre (local-path ne fait pas de RWX).
- Pas d'endpoint REST "bundles" en 3.3 : `GET /api/v2/dags` expose `bundle_name`, et la table
  `dag_bundle` liste les bundles configurés.
- Les tables `edge_*` sont créées par `airflow db migrate` grâce à
  `[database] external_db_managers = airflow.providers.edge3.models.db.EdgeDBManager`.
- Les Jobs du chart ont `ttlSecondsAfterFinished: 300` : ils disparaissent après 5 min ; `helm upgrade`
  les recrée (migration rejouée, sans effet si la base est à jour).
