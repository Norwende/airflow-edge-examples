# Solution de l'exercice `docs/09-exercice-qualite-air.md`

Solution complète, **déroulée et vérifiée le 2026-10-17** sur le cluster de la démo (Airflow 3.3.2,
edge3 4.3.2, chart 1.22.0). Elle est rangée **hors de l'arborescence de l'exercice** : le repo reste
l'énoncé, ce dossier contient le corrigé.

```
archive/solution/
  README.md        ce fichier : contenu, mode d'emploi, résultats observés
  apply.py         superpose la solution au repo (ou la retire) : uv run archive/solution/apply.py [--check|--revert]
  verify.py        rejoue les contrôles de chaque étape : uv run archive/solution/verify.py all [--edge] [--deadline]
  overlay/         fichiers de la solution, aux chemins du repo (nouveaux + versions modifiées des fichiers de base)
  base/            versions « énoncé » des fichiers de base que la solution modifie (pour --revert)
```

## 1. Ce que contient la solution

| Chemin (dans `overlay/`) | Rôle |
|---|---|
| `teams/team-iot/` | l'équipe créée à l'étape 1 : `Dockerfile` (py3.10 + numpy + scikit-learn + `amazon[s3fs]`), `k8s/` (namespace, Secret, Deployment `team-iot-worker-1`, `worker-2.yaml` pour 13.3/13.7), `k8s-gitbundle/` (variante GitDagBundle, 15.a), `dags/`, `tests/`, `README.md` |
| `teams/team-iot/dags/iot_triggers.py` | `S3NewObjectTrigger(BaseEventTrigger)` : un événement par cycle de poll avec la liste des nouvelles clés (étape 3) |
| `teams/team-iot/dags/iot_common.py` | assets, helpers S3/parquet, règle de dérive, `MinioAlertNotifier`, `HitlLinkSmtpNotifier`, `AirHITLBranchOperator` (XCom `hitl_response`) |
| `teams/team-iot/dags/iot_ingest_measurements.py` | le DAG principal : watcher, mapping nommé, params, short-circuit, setup/teardown, branche qualité, HITL (branch + entry), alias par station, deadline, callbacks (étapes 3-7, 11) |
| `teams/team-iot/dags/iot_hourly_stats*.py` | partitions : `CronPartitionTimetable(run_offset=-1)` et `PartitionedAtRuntime` (étape 8) |
| `teams/team-iot/dags/iot_station_report.py`, `iot_reconcile.py`, `iot_weekly_recap.py`, `iot_manual_reconcile.py` | alias, expression `&`/`\|`, `AssetOrTimeSchedule` (étape 7) |
| `teams/team-iot/dags/iot_where_am_i.py`, `iot_slow_task.py` | preuve du lieu d'exécution ; tâche lente paramétrable pour les exercices Edge |
| `teams/team-search/dags/search_index_air.py` (+ helpers ajoutés à `search_common.py`, tests) | `Asset.ref`, `HttpSensor(deferrable=True)` sans `response_check`, `@task.sensor`, index ES `air` avec `geo_point` (étape 9) |
| `teams/laptop/dags/laptop_air_common.py`, `laptop_air_bulletin.py`, `laptop_air_bulletin_cron.py` (+ `Dockerfile`, `requirements.txt`, `docker-compose.yml`, tests) | `PartitionedAssetTimetable(StartOfDayMapper)`, cron + backfill, `@task.virtualenv` avec cache de venv (étapes 8, 10) |
| `platform/central-image/plugins/` | `iot_triggers.py` (copie identique, pour le triggerer), `iot_callbacks.py` (coroutine de deadline), `air_views.py` (plugin UI `external_views`) ; `Dockerfile` central : `COPY plugins/` + `s3fs` |
| `platform/helm-values/airflow.yaml` | bundle/volume/montage `team-iot`, XCom object storage (`[common.io]`), logging distant S3 ; `airflow-gitbundle.yaml` : overlay GitDagBundle (15.a) |
| `scripts/air_stations.py` | simulateur de stations (étape 2) ; `scripts/_common.py`, `up.py`, `down.py`, `smoke_test.py` : connaissent `team-iot` |

Restent **dans l'énoncé** (outillage de plateforme fourni) : `platform/mailpit/mailpit.yaml`,
`platform/gitea/values.yaml`, `ruff.toml` (noms de modules first-party).

## 2. Appliquer la solution et la déployer

Depuis la racine du repo, démo de base déjà déployée (`uv run scripts/smoke_test.py` vert) :

```bash
uv run archive/solution/apply.py            # copie overlay/ sur le repo (--check : diff seulement)

# plateforme : image centrale (plugins + s3fs), values (bundle team-iot, XCom S3, logs S3), Mailpit
docker build -t airflow-edge/airflow-central:3.3.2 platform/central-image
helm upgrade --install airflow apache-airflow/airflow -n airflow-edge --version 1.22.0 \
  -f platform/helm-values/airflow.yaml --timeout 15m
kubectl apply -f platform/mailpit/mailpit.yaml
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow connections add smtp_default \
  --conn-type smtp --conn-host mailpit.airflow-edge.svc.cluster.local --conn-port 1025 \
  --conn-extra '{"disable_tls": true, "disable_ssl": true, "from_email": "airflow@airflow-edge.localhost", "timeout": 10, "retry_limit": 1}'
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow connections add es_http \
  --conn-type http --conn-host elasticsearch.airflow-edge.svc.cluster.local --conn-schema http --conn-port 9200
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow pools set team-iot 8 "pool team-iot"

# équipes
docker build -t airflow-edge/worker-team-iot:dev teams/team-iot
kubectl apply -k teams/team-iot/k8s
uv run scripts/dags.py apply team-iot
uv run scripts/dags.py apply team-search
( cd teams/laptop && docker compose up -d --build )   # image laptop : virtualenv, cloudpickle, /opt/airflow/.venvs
uv run scripts/dags.py apply laptop
```

Puis **dépauser** les DAGs team-iot (le watcher de l'étape 3 ne démarre qu'à la dépause) :
`uv run archive/solution/verify.py 1` le fait et vérifie l'étape 1. Bonus 15.a (Gitea) :

```bash
helm repo add gitea-charts https://dl.gitea.com/charts/
helm upgrade --install gitea gitea-charts/gitea -n airflow-edge --version 12.7.0 -f platform/gitea/values.yaml
# créer le dépôt gitea_admin/team-iot-dags (UI ou API), y pousser teams/team-iot/dags/*.py, puis :
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow connections add gitea_iot --conn-type git \
  --conn-host http://gitea-http.airflow-edge.svc.cluster.local:3000/gitea_admin/team-iot-dags.git \
  --conn-login gitea_admin --conn-password gitea12345
helm upgrade --install airflow apache-airflow/airflow -n airflow-edge --version 1.22.0 \
  -f platform/helm-values/airflow.yaml -f platform/helm-values/airflow-gitbundle.yaml --timeout 15m
kubectl apply -k teams/team-iot/k8s-gitbundle
```

Retour à l'énoncé : `uv run archive/solution/apply.py --revert`, `kubectl delete -k teams/team-iot/k8s`,
`helm upgrade ... -f platform/helm-values/airflow.yaml` avec les values de base, rebuild de l'image centrale.

## 3. Vérifier

```bash
uv run archive/solution/verify.py all              # étapes 1-10, 12, 14, 15 (~15 min, dépose des fichiers, répond aux HITL)
uv run archive/solution/verify.py all --edge       # + 13.1-13.4 (~10 min de plus)
uv run archive/solution/verify.py 11 --deadline    # deadline (attente ~11 min) ; 13.6 idem
uv run archive/solution/verify.py 6                # une seule étape
uv run archive/solution/verify.py 1 --read-only    # sans rien déclencher
```

Chaque contrôle affiche `OK`/`KO` ; résumé « N/M contrôles » et code de sortie 1 en cas d'échec.
13.5 (kill -9) et 13.7 (versions) ne sont pas automatisés : le script affiche les commandes.
Dernier passage complet (`all --edge`, 2026-10-17) : 127 contrôles, 124 OK au premier passage ; les 3 KO
étaient des défauts du script (lecture d'un run encore `running`, attente erronée sur la concurrence), corrigés.

## 4. Résultats observés (corrigé)

Le détail est dans chaque étape de `docs/09` (« Observé (solution) »). Synthèse :

| Étape | Résultat |
|---|---|
| 1 | worker `idle` 8 s après le rollout ; Python 3.10.21, scikit-learn 1.5.2 ; `iot_where_am_i` sur `team-iot-worker-1` |
| 2 | `--drift` : moyenne pm25 ≈ 60 (seuil 45) ; `--samples 3600` ≈ 170 Kio |
| 3 | watcher dans le triggerer ; **3 événements en 20 ms → 1 seul rattaché au run** (correctif : un événement par cycle + listing de l'inbox) |
| 4 | `rendered_map_index` = clé S3 ; formulaire à 3 params ; `dry_run` sans écriture ni événement |
| 5 | `fail_publish` → run `failed`, mail Mailpit, `air/alerts/…json`, teardown exécuté ; aucun fichier → tout `skipped` |
| 6 | `awaiting_input` avec 0 job worker ; 3 chemins (calibrate −40, quarantine + incident, ignore) ; réponse API `responded_by.id="1"` ; `{"id": "admin"}` → 403 |
| 7 | alias → `air_station_st-0N`, `triggering_asset_events` renseigné (2 entrées) ; `Asset.ref` → vide ; `iot_reconcile` après les deux assets |
| 8 | `partition_key` horaire (`run_offset=-1`), `StartOfDayMapper` → `2026-10-17`, `add_partitions` → un événement par clé |
| 9 | `search_index_air` sur `team-search-worker-1`, ES `air` = 12 docs ; `deferred` avec `jobs_active=0` (`wait_for_status=green`) |
| 10 | venv 8,2 s puis 1,1 s ; backfill 7 jours : `queued` tant que le DAG est en pause, puis 130 s, 2 simultanés ; run `logical_date=null` |
| 11 | deadline `missed` à +600 s, JSON + mail ; retour `dict` du callback → triggerer en boucle (corrigé : chaîne) |
| 12 | XCom déporté : référence `s3://minio_s3@datalake/xcom/…gz` (55 Kio) ; logs S3 lisibles après redémarrage de l'api-server |
| 13.1 | request → pending (12 s) → mode (fin de tâche) ; survit au redémarrage du pod ; `maintenance off` local |
| 13.2 | queue ajoutée vue en 2 s ; tâche team-data sur team-iot : `Requested bundle 'team-data' is not configured` |
| 13.3 | 6 runs → 3/3 entre les deux workers, 67 s |
| 13.4 | delete pod : `terminating` 3 s → tâche finie `success` → `offline` 57 s → `idle` 63 s ; queues et concurrence revenues aux valeurs `-q`/`-c` |
| 13.5 | kill -9 : `unknown` à 145 s (5 × 30 s **du scheduler**), 5 redémarrages, zombie à 301 s, retry OK |
| 13.6 | `task_queued_timeout` : révocation + remise en file (même essai) à 661 s |
| 13.7 | 400 `Version mismatch` ; avec `minimum_acceptable_edge_version_for_workers=4.3.1` : `Healthy but version mismatch` |
| 14 | `GET /api/v2/plugins` : `air_views` (3 vues) ; rendu visuel non vérifié automatiquement |
| 15.a | run v1 `bundle_version=4c7e044e`, run v2 `f84c4a0d` ; relance du run v1 → code v1 (`versions/4c7e044e…`) |
| 15.b | non déroulé |

## 5. Limites

- Session automatisée sans navigateur : les pages UI (Required Actions, Deadlines, menu Browse du plugin) ont été
  vérifiées par leurs routes API/UI, pas visuellement.
- Secrets de démo en clair (Gitea, SMTP sans auth, JWT partagé).
- `RollupMapper`, `SyncCallback`, multi-team : non testés.
