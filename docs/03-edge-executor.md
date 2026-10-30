# 03 — L'Edge Executor en détail

Provider `apache-airflow-providers-edge3` **4.3.2** (version épinglée par `constraints-3.3.2` ; la 5.0.0
existe mais sort des contraintes). Il apporte trois choses :

1. un **executor** (`EdgeExecutor`) qui tourne dans le scheduler et écrit les tâches dans la table `edge_job` ;
2. un **plugin** de l'api-server qui expose `/edge_worker/*` (API des workers, pages UI, doc OpenAPI) ;
3. une **CLI** `airflow edge ...` qui lance et pilote un worker.

Le worker **tire** le travail en HTTP (pas de broker, pas de connexion entrante vers le worker) :
il peut donc tourner derrière un NAT, sur un poste, dans un autre cluster. Cycle complet :
[01-architecture.md §3](01-architecture.md#3-cycle-de-vie-dun-edge-worker).

L'image officielle `apache/airflow:3.3.2` n'inclut pas edge3 : d'où l'image centrale
[`platform/central-image/Dockerfile`](../platform/central-image/Dockerfile).

## 1. Configuration côté serveur (values Helm)

Extrait de [`platform/helm-values/airflow.yaml`](../platform/helm-values/airflow.yaml) :

```yaml
executor: "LocalExecutor,airflow.providers.edge3.executors.EdgeExecutor"
jwtSecret: "airflow-edge-demo-jwt-secret-please-change-0123456789"
config:
  core:
    parallelism: 8
  database:
    external_db_managers: airflow.providers.edge3.models.db.EdgeDBManager
  edge:
    api_enabled: "True"
    api_url: http://airflow-api-server.airflow-edge.svc.cluster.local:8080/edge_worker/v1/rpcapi
apiServer:
  replicas: 1
```

| Clé | Variable d'environnement | Pourquoi |
|---|---|---|
| `executor` (chart) | `AIRFLOW__CORE__EXECUTOR` | deux executors, le **premier** est le défaut. Le chart veut le **chemin complet** d'EdgeExecutor (ses templates testent cette chaîne) |
| `jwtSecret` (chart) | `AIRFLOW__API_AUTH__JWT_SECRET` | doit être **fixé** : généré, il change à chaque `helm upgrade` et tous les workers prennent 403 |
| `[edge] api_enabled` | `AIRFLOW__EDGE__API_ENABLED` | monte le plugin `/edge_worker/*` sur l'api-server ; sinon 405 |
| `[edge] api_url` | `AIRFLOW__EDGE__API_URL` | URL de l'API vue par l'executor (le worker a la sienne) |
| `[database] external_db_managers` | `AIRFLOW__DATABASE__EXTERNAL_DB_MANAGERS` | `airflow db migrate` crée les tables `edge_worker`, `edge_job`, `edge_logs` |
| `[core] parallelism` | `AIRFLOW__CORE__PARALLELISM` | nombre de process LocalExecutor **pré-forkés** et de créneaux **par executor** (Edge compris) ; 32 par défaut ≈ 1 Go |
| `apiServer.replicas: 1` | — | les logs Edge arrivent sur le disque local de l'api-server |

Le chart **ne crée aucun worker edge** et n'a pas de clé de values dédiée : tout passe par `config.edge.*`.

Autres clés `[edge]` de la 4.3.2 (valeurs par défaut lues par `airflow config list --section edge`) :
`job_poll_interval = 5`, `heartbeat_interval = 30`, `worker_concurrency = 8`, `drain_timeout_sec = 0`
(attente illimitée), `drain_kill_grace_sec = 30`, `job_success_purge = 5` (min), `job_fail_purge = 60` (min),
`push_logs = True`, `push_log_chunk_size = 524288`, `automatic_maintenance_on/exit = Off`, et
`minimum_acceptable_core_version_for_workers` / `minimum_acceptable_edge_version_for_workers` (non définies).

## 2. Configuration côté worker

Le worker a besoin de **très peu** : pas de base, pas de fernet, pas de connexion.

| Variable | Valeur dans la démo | Rôle |
|---|---|---|
| `AIRFLOW__CORE__EXECUTOR` | `airflow.providers.edge3.executors.EdgeExecutor` | active la CLI `airflow edge` |
| `AIRFLOW__EDGE__API_URL` | k8s : `http://airflow-api-server.airflow-edge.svc.cluster.local:8080/edge_worker/v1/rpcapi` ; laptop : `http://airflow-edge.localhost/edge_worker/v1/rpcapi` | URL de base de l'API ; l'Execution API en est déduite (`.../execution`) |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Secret `airflow-edge-worker` / `JWT_SECRET` | signe les requêtes ; **même valeur** que le serveur |
| `AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST` | `[{"name":"team-data","classpath":"airflow.dag_processing.bundles.local.LocalDagBundle","kwargs":{"path":"/opt/airflow/dags/team-data/..data"}}]` | **même nom** de bundle qu'au centre ; chemin local |
| `AIRFLOW__LOGGING__BASE_LOG_FOLDER` | `/opt/airflow/logs` | répertoire inscriptible (copie locale des logs) |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `False` | |
| `AIRFLOW__EDGE__HEARTBEAT_INTERVAL` / `JOB_POLL_INTERVAL` | `10` / `2` | démo plus réactive (défauts 30 / 5) |
| `AIRFLOW__EDGE__DRAIN_TIMEOUT_SEC` | `30` (laptop seulement) | borne le drain à l'arrêt |

Commande : `airflow edge worker -q <queue> -c 2 -H <hostname>` (`-q` liste de queues séparées par des
virgules, toutes si absent ; `-c` concurrence ; `-H` identité ; `-t` nom d'équipe pour le mode
multi-team **expérimental** — simple indication UI/REST, non appliquée par l'Execution API).

## 3. Authentification

- Pas de compte utilisateur ni de token stocké : chaque requête porte un **JWT généré par le worker**
  avec `[api_auth] jwt_secret`, de courte validité (`jwt_leeway`, 30 s par défaut), dont le claim
  `method` est le chemin appelé.
- Le secret partagé est donc une **clé de confiance** : qui l'a peut se déclarer worker et recevoir
  des workloads (et leurs jetons d'Execution API). À traiter comme un secret de production.
- Pendant l'exécution, la tâche utilise le **jeton du workload** (émis par le scheduler) pour
  l'Execution API, pas le secret partagé.

## 4. Règle des versions

À l'enregistrement, le serveur compare `airflow_version` et `edge_provider_version` envoyées par le worker
aux siennes. Par défaut (clés `minimum_acceptable_*` non définies), **toute différence → HTTP 400** et le
worker s'arrête. Avec ces clés, un worker plus ancien mais au-dessus du minimum est accepté en état
"version mismatch" (avertissement).

La **version de Python n'est que rapportée** : 3.12.14, 3.13.15 et 3.11.17 cohabitent ici
(`GET /edge_worker/ui/worker`, champ `sysinfo.python_version`).

## 5. Routage : executor et queue

| Ce que déclare la tâche | Où elle tourne |
|---|---|
| rien | executor par défaut = **LocalExecutor**, dans le pod scheduler |
| `queue="team-data"` seul | **LocalExecutor** quand même : la queue n'est lue que par l'executor choisi |
| `executor="EdgeExecutor"` sans queue | EdgeExecutor, queue `default` : seul un worker qui écoute `default` (ou toutes les queues) la prendra |
| `executor="EdgeExecutor", queue="team-data"` | le(s) worker(s) qui écoutent `team-data` |
| `@asset(...)` | **LocalExecutor** : le décorateur n'accepte ni `executor` ni `queue` (ni `default_args`) en 3.3.2 |

Convention des équipes (voir [`data_ingest_orders.py`](../teams/team-data/dags/data_ingest_orders.py)) :

```python
default_args={"executor": "EdgeExecutor", "queue": "team-data", "owner": "team-data"},
```

Preuve pour `@asset` : `demo_shorthand` ([`asset_shorthand_demo.py`](../teams/team-data/dags/asset_shorthand_demo.py))
a tourné sur `airflow-scheduler-0.airflow-scheduler.airflow-edge.svc.cluster.local` (log du scénario `assets`).
C'est pour ça que le scheduler monte aussi les ConfigMaps d'équipe, et que le corps de `demo_shorthand`
n'utilise que la bibliothèque standard.

Une tâche Edge sans worker vivant sur sa queue reste **`queued`** ; aucun autre executor ne la prend
(scénario `isolation`).

## 6. CLI

Sur le worker (`kubectl -n team-data exec deploy/team-data-worker -- ...` ou `docker compose exec laptop-worker ...`) :

| Commande | Effet |
|---|---|
| `airflow edge worker -q Q -c N -H NOM` | démarre le worker |
| `airflow edge status` | état du worker local |
| `airflow edge maintenance on -c "commentaire"` / `off` | maintenance du worker local |
| `airflow edge stop [--wait]` | arrêt propre (drain) du worker local |

Au centre (accès DB nécessaire → dans le scheduler : `kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- ...`) :

| Commande | Effet |
|---|---|
| `airflow edge list-workers` | tableau des workers (état, queues, jobs actifs) |
| `airflow edge remote-edge-worker-request-maintenance -H NOM -c "texte"` | mise en maintenance à distance |
| `airflow edge remote-edge-worker-exit-maintenance -H NOM` | sortie de maintenance |
| `airflow edge remote-edge-worker-update-maintenance-comment -H NOM -c "texte"` | commentaire |
| `airflow edge add-worker-queues` / `remove-worker-queues` | change les queues d'un worker |
| `airflow edge set-worker-concurrency` | change sa concurrence |
| `airflow edge shutdown-remote-edge-worker -H NOM` / `shutdown-all-workers` | arrêt propre demandé à distance |
| `airflow edge remove-remote-edge-worker -H NOM` | supprime une entrée (worker fantôme, doit être offline) |

## 7. UI et routes HTTP du plugin

| Route | Quoi |
|---|---|
| `/plugin/edge_executor` | page React (menu **Admin → Edge Executor**) : workers, jobs, maintenance |
| `/edge_worker/docs` | OpenAPI du plugin (menu Docs → Edge Worker API docs) |
| `/edge_worker/v1/health` | sonde, `200 {"status":"healthy"}` |
| `/edge_worker/v1/worker/{nom}` | POST = enregistrement, PATCH = heartbeat (JWT worker) |
| `/edge_worker/v1/jobs/fetch/{nom}`, `/v1/jobs/state/...`, `/v1/logs/push/...` | API du worker (JWT worker) |
| `GET /edge_worker/ui/worker`, `GET /edge_worker/ui/jobs` | JSON des workers / jobs (token utilisateur) |
| `POST` / `PATCH` / `DELETE /edge_worker/ui/worker/{nom}/maintenance` | entrer (`{"maintenance_comment": "..."}`), modifier, sortir de maintenance |
| `POST /edge_worker/ui/worker/{nom}/shutdown`, `DELETE /edge_worker/ui/worker/{nom}` | arrêt demandé, suppression |
| `PUT` / `DELETE /edge_worker/ui/worker/{nom}/queues/{queue}`, `PATCH .../concurrency` | queues, concurrence |
| `/edge_worker/v1/rpcapi` | **404** : ce n'est qu'une base d'URL |

Exemple (Git Bash ; jeton par `POST /auth/token`) :

```bash
TOKEN=$(curl -s -X POST -H "Host: airflow-edge.localhost" -H "Content-Type: application/json" -d '{"username":"admin","password":"admin"}' http://127.0.0.1/auth/token | sed -E 's/.*"access_token":"([^"]+)".*/\1/')
curl -s -H "Host: airflow-edge.localhost" -H "Authorization: Bearer $TOKEN" http://127.0.0.1/edge_worker/ui/worker
curl -s -X POST -H "Host: airflow-edge.localhost" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"maintenance_comment":"demo"}' http://127.0.0.1/edge_worker/ui/worker/team-data-worker-1/maintenance
curl -s -X DELETE -H "Host: airflow-edge.localhost" -H "Authorization: Bearer $TOKEN" http://127.0.0.1/edge_worker/ui/worker/team-data-worker-1/maintenance
```

## 8. Logs

Le worker écrit le log de la tâche localement puis le **pousse par morceaux** à l'api-server
(`push_logs = True`), qui le stocke dans `edge_logs` et sur son disque. L'UI et
`GET /api/v2/dags/{dag}/dagRuns/{run}/taskInstances/{task}/logs/{try}?full_content=true` le servent
aussitôt (le scénario relève "disponible via l'API après 0 s"). Conséquences : **1 replica**
d'api-server, et logs perdus si son `emptyDir` disparaît (pas de RWX avec local-path).
Pour la prod : logging distant (S3...) configuré sur le centre et les workers.

## 9. États et cycle de vie

| État du worker | Signification |
|---|---|
| `starting` | enregistré, pas encore de heartbeat |
| `idle` / `running` | vivant, sans / avec jobs |
| `maintenance request` → `maintenance pending` → `maintenance mode` | finit ses jobs en cours, n'en prend plus |
| `maintenance exit` | sortie demandée |
| `terminating` / `shutdown request` | arrêt en cours |
| `offline` / `offline maintenance` | arrêté proprement (hors / en maintenance) ; l'entrée reste en base |
| `unknown` | heartbeat trop ancien |

Identité = **hostname** (`-H`). Sans nom fixe, chaque pod k8s redémarré crée une nouvelle entrée et
laisse un **fantôme** `offline`. D'où `-H team-data-worker-1`, `hostname: team-data-worker-1` dans le pod
(pour que le champ `hostname` des task instances, qui vient de `socket.getfqdn()`, affiche ce nom),
`replicas: 1` et `strategy: Recreate`. Pour scaler une équipe : un Deployment (ou StatefulSet) par nom.

## 10. Arrêt propre

Le worker installe deux gestionnaires de signaux :

| Signal | Comportement |
|---|---|
| `SIGINT` | **drain** : ne prend plus de job, attend la fin des jobs en cours (au plus `drain_timeout_sec` si > 0, puis SIGTERM aux jobs, puis SIGKILL après `drain_kill_grace_sec`) |
| `SIGTERM` | envoie SIGTERM à tous les jobs en cours et quitte |

Le compose du laptop utilise `stop_signal: SIGINT`, `stop_grace_period: 60s` et `DRAIN_TIMEOUT_SEC=30`.

Kubernetes envoie **SIGTERM** au PID 1 à l'arrêt d'un pod. Dans l'image officielle, le PID 1 est
`dumb-init` (`DUMB_INIT_SETSID=1`, il relaie le signal au groupe du process) et le worker
`airflow edge worker` est son fils (PID 7) : sans rien faire, les jobs en cours seraient tués.
Les Deployments `team-data` / `team-search` ont donc un hook **`preStop`**, exécuté **avant** le SIGTERM :

```yaml
terminationGracePeriodSeconds: 90          # > drain (45 s) + drain_kill_grace_sec (30 s)
lifecycle:
  preStop:
    exec:
      command: [sh, -c, 'pid=$(cat /opt/airflow/airflow-edge-worker.pid) && kill -INT "$pid" &&
                while kill -0 "$pid" 2>/dev/null; do sleep 1; done']
env: AIRFLOW__EDGE__DRAIN_TIMEOUT_SEC=45   # drain borné (défaut 0 = attente infinie)
```

Le SIGINT vise **le worker seul** (PID lu dans le pid file qu'il écrit par défaut sous `$AIRFLOW_HOME`,
celui qu'utilise `airflow edge stop`), pas `kill -INT 1` : dumb-init le relaierait à tout le groupe, donc
aussi aux jobs (qui ont remis les gestionnaires par défaut). L'image n'a ni `ps` ni `pkill`.
`airflow edge stop --wait` ferait la même chose mais démarre une CLI Airflow complète (plus lent).

Validé (`scale --replicas=0` 2 s après le démarrage d'une tâche de 20 s) : worker `running` →
`terminating` → `offline`, la tâche finit en `success` (essai 1), puis le pod disparaît ~18 s après
la demande d'arrêt ; au `--replicas=1`, le worker se ré-enregistre sous le même nom (aucune entrée
fantôme). Les jobs encore en file restent `queued` et repartent avec le worker.

## 11. Pièges

| Symptôme | Cause | Correctif |
|---|---|---|
| worker : `403 Forbidden` | `AIRFLOW__API_AUTH__JWT_SECRET` ≠ `jwtSecret` du release (ou secret régénéré par `helm upgrade`) | fixer `jwtSecret` dans les values, même valeur dans le Secret du worker |
| worker : `405 Method Not Allowed` sur `/edge_worker/...` | plugin non monté | `config.edge.api_enabled: "True"` |
| `curl .../edge_worker/v1/rpcapi` → 404 | normal : URL de base | sonder `/edge_worker/v1/health` |
| worker refusé (HTTP 400, "Rejecting access") | Airflow ou edge3 différents du centre | mêmes versions partout (contraintes 3.3.2) |
| entrées `offline` en double | hostname de pod aléatoire | `-H` fixe + `hostname:` du pod + 1 replica |
| tâches tuées (`failed` / relancées) à l'arrêt ou au rollout d'un worker k8s | SIGTERM de kubelet = arrêt dur (SIGTERM aux jobs) | `preStop` qui envoie SIGINT au PID du pid file et attend ; `DRAIN_TIMEOUT_SEC` + `terminationGracePeriodSeconds` plus grand |
| tâche `queued` indéfiniment | aucun worker sur la queue, ou worker en maintenance / offline | `airflow edge list-workers` |
| tâche d'équipe exécutée au centre | `queue` sans `executor="EdgeExecutor"`, ou `@asset` | `default_args={"executor": "EdgeExecutor", "queue": ...}` |
| `ModuleNotFoundError: polars` **au parsing** | import d'une lib d'équipe en tête de fichier | imports dans les fonctions de tâche (règle D3, [06](06-isolation-equipes.md#2-la-règle-d3)) |
| `Detected recursive loop when walking DAG directory` | bundle pointé sur la racine d'un montage ConfigMap | pointer sur `<montage>/..data` |
| worker : DAG ou tâche introuvable juste après `dags.py apply` | ConfigMap pas encore propagée dans le pod (~60-80 s) | attendre la propagation des deux côtés |
| scheduler à ~1 Go de RAM au repos | 32 process LocalExecutor pré-forkés | `[core] parallelism: 8` |
| logs Edge introuvables après redémarrage | logs sur l'`emptyDir` de l'api-server | logging distant |
| worker Windows natif | Airflow exige `fork` / signaux POSIX | Docker ou WSL |
| client ES 9.x → `400 media_type_header_exception` | serveur ES 8.x | `elasticsearch>=8.15,<9` (voir [`teams/team-search/Dockerfile`](../teams/team-search/Dockerfile)) |
