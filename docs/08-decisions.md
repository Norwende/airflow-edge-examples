# 08 — Décisions (ADR)

Dérivées de [`PLAN.md`](../PLAN.md) (décisions D1 à D8 et changements de contrat en cours de route).
Format court : contexte, décision, conséquences.

## D1 — Executors hybrides `LocalExecutor,EdgeExecutor`

- **Contexte** : montrer côte à côte l'"ancienne" idéologie (tout tourne au centre) et la nouvelle (tâches sur les workers des équipes).
- **Décision** : `executor: "LocalExecutor,airflow.providers.edge3.executors.EdgeExecutor"` ; le premier, Local, est le défaut.
- **Conséquences** : les DAGs `core` tournent dans le pod scheduler sans rien déclarer ; les tâches d'équipe
  doivent porter `executor="EdgeExecutor"` (une `queue` seule ne suffit pas) ; `@asset`, qui n'accepte pas
  d'executor, tourne au centre. Le scheduler devient un StatefulSet (PVC de logs) et pré-forke
  `parallelism` process → `parallelism: 8`.

## D2 — DAGs `core` dans l'image, DAGs d'équipe par ConfigMap, un bundle par équipe

- **Contexte** : pas de serveur git local ; une équipe doit livrer ses DAGs sans toucher au release Helm.
- **Décision** : bundle `core` cuit dans l'image centrale ; bundles `team-data`, `team-search`, `laptop` =
  `LocalDagBundle` sur les ConfigMaps `dags-<team>` montées (sans `subPath`, `optional: true`).
- **Conséquences** : publication en une commande (`dags.py apply`), propagation ~1 min, limite de 1 MiB par
  ConfigMap, pas d'épinglage du code par run. `GitDagBundle` est la voie production documentée
  ([05](05-assets-versioning-hitl.md#2-versioning-de-dags)).

## D3 — Un DAG d'équipe se parse au centre sans les libs de l'équipe

- **Contexte** : le dag-processor central n'a ni polars, ni elasticsearch, ni duckdb ; c'est tout l'intérêt.
- **Décision** : imports spécifiques **dans** les fonctions de tâche (ou sous `TYPE_CHECKING`).
- **Conséquences** : un test par équipe parse chaque fichier avec ces modules bloqués dans `sys.modules`
  ([06 §2](06-isolation-equipes.md#2-la-règle-d3)). Les helpers partagés d'une équipe suivent la même règle.

## D4 — MinIO et Elasticsearch mono-nœud dans `airflow-edge`

- **Contexte** : il faut des systèmes externes réalistes, avec des libs clientes spécifiques, sur un cluster à RAM limitée.
- **Décision** : MinIO (bucket `datalake`, Job `minio-init`) ; Elasticsearch 8.19.6, sécurité off, heap 256m.
- **Conséquences** : ES est le pod le plus gros (~800 Mi). Le client Python 9.x imposé par les contraintes
  3.3.2 est refusé par un serveur 8.x → team-search installe `elasticsearch>=8.15,<9` hors contraintes.

## D5 — Trois workers aux environnements différents

- **Contexte** : prouver l'isolation des libs, de la version de Python et du lieu d'exécution.
- **Décision** : `team-data` (k8s, py3.12, polars), `team-search` (k8s, py3.13, elasticsearch/pandas/pyarrow),
  `laptop` (docker compose hors cluster, py3.11 slim + duckdb).
- **Conséquences** : trois styles d'emballage ([06 §3](06-isolation-equipes.md#3-trois-façons-demballer-un-worker)) ;
  le laptop joint l'api-server par Traefik via `extra_hosts: host-gateway` (validé, pas besoin de port-forward)
  et surcharge l'endpoint S3 par `MINIO_ENDPOINT_URL`.

## D6 — Secrets fixes et connus

- **Contexte** : des workers hors cluster doivent partager le secret JWT ; un secret généré par le chart change à chaque `helm upgrade`.
- **Décision** : `jwtSecret`, `fernetKey`, `apiSecretKey` fixés dans les values ; Secret `airflow-edge-worker` dans chaque namespace d'équipe.
- **Conséquences** : reproductible, mais secrets en clair dans le repo — inacceptable hors démo
  ([06 §8](06-isolation-equipes.md#8-recommandations-production)).

## D7 — Scripts Python PEP 723 lancés par `uv run`

- **Contexte** : poste Windows ; cohérence avec le projet voisin `mlops-k8s-demo`.
- **Décision** : `scripts/*.py` (stdlib, ou `boto3` déclaré en en-tête PEP 723), qui affichent chaque commande lancée ; commandes brutes documentées à côté.
- **Conséquences** : PowerShell et Git Bash ; contournement du résolveur Windows (`127.0.0.1` + `Host:`).

## D8 — Namespace `airflow-edge` et Ingress Traefik `*.localhost`

- **Contexte** : le cluster héberge déjà un release `airflow` et une Gateway (projet voisin).
- **Décision** : namespace et release dédiés, Ingress classe `traefik` : `airflow-edge.localhost`, `minio.localhost`, `minio-console.localhost`, `es.localhost`.
- **Conséquences** : autonome, n'abîme pas l'autre projet ; RAM partagée avec lui.

## C1 — Bundles d'équipe sur `<montage>/..data`

- **Contexte** : un volume ConfigMap contient `..<horodatage>/` et le lien `..data` ; le parcours de bundle
  d'Airflow plante (`Detected recursive loop`) sur la racine du montage, même ConfigMap absente.
- **Décision** : chemins `/opt/airflow/dags/<team>/..data` au centre et sur les workers k8s ; montage toujours sur `/opt/airflow/dags/<team>`.
- **Conséquences** : seuls les vrais fichiers sont vus, toujours la dernière version. Le laptop (bind mount d'un dossier ordinaire) n'utilise pas `..data`.

## C2 — La même ConfigMap dans le namespace central et dans celui de l'équipe

- **Contexte** : le worker résout le fichier par nom de bundle + chemin relatif ; avec des DAGs cuits dans
  l'image du worker, chaque changement de DAG imposait un rebuild et le centre pouvait diverger.
- **Décision** : `dags.py apply <team>` écrit `dags-<team>` dans `airflow-edge` **et** dans `<team>` ; le worker la monte (volume non optionnel).
- **Conséquences** : source unique, démo du versioning sans rebuild (nouvelle version en 50-65 s) ;
  le namespace de l'équipe doit exister avant `dags.py apply` ; le `COPY dags/` des images ne sert plus qu'aux tests.

## C3 — Hostname du pod = nom du worker

- **Contexte** : le champ `hostname` des task instances vient de `socket.getfqdn()` ; l'identité du worker vient de `-H`.
- **Décision** : `hostname: <nom>` dans le pod et le service compose, identique à `-H` ; 1 replica, `Recreate`.
- **Conséquences** : pas de worker fantôme après redémarrage ; l'UI et l'API montrent `team-data-worker-1`. Scaler = un Deployment par nom.

## C4 — Le scheduler monte aussi les ConfigMaps d'équipe

- **Contexte** : une tâche d'équipe sur l'executor par défaut (`@asset`) s'exécute au centre et doit trouver son fichier.
- **Décision** : mêmes `extraVolumes` / `extraVolumeMounts` pour `dagProcessor` et `scheduler`.
- **Conséquences** : `demo_shorthand` s'exécute sur `airflow-scheduler-0` ; son code reste limité à la stdlib (D3).

## C5 — `S3Hook` du provider Amazon dans les DAGs `core`

- **Contexte** : l'"ancienne" idéologie s'appuie sur des providers installés dans l'image centrale.
- **Décision** : les DAGs classiques lisent/écrivent MinIO par `S3Hook(aws_conn_id="minio_s3")` au lieu d'un client boto3 construit à la main.
- **Conséquences** : dépendance au provider Amazon de l'image officielle ; illustre "la lib vient du centre"
  (voir [04](04-ancien-vs-nouveau.md#4-dépendances)). Les DAGs d'équipe gardent boto3 dans leur worker.

## C6 — Drain des workers k8s par `preStop`

- **Contexte** : kubelet envoie SIGTERM au PID 1 (`dumb-init`) ; le worker edge traite SIGTERM comme un arrêt dur (SIGTERM aux jobs).
- **Décision** : hook `preStop` qui envoie SIGINT au seul process worker (PID du pid file `/opt/airflow/airflow-edge-worker.pid`) et attend sa sortie ; `AIRFLOW__EDGE__DRAIN_TIMEOUT_SEC=45`, `terminationGracePeriodSeconds: 90` (au lieu de 60).
- **Conséquences** : scale 0 / rollout / drain de nœud laissent finir les tâches en cours (validé : tâche de 20 s en `success`, worker `offline`) ; un arrêt peut prendre jusqu'à ~75 s ; une tâche plus longue que le drain est tuée puis relancée par ses retries.
