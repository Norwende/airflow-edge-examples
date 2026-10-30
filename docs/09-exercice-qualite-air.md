# 09 — Exercice guidé : « Observatoire de la qualité de l'air »

Un **second cas d'usage à construire soi-même** sur la plateforme de la démo, pour s'exercer à
l'ensemble des fonctionnalités d'Airflow 3 (3.0 → 3.3) et de l'Edge Executor. Les étapes sont
progressives ; chacune donne l'objectif, ce qu'il faut produire, des indices avec les liens vers la
documentation officielle, des critères de validation, des **pistes de solution repliées** (`▶ Pistes`)
et, en fin d'étape, **ce qui a été observé** en déroulant la solution sur le cluster (le « corrigé »).

Une solution complète, exécutée et vérifiée le 2026-10-17, est rangée dans [`archive/solution/`](../archive/solution/README.md)
(fichiers à superposer au repo, `apply.py`, et `verify.py` qui rejoue les contrôles de chaque étape).
Ne l'ouvrir qu'après avoir essayé. Versions : Airflow 3.3.2, Task SDK 1.3, edge3 4.3.2, provider
standard 1.19.0, http 6.1.0, git 0.5.0, chart 1.22.0.

## 0. Avant de commencer

**Prérequis** : la démo tourne (`uv run scripts/smoke_test.py` vert, [02-deploiement.md](02-deploiement.md)),
les docs [03](03-edge-executor.md), [05](05-assets-versioning-hitl.md) et [06](06-isolation-equipes.md) lues.
Budget indicatif : 1 à 2 h par niveau ; ~1,2 Gi de RAM en plus (un worker, Mailpit, Gitea en bonus).

**Règles du jeu**

1. Le centre ne reçoit **aucune lib métier** : règle D3 ([06 §2](06-isolation-equipes.md#2-la-règle-d3)).
2. Tout ce qu'une équipe fait passe par **son** bundle, **son** image, **sa** queue, **son** namespace.
3. Chaque étape se termine par une **preuve** (UI, API, log, objet MinIO). Noter ce qu'on observe :
   plusieurs comportements méritent d'être comparés à la doc.
4. Rien ne se fait « à la main » dans la base : CLI, API `/api/v2`, UI.

**Repères utiles** : jeton API et exemples `curl` dans [03 §7](03-edge-executor.md#7-ui-et-routes-http-du-plugin)
et [07 §1](07-exploitation.md#1-aide-mémoire) ; `*.localhost` ne se résout pas depuis PowerShell/Python
Windows (`127.0.0.1` + en-tête `Host`, voir `scripts/_common.py` et `s3_client()` dans `scripts/scenario.py`) ;
lint : `uvx ruff check .` / `uvx ruff format .` (ruff n'est pas une dépendance du projet, `uv run ruff` ne marche pas).

### Carte des fonctionnalités couvertes

| Fonctionnalité | Depuis | Étape |
|---|---|---|
| Nouveau worker edge : image, namespace, queue, bundle, `helm upgrade` | edge3 | 1 |
| Planification **événementielle** : `AssetWatcher`, trigger `BaseEventTrigger` custom, payload | 3.0 / 3.3 | 3 |
| `expand_kwargs`, `map_index_template`, `max_active_tis_per_dag`, `ObjectStoragePath`, `Param` (formulaire) | 3.0 | 4 |
| `@task.branch`, `@task.short_circuit`, setup/teardown, trigger rules, callbacks/notifiers | 3.0 (`ALL_DONE_MIN_ONE_SUCCESS` 3.1) | 5 |
| HITL : `HITLBranchOperator`, `HITLEntryOperator`, `params`, `assigned_users`, `notifiers`, API | 3.1 (`awaiting_input` 3.3) | 6 |
| `AssetAlias`, expressions `&`/`\|`, `AssetOrTimeSchedule` | 3.0 | 7 |
| **Partitions d'assets** : `CronPartitionTimetable`, `PartitionedAssetTimetable`, mappers, `PartitionedAtRuntime` | 3.2 / 3.3 | 8 |
| `Asset.ref` inter-équipes, sensor **deferrable** depuis un edge worker, `@task.sensor` | 3.0 | 9 |
| **Backfill** (UI / API / CLI), `logical_date=None`, `@task.virtualenv` | 3.0 | 10 |
| **Deadline Alerts** (`DeadlineAlert`, `AsyncCallback`/`SyncCallback`), page *Deadlines* | 3.1 / 3.2 / 3.3 | 11 |
| XCom sur object storage, **logging distant** S3 pour les tâches edge | 3.x | 12 |
| Edge : maintenance, queues/concurrence à chaud, 2 workers par queue, perte de worker, versions | edge3 | 13 |
| Plugins UI 3.1 (`external_views`, iframe) | 3.1 | 14 |
| Bonus : `GitDagBundle` (code épinglé par run), **multi-team** | 3.0 / 3.2 | 15 |

## 1. Le cas d'usage

Des **stations de mesure** déposent des fichiers CSV horaires (PM2.5, PM10, NO₂, O₃, température)
dans MinIO. Une nouvelle équipe **team-iot** les ingère **dès l'arrivée du fichier**, contrôle la
qualité (valeurs impossibles, **dérive** d'un capteur), demande à un humain quoi faire d'une station
suspecte (calibrer avec un offset, mettre en quarantaine, ignorer), publie des mesures validées et des
statistiques horaires **partitionnées**. team-search indexe les mesures dans Elasticsearch ; le laptop
produit un **bulletin quotidien** (DuckDB, graphique) et peut le **rejouer sur 7 jours**. Un
dépassement de délai de la chaîne lève une **alerte**.

```mermaid
flowchart LR
  sim["simulateur de stations<br/>(uv run scripts/air_stations.py)"] -- CSV --> inbox[("s3://datalake/air/inbox")]
  inbox -. AssetWatcher<br/>(triggerer, centre) .-> ingest["iot_ingest_measurements<br/>team-iot-worker-1 (py3.10, sklearn)"]
  ingest --> hitl{"HITL<br/>calibrer / quarantaine / ignorer"}
  ingest --> meas[("air_measurements<br/>s3://datalake/air/measurements")]
  hitl --> quar[("air_quarantine")]
  meas --> hourly["iot_hourly_stats<br/>CronPartitionTimetable"] --> hstats[("air_hourly<br/>partitionné")]
  meas -. Asset.ref .-> search["search_index_air<br/>team-search-worker-1"] --> es[("ES index air")]
  hstats -. PartitionedAssetTimetable .-> bulletin["laptop_air_bulletin<br/>laptop (duckdb, venv matplotlib)"] --> bul[("air_bulletin")]
  ingest -. DeadlineAlert .-> alert["callback (triggerer)"]
```

**Contrat du cas d'usage** (noms figés, à réutiliser tels quels) :

| Élément | Valeur |
|---|---|
| équipe / namespace / queue / bundle | `team-iot` |
| worker | `team-iot-worker-1`, image `airflow-edge/worker-team-iot:dev` (`apache/airflow:3.3.2-python3.10` + numpy + scikit-learn + `amazon[s3fs]`) |
| préfixes S3 (bucket `datalake`) | `air/inbox/<station>/<AAAAMMJJTHHMM>.csv`, `air/processed/`, `air/measurements/ds=<date>/`, `air/quarantine/`, `air/hourly/hour=<clé>/`, `air/bulletin/`, `air/alerts/`, `air/tmp/<run_id>/` |
| assets | `air_inbox` (surveillé), `air_measurements`, `air_quarantine`, `air_hourly` (partitionné), `air_index` (`es://air`), `air_bulletin`, `air_manual_reconcile` ; alias `air_stations` → `air_station_<id>` |
| colonnes CSV | `ts,station,lat,lon,pm25,pm10,no2,o3,temp` |
| anomalies simulées | valeur négative (ligne rejetée) ; **dérive** : `pm25` décalé de +40 sur une station (moyenne ≈ 60, seuil de détection **45**) |

---

## Niveau 1 — Une équipe de plus

### Étape 1 — Créer l'équipe team-iot et son worker

**Objectif** : un 4ᵉ worker edge, dans son namespace, avec **Python 3.10** et des libs que personne
d'autre n'a (`numpy`, `scikit-learn`), visible dans **Admin → Edge Executor**.

**À faire**

- `teams/team-iot/` : `Dockerfile`, `requirements.txt`, `dags/iot_where_am_i.py` (copie adaptée de
  `teams/laptop/dags/laptop_where_am_i.py`), `k8s/` (namespace, Secret JWT, Deployment, kustomization), `tests/`.
- Déclarer le bundle `team-iot` **au centre** (dag-processor **et** scheduler) et republier le chart.
- Ajouter l'équipe à `TEAMS` dans `scripts/_common.py` (et aux listes de `up.py`, `down.py`,
  `smoke_test.py` qui comptent les bundles) pour que les scripts la connaissent.

**Indices**

- Modèle : `teams/team-data/` (Deployment avec `hostname`, `preStop`, `DRAIN_TIMEOUT_SEC`) et
  [`platform/helm-values/airflow.yaml`](../platform/helm-values/airflow.yaml) (`dagBundleConfigList`, ancres `&teamDagVolumes`).
- Bundle sur `<montage>/..data` ([08 C1](08-decisions.md#c1--bundles-déquipe-sur-montagedata)).
- Versions Airflow **et** edge3 identiques au centre, sinon HTTP 400 à l'enregistrement ([03 §4](03-edge-executor.md#4-règle-des-versions)).
- edge3 **5.0.0** (2026-10-13) abandonne Python 3.10 : choisir 3.10 ici est un choix pédagogique
  (voir étape 13) — [changelog edge3](https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/changelog.html).
- Déploiement du worker : [edge3 → Deployment](https://airflow.apache.org/docs/apache-airflow-providers-edge3/4.3.2/deployment.html) ;
  bundles : [DAG bundles](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html).

**Validation**

- `airflow edge list-workers` (dans le scheduler) : `team-iot-worker-1` en `idle`, queue `team-iot`.
- `GET /edge_worker/ui/worker` : `sysinfo.python_version` commence par `3.10`.
- `iot_where_am_i` tourne avec `hostname=team-iot-worker-1` et `import sklearn` réussit **dans la tâche**
  alors que le DAG est parsé au centre sans sklearn (`uv run scripts/dags.py status` : aucune erreur d'import).

<details><summary>▶ Pistes</summary>

```dockerfile
FROM apache/airflow:3.3.2-python3.10
USER airflow
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "apache-airflow-providers-edge3==4.3.2" \
      "apache-airflow-providers-amazon[s3fs]" numpy scikit-learn boto3 pytest \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.10.txt
COPY --chown=airflow:root dags/ /opt/airflow/dags/team-iot/
CMD ["airflow", "edge", "worker", "-q", "team-iot", "-c", "2", "-H", "team-iot-worker-1"]
```

Values Helm : une entrée `{name: team-iot, classpath: ...LocalDagBundle, kwargs: {path: /opt/airflow/dags/team-iot/..data}}`,
un volume `configMap: {name: dags-team-iot, optional: true}` et son montage dans les ancres partagées
dag-processor / scheduler. Puis :

```bash
docker build -t airflow-edge/worker-team-iot:dev teams/team-iot
helm upgrade --install airflow apache-airflow/airflow -n airflow-edge --version 1.22.0 \
  -f platform/helm-values/airflow.yaml --timeout 15m      # ou : uv run scripts/up.py
kubectl apply -k teams/team-iot/k8s
uv run scripts/dags.py apply team-iot
kubectl -n airflow-edge exec sts/airflow-scheduler -c scheduler -- airflow pools set team-iot 8 "pool team-iot"
```

Le pod reste `ContainerCreating` tant que la ConfigMap `dags-team-iot` n'existe pas dans `team-iot`
(volume non optionnel côté worker) : c'est voulu. Solution : `archive/solution/teams/team-iot/`.
</details>

**Observé (solution)** : image construite sans conflit de contraintes (Python 3.10.21, scikit-learn 1.5.2,
numpy 2.2.6, s3fs 2026.7.0) ; worker `idle` **8 s** après le rollout ; `helm upgrade` (révision +1) a
redémarré les 4 composants en moins d'une minute et les trois autres workers se sont ré-enregistrés
aussitôt. Effet de bord à connaître : chaque redémarrage de l'api-server **perd les logs** des runs edge
antérieurs (`emptyDir`, [03 §8](03-edge-executor.md#8-logs)) — d'où l'étape 12.

### Étape 2 — Le simulateur de stations

**Objectif** : un script `scripts/air_stations.py` (PEP 723, `uv run`) qui dépose des CSV dans
`air/inbox/<station>/`, avec des options pour injecter des anomalies.

**À faire** : `--stations 3`, `--hours 2`, `--start 2026-10-17T13:00`, `--negative st-01` (une ligne négative),
`--drift st-02` (+40 sur `pm25`), `--samples N` (lignes par fichier, 12 par défaut), `--seed`, `--dry-run` ;
afficher les clés écrites et la moyenne `pm25`.

**Indices** : réutiliser `s3_client()` de `scripts/scenario.py` (Host `minio.localhost` injecté avant la
signature SigV4) ; stations avec lat/lon fixes (Paris, Lyon, Marseille…) pour l'étape 9.

**Validation** : les objets sont visibles sur http://minio-console.localhost ; `--drift` produit un fichier
dont la moyenne `pm25` est ≈ 60 (base 5-35) ; `--samples 3600` donne un fichier de ~170 Kio (étape 12).

**Observé** : solution dans `archive/solution/scripts/air_stations.py` (importe `s3_client` de `scenario.py`).

---

## Niveau 2 — Ingestion et qualité (Task SDK)

### Étape 3 — Ingestion événementielle : `AssetWatcher`

**Objectif** : `iot_ingest_measurements` démarre **dès qu'un fichier arrive** dans `air/inbox/`, sans
cron ni sensor, et reçoit les clés des fichiers dans l'événement.

**À faire**

- Un trigger `S3NewObjectTrigger(BaseEventTrigger)` qui liste `air/inbox/` périodiquement et émet un
  `TriggerEvent` pour les **nouveaux** objets.
- `AIR_INBOX = Asset(name="air_inbox", uri="s3://datalake/air/inbox", watchers=[AssetWatcher(name="inbox", trigger=...)])`
  et `@dag(schedule=[AIR_INBOX])`.
- La première tâche lit `context["triggering_asset_events"][AIR_INBOX]` → `event.extra["payload"]`.

**Questions à se poser avant de coder**

1. **Où s'exécute le trigger ?** Dans le **triggerer**, au centre. Il a boto3 (provider Amazon de
   l'image officielle) mais **pas** les libs d'équipe.
2. **Qui doit pouvoir importer la classe du trigger ?** Le dag-processor (parsing), le **worker**
   (il re-parse le fichier DAG avant d'exécuter la tâche), le **triggerer** (il importe par classpath
   depuis `serialize()`). Un module dans le bundle `team-iot` couvre les deux premiers (le dossier du bundle
   est dans `sys.path`) ; le triggerer, lui, ne charge pas les bundles → il faut aussi le module dans
   l'image centrale, sous le **même nom de module** (`iot_triggers.py`, dossier `plugins/`).
3. **« Condition qui reste vraie »** : la doc avertit qu'un trigger qui redevient vrai à chaque poll
   (« le fichier existe ») replanifie à l'infini. Le DAG doit **déplacer** le fichier vers `air/processed/`
   et le trigger n'émettre que les clés nouvelles (état en mémoire + idempotence côté DAG).
4. **Un événement par fichier ou par cycle ?** Voir « Observé » : un événement **par cycle de poll**, avec la
   liste des clés, et une tâche `new_keys` qui **liste aussi l'inbox** (rien n'est laissé derrière).

**Indices**

- [Event-driven scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html)
  (`from airflow.triggers.base import BaseEventTrigger, TriggerEvent` ; 3.3 ajoute les *shared streams*).
- Alternative « clé en main » : `MessageQueueTrigger(scheme="sqs"|"kafka"|"redis+pubsub", ...)` du provider
  `common.messaging` ([triggers](https://airflow.apache.org/docs/apache-airflow-providers-common-messaging/stable/triggers.html)) —
  pas de broker dans la démo, d'où le trigger custom.
- Le `plugins_folder` central (`/opt/airflow/plugins`) est ajouté au `sys.path` dès `import airflow` :
  un `COPY plugins/` dans l'image centrale suffit. **Redémarrer le triggerer** après toute modification du trigger
  (les kwargs n'ayant pas changé, la ligne `trigger` en base est réutilisée telle quelle).
- Un trigger est **asynchrone** : enrober boto3 dans `asyncio.to_thread(...)` ; la connexion se lit avec
  `await Connection.async_get("minio_s3")` (`airflow.sdk`), disponible dans le triggerer.
- Le watcher n'est créé qu'une fois le DAG **dépausé** (`PATCH /api/v2/dags/{id} {"is_paused": false}`) ;
  le triggerer logue alors `trigger ID n starting` puis `1 watchers currently running`.

**Validation**

- `GET /api/v2/assets/events?name_pattern=air_inbox&order_by=-timestamp` : `extra.from_trigger=true`,
  `extra.payload.keys` renseigné ; `GET /api/v2/dags/iot_ingest_measurements/dagRuns` : `run_type=asset_triggered`.
- Log de la tâche sur `team-iot-worker-1` : les clés reçues ; les objets ont été déplacés dans `air/processed/`.
- Redéposer le **même** fichier → aucun nouveau run (le trigger l'a déjà vu), pas de boucle.

<details><summary>▶ Pistes</summary>

```python
# teams/team-iot/dags/iot_triggers.py  (copie identique : platform/central-image/plugins/iot_triggers.py)
class S3NewObjectTrigger(BaseEventTrigger):
    def __init__(self, prefix, poke_interval=15.0, bucket="datalake", aws_conn_id="minio_s3", **kw): ...
    def serialize(self):
        return (f"{type(self).__module__}.{type(self).__qualname__}", {...})   # -> "iot_triggers.S3NewObjectTrigger"
    async def run(self):
        conn = await Connection.async_get(self.aws_conn_id)          # endpoint_url dans l'extra
        seen = await asyncio.to_thread(self._list, conn)             # l'existant n'est pas "nouveau"
        while True:
            await asyncio.sleep(self.poke_interval)
            now = await asyncio.to_thread(self._list, conn)
            new = sorted(now.keys() - seen.keys())
            if new:
                yield TriggerEvent({"keys": new, "count": len(new), "bucket": self.bucket})   # UN événement par cycle
            seen = now
```

```python
AIR_INBOX = Asset(name="air_inbox", uri="s3://datalake/air/inbox",
                  watchers=[AssetWatcher(name="inbox", trigger=S3NewObjectTrigger(prefix="air/inbox/"))])

@task
def new_keys(**context) -> list[dict]:
    keys = {k for e in context["triggering_asset_events"][AIR_INBOX] for k in e.extra["payload"]["keys"]}
    keys |= set(list_keys("air/inbox/"))          # filet de sécurité : tout ce qui attend encore
    return [{"key": k} for k in sorted(keys)]
```

Solution : `archive/solution/teams/team-iot/dags/iot_triggers.py`, `iot_ingest_measurements.py`.
</details>

**Observé (solution)** : le watcher a démarré dans le triggerer ~10 s après la dépause et logue
`air/inbox/ : 0 objets déjà présents (ignorés)`. **Piège réel** : avec une première version qui émettait
**un événement par fichier**, 3 fichiers déposés ensemble ont produit 3 événements en 20 ms ; le scheduler a
créé **un** run rattaché au **premier** événement seulement (`upstreamAssetEvents` = 1), et les deux autres
n'ont jamais déclenché de run (file `asset_dag_run_queue` vide) : deux fichiers restaient dans l'inbox, déjà
« vus » par le trigger. D'où la conception retenue (un événement par cycle + listing de l'inbox) : le run
suivant a traité 11 fichiers, dont les retardataires. Redéposer une clé existante n'émet rien.

### Étape 4 — Mapping dynamique, `ObjectStoragePath`, paramètres

**Objectif** : traiter chaque fichier reçu comme une **instance mappée** lisible dans l'UI, lire et écrire
S3 sans client maison, et offrir un **formulaire de déclenchement** manuel.

**À faire**

- `parse_file.partial(...).expand_kwargs(new_keys())` avec `map_index_template="{{ task.op_kwargs['key'] }}"`
  et `max_active_tis_per_dag=4`.
- Lecture/écriture par `ObjectStoragePath("s3://minio_s3@datalake/air/...")` ; parquet validé écrit dans
  `air/measurements/ds=<date>/<station>-<HHMM>.parquet`.
- `params={"stations": Param([], type="array", items={"type": "string"}), "reprocess_prefix": Param(None, type=["null","string"]), "dry_run": Param(False, type="boolean")}` :
  un run **manuel** (`logical_date=None`) retraite `air/processed/<prefix>` au lieu d'attendre un événement.
- `pool="team-iot"` (créé via CLI/API) ; `priority_weight` : voir « Observé ».

**Indices**

- [Dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html),
  [Object storage](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html)
  (S3 exige `apache-airflow-providers-amazon[s3fs]` **dans l'image du worker** ; les déplacements ne sont pas atomiques),
  [Params](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html)
  (`description_md`, `section`, `enum`, `values_display` ; un type sans `"null"` rend le champ obligatoire),
  [Pools / priority weight](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/priority-weight.html).
- Les `pool_slots` d'une tâche consomment aussi des **slots du worker edge**
  ([concurrency slot handling](https://airflow.apache.org/docs/apache-airflow-providers-edge3/4.3.2/edge_executor.html#concurrency-slot-handling)).
- `context["params"]` dans la tâche ; `context["dag"].params` ne contient que les défauts ;
  `[core] dag_run_conf_overrides_params` conditionne la surcharge au déclenchement.

**Validation** : la grille montre un index par **clé de fichier** (pas 0, 1, 2) ; le formulaire
*Trigger* affiche les trois champs typés ; un run manuel avec `reprocess_prefix` produit les mêmes parquets ;
`dry_run=true` n'écrit rien (vérifier MinIO).

**Observé (solution)** : `rendered_map_index` = `air/inbox/st-02/20261017T1300.csv` dans l'API et l'UI ;
`priority_weight` ne peut pas différer **par instance mappée** (c'est un attribut de la tâche) : la priorité
« fichier en dérive » n'est pas implémentable ainsi — à faire par un DAG ou une queue dédiée ; en `dry_run`,
`publish` lève `AirflowSkipException` pour **ne pas émettre** d'événement d'asset.

### Étape 5 — Contrôle qualité : branches, court-circuit, setup/teardown, notifications

**Objectif** : une structure de DAG robuste : ne rien faire s'il n'y a rien à faire, isoler une station
suspecte, nettoyer même en cas d'échec, prévenir en cas de panne.

**À faire**

- `@task.short_circuit` : aucun fichier nouveau → le reste est *skipped*.
- `@task.branch` `quality_gate` : si une station dérive (moyenne `pm25` > 45 ; `IsolationForest` de
  scikit-learn en information), aller vers `hitl_decide` (étape 6), sinon `publish`.
- Setup/teardown : `make_tmp_prefix` crée `air/tmp/<run_id>/`, `drop_tmp_prefix` le supprime **toujours**
  (`as_teardown(setups=...)`), sans faire échouer le run.
- Tâche finale `report` avec `trigger_rule=ALL_DONE_MIN_ONE_SUCCESS` (3.1).
- `on_failure_callback` : un **notifier**. Option visible : déployer **Mailpit** (SMTP 1025 / UI 8025,
  [`platform/mailpit/`](../platform/mailpit/mailpit.yaml)) dans `airflow-edge`, connexion `smtp_default`
  (extra `{"disable_tls": true, "disable_ssl": true, "from_email": ...}`), `SmtpNotifier` ; plus un `BaseNotifier`
  maison qui écrit `air/alerts/<dag>-<run_id>.json` dans MinIO.

**Indices**

- [Setup and teardown](https://airflow.apache.org/docs/apache-airflow/stable/howto/setup-and-teardown.html),
  [TaskFlow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html) (`@task.branch`, `@task.short_circuit`),
  [Notifications](https://airflow.apache.org/docs/apache-airflow/stable/howto/notifications.html) (`from airflow.sdk import BaseNotifier`),
  [Callbacks](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html).
- Un callback de **tâche** s'exécute sur le worker (donc avec les libs de l'équipe) ; les erreurs des callbacks
  de **DAG** sont loguées côté dag-processor, pas dans l'UI : regarder `kubectl logs` du dag-processor.
- Où vit la logique « IsolationForest » ? Dans la fonction de tâche (D3) ; `sklearn` absent du centre.

**Validation** : run sans fichier → tout *skipped*, `drop_tmp_prefix` quand même *success* ; run avec
`--drift` → `hitl_decide` activée ; faire échouer `publish` volontairement (`conf {"fail_publish": true}`)
→ mail dans Mailpit et objet `air/alerts/...`, `drop_tmp_prefix` exécutée.

**Observé (solution)** — quatre points de conception qui ne se voient qu'en exécutant :
- ordre `new_keys >> make_tmp_prefix (setup) >> has_work (short-circuit)` : si le setup venait **après** le
  court-circuit, il serait sauté et le teardown aussi ;
- `publish` est en aval de **toutes** les branches (`NONE_FAILED_MIN_ONE_SUCCESS`) : une dérive sur une
  station n'empêche pas de publier les autres ;
- `report` doit **échouer** si une tâche a échoué : sinon la feuille marque le run `success` et le
  `on_failure_callback` du DAG ne part jamais ;
- une exception dans un **notifier** fait échouer la tâche qui le porte (vu avec un SMTP injoignable sur
  `hitl_decide`) : la solution enrobe l'envoi dans un `SmtpNotifier` dérivé qui ne lève jamais.
`IsolationForest(contamination="auto")` marque 36-67 % de lignes saines et ne voit pas un décalage uniforme
de +40 (c'est un *cluster*, pas des points isolés) : il reste informatif, la moyenne décide.

### Étape 6 — Human-in-the-loop avancé

**Objectif** : l'humain **choisit la suite** et **saisit une valeur** ; seuls certains utilisateurs peuvent
répondre ; la réponse arrive par UI et par API.

**À faire**

- `hitl_decide = HITLBranchOperator(subject=..., body=..., options=["calibrate", "quarantine", "ignore"],
  options_mapping={"calibrate": "apply_calibration", "quarantine": "incident_note", "ignore": "publish"},
  params={"offset": Param(0.0, type="number", minimum=-50, maximum=50)}, defaults="quarantine",
  response_timeout=timedelta(minutes=30), assigned_users=[{"id": "1", "name": "admin"}])`.
- `apply_calibration` lit l'offset saisi, corrige et publie.
- `quarantine_station` est précédée d'un `HITLEntryOperator(params={"incident": Param("", type="string")})`
  : le commentaire part dans l'`extra` de l'événement `air_quarantine`.
- Avec Mailpit : `notifiers=[SmtpNotifier(...)]` sur l'opérateur HITL, corps contenant le lien
  `HITLOperator.generate_link_to_ui_from_context(context=context, base_url="http://airflow-edge.localhost")`.

**Indices**

- [Tutoriel HITL](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/hitl.html) et
  [référence des opérateurs](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/hitl/index.html).
- `defaults` ⊂ `options`, appliqué à l'expiration de `response_timeout` ; **sans timeout, attente infinie**
  (`HITLEntryOperator` n'en a pas par défaut : en mettre un).
- En 3.3 la tâche attend en **`awaiting_input`** : aucun slot worker, aucun trigger — vérifier sur la page
  Edge Executor que `team-iot-worker-1` a 0 job pendant l'attente ([05 §3](05-assets-versioning-hitl.md#3-human-in-the-loop-hitl)).
- API : `GET /api/v2/dags/~/dagRuns/~/hitlDetails?response_received=false` ;
  `PATCH .../taskInstances/hitl_decide/-1/hitlDetails` avec `{"chosen_options":["calibrate"],"params_input":{"offset":-40}}`.
- `airflow dags test` n'applique ni `response_timeout` ni `defaults`.

**Validation** : trois runs avec `--drift`, trois réponses différentes (UI, API) → trois chemins dans la
grille ; `apply_calibration` reprend bien sur `team-iot-worker-1` (champ `hostname`) ; une réponse d'un
utilisateur non assigné est refusée (403).

<details><summary>▶ Pistes</summary>

- **`assigned_users` : l'`id` est celui du gestionnaire d'authentification**, pas le login. Avec le FAB
  auth manager du chart, `admin` a l'id `1` (`airflow users list`) ; `{"id": "admin"}` provoque à la réponse
  `403 User=admin (id=1) is not a respondent for the task`. C'est aussi la preuve demandée par la validation.
- **`HITLBranchOperator` ne renvoie pas la réponse** : sa valeur de retour (XCom `return_value`) est la liste
  des `task_id` de branche, et quand des tâches sont sautées il lève `DownstreamTasksSkipped` avant tout
  `return`. Pour lire `params_input["offset"]` en aval, dériver l'opérateur et pousser la réponse sous une clé
  XCom dédiée (`hitl_response`) avant d'appeler `super().execute_complete(...)` ; voir
  `AirHITLBranchOperator` dans `archive/solution/teams/team-iot/dags/iot_common.py`.
- Le mapping `quarantine → incident_note` (et non `quarantine_station`) : l'opérateur de saisie s'intercale.
</details>

**Observé (solution)** : `hitl_decide` passe en `awaiting_input` ~20 s après le début du run ; pendant
l'attente `GET /edge_worker/ui/worker` donne `jobs_active: 0` et `GET /edge_worker/ui/jobs` ne liste aucun job
actif ; un mail « décision attendue » avec le lien UI arrive dans Mailpit à chaque demande ; réponse API
`200` avec `responded_by {"id": "1", "name": "admin"}` ; les trois chemins (`calibrate` → `apply_calibration`
→ `publish`, `calibrate_offset=-40` dans l'`extra` de `air_measurements` ; `quarantine` → `incident_note`
(`awaiting_input` à son tour) → `quarantine_station` → événement `air_quarantine`
`{"stations": ["st-03"], "incident": "..."}` ; `ignore` → `publish`) terminent en **15 s** après la réponse,
sur `team-iot-worker-1`. XCom `hitl_response` : `{"chosen_options": ["ignore"], "branches": ["publish"], "responded_by_user": {...}}`.

---

## Niveau 3 — Assets et planification

### Étape 7 — Alias, expressions, temps + assets

**Objectif** : un asset **par station** sans connaître la liste à l'avance, et des consommateurs
planifiés par des **combinaisons**.

**À faire**

- `publish` déclare `outlets=[AIR_MEASUREMENTS, AssetAlias("air_stations")]` et, par station traitée,
  `outlet_events[AssetAlias("air_stations")].add(Asset(name=f"air_station_{station}", uri=...), extra={...})`.
- `iot_station_report` : `schedule=AssetAlias("air_stations")`.
- `iot_reconcile` : `schedule=(AIR_MEASUREMENTS & AIR_QUARANTINE) | Asset.ref(name="air_manual_reconcile")`.
- `iot_weekly_recap` : `AssetOrTimeSchedule(timetable=CronTriggerTimetable("0 6 * * 1", timezone="Europe/Paris"), assets=AIR_MEASUREMENTS)`.

**Indices** : [Asset scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html),
[Timetables](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html).
Rappel 3.3.2 observé : un DAG planifié par `Asset.ref` n'a pas de `triggering_asset_events` ; lire
`inlet_events[...][-1]` ([05 §1](05-assets-versioning-hitl.md#1-assets)).

**Validation** : la vue **Assets** montre `air_stations` → `air_station_st-01..03` créés dynamiquement ;
`iot_reconcile` ne démarre qu'une fois **les deux** assets mis à jour depuis son dernier run ;
`GET /api/v2/dags/iot_reconcile/details` : `asset_expression` reflète l'expression.

**Observé (solution)** : les assets `air_station_st-0N` apparaissent au premier `publish` ; pour le DAG
planifié par **alias**, `triggering_asset_events` est **renseigné** (deux entrées : l'alias et l'asset résolu,
même `extra`) — contrairement au cas `Asset.ref` (`search_index_air` logue `triggering_asset_events : vide`) ;
`iot_reconcile` est resté en attente après le premier `air_measurements` (ligne `asset_dag_run_queue`) et a
démarré 20 s après l'événement `air_quarantine` ; `iot_weekly_recap` a été déclenché par l'asset en plus de
son run cron ; `asset_expression` = `AssetAny(AssetAll(air_measurements, air_quarantine), AssetNameRef(air_manual_reconcile))`.

### Étape 8 — Partitions d'assets (3.2 / 3.3)

**Objectif** : des statistiques **horaires** produites avec une **clé de partition**, et un consommateur
qui ne se déclenche que pour la partition concernée.

**À faire**

- `iot_hourly_stats` : `schedule=CronPartitionTimetable("0 * * * *", timezone="UTC", run_offset=-1)`, lit
  `air/measurements/` pour l'heure `dag_run.partition_date`, écrit `air/hourly/hour=<clé>/stats.parquet`,
  `outlets=[AIR_HOURLY]`.
- Consommateur laptop (étape 10) : `schedule=PartitionedAssetTimetable(assets=Asset.ref(name="air_hourly"), default_partition_mapper=StartOfDayMapper())`.
  Variante à explorer : `RollupMapper(upstream_mapper=StartOfHourMapper(), window=DayWindow(), wait_policy=WaitForAll())` (3.3).
- Variante runtime : `iot_hourly_stats_runtime`, `schedule=PartitionedAtRuntime()`, dont la tâche appelle
  `outlet_events[AIR_HOURLY].add_partitions([...])`.

**Indices** : [Assets → Asset partitions](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html#asset-partitions)
(tout s'importe depuis `airflow.sdk` ; `[scheduler] partition_mapper_max_downstream_keys` = 1000). Pièges listés par
la doc : un événement partitionné ne déclenche **pas** un DAG non partitionné ; des clés non alignées entre
assets amont ne déclenchent rien ; incohérence mapper/fenêtre → `TypeError` au parsing ; backfill = un run par partition.
**`run_offset=-1`** : sans lui, le run de 09:00 reçoit la clé `09:00`, c'est-à-dire une heure qui n'a pas encore
eu lieu ; avec, il traite `08:00`.

**Validation** : `GET /api/v2/dags/iot_hourly_stats/dagRuns` : `partition_key` renseigné ; le template
`{{ partition_date | ds }}` rend la date dans le log ; le bulletin du laptop ne reçoit qu'une partition « jour ».

**Observé (solution)** : run `scheduled__...T12:00__2026-10-17T11:00:00` avec `partition_key=2026-10-17T11:00:00`
et `logical_date=None` ; l'événement `air_hourly` porte la clé et a créé `laptop_air_bulletin` avec
`partition_key=2026-10-17` (`StartOfDayMapper`, format `%Y-%m-%dT%H:%M:%S` → `%Y-%m-%d`) ; `add_partitions`
depuis `PartitionedAtRuntime` émet un événement **par clé**, chacun déclenchant un run du bulletin pour le même
jour (pas d'attente des 24 heures avec `StartOfDayMapper` ; le `RollupMapper` n'a pas été testé). Le fichier
d'une heure sans mesure est écrit vide : le consommateur doit en tenir compte (étape 10).

### Étape 9 — team-search : `Asset.ref` et sensor deferrable depuis un edge worker

**Objectif** : indexer les mesures dans ES (`geo_point` pour la carte) et observer qu'une tâche **déférée**
depuis un edge worker **libère son slot** puis **reprend sur la même queue**.

**À faire**

- `search_index_air` : `schedule=[Asset.ref(name="air_measurements")]`, mapping `location: geo_point`,
  `outlets=[Asset(name="air_index", uri="es://air")]`, ids déterministes (`<station>-<ts>`) pour l'idempotence.
- Avant l'indexation : `HttpSensor(http_conn_id="es_http", endpoint="_cluster/health", deferrable=True, ...)`
  (connexion `http` vers `elasticsearch.airflow-edge.svc.cluster.local:9200`, à créer) et, en parallèle, un
  `@task.sensor(mode="reschedule")` équivalent ; comparer.

**Indices** : [Deferring](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)
(le trigger tourne au centre : c'est **lui** qui doit joindre ES, pas le worker) ; edge3 : un job remis en file
après `defer` est repris par **n'importe quel** worker de la queue (correctif #53610 du changelog). `@task.sensor`
avec `PokeReturnValue` : [TaskFlow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html).

**Validation** : état `deferred` dans la grille, **0 job** sur `team-search-worker-1` pendant l'attente
(`GET /edge_worker/ui/worker`), reprise sur `team-search-worker-1` ; `GET http://es.localhost/air/_count` > 0.

<details><summary>▶ Pistes</summary>

- **`HttpSensor(deferrable=True, response_check=...)` ne défère jamais** (provider http 6.1.0 :
  `if not self.deferrable or self.response_check: return super().execute(context)`). Pour déférer vraiment :
  pas de `response_check`, et faire porter la condition par l'API ES elle-même
  (`request_params={"wait_for_status": "{{ params.es_wait_for_status }}", "timeout": "5s"}`,
  `response_error_codes_allowlist=["404", "408"]`). Un cluster mono-nœud reste `yellow` : demander `green`
  (`conf {"es_wait_for_status": "green"}`) met le sensor en `deferred` indéfiniment, parfait pour observer.
- Le trigger `HttpSensorTrigger` ne gère pas une connexion refusée : ES arrêté (`scale --replicas=0`) fait
  **échouer** la tâche (puis `retries`) au lieu d'attendre. La validation « arrêter ES pendant l'attente » a
  donc été remplacée par le `wait_for_status=green`.
- `inlets=[Asset.ref(name="air_measurements")]` plante au démarrage de la tâche (`ASSET_NOT_FOUND`) tant que
  l'asset n'a pas été **déclaré** par un DAG parsé au centre : déployer team-iot avant de tester.
</details>

**Observé (solution)** : run `asset_triggered` 20 s après `publish`, les quatre tâches sur
`team-search-worker-1`, `fetch_measurements` logue `triggering_asset_events : vide` puis lit
`inlet_events[ref][-1].extra["keys"]` ; `GET /air/_count` = 12 après le premier run ; avec `green`,
`wait_es` est passé `running → deferred`, `jobs_active=0` sur le worker pendant plus de 60 s, le triggerer
loguant `1 triggers currently running`, et `wait_es_poke` en `up_for_reschedule`.

### Étape 10 — laptop : bulletin quotidien, backfill, `@task.virtualenv`

**Objectif** : un DAG **temporel** rejouable sur 7 jours, un graphique produit dans un **venv** créé par
le worker, hors cluster.

**À faire**

- `laptop_air_bulletin` (planifié par partition, étape 8) et `laptop_air_bulletin_cron` (`schedule="0 7 * * *"`,
  `catchup=False`, `max_active_runs=2`), mêmes tâches (module partagé `laptop_air_common.py`) : DuckDB lit
  `air/hourly/` du jour (repli `air/measurements/ds=<ds>/`), écrit `air/bulletin/<ds>.md` ; `outlets=[AIR_BULLETIN]`.
- `render_chart` en `@task.virtualenv(requirements=["matplotlib", "boto3"], system_site_packages=False, venv_cache_path="/opt/airflow/.venvs")`
  → PNG dans `air/bulletin/<ds>.png`.
- **Backfill** des 7 derniers jours : UI (*Trigger → Backfill*), API
  `POST /api/v2/backfills {"dag_id": ..., "from_date": ..., "to_date": ..., "reprocess_behavior": "none", "max_active_runs": 2}`,
  CLI `airflow backfill create --dag-id laptop_air_bulletin_cron --from-date ... --to-date ... --dry-run`.
- Un run manuel sans date : `POST /api/v2/dags/laptop_air_bulletin_cron/dagRuns {"logical_date": null, "conf": {"ds": "2026-10-10"}}`.

**Indices** : [Backfill](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html)
(`run_on_latest_version` expérimental, `run_backwards` incompatible avec `depends_on_past`),
[DAG runs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html) (`logical_date` obligatoire mais nullable, `run_after`),
[PythonVirtualenvOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/python.html)
(imports **dans** la fonction, pas de `ti`/`var` sérialisables, cache de venv jamais nettoyé ; avec `uv` dans
l'image, `[standard] venv_install_method=auto` l'utilise).
Test local avant publication : `docker run ... airflow-edge/worker-laptop:dev airflow dags list-import-errors --local -B laptop`
et `airflow dags test laptop_air_bulletin` (nécessite une base SQLite initialisée : `airflow db migrate`).

**Validation** : 7 runs `backfill` visibles avec leur `logical_date`, exécutés sur `laptop-<user>` à 2 en
parallèle au plus ; un `.md` et un `.png` par jour dans MinIO ; le venv est réutilisé au second run
(`venv-<hash>` présent dans `/opt/airflow/.venvs`, durée de tâche divisée).

<details><summary>▶ Pistes</summary>

- **DuckDB `httpfs` ne passe pas par l'Ingress** depuis le conteneur laptop : son résolveur ignore l'entrée
  `host-gateway` de `/etc/hosts` (« Could not connect to server ») et avec l'IP Traefik n'a pas l'en-tête
  `Host`. Comme `laptop_duckdb_analytics`, télécharger les parquets avec boto3 (endpoint `MINIO_ENDPOINT_URL`)
  dans un `TemporaryDirectory` et laisser DuckDB lire en local.
- Les identifiants MinIO entrent dans le venv par `op_args` templatés (`{{ conn.minio_s3.login }}`), masqués
  dans l'UI ; pas de contexte Airflow dans la fonction (`expect_airflow=False`).
- **Un backfill d'un DAG en pause reste `queued`** : dépauser d'abord (et attention, dépauser un DAG cron avec
  `catchup=False` crée aussitôt son dernier run planifié).
</details>

**Observé (solution)** : premier `render_chart` **8,2 s** (création du venv : `uv venv` 0,6 s + `uv pip install`
5,5 s, 189 Mo), second **1,1 s** (`Reusing cached Python virtual environment`) ; deux tâches concurrentes se
partagent le verrou `venv-<hash>.lock`. Backfill API : `dry_run` renvoie les 7 `logical_date` (07:00 UTC) ;
les 7 runs restent `queued` tant que le DAG est en pause, puis **130 s** pour les 7 (2 simultanés maximum
observés, `conf` du backfill propagée) ; CLI `--dry-run` liste les paramètres (`run_on_latest_version = True`) ;
run manuel : `logical_date=None`, `run_after` à l'instant du déclenchement. Le run cron du jour couvre une
journée partielle (07:00) : un bulletin « J-1 » serait plus pertinent en production.

---

## Niveau 4 — Exploitation

### Étape 11 — Deadline Alerts

**Objectif** : être prévenu si l'ingestion dépasse 10 minutes après sa mise en file, et voir l'alerte dans l'UI.

**À faire**

- Sur `iot_ingest_measurements` : `deadline=DeadlineAlert(reference=DeadlineReference.DAGRUN_QUEUED_AT, interval=timedelta(minutes=10), callback=AsyncCallback("iot_callbacks.notify_deadline", kwargs={...}))`.
- `notify_deadline` : coroutine **de premier niveau** importable par le triggerer (module dans les plugins
  centraux, comme le trigger de l'étape 3) ; écrit `air/alerts/deadline-<dag>-<run_id>.json` et envoie un mail (Mailpit).
- Variante 3.2 : `SyncCallback(callback_callable, kwargs=None, executor=None)` exécuté comme tâche par un executor ;
  essayer `DeadlineReference.AVERAGE_RUNTIME(max_runs=10)`.
- Provoquer le dépassement : mettre `team-iot-worker-1` en maintenance (étape 13) ou à 0 replica, puis déposer un fichier.

**Indices** : [Deadline alerts](https://airflow.apache.org/docs/apache-airflow/stable/howto/deadline-alerts.html)
(`from airflow.sdk import DeadlineAlert, DeadlineReference, AsyncCallback, SyncCallback` ; « experimental » ;
redémarrer le triggerer après changement de callback ; `context` est un nom de kwarg réservé). UI 3.3 : **Browse → Deadlines**.
Signatures vérifiées dans l'image : `DeadlineAlert(reference, interval, callback, name=None)`,
`AsyncCallback(callback_callable: Callable | str, kwargs=None)` — passer le **chemin pointé** en chaîne, sinon le
fichier DAG doit importer le module sur le worker aussi ; le triggerer appelle `await callback(**kwargs, context=context)`
avec `context = {"dag_run": ..., "deadline": {"id", "deadline_time"}}` (pas un contexte de tâche).

**Validation** : la page *Deadlines* liste l'échéance, puis son dépassement ; l'objet/mail d'alerte existe ;
relancer le worker : le run finit quand même (l'alerte n'annule rien).

**Observé (solution)** : la deadline est créée à la mise en file (`deadline_time` = `queued_at` + 600 s,
`reference_type: DagRunQueuedAtDeadline`, visible par `GET /ui/dags/{dag}/deadlineAlerts` et
`GET /ui/dags/{dag}/dagRuns/{run}/deadlines` ; **`/api/v2/deadlines` n'existe pas**, 404) ; à l'échéance,
`missed: true`, le triggerer exécute la coroutine (`deadline dépassée : s3://datalake/air/alerts/...` dans ses
logs), l'objet JSON et le mail `[deadline] ...` existent. Le `context` reçu est `{"dag_run": {...}, "deadline": {"id", "deadline_time"}}`.
**Piège** : le retour du callback est stocké en base (`callback.output`, texte) : un `dict` fait planter le
triggerer (`psycopg2.ProgrammingError: can't adapt type 'dict'`), qui redémarre en boucle et **rejoue** le
callback à chaque fois (3 mails reçus) — renvoyer une chaîne ou `None`. Détail des mesures : étape 13.6.

### Étape 12 — XCom sur object storage et logs distants

**Objectif** : sortir de la base les XCom volumineux et rendre les logs des tâches edge indépendants du
disque de l'api-server ([03 §8](03-edge-executor.md#8-logs)).

**À faire**

- Partout où des tâches s'exécutent **et** sur l'api-server : `[core] xcom_backend = airflow.providers.common.io.xcom.backend.XComObjectStorageBackend`,
  `[common.io] xcom_objectstorage_path = s3://minio_s3@datalake/xcom`, `xcom_objectstorage_threshold = 65536`,
  `xcom_objectstorage_compression = gzip`. Déposer un fichier de 3600 lignes (`--samples 3600`) pour dépasser le seuil.
- `[logging] remote_logging = True`, `remote_base_log_folder = s3://datalake/logs`, `remote_log_conn_id = minio_s3`
  dans les values Helm (`config.logging.*`) **et** dans l'env des workers. Le worker n'a pas de base : lui fournir
  la connexion par `AIRFLOW_CONN_MINIO_S3='{"conn_type": "aws", ...}'`.

**Indices** : [XCom object storage backend](https://airflow.apache.org/docs/apache-airflow-providers-common-io/stable/xcom_backend.html),
[S3 task handler](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/logging/s3-task-handler.html),
[Logging for tasks](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/logging-tasks.html).
En Airflow 3, c'est le **superviseur de tâche** (sur le worker) qui téléverse le log en fin de tâche ; l'api-server
le relit depuis S3. Les logs edge continuent d'être poussés à l'api-server (`push_logs`) : les deux coexistent.

<details><summary>▶ Pistes</summary>

- En variable d'environnement, la section `[common.io]` s'écrit **`AIRFLOW__COMMON_IO__...`** (le point devient
  un `_`) ; `AIRFLOW__COMMON.IO__...` est ignoré silencieusement. Dans les values Helm, `config: {"common.io": {...}}`
  rend bien la section `[common.io]` d'`airflow.cfg`.
- L'image centrale officielle **n'a pas `s3fs`** : sans lui l'api-server affiche les petits XCom mais lève
  `ImportError ... requires the s3fs library` pour une valeur déportée. Ajouter `apache-airflow-providers-amazon[s3fs]`
  (et `s3fs` explicitement) au Dockerfile central. Les workers aussi.
</details>

**Observé (solution)** : `GET .../xcomEntries/return_value` renvoie la **référence**
`s3://minio_s3@datalake/xcom/iot_ingest_measurements/<run_id>/parse_file/<uuid>.gz` ; objets de 55 Kio (gzip)
sous `datalake/xcom/` ; les logs de chaque tâche team-iot sont sous `datalake/logs/dag_id=.../attempt=1.log`
dès la fin de la tâche, et restent lisibles dans l'UI après un redémarrage de l'api-server (vérifié après le
rebuild de l'image centrale), alors que ceux des autres équipes (sans logging distant) donnent « Could not read served logs ».

### Étape 13 — Exercices Edge Executor

Chaque sous-exercice se fait sur `team-iot-worker-1` avec `iot_slow_task` (param `seconds`, `retries=1`).

| # | Faire | Attendu | Observé (solution) |
|---|---|---|---|
| 13.1 | Maintenance par **API** `POST /edge_worker/ui/worker/team-iot-worker-1/maintenance` pendant une tâche de 90 s, puis `kubectl delete pod`, puis **CLI locale** `airflow edge maintenance off` | `maintenance request → pending → mode` ; les jobs en cours finissent, les nouveaux restent `queued` ; le worker redémarre **en maintenance** | request à t+0, pending à t+12 s, mode quand la tâche finit (t+90 s) ; run soumis pendant la maintenance : `edge_job queued`, aucun worker ; après delete pod : `offline maintenance` (3 s) → `maintenance request` (9 s) → `maintenance mode` (12 s), commentaire conservé ; `maintenance off` → `idle`, le run en attente s'exécute (25 s) |
| 13.2 | `airflow edge add-worker-queues -H team-iot-worker-1 -q team-data` et `set-worker-concurrency -c 3` (au centre), team-data-worker-1 en maintenance, déclencher `data_ingest_orders` | le worker prend la tâche au heartbeat suivant et **échoue** : une queue = un environnement ; la queue ajoutée et la concurrence changée disparaissent au redémarrage (`-q` / `-c` font foi) | queues `['team-data','team-iot']` et concurrence 3 vus **2 s** après ; `list_regions` prise par team-iot-worker-1 et échouée en 3 s — **avant même l'import de polars** : `ValueError: Requested bundle 'team-data' is not configured` (le worker résout le DAG par nom de bundle) ; `remove-worker-queues` effectif au heartbeat suivant |
| 13.3 | Un 2ᵉ Deployment `team-iot-worker-2` (`-H team-iot-worker-2`, même queue, `teams/team-iot/k8s/worker-2.yaml`) ; 6 runs de 30 s | répartition FIFO « premier poll gagnant », pas de priorité côté edge | worker-2 `idle` 9 s après `apply` ; 6 runs → **3 / 3**, 4 en parallèle (2 slots chacun), 67 s au total ; après `delete deploy`, worker-2 `offline` en 3 s (drain `preStop`) |
| 13.4 | `kubectl -n team-iot delete pod ...` pendant un run de 60 s | `preStop` → SIGINT → drain : la tâche finit ([03 §10](03-edge-executor.md#10-arrêt-propre)) ; queues revenues à `['team-iot']` | `running` → `terminating` à t+3 s → `offline` à t+57 s (la tâche de 60 s a fini, `success`, essai 1) → nouveau pod `idle` à t+63 s ; queues `['team-iot']` et **concurrence revenue à 2** (la valeur `-c`) : les réglages à distance de 13.2 ne survivent pas à un redémarrage |
| 13.5 | `kubectl exec ... -- sh -c 'kill -9 $(cat /opt/airflow/airflow-edge-worker.pid)'` pendant un run de 150 s | le conteneur redémarre, le nouveau worker est refusé tant que l'ancien n'est pas `unknown` (5 × `heartbeat_interval`) ; la tâche orpheline devient zombie après `[scheduler] task_instance_heartbeat_timeout` (300 s) puis **retry** | pod en `CrashLoopBackOff`, message `A worker with the name 'team-iot-worker-1' is already active` (HTTP 409) ; `unknown` à **t+145 s** — le seuil est 5 × le `heartbeat_interval` **du scheduler** (30 s par défaut), pas celui du worker (10 s) ; `idle` à t+205 s après 5 redémarrages (backoff k8s) ; tâche `up_for_retry` à t+301 s, essai 2 `running` 30 s plus tard, `success` à t+8 min |
| 13.6 | Worker à **0 replica**, déclencher une tâche **et** déposer un fichier (deadline de l'étape 11) | `queued` ; au bout de `[scheduler] task_queued_timeout` (600 s) l'executor révoque le job ; la deadline de 10 min déclenche le callback | à 661 s le scheduler logue `Revoked task instance ... from EdgeExecutor` puis `Task stuck in queued; will try to requeue` : nouveau `edge_job`, tâche toujours `queued` **essai 1** (pas d'échec) ; worker remis à 1 → les deux runs finissent en 10 s. Deadline : `missed=true` à 13:11:19 (`/ui/dags/{dag}/dagRuns/{run}/deadlines`, pas d'`/api/v2/deadlines` : 404), objet `air/alerts/deadline-...json` et mail `[deadline] ...` ; **piège** : un callback qui renvoie un `dict` fait planter le triggerer (`can't adapt type 'dict'` sur `callback.output`), qui redémarre et rejoue le callback (3 mails) → renvoyer une chaîne |
| 13.7 | Image worker-2 avec `apache-airflow-providers-edge3==4.3.1` | HTTP 400 à l'enregistrement, le worker s'arrête ; puis `[edge] minimum_acceptable_edge_version_for_workers = 4.3.1` au centre → accepté, `status_text: "Healthy but version mismatch"` | `400 Bad Request` sur `POST /edge_worker/v1/worker/team-iot-worker-2`, log `Version mismatch of Edge worker and Core. Shutting down worker.`, 2 redémarrages en 40 s ; avec la clé (env de l'api-server) : `idle`, `sysinfo.status=30`, `status_text: Healthy but version mismatch`, `edge_provider_version: 4.3.1` ; nettoyage : `airflow edge remove-remote-edge-worker -H team-iot-worker-2` (pas d'option `-y`) |
| 13.8 | Lire le changelog **5.0.0** | ce qui casserait ici : isolation des jobs par UUID (api-server avant scheduler), `[core] parallelism` respecté (8 dans la démo), **Python 3.10 abandonné** → team-iot devrait passer en 3.11+ | — |

Références : [deployment (maintenance, CLI)](https://airflow.apache.org/docs/apache-airflow-providers-edge3/4.3.2/deployment.html#worker-maintenance-mode),
[état des workers](https://airflow.apache.org/docs/apache-airflow-providers-edge3/4.3.2/architecture.html#edge-worker-state-model),
[UI plugin](https://airflow.apache.org/docs/apache-airflow-providers-edge3/4.3.2/ui_plugin.html),
[config edge](https://airflow.apache.org/docs/apache-airflow-providers-edge3/4.3.2/configurations-ref.html),
[changelog](https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/changelog.html). Les
délais (`unknown` à 5 heartbeats, 409 au redémarrage, révocation à `task_queued_timeout`) viennent du code source
4.3.2, pas de la doc : les **mesurer** fait partie de l'exercice.

### Étape 14 — Un plugin UI (3.1)

**Objectif** : Mailpit et la console MinIO accessibles **depuis le menu Airflow**.

**À faire** : `plugins/air_views.py` dans l'image centrale, `external_views` avec `category: "browse"`,
`url_route` (iframe) pour Mailpit, lien simple (sans `url_route`) pour MinIO ; `nav_top_level: True` pour l'un
des deux ; une vue `destination: "dag_run"` dont le `href` utilise `{RUN_ID}` vers
`http://minio-console.localhost/browser/datalake/air%2Ftmp%2F{RUN_ID}`.

**Indices** : [Plugins](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/plugins.html)
(chargés par l'api-server ; `airflow plugins` et **Admin → Plugins** pour vérifier ; une app FastAPI de plugin
n'est **pas** authentifiée par défaut). Un site refuse l'iframe s'il envoie `X-Frame-Options`/CSP : MinIO console
le fait probablement, d'où le lien simple.

**Validation** : `GET /api/v2/plugins` liste `air_views` et ses trois vues ; les entrées apparaissent dans le
menu ; l'onglet d'un run affiche la vue contextuelle avec le bon `RUN_ID`.

**Observé (solution)** : `airflow plugins -o json` et `GET /api/v2/plugins` listent `air_views` avec
`Mailpit` (iframe, `nav_top_level`), `MinIO Console` (lien) et `Fichiers temporaires du run` (`destination: dag_run`)
dès le premier démarrage de l'api-server sur la nouvelle image. Le rendu visuel dans le navigateur n'a **pas** été
vérifié par la session automatisée : à faire à la main.

### Étape 15 — Bonus : `GitDagBundle` et multi-team

**15.a GitDagBundle** — remplacer la ConfigMap de team-iot par un dépôt git, pour obtenir le **code épinglé par run**.

- Déployer Gitea ([`platform/gitea/values.yaml`](../platform/gitea/values.yaml) : SQLite, sans PostgreSQL ni Valkey,
  `http://gitea.localhost`, `gitea_admin / gitea12345`), créer le dépôt `team-iot-dags` et y pousser `teams/team-iot/dags`
  (depuis Windows : `git -c http.extraHeader="Host: gitea.localhost" push http://gitea_admin:...@127.0.0.1/gitea_admin/team-iot-dags.git main`).
- Connexion `gitea_iot` (type `git`, host = URL du dépôt **vue du cluster** `http://gitea-http.airflow-edge.svc.cluster.local:3000/gitea_admin/team-iot-dags.git`, login/password).
- Bundle `team-iot` → `classpath: airflow.providers.git.bundles.git.GitDagBundle`,
  `kwargs: {git_conn_id: gitea_iot, tracking_ref: main, refresh_interval: 60}` au centre
  ([`platform/helm-values/airflow-gitbundle.yaml`](../platform/helm-values/airflow-gitbundle.yaml), overlay à passer
  après `airflow.yaml`) **et** sur le worker, qui clone lui-même (`AIRFLOW_CONN_GITEA_IOT` en env :
  [`teams/team-iot/k8s-gitbundle/`](../teams/team-iot/k8s-gitbundle/kustomization.yaml)). `git` 2.39 et le provider
  git 0.5.0 sont déjà dans l'image officielle.
- Faire deux commits, déclencher entre les deux, puis **relancer une tâche de l'ancien run** : elle exécute
  l'ancien commit (`bundle_version` du run = SHA). Comparer avec [05 §2](05-assets-versioning-hitl.md#2-versioning-de-dags).
- Docs : [Git bundles](https://airflow.apache.org/docs/apache-airflow-providers-git/stable/bundles/index.html),
  [connexion git](https://airflow.apache.org/docs/apache-airflow-providers-git/stable/connections/git.html).
  Pièges : un `tracking_ref` en SHA ne change qu'après redémarrage du dag-processor ; `disable_bundle_versioning` existe.

**Observé (solution)** : `helm upgrade` avec l'overlay puis `kubectl apply -k teams/team-iot/k8s-gitbundle` ;
le dag-processor logue `Checking for new files in bundle team-iot every 60 seconds` et le DAG réapparaît
avec `bundle_version = 4c7e044e…` (commit v1) ; le run v1 porte ce SHA, et le log de sa tâche montre
`Filling up the DagBag from /tmp/airflow/dag_bundles/team-iot/versions/4c7e044e…/iot_where_am_i.py` : le
**worker a cloné** le dépôt et exécute la version épinglée. Commit v2 (un `print('code v2')` au niveau module)
poussé → nouveau SHA vu au centre en < 60 s ; le run v2 porte `f84c4a0d…` et son log contient `code v2` ;
**relance** de `report_environment` du run v1 (`POST /api/v2/dags/{dag}/clearTaskInstances`
`{"dag_run_id": ..., "task_ids": [...], "only_failed": false}`) → essai 2 exécuté depuis `versions/4c7e044e…`,
sans `code v2`. Nuance : comme la **structure** du DAG n'a pas changé, `GET .../dagVersions` ne montre
qu'une version (son `bundle_version` est mis à jour) : la version de DAG suit la structure, le `bundle_version`
du run suit le commit. Gitea 1.27 via son chart 12.7.0 (SQLite, ~200 Mi) ; `git` 2.39 et le provider git 0.5.0
sont dans l'image officielle.

**15.b Multi-team (3.2, expérimental)** — `[core] multi_team = True`, `executor = LocalExecutor,airflow.providers.edge3.executors.EdgeExecutor;team-iot=airflow.providers.edge3.executors.EdgeExecutor`,
`airflow teams create team-iot`, `"team_name": "team-iot"` sur le bundle, pool et connexion d'équipe
(`--team-name`, `AIRFLOW_CONN__TEAM_IOT___MINIO_S3`), worker lancé avec `--team-name team-iot`.
À lire d'abord : [Multi-team](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/multi-team.html) —
« isolation logique seulement, pas une frontière de sécurité » : même JWT, Execution API non cloisonnée.
Prévoir de pouvoir revenir en arrière (`helm upgrade` avec les anciennes values). **Non déroulé** par la solution.

---

## Grille d'auto-évaluation

| Je sais… | Preuve attendue |
|---|---|
| ajouter une équipe sans toucher aux autres | worker `idle`, bundle visible, DAG exécuté avec ses libs |
| déclencher un DAG sur un événement externe | `run_type=asset_triggered`, payload lu, pas de boucle, rien d'oublié dans l'inbox |
| écrire un DAG Task SDK idiomatique | mapping nommé, params typés, setup/teardown, branches |
| faire intervenir un humain | trois chemins HITL, `awaiting_input` sans slot, réponse API, 403 pour un non-assigné |
| composer des assets | alias dynamiques, expression `&`/`\|`, partitions avec `partition_key` |
| différer et rejouer | `deferred` sans slot edge, backfill 7 jours, run `logical_date=null` |
| exploiter des workers edge | maintenance persistante, queue ≠ bundle, 409 après `kill -9`, révocation `task_queued_timeout`, mismatch de versions |
| sortir les logs et XCom du centre | objets S3 présents, logs lisibles après perte de l'api-server |
| étendre l'UI | menu Browse enrichi, vue contextuelle de run |

## Observations consignées (3.3.2 / edge3 4.3.2, 2026-10-17)

| Sujet | Observation |
|---|---|
| Événements d'un watcher en rafale | n événements en quelques ms → 1 run rattaché au 1er, les autres perdus : émettre un événement par cycle et relister la source |
| `AssetAlias` vs `Asset.ref` | `triggering_asset_events` renseigné pour l'alias (2 entrées), vide pour `Asset.ref` |
| `assigned_users` | `id` = identifiant du gestionnaire d'auth (`1` pour `admin` avec FAB), sinon 403 à la réponse |
| `HITLBranchOperator` | XCom `return_value` = branches ; `DownstreamTasksSkipped` empêche tout `return` → pousser la réponse soi-même |
| Partitions | `run_offset=-1` indispensable ; `StartOfDayMapper` déclenche à chaque heure, pas une fois par jour |
| `HttpSensor` deferrable | n'est différé que sans `response_check` ; connexion refusée = échec du trigger |
| DuckDB httpfs | ne traverse pas un Ingress piloté par `Host` : passer par boto3 |
| Backfill | runs `queued` tant que le DAG est en pause |
| `[common.io]` en env | `AIRFLOW__COMMON_IO__...` |
| api-server et XCom déporté | `s3fs` requis dans l'image centrale |
| Queue ≠ environnement | un worker qui prend une queue étrangère échoue d'abord sur le **bundle** inconnu |
| Réglages à distance (`add-worker-queues`, `set-worker-concurrency`) | appliqués au heartbeat suivant, **perdus** au redémarrage du worker (`-q`/`-c` de la ligne de commande font foi) |
| Worker `unknown` | seuil = 5 × `[edge] heartbeat_interval` **du scheduler** (150 s par défaut), quel que soit l'intervalle du worker |
| Redémarrage après `kill -9` | refusé (« already active », 409) tant que l'entrée n'est pas `unknown` ; CrashLoopBackOff en attendant |
| `task_queued_timeout` | la tâche est **remise en file** (même essai), pas échouée ; le job edge est révoqué puis recréé |
| Callback de deadline | coroutine, chemin pointé, résultat stocké en texte : renvoyer une **chaîne** ; pas d'API `/api/v2/deadlines` (routes `/ui/...`) |
| `SyncCallback` d'une deadline, `RollupMapper`, multi-team | non testés |

## Liens

| Sujet | Lien |
|---|---|
| Notes de version 3.1 / 3.2 / 3.3 | https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html |
| Task SDK (API `airflow.sdk`) | https://airflow.apache.org/docs/task-sdk/stable/api.html |
| Planification événementielle | https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html |
| Assets, partitions | https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html |
| Asset scheduling (alias, expressions) | https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html |
| Timetables | https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html |
| Dynamic task mapping | https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html |
| Deferring | https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html |
| Setup / teardown | https://airflow.apache.org/docs/apache-airflow/stable/howto/setup-and-teardown.html |
| Params | https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html |
| Object storage | https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html |
| Backfill, DAG runs | https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html · https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html |
| HITL | https://airflow.apache.org/docs/apache-airflow/stable/tutorial/hitl.html |
| Deadline alerts | https://airflow.apache.org/docs/apache-airflow/stable/howto/deadline-alerts.html |
| Notifications, callbacks | https://airflow.apache.org/docs/apache-airflow/stable/howto/notifications.html · https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html |
| Plugins (3.1) | https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/plugins.html |
| DAG bundles, Git bundle | https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html · https://airflow.apache.org/docs/apache-airflow-providers-git/stable/bundles/index.html |
| Multi-team | https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/multi-team.html |
| XCom object storage | https://airflow.apache.org/docs/apache-airflow-providers-common-io/stable/xcom_backend.html |
| Logs S3 | https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/logging/s3-task-handler.html |
| API REST, JWT | https://airflow.apache.org/docs/apache-airflow/stable/security/api.html · https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html |
| Debug (`dag.test`, CLI) | https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/debug.html · https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html |
| Edge3 4.3.2 | https://airflow.apache.org/docs/apache-airflow-providers-edge3/4.3.2/ (deployment, edge_executor, architecture, ui_plugin, configurations-ref) |
| Edge3 source 4.3.2 | https://github.com/apache/airflow/tree/providers-edge3/4.3.2/providers/edge3 |
