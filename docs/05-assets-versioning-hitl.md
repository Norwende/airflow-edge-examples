# 05 — Assets, versioning de DAGs, Human-in-the-loop

Les trois fonctionnalités d'Airflow 3 que la démo exécute sur les edge workers.
Scénarios correspondants : `uv run scripts/scenario.py assets|versioning|hitl`.

## 1. Assets

Un **asset** (ex-*Dataset* d'Airflow 2) est une donnée logique identifiée par un **nom** (et une URI).
Une tâche le déclare en `outlets` ; à son succès, Airflow enregistre un **événement d'asset**. Un DAG
`schedule=[asset]` démarre sur cet événement : le consommateur s'abonne à la donnée, le producteur
ignore qui la consomme.

### Définir, produire, attacher des métadonnées

```python
# teams/team-data/dags/data_ingest_orders.py
ORDERS_RAW = Asset(name="orders_raw", uri="s3://datalake/orders/raw")

@task(outlets=[ORDERS_RAW])
def publish(results: list[dict]):
    ...
    yield Metadata(ORDERS_RAW, {"rows": total, "ds": ds, "keys": keys})
    return {"rows": total, "ds": ds, "keys": keys}  # valeur de retour -> XCom
```

Variante équivalente, sans générateur (dans `data_orders_daily.py`) :

```python
context["outlet_events"][ORDERS_DAILY].extra = {"key": key, "rows": daily.height, "ds": ds}
```

L'`extra` voyage avec l'événement : le consommateur y lit les clés S3, **sans XCom entre DAGs**.
Relevé du scénario : événement `orders_raw` avec `extra.rows=1500` (3 régions × 500 lignes).

### Consommer

```python
# teams/team-data/dags/data_orders_daily.py
@dag(dag_id="data_orders_daily", schedule=[ORDERS_RAW], ...)
...
@task(inlets=[ORDERS_RAW], outlets=[ORDERS_DAILY])
def aggregate_daily(**context) -> dict:
    src = _source_extra(context)   # triggering_asset_events, sinon inlet_events[ORDERS_RAW][-1]
```

| Accès | Contenu |
|---|---|
| `context["triggering_asset_events"][asset]` | les événements qui ont déclenché **ce** run |
| `context["inlet_events"][asset]` | tous les événements de l'asset déclaré en `inlets` ; `[-1]` = le dernier |
| `context["outlet_events"][asset].extra` | l'extra de l'événement que la tâche va émettre |

### Entre équipes : `Asset.ref`

team-search et laptop consomment `orders_daily` **par son nom**, sans importer le code de team-data :

```python
# teams/team-search/dags/search_common.py
ORDERS_DAILY_REF = Asset.ref(name="orders_daily")
# teams/team-search/dags/search_index_orders.py
@dag(dag_id="search_index_orders", schedule=[ORDERS_DAILY_REF], ...)
    @task(inlets=[ORDERS_DAILY_REF])
    def fetch_daily(**context): ...
        events = context["inlet_events"][ORDERS_DAILY_REF]
```

**Comportement observé en 3.3.2** : un DAG planifié par `Asset.ref(name=...)` est bien déclenché par
l'événement, mais son run n'y est **pas rattaché** : `upstreamAssetEvents` du run et `created_dagruns`
de l'événement sont vides, donc `triggering_asset_events` est vide dans la tâche. `GET /api/v2/assets`
ne liste pas non plus ces DAGs dans `scheduled_dags` ; le lien n'apparaît que dans l'`asset_expression`
de `GET /api/v2/dags/{id}/details`. Les DAGs de la démo lisent donc `inlet_events[...][-1].extra`
(dernier événement connu), ce qui suffit ici mais n'est pas strictement "l'événement déclencheur".
`inlet_events` résout la référence au démarrage de la tâche : l'asset doit déjà exister au centre.
Aucune issue GitHub ne décrit exactement ce comportement (recherche du 2026-10-17) ; la plus proche est
[apache/airflow#47970](https://github.com/apache/airflow/issues/47970) (`consuming_dags` / `producing_tasks`
ignorent `Asset.ref`), fermée *not planned*.

### Graphe et API

- UI : vue **Assets** (graphe producteurs → consommateurs), onglet *Assets* d'un DAG.
- Relevé du scénario (graphe reconstruit par l'API) :

```
orders_raw     s3://datalake/orders/raw    produit par data_ingest_orders.publish     -> déclenche data_orders_daily
orders_daily   s3://datalake/orders/daily  produit par data_orders_daily.aggregate_daily -> déclenche laptop_duckdb_analytics, search_index_orders
orders_index   es://orders/                produit par search_index_orders.publish    -> déclenche -
```

| Route | Usage |
|---|---|
| `GET /api/v2/assets` | assets, producteurs |
| `GET /api/v2/assets/events?source_dag_id=...&name_pattern=...&order_by=-timestamp` | événements et leur `extra` |
| `GET /api/v2/dags/{dag}/dagRuns/{run}/upstreamAssetEvents` | événements rattachés à un run |
| `GET /api/v2/dags/{dag}/details` | `asset_expression` (planification) |

### `@asset` : le raccourci et sa limite

`@asset` crée un asset, un DAG et une tâche d'un coup
([`asset_shorthand_demo.py`](../teams/team-data/dags/asset_shorthand_demo.py)). En 3.3.2 il n'accepte ni
`executor` ni `queue` : sa tâche tourne sur l'executor par défaut, **au centre** (relevé :
`hostname=airflow-scheduler-0...`). Pour un asset produit sur un edge worker, utiliser
`@task(outlets=[...])` avec `default_args` Edge.

## 2. Versioning de DAGs

### Ce qui crée une version

Le dag-processor sérialise chaque DAG. Si la **structure** sérialisée change (tâche ajoutée, dépendance,
paramètre de DAG...), Airflow crée une nouvelle **version de DAG** (`dag_version`) ; modifier seulement le
corps d'une fonction n'en crée pas. Chaque run garde la version avec laquelle il a été créé : la grille
d'un ancien run affiche **sa** structure.

| | `LocalDagBundle` (démo) | `GitDagBundle` (prod) |
|---|---|---|
| version de DAG (structure) | oui | oui |
| `bundle_version` | `None` | commit git |
| code d'un run | fichier **actuel** (une relance d'une tâche d'un vieux run exécute le code courant) | code **épinglé** au commit du run |
| livraison | ConfigMap / volume | `git push` |

UI : sélecteur **Dag versions** en haut de la vue du DAG ; API : `GET /api/v2/dags/{dag_id}/dagVersions`.

### Démo (scénario `versioning`)

[`versions/v2/data_ingest_orders.py`](../teams/team-data/versions/v2/data_ingest_orders.py) ajoute une tâche `validate` :

```python
# v2 : generate_orders (mappé) >> validate >> publish
publish(validate(generate_orders.expand(region=list_regions())))
```

```bash
uv run scripts/dags.py apply team-data --version v2   # publie v2 (centre + ns team-data)
# ... attendre la nouvelle version, déclencher data_ingest_orders
uv run scripts/dags.py apply team-data                # retour à dags/ : encore une nouvelle version
```

Relevés du run du 2026-10-17 :

| Étape | Mesure |
|---|---|
| version avant | v3 (tâches `generate_orders`, `list_regions`, `publish`) |
| v2 publiée → nouvelle version visible au centre | **65 s** (v4) |
| fichier à jour dans le pod worker | **66 s**, sans rebuild ni redémarrage |
| run en v2 | success en 12 s, `validate` exécutée sur `team-data-worker-1` |
| retour v1 → nouvelle version | **50 s** au centre (v5), 61 s dans le worker |
| historique | v1..v5, `bundle_version=None` ; le run v2 référence toujours `dag_versions=[4]` et sa grille contient `validate` |

Délai = propagation kubelet de la ConfigMap (~60 s) + `refresh_interval` du dag-processor (30 s).
Attendre **les deux côtés** avant de déclencher, sinon le worker peut exécuter l'ancien fichier.

## 3. Human-in-the-loop (HITL)

Opérateurs du provider standard (Airflow 3.1+) : `ApprovalOperator`, `HITLOperator`, `HITLBranchOperator`...
La démo utilise `ApprovalOperator` dans
[`search_reindex_with_approval.py`](../teams/team-search/dags/search_reindex_with_approval.py) :

```python
approve = ApprovalOperator(
    task_id="approve_reindex",
    subject="Recréer l'index orders ?",
    body=("L'index Elasticsearch **orders** va être **supprimé** puis recréé ..."),
    defaults="Reject",
    response_timeout=timedelta(hours=4),
)
```

| Phase | Ce qui se passe |
|---|---|
| exécution | la tâche démarre sur `team-search-worker-1` (EdgeExecutor), crée la demande, passe en **`awaiting_input`** |
| attente | **aucun slot de worker occupé** (relevé : 0 job actif sur le worker), pas de triggerer |
| réponse | UI **Browse → Required Actions** (ou onglet *Required Actions* du run), ou API |
| reprise | la tâche repasse en *scheduled* et reprend sur **son** executor (Edge, queue `team-search`) |
| timeout | sans réponse sous 4 h → `defaults="Reject"` |
| Reject | aval direct *skipped* ; `cancelled` (`trigger_rule=ALL_SKIPPED`) s'exécute |

API :

```bash
# demandes en attente, tous DAGs
curl -s -H "Host: airflow-edge.localhost" -H "Authorization: Bearer $TOKEN" "http://127.0.0.1/api/v2/dags/~/dagRuns/~/hitlDetails?response_received=false"
# approuver (map_index -1 = tâche non mappée ; run_id encodé en URL)
curl -s -X PATCH -H "Host: airflow-edge.localhost" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"chosen_options":["Approve"],"params_input":{}}' "http://127.0.0.1/api/v2/dags/search_reindex_with_approval/dagRuns/<run_id>/taskInstances/approve_reindex/-1/hitlDetails"
```

Relevé : réponse `['Approve']` par `admin`, run `success` en 9 s, index `orders` recréé (3 documents).

## 4. Repères Airflow 3.x utiles à la démo

| Version | Nouveauté | Où dans la démo |
|---|---|---|
| 3.0 | **Task SDK** (`airflow.sdk`) : le code DAG/tâche ne touche plus la DB, tout passe par l'Execution API | toutes les tâches ; condition pour des workers sans DB |
| 3.0 | DAG bundles, versioning de DAGs, assets (ex-datasets), `@asset`, UI React, API `/api/v2` | bundles par équipe, `dagVersions`, chaîne d'assets |
| 3.0 | `logical_date` peut être `None` pour un run manuel | `_run_date()` retombe sur `dag_run.run_after` |
| 3.1 | **Human-in-the-loop** (provider standard ; en 3.1 la tâche attend en `deferred`) | `search_reindex_with_approval` (en 3.3.2 : état `awaiting_input`, observé) |
| 3.1 | **plugins React** (AIP-68) : pages intégrées à la navigation de l'UI | page `/plugin/edge_executor` du provider edge3 |
| 3.2 | **partitionnement d'assets** : seule la partition modifiée déclenche l'aval | non utilisé |
| 3.2 | **multi-team** (expérimental) : plusieurs équipes isolées dans un déploiement | non activé ; le worker a déjà l'option `-t/--team-name` |

Sources : notes de version officielles 3.1.0 et 3.2.0. La démo n'active ni le multi-team ni le
partitionnement et ne les a pas testés.
