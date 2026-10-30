# team-search — edge worker « recherche »

Worker Edge (provider `edge3` 4.3.2) de l'équipe **search** : Python **3.13** (image `apache/airflow:3.3.2`),
libs `elasticsearch` 8.x, `pandas`, `pyarrow`, `boto3`. Queue **`team-search`**, hostname `team-search-worker-1`.

## DAGs (`dags/`, bundle `team-search`)

| DAG | Planif. | Montre |
|---|---|---|
| `search_index_orders` | `[Asset.ref(name="orders_daily")]` | dépendance inter-équipes **par nom d'asset**, `inlet_events`, params de repli, asset `orders_index` en sortie |
| `search_reindex_with_approval` | manuel | **Human-in-the-loop** (`ApprovalOperator`) avant de recréer l'index `orders` |
| `search_health_check` | `*/30 * * * *` (en pause) | libs, hostname et Python du worker |

`search_common.py` : helpers partagés (pas de DAG). Le dossier du bundle est dans le `sys.path`
(dag-processor et worker utilisent `BundleDagBag`).

**Règle D3** : `elasticsearch`, `pandas`, `pyarrow`, `boto3` sont importés **dans** les tâches → les fichiers se
parsent au central qui n'a pas ces libs (vérifié par les tests).

## HITL et Edge

En 3.3, `ApprovalOperator` met la tâche à l'état `awaiting_input` : pas de triggerer, **pas de slot worker**
occupé pendant l'attente. Après la réponse (UI **Browse → Required Actions**), le scheduler relance
`execute_complete` sur l'executor de la tâche → elle reste sur l'**EdgeExecutor** (queue `team-search`).
Pas besoin de la garder sur l'executor central.

## Elasticsearch : client 8.x

Les contraintes 3.3.2 épinglent `elasticsearch==9.5.1` / `elastic-transport==9.4.2`. Le client 9.x envoie
`compatible-with=9` et un serveur **8.x** répond `400 media_type_header_exception` (testé contre ES 8.19.4).
Le Dockerfile retire ces deux pins et installe `elasticsearch>=8.15,<9` (→ 8.19.3).

## Commandes

```bash
docker build -t airflow-edge/worker-team-search:dev teams/team-search
docker run --rm -v "$PWD/teams/team-search:/work" -w /work airflow-edge/worker-team-search:dev python -m pytest -q tests
kubectl apply -k teams/team-search/k8s          # namespace, secret JWT, Deployment
uv run scripts/dags.py apply team-search         # publie les DAGs au central (ConfigMap dags-team-search)
```

`dags.py apply team-search` écrit la ConfigMap `dags-team-search` **au centre et dans le namespace
`team-search`** (tous les `*.py`, `search_common.py` compris) ; le worker la monte sur
`/opt/airflow/dags/team-search` (bundle `.../..data`). Une modification de DAG = une commande, propagée
en ~1 min, sans rebuild. La copie de l'image (`COPY dags/`) ne sert qu'aux tests locaux. Créer le
namespace (`kubectl apply -k`) avant `dags.py apply` ; le pod attend la ConfigMap en `ContainerCreating`.
