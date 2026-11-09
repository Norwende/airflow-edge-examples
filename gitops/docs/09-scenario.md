# 09 — Scénario de bout en bout (ce qui a réellement été joué)

Chronologie du 2026-11-07, après `platform.py up` et `gitlab_bootstrap.py all`. Les numéros de
pipelines et de MR sont ceux de l'instance du lab (`http://gitlab.127-0-0-1.sslip.io/gitops/...`).

## Étape 0 — Bootstrap : trois premières pipelines, un vrai échec de test

| Pipeline | Ref | Résultat | Enseignement |
|---|---|---|---|
| `notes-api` #5 | `develop` | **failed** sur `test` (lint, typecheck verts) ; `build-develop`, `promote-dev` non exécutés | la CI a bloqué une image non testée : c'est son rôle |
| `notes-api` #6 | `main` | **failed** sur `test` ; `release`, `build-main`, `promote-prod` non exécutés | aucune version `v1.0.0` publiée sur un code dont un test échoue |
| `manifests` #7 | `main` | **success** (`yamllint`, `validate-versions`, `helm-lint`, `kubeconform`) | le dépôt d'état désiré est valide |

Argo CD a immédiatement créé les 8 Applications (`gitops-{dev,prod}-{notes-api,postgres,elastic,filebeat}`)
depuis l'Application racine : PostgreSQL, Elasticsearch, Kibana (data view et dashboard importés par
le hook PostSync, `successCount: 4`) et Filebeat sont `Synced/Healthy` en ~3 minutes ; `notes-api`
reste `Progressing` (`ImagePullBackOff` sur le tag de départ `"0.0.0"`) **jusqu'à la première
promotion** : rien ne tourne qui n'ait été construit par la CI.

**Le test qui a échoué** : `test_open_pool_fails_fast_on_bad_credentials` passait en local (PostgreSQL
dans un conteneur, joint par un port publié) mais pas en CI : l'image officielle `postgres` fait
**confiance aux connexions locales** (`host all all 127.0.0.1/32 trust`) et, avec l'executor Kubernetes,
le service tourne dans le pod du job et est joint via `127.0.0.1` ; le mauvais mot de passe était donc
accepté et le `PoolTimeout` attendu n'arrivait jamais. Le rapport JUnit et la trace du job le montrent
dans GitLab (onglet *Tests* : `1 failed, 46 passed`, couverture 99 %).

## Étape 1 — Correction par une MR de développeuse → dev

1. `dev` : branche `fix/integration-test-trust-auth`, commit `fix(tests): target a missing database
   instead of a wrong password`, **MR !1 → `develop`**.
2. Pipeline de MR #8 : `commitlint`, `lint`, `typecheck`, `test` (**47 tests, 0 échec, couverture
   100 %** dans le widget de la MR), `build-mr` (kaniko, `--no-push`) : vert.
3. `lead` merge (`develop` : personne ne pousse, merge si pipeline verte, branche source supprimée).
4. Pipeline #9 sur `develop` : tests, `build-develop` → `registry…/gitops/notes-api/develop/notes-api:e0ba15c3`,
   `promote-dev` → **MR !1 sur `manifests`** (`chore(notes-api): dev -> e0ba15c3`), merge automatique
   demandé ; pipeline de lint #10 verte → **mergée sans intervention**.
5. Argo CD (≤ 60 s) : `gitops-dev-notes-api` `Synced/Healthy`, pod avec l'image `e0ba15c3`.
   `GET /version` → `{"version":"e0ba15c3","environment":"dev"}` ; `POST /notes` 201 ; la ligne de
   log de la requête `X-Request-ID: demo-dev-001` est retrouvée dans l'Elasticsearch de dev
   (`request_id`, `http.status_code`, `kubernetes.pod.name`), et l'index de dev ne contient que le
   namespace `gitops-dev`.

## Étape 2 — Livraison en production : `develop → main`

1. `dev` ouvre la **MR !2 `develop → main`** ; pipeline #12 verte. `dev` tente de merger : **401**
   (seuls les Maintainers mergent `main`). `lead` merge.
2. Pipeline #13 sur `main` : tests, **`release`** → semantic-release analyse `feat: initial import` +
   `fix(tests): …` → **tag `v1.0.0`**, Release GitLab avec notes générées (*Features*, *Bug Fixes*) ;
   `build-main` → `main/notes-api:1.0.0` ; `promote-prod` → **MR !2 sur `manifests`**
   (`chore(notes-api): prod -> 1.0.0`), **sans auto-merge**.
3. `lead` relit le diff (2 lignes utiles : `tag`, `ref`), pipeline de lint verte, merge.
4. Argo CD : `gitops-prod-notes-api` `Healthy`, **2 réplicas** sur `main/notes-api:1.0.0`,
   `GET /version` → `1.0.0` ; dev reste sur `e0ba15c3`. Le log « note created » de la requête
   `demo-prod-001` est dans l'Elasticsearch de **prod** avec `service.version: 1.0.0`.

## Étape 3 — Deuxième itération avec le script de démo → `v1.1.0`

`uv run scripts/demo.py feature changelog "feat: add a CHANGES file describing the release flow"`
→ MR !3 (`dev`), pipeline verte, merge (`lead`) → pipeline #19 `develop` → image `develop/notes-api:0d206832`
→ MR !3 manifests auto-mergée → dev sur `0d206832`. Puis MR !4 `develop → main`, merge → pipeline #34 :
**`v1.1.0`** (un `feat` = mineure), `main/notes-api:1.1.0`, MR !8 manifests → merge par `lead` → prod
sur `1.1.0`.

## Étape 4 — Rollback en production (80 secondes)

`lead` : branche `revert/prod-1.1.0`, `git revert` du commit de promotion (API GitLab), **MR !10**
(labels `rollback`, `env::prod`), pipeline de lint verte, merge. **81 s après le merge**,
`GET /version` en prod répond `1.0.0` : l'image précédente est toujours dans le registre, Argo CD
l'a redéployée. Aucune commande `kubectl`, aucun redéploiement manuel.

Retour à `1.1.0` : relance du job `promote-prod` de la pipeline #34 (idempotent : il constate que
`main` porte `1.0.0`, recrée la branche et la **MR !11**), merge par `lead`, prod de nouveau en `1.1.0`.

## Étape 5 — Renovate : 11 MR en une exécution

Pipeline #16 de `gitops/renovate` (exécution réelle, ~4 min dont le tirage de l'image `-full`) :

| Dépôt | MR | Ce qu'elle change |
|---|---|---|
| `notes-api` | !5 `pin gcr.io/distroless/cc-debian12 docker tag to 9dac0a7` | digest de l'image de base (Dockerfile) |
| `notes-api` | !6 `update all non-major dependencies` | paquets Python (`pyproject.toml` + **`uv.lock`** réécrit), npm, images CI mineures |
| `notes-api` | !7 `update ghcr.io/astral-sh/uv docker tag to v0.13.7` | majeure uv, label `major` |
| `notes-api` | !8 `update node.js to v26`, !9 `update postgres docker tag to v18`, !10 `update release tooling (major)` | majeures séparées (image CI, service de test, semantic-release 25) |
| `manifests` | !4 `update elastic stack to v8.19.24` | **les 4 `version.yaml`** (elastic + filebeat, dev + prod) ensemble |
| `manifests` | !5 non-major, !6 `elastic stack to v9`, !7 `cloudnative-pg/postgresql to v18` (label `postgres-major`) | mineures groupées ; majeures séparées |
| `renovate` | !1 `update renovate/renovate docker tag to v44.222.2` | le bot se met à jour lui-même |

Toutes les MR passent par la pipeline du dépôt (tests pour `notes-api`, lint/kubeconform pour
`manifests`) ; un Maintainer décide. Les `version.yaml` des apps n'ont **pas** été touchés (propriété
de la CI).

## Incident rencontré : quota du namespace `ci-jobs`

Quand Renovate a ouvert ses 11 MR, plus de dix pipelines ont démarré en même temps ; plusieurs jobs
ont échoué en `runner_system_failure` : `pods "runner-…" is forbidden: exceeded quota: ci-jobs-quota`
(6 Gi de requests mémoire pour le namespace des jobs, 4 jobs concurrents + pods en terminaison).
Mesure (message complet du quota) : au pic, **10 pods de jobs** vivants ou en terminaison pour
`concurrent: 4`, soit `limits.cpu=15, limits.memory=23040Mi, requests.memory=5760Mi` : les trois
plafonds (16 CPU, 24 Gi, 6 Gi) étaient touchés. Réponse : (1) MR !9 sur `manifests` qui ajoute
`retry: {max: 1, when: [runner_system_failure]}` à la CI (déjà présent sur `notes-api`) ; (2) quota
relevé dans `platform/00-namespaces/ci-jobs.yaml` (requests 8 Gi, limits 32 CPU / 48 Gi, 40 pods : les
limits ne réservent rien, seules les requests comptent pour le nœud) ; (3) relance des pipelines :
**les 11 MR Renovate sont vertes**. Le quota et `concurrent` restent des garde-fous volontaires d'un
lab mono-nœud ; en production on dimensionne (`concurrent`, quota, autoscaling des runners) et on
alerte sur `exceeded quota` dans les évènements du namespace.

## Ce que le scénario démontre

| Exigence | Preuve |
|---|---|
| MR depuis `develop`, images `registry/develop/<image>:<sha>` | `develop/notes-api:e0ba15c3`, `0d206832` |
| Version semantic-release sur `main` | tags `v1.0.0`, `v1.1.0`, Releases avec notes, images `main/notes-api:1.0.0`, `1.1.0` |
| `version.yaml` de `envs/dev` mis à jour + MR vers `main` du dépôt manifests | MR !1, !3 (auto-mergées) |
| `version.yaml` de `envs/prod` + MR | MR !2, !8, !11 (mergées par `lead`) |
| Argo CD dev lit `envs/dev`, prod lit `envs/prod`, namespaces séparés | `gitops-dev` sur `0d206832`, `gitops-prod` sur `1.1.0` au même instant |
| Logs structlog visibles dans Kibana | data view + dashboard importés, `request_id` retrouvé par environnement |
| Tests unitaires et d'intégration visibles dans GitLab | JUnit 47 tests, couverture 100 %, Code Quality, échec initial détecté |
| Renovate (Python, Docker, images CI, infra) | 11 MR |
| Rollback | revert → 81 s |
