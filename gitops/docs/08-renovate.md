# 08 — Renovate : montées de version automatiques

Objectif : que les dépendances des deux dépôts restent à jour **sans que personne n'y pense**, et que
rien n'arrive dans un environnement sans passer par une MR et sa pipeline. Dependabot n'existe pas sur
GitLab auto-hébergé : on héberge **Renovate** dans une job planifiée du projet `gitops/renovate`.

## Le bot (`repos/renovate/`)

| Élément | Valeur |
|---|---|
| Image | `renovate/renovate:<version>-full` (épinglée ; la variante *full* embarque uv, helm, node… nécessaires pour réécrire `uv.lock` et `package-lock.json`) |
| Déclenchement | schedule **`renovate hourly`** (`0 * * * *`, Europe/Paris, posé par le bootstrap), *Run pipeline* (variables `RENOVATE_DRY_RUN`, `LOG_LEVEL` pré-décrites), push sur `main` |
| Exécution | `resource_group: renovate` (jamais deux en parallèle), `interruptible: false`, 1–2 Gi de mémoire via `KUBERNETES_MEMORY_*` (autorisé par la config du runner) |
| Authentification | `RENOVATE_TOKEN` = PAT de l'utilisateur **`renovate-bot`** (Developer du groupe `gitops`), variable CI masquée et protégée du projet ; jamais dans un fichier |
| `config.js` | `platform: gitlab`, `endpoint` = API du lab, `autodiscover` + `autodiscoverFilter: ["gitops/*"]`, `onboarding: false` / `requireConfig: optional`, `gitAuthor`, pas de changelogs (pas de token GitHub), `binarySource: global`, cache de dépôt conservé par le cache GitLab |
| Preset partagé `default.json` | `config:recommended`, commits/MR `chore(deps): …`, label `renovate`, fuseau Europe/Paris, **regroupement des mineures/patchs** (`group:allNonMajor`), **majeures séparées** avec label `major`, digests épinglés seulement sur les images de base des Dockerfile, tableau de bord des dépendances (issue) |
| MR du bot lui-même | job `validate` : `renovate-config-validator --strict` sur `config.js`, `default.json`, `renovate.json` |

Chaque dépôt référence le preset en une ligne : `"extends": ["local>gitops/renovate"]`. Changer une
règle pour tout le monde = une MR sur `gitops/renovate`.

## Ce que Renovate suit, dépôt par dépôt

| Dépôt | Managers | Fichiers | Exemples de MR attendues |
|---|---|---|---|
| `gitops/notes-api` | `pep621` (+ lock **uv**), `dockerfile`, `gitlabci`, `npm` | `pyproject.toml` + `uv.lock`, `Dockerfile` (uv, distroless), `.gitlab-ci.yml` (images des jobs), `package.json` + `package-lock.json` (semantic-release, commitlint) | `chore(deps): update all non-major dependencies` (fastapi, structlog, uv…), `chore(deps): update ghcr.io/astral-sh/uv docker tag to v0.11` (majeure, label `major`) |
| `gitops/manifests` | `helm-values` (motifs : `charts/infra/**/envs/*/version.yaml` et `values.yaml`), `gitlabci`, `helmv3` | `version.yaml` des charts **infra** (PostgreSQL CNPG, Elasticsearch, Filebeat), images de la CI | `chore(deps): update elastic stack to v8.19.7` (groupe Elasticsearch + Filebeat : ils doivent bouger ensemble), `… update ghcr.io/cloudnative-pg/postgresql to v18` (label `postgres-major`, relecture obligatoire) |
| `gitops/renovate` | `gitlabci` | sa propre image | `chore(deps): update renovate/renovate docker tag` |

Règles importantes du dépôt de manifests :
- `charts/apps/**/version.yaml` est **ignoré** : ces fichiers appartiennent à la CI de `notes-api`.
- `pinDigests: false` sur `helm-values` : le chart `elastic` utilise `image.tag` comme `spec.version`
  ECK ; un digest casserait le rendu.

## Pourquoi cette organisation

- **Un seul bot pour tous les dépôts** : un endroit à exploiter, auditer, mettre à jour ; les règles
  sont héritées.
- **Tout passe par une MR et la pipeline du dépôt** : sur `manifests`, `helm lint`/`kubeconform`
  valident le rendu ; sur `notes-api`, les tests tournent avec les nouvelles versions. Un patch mergé
  en manifests est déployé par Argo CD comme n'importe quel commit (dev **et** prod : la MR Renovate
  modifie les deux `envs/`, donc la relecture compte).
- **Majeures isolées** : une MR par dépendance, label dédié, jamais groupées.
- Pour aller plus loin : `automerge` des patchs sur les charts infra en dev uniquement, `schedule`
  nocturne, et `osvVulnerabilityAlerts` pour prioriser les CVE.

## Lancer et lire

```bash
GL=http://gitlab.127-0-0-1.sslip.io/api/v4 ; TOKEN=<PAT root>
# dry run complet (rien n'est poussé)
curl -s -X POST -H "PRIVATE-TOKEN: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"ref":"main","variables":[{"key":"RENOVATE_DRY_RUN","value":"full"}]}' "$GL/projects/gitops%2Frenovate/pipeline"
# exécution réelle
curl -s -X POST -H "PRIVATE-TOKEN: $TOKEN" "$GL/projects/gitops%2Frenovate/pipeline?ref=main"
```

Lignes à chercher dans le log : `Autodiscovered repositories`, `Repository started`, `packageFiles
with updates`, `PR created` / `DRY-RUN: Would create PR`, `Repository finished`.
