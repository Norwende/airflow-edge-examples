# gitops/manifests

État désiré des environnements `dev` et `prod` du scénario GitOps, déployé par **Argo CD**
(charts **Helm**). Une seule branche, `main` : **tout passe par merge request**, et une MR ne peut
être mergée que si la pipeline est verte. Le contrat technique complet est dans `CONTRACT.md`
(dépôt de la plateforme) ; ce README en résume ce qui concerne ce dépôt.

## Organisation

```
manifests/
├── .gitlab-ci.yml        # yamllint, validate-versions, helm-lint (lint + rendu par env), kubeconform
├── .yamllint.yaml        # templates Helm exclus (vérifiés après rendu)
├── renovate.json         # montées de version des charts infra (version.yaml) et des images de CI
├── scripts/
│   ├── validate_versions.py   # contrat des version.yaml + contrôle des manifests argocd/ (stdlib)
│   └── render.sh              # helm lint --strict + helm template de chaque chart, pour chaque env
├── argocd/               # Argo CD lui-même (appliqué par l'Application racine « gitops-root »)
│   ├── project.yaml           # AppProject gitops
│   ├── appset-apps-dev.yaml   # ApplicationSet gitops-apps-dev   → charts/apps/*/envs/dev/version.yaml
│   ├── appset-apps-prod.yaml  # ApplicationSet gitops-apps-prod  → charts/apps/*/envs/prod/version.yaml
│   ├── appset-infra-dev.yaml  # ApplicationSet gitops-infra-dev  → charts/infra/*/envs/dev/version.yaml
│   └── appset-infra-prod.yaml # ApplicationSet gitops-infra-prod → charts/infra/*/envs/prod/version.yaml
└── charts/
    ├── apps/notes-api/   # l'application (Deployment, Service, Ingress, PDB/HPA/NetworkPolicy optionnels)
    └── infra/
        ├── postgres/     # Cluster CloudNativePG notes-pg (Secret généré notes-pg-app)
        ├── elastic/      # Elasticsearch notes-es + Kibana notes-kibana (ECK), Ingress, import du dashboard
        └── filebeat/     # Beat ECK filebeat-<env> (DaemonSet dans logging, ne lit que gitops-<env>)
```

Chaque chart possède `envs/<env>/values.yaml` (réglages de l'environnement, modifiés à la main par
MR) et `envs/<env>/version.yaml` (**quelle image** tourne dans cet environnement) :

```yaml
image:
  repository: registry.127-0-0-1.sslip.io/gitops/notes-api/develop/notes-api
  tag: "1a2b3c4d"          # toujours entre guillemets
source:                    # traçabilité (apps uniquement)
  commit: 1a2b3c4d5e6f…
  ref: develop
  pipeline: http://gitlab.127-0-0-1.sslip.io/gitops/notes-api/-/pipelines/123
```

| Fichier | Qui l'écrit |
|---|---|
| `charts/apps/*/envs/*/version.yaml` | la CI de `gitops/notes-api` (job `promote-<env>`), jamais à la main ; **ignoré par Renovate** |
| `charts/infra/*/envs/*/version.yaml` | Renovate (MR `chore(deps): …`) ; version de PostgreSQL ou de la pile Elastic |
| `envs/*/values.yaml`, templates | les humains, par MR |

Le dossier `argocd/` est lu par l'Application racine (créée par la plateforme) comme une source
« répertoire » simple, **non récursive** : il ne doit contenir que des manifests YAML complets.

## Comment une version arrive dans un environnement

Les ApplicationSets utilisent le générateur git **files** : chaque fichier
`charts/<kind>/<chart>/envs/<env>/version.yaml` sur `main` produit l'Application
`gitops-<env>-<chart>` (projet `gitops`, namespace `gitops-<env>`). Le contenu du `version.yaml`
est passé au chart en **paramètres Helm** (`image.repository`, `image.tag`, `forceString`) et
`envs/<env>/values.yaml` est chargé comme fichier de values : le `version.yaml` fait foi, un tag
écrit dans `values.yaml` n'a aucun effet.

1. **dev** : un merge sur `develop` dans `gitops/notes-api` construit
   `…/develop/notes-api:<sha8>` puis ouvre ici une MR « `chore(notes-api): dev -> <sha8>` » qui
   réécrit `charts/apps/notes-api/envs/dev/version.yaml`. La MR est **mergée automatiquement**
   quand la pipeline de ce dépôt est verte.
2. **prod** : un merge sur `main` dans `gitops/notes-api` crée la release `vX.Y.Z` (semantic-release),
   construit `…/main/notes-api:X.Y.Z` et ouvre la MR « `chore(notes-api): prod -> X.Y.Z` ».
   Le **lead relit et merge**.
3. Argo CD relit `main` (au plus 60 s via le générateur, ou immédiatement via webhook), met à jour
   l'Application et synchronise (sync automatique, `prune` + `selfHeal`). L'historique de `main`
   est donc le journal des déploiements de chaque environnement.

Les charts infra sont synchronisés en vague `-1`, les apps en vague `1`. Les ressources à données
(`Cluster` CNPG, `Elasticsearch`) portent `argocd.argoproj.io/sync-options: Prune=false`, et les
Applications infra n'ont pas de finalizer (`preserveResourcesOnDeletion`) : renommer un dossier ou
supprimer une Application ne détruit pas une base.

## Vérifier localement

```sh
sh scripts/render.sh                                  # lint + rendu dans rendered/<env>/
uv run --no-project --with pyyaml==6.0.3 scripts/validate_versions.py .
uv run --no-project --with yamllint==1.37.1 yamllint --strict .
```

## Ajouter une application

1. Créer `charts/apps/<app>/` (chart Helm : `Chart.yaml`, `values.yaml`, `values.schema.json`,
   `templates/`) en reprenant les conventions de `notes-api` (labels, securityContext restreint,
   `image.repository` / `image.tag` obligatoires dans le schéma).
2. Créer `envs/dev/` et `envs/prod/` avec `values.yaml` et un `version.yaml` initial
   (`tag: "0.0.0"`).
3. Brancher la CI de l'application sur `charts/apps/<app>/envs/<env>/version.yaml`.
4. Ouvrir la MR : au merge, les ApplicationSets créent `gitops-dev-<app>` et `gitops-prod-<app>`,
   sans toucher à `argocd/`.

Un composant d'infrastructure s'ajoute de la même façon sous `charts/infra/<composant>/` ; son
`version.yaml` sera suivi par Renovate.

## Ajouter un environnement (ex. `staging`)

1. Ajouter `envs/staging/{values,version}.yaml` dans chaque chart concerné (hôtes
   `*.staging.127-0-0-1.sslip.io`, tailles…).
2. Copier `argocd/appset-apps-prod.yaml` et `argocd/appset-infra-prod.yaml` en
   `appset-*-staging.yaml` en remplaçant `prod` par `staging` (nom, chemin du générateur, labels,
   `valueFiles`, namespace).
3. Ajouter la destination `gitops-staging` dans `argocd/project.yaml`.
4. Côté plateforme : Secret `registry-credentials` dans `gitops-staging` ; côté `notes-api` : job
   de promotion vers ce nouvel environnement.

## Revenir en arrière (rollback)

Le rollback est un commit comme un autre : on **annule le commit de promotion** sur `main`.

```sh
git switch main && git pull
git log --oneline -- charts/apps/notes-api/envs/prod/version.yaml   # repérer « chore(notes-api): prod -> X.Y.Z »
git switch -c revert/notes-api-prod-X.Y.Z
git revert <sha-du-commit-de-promotion>       # ou du commit de merge : git revert -m 1 <sha>
git push -u origin HEAD                         # puis MR vers main, pipeline verte, merge
```

Argo CD redéploie alors l'image précédente. Éviter le bouton « Rollback » d'Argo CD sur une
Application en sync automatique : il est refusé (ou annulé au prochain sync) car Git reste la
source de vérité. Pour l'infra, un `git revert` d'une montée de version Renovate fonctionne de la
même manière, **sauf** pour une montée majeure de PostgreSQL ou d'Elasticsearch, qui n'est pas
réversible par simple changement d'image (restaurer une sauvegarde).
