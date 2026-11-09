# 03 — GitLab : organisation, protections, tokens

Tout est posé par `scripts/gitlab_bootstrap.py` (PEP 723, idempotent, API REST uniquement), à partir
du PAT root créé par la plateforme (`.lab/secrets.json`, clé `gitlab-root-pat`).

```bash
uv run scripts/gitlab_bootstrap.py all            # settings users groups projects tokens push protect argocd report
uv run scripts/gitlab_bootstrap.py report         # état courant
uv run scripts/gitlab_bootstrap.py tokens --rotate
uv run scripts/gitlab_bootstrap.py push --force-push   # réécrit les branches distantes depuis repos/
```

## Groupe, projets, comptes

```
gitops/                 groupe (visibilité internal)
├── notes-api           code + CI       défaut : develop     branches develop, main
├── manifests           état désiré     défaut : main
└── renovate            bot             défaut : main
```

| Compte | Rôle dans `gitops` | Usage |
|---|---|---|
| `root` | admin | scripts (PAT `lab-bootstrap`, 1 an) |
| `dev` | Developer | ouvre les MR (`scripts/demo.py feature …`), ne peut pas pousser sur `develop`/`main` ni merger `main` |
| `lead` | Maintainer | merge `develop → main`, merge les MR de **prod** dans `manifests` |
| `renovate-bot` | Developer | PAT → variable `RENOVATE_TOKEN` du projet `renovate` ; ouvre des MR partout dans le groupe |

Mot de passe des comptes du lab : `Gitops-Lab-2026!`. Réglages d'instance : inscriptions fermées,
**Auto DevOps désactivé** (sinon GitLab crée des pipelines par défaut), requêtes locales autorisées
pour les webhooks/intégrations.

Réglages des projets : `only_allow_merge_if_pipeline_succeeds` (sauf `renovate`),
`remove_source_branch_after_merge`, méthode de merge « merge » (les commits conventionnels sont
conservés tels quels, squash optionnel), registre activé, runner d'instance partagé.

## Branches et tags protégés (ce que la plateforme impose)

| Projet | Ref | Push | Merge / create | Effet |
|---|---|---|---|---|
| `notes-api` | `develop` (défaut) | personne | Developers + | tout passe par MR + pipeline verte ; la CI construit `develop/notes-api:<sha8>` et promeut en dev |
| `notes-api` | `main` | personne | **Maintainers** | seul un lead livre en production (MR `develop → main`) |
| `notes-api` | tags `v*` | — | Maintainers | seul le token de semantic-release (Maintainer) pose des tags de version |
| `manifests` | `main` (défaut) | personne | Developers + | les promotions (bot Developer), Renovate et les humains passent par MR + pipeline de lint |
| `renovate` | `main` | Maintainers | Maintainers | configuration du bot = équipe plateforme |

Ordre du bootstrap : **push avant protect** (avec `push -o ci.skip`), puis les protections, puis les
premières pipelines lancées par l'API : les variables protégées ne sont visibles que sur une branche
protégée, une pipeline lancée avant la protection échouerait sur `GITLAB_TOKEN`/`MANIFESTS_TOKEN`.

## Tokens : qui a le droit de faire quoi

| Token | Type | Porté par | Rôle / scopes | Où il vit |
|---|---|---|---|---|
| `semantic-release` | project access token de `notes-api` | job `release` | Maintainer, `api`, `write_repository` : pousser le tag `vX.Y.Z` (protégé) et créer la Release | variable CI `GITLAB_TOKEN` (masquée, protégée) |
| `notes-api-promoter` | project access token de `manifests` | jobs `promote-*` | Developer, `api`, `write_repository` : branche, commit, MR, merge automatique de `main` (autorisé aux Developers) | variable CI `MANIFESTS_TOKEN` (masquée, protégée) sur `notes-api` + `MANIFESTS_PROJECT` |
| `renovate-bot` | PAT utilisateur | job `renovate` | Developer du groupe, `api`, `write_repository` | variable CI `RENOVATE_TOKEN` (masquée, protégée) sur `renovate` |
| `argocd` | deploy token de `manifests` | Argo CD | `read_repository` | Secret `argocd/repo-gitops-manifests` (label `argocd.argoproj.io/secret-type: repository`) |
| `kubelet` | deploy token de `notes-api` | kubelet (pull des images) | `read_registry` | Secret `registry-credentials` (`kubernetes.io/dockerconfigjson`) dans `gitops-dev` et `gitops-prod` |

Principes : **moindre privilège** (un token = un usage, un rôle minimal), **masqué + protégé** (jamais
visible dans une pipeline de MR venant d'une branche de fonctionnalité), durée de vie bornée (364 jours,
`--rotate` pour ré-émettre), aucun token dans un fichier versionné ; les tokens non relisibles sont
conservés dans `.lab/secrets.json` (gitignoré). En production : un coffre (Vault + External Secrets
pour les Secrets Kubernetes, `id_tokens` OIDC des jobs pour la CI) remplace les variables CI.

## Ce que GitLab CE ne permet pas (et comment on compense)

| Besoin | CE (Free) | Dans le lab |
|---|---|---|
| Protéger `charts/*/envs/prod/**` différemment du reste (CODEOWNERS contraignants) | Premium | la MR de prod n'est **jamais** auto-mergée par le bot ; un lead la merge. Variante CE plus stricte : un dépôt `manifests-prod` séparé avec merge réservé aux Maintainers |
| Règles d'approbation par branche/chemin | Premium | la pipeline verte obligatoire + le rôle Maintainer sur `main` de `notes-api` |
| Environnements protégés avec approbateurs | Premium | la MR de promotion **est** l'approbation |
| Group access tokens | Premium | project access tokens + un utilisateur bot pour Renovate |

## Vérifier à la main

```bash
GL=http://gitlab.127-0-0-1.sslip.io/api/v4 ; TOKEN=<gitlab-root-pat>
curl -s -H "PRIVATE-TOKEN: $TOKEN" $GL/projects/gitops%2Fnotes-api/protected_branches | jq
curl -s -H "PRIVATE-TOKEN: $TOKEN" $GL/projects/gitops%2Fnotes-api/variables | jq '.[].key'
curl -s -H "PRIVATE-TOKEN: $TOKEN" "$GL/projects/gitops%2Fnotes-api/pipelines?per_page=5" | jq '.[] | {id,ref,status}'
kubectl -n argocd get secret repo-gitops-manifests -o jsonpath='{.data.url}' | base64 -d
```
