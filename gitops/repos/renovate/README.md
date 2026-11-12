# gitops/renovate — bot Renovate auto-hébergé

Ce projet fait tourner [Renovate](https://docs.renovatebot.com/) dans GitLab CI pour tous les
projets du groupe `gitops` (CONTRACT.md §6). Il contient aussi le **preset partagé** des dépôts du
lab.

| Fichier | Rôle |
|---|---|
| `.gitlab-ci.yml` | Job `renovate` (image `renovate/renovate:<version>-full`, épinglée) : planifié toutes les heures, lançable à la main ; job `validate` sur les MR. |
| `config.js` | Configuration **globale** du bot : plateforme GitLab, `autodiscover` sur `gitops/*`, pas d'onboarding, auteur des commits. |
| `default.json` | Preset partagé, référencé `local>gitops/renovate` par les `renovate.json` des dépôts : `config:recommended`, commits conventionnels `chore(deps): …`, label `renovate`, fuseau `Europe/Paris`, mineures/patchs groupés, majeures séparées, digests épinglés sur les images de base des Dockerfile. |
| `renovate.json` | Configuration de ce dépôt : le bot met à jour **sa propre image** (manager `gitlabci`). |

## Fonctionnement

- Le job s'exécute avec le PAT de l'utilisateur `renovate-bot` (Developer du groupe `gitops`),
  fourni par la variable CI masquée et protégée `RENOVATE_TOKEN` (posée par
  `scripts/gitlab_bootstrap.py tokens`).
- Planification : schedule `renovate hourly`, `0 * * * *`, fuseau `Europe/Paris`, sur `main`
  (posé par `scripts/gitlab_bootstrap.py protect`).
- `resource_group: renovate` : jamais deux exécutions simultanées.
- Les MR ouvertes par le bot passent par les mêmes règles que celles des humains (pipeline verte,
  relecture) ; sur `notes-api` elles visent `develop` (branche par défaut), leurs commits
  `chore(deps): …` ne déclenchent pas de release.
- Pas de token GitHub dans le lab : les release notes et changelogs ne sont pas récupérés.

## Lancer à la main

GitLab → `gitops/renovate` → **Build > Pipelines > Run pipeline** sur `main`, avec au besoin :

| Variable | Valeurs | Effet |
|---|---|---|
| `RENOVATE_DRY_RUN` | *(vide)*, `extract`, `lookup`, `full` | vide = exécution réelle ; sinon simulation, aucune MR |
| `LOG_LEVEL` | `debug`, `info`, `warn` | verbosité |

Par l'API : `curl -X POST -H "PRIVATE-TOKEN: …" "$GITLAB/api/v4/projects/gitops%2Frenovate/pipeline?ref=main"`.

## Valider une modification de configuration

```bash
npx --package renovate@44.222.1 renovate-config-validator --strict config.js
npx --package renovate@44.222.1 renovate-config-validator --strict --no-global default.json renovate.json
```

## Prérequis côté runner

Le job demande 1 Gi / 2 Gi de mémoire (`KUBERNETES_MEMORY_REQUEST` / `KUBERNETES_MEMORY_LIMIT`) :
la configuration du GitLab Runner doit autoriser ces surcharges
(`memory_request_overwrite_max_allowed`, `memory_limit_overwrite_max_allowed`).
