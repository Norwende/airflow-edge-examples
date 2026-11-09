# Documentation

Parcours conseillé, dans l'ordre. Les commandes brutes sont dans chaque page ; les scripts
`uv run scripts/<x>.py` les automatisent.

| # | Page | Contenu |
|---|---|---|
| — | [CONTRACT.md](../CONTRACT.md) | **le contrat** : noms, dépôts, branches, images, `version.yaml`, tokens, protections |
| 00 | [Prérequis](00-prerequis.md) | Rancher Desktop, mémoire, outils, DNS `sslip.io`, CA du registre |
| 01 | [Architecture et flux](01-architecture.md) | vue d'ensemble, chemin d'un commit, pourquoi ces choix, pile de logs |
| 02 | [Plateforme](02-plateforme.md) | installation composant par composant (GitLab, runner, CNPG, RustFS, ECK, Argo CD) |
| 03 | [GitLab : organisation, protections, tokens](03-gitlab.md) | groupe, projets, branches protégées, utilisateurs, variables CI, bootstrap |
| 04 | [L'application notes-api](04-application.md) | FastAPI, structlog, tests unitaires/intégration, Dockerfile distroless + uv |
| 05 | [Pipeline CI](05-pipeline-ci.md) | jobs, règles, rapports visibles dans GitLab, kaniko, semantic-release, promotion |
| 06 | [Dépôt de manifests et Argo CD](06-manifests-argocd.md) | charts, `envs/`, ApplicationSets, politique de sync, rollback |
| 07 | [Logs : Filebeat, Elasticsearch, Kibana](07-logs.md) | pile retenue, configuration, lecture dans Kibana |
| 08 | [Renovate](08-renovate.md) | bot auto-hébergé, presets, ce qui est suivi dans chaque dépôt |
| 09 | [Scénario de bout en bout](09-scenario.md) | les MR et pipelines réellement jouées, avec les résultats observés |
| 10 | [Exploitation et dépannage](10-exploitation.md) | commandes utiles, incidents rencontrés, désinstallation |
