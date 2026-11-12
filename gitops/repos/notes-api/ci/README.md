# ci/ — scripts de la pipeline

| Fichier | Rôle |
|---|---|
| `promote.py` | Jobs `promote-dev` / `promote-prod` : met à jour `charts/apps/notes-api/envs/<env>/version.yaml` dans `gitops/manifests` **par l'API GitLab** (branche `promote/notes-api-<env>-<tag>`, commit `chore(notes-api): <env> -> <tag>`, MR labellisée `promotion` + `env::<env>`). Idempotent. `--auto-merge` (dev) : merge automatique quand la pipeline de la MR est verte. Stdlib uniquement. |
| `test_promote.py` | Auto-test de `promote.py` : rendu/relecture du `version.yaml`, parcours complet contre un faux GitLab local. |
| `wait_for_postgres.py` | Job `test` : attend que le service PostgreSQL accepte les connexions. |

Variables lues par `promote.py` : `MANIFESTS_TOKEN` (en-tête `PRIVATE-TOKEN`, jamais affiché),
`MANIFESTS_PROJECT` (défaut `gitops/manifests`), `CI_API_V4_URL`, `CI_PROJECT_URL`,
`CI_COMMIT_SHA`, `CI_COMMIT_REF_NAME`, `CI_PIPELINE_URL` (valeurs par défaut des options).

```bash
# Plan + fichier rendu, sans rien écrire (sans token : l'état actuel n'est pas lu)
uv run python ci/promote.py --env dev \
  --repository registry.127-0-0-1.sslip.io/gitops/notes-api/develop/notes-api \
  --tag 1a2b3c4d --ref develop --commit 1a2b3c4d5e6f7890 --dry-run

# Auto-test
uv run python -m unittest discover -s ci -p "test_*.py" -v
```
