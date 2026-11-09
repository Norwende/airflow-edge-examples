# 02 — Installation de la plateforme

Tout est dans `platform/<composant>/` (values Helm + manifestes commentés) et piloté par
`scripts/platform.py` (PEP 723, idempotent). Les secrets générés vont dans `.lab/secrets.json`
(gitignoré).

```bash
uv run scripts/platform.py up               # tout, dans l'ordre (~15 min la première fois)
uv run scripts/platform.py up gitlab runner # un ou plusieurs composants
uv run scripts/platform.py status
uv run scripts/platform.py down             # ordre inverse (cert-manager + CA conservés)
```

## Ordre et rôle de chaque composant

| # | Composant | Chart / version | Namespace | Rôle | Points de configuration |
|---|---|---|---|---|---|
| 1 | `namespaces` | manifestes | — | namespaces plateforme + `gitops-dev`, `gitops-prod`, `logging` ; quota/LimitRange/NetworkPolicy de `ci-jobs` | labels **Pod Security** : `enforce: baseline`, `warn/audit: restricted` ; `logging` en `privileged` (hostPath de Filebeat) |
| 2 | `dns` | ConfigMap `coredns-custom` | kube-system | `*.127-0-0-1.sslip.io` → ClusterIP Traefik dans le cluster | la ClusterIP est lue au moment de l'install ; CoreDNS redémarré seulement si le contenu change |
| 3 | `certmanager` | `jetstack/cert-manager` v1.21.3 | cert-manager | CA du lab (`ClusterIssuer lab-ca`) pour le TLS du registre | CA conservée entre deux installations (déjà approuvée par dockerd) |
| 4 | `cnpg` | `cnpg/cloudnative-pg` 0.29.1 | cnpg-system | PostgreSQL « as a service » : base de GitLab et base de l'application | opérateur cluster-wide, `Cluster` déclarés à côté des consommateurs |
| 5 | `rustfs` | `rustfs/rustfs` 1.0.3 | rustfs | S3 pour GitLab (artefacts, registre, uploads, LFS…) et cache du runner | Secret `rustfs-credentials` ; Job `rustfs-init` crée les buckets ; console `http://s3-console.127-0-0-1.sslip.io` |
| 6 | `gitlab` | `gitlab/gitlab` 10.5.1 (CE 19.5.1) | gitlab | forge, registre, CI | PostgreSQL CNPG `gitlab-pg`, Redis `gitlab-redis`, S3 consolidé, Ingress Traefik HTTP, TLS registre, 1 replica partout, PAT root créé via `gitlab-rails runner` |
| 7 | `runner` | `gitlab/gitlab-runner` 0.94.1 | gitlab-runner | exécute les jobs dans `ci-jobs` | runner d'instance `k8s-lab` créé par l'API (`POST /user/runners`, token `glrt-…`), executor Kubernetes, pas de conteneur privilégié, pas de token de ServiceAccount, cache S3 |
| 8 | `eck` | `elastic/eck-operator` 3.5.0 | elastic-system | Elasticsearch, Kibana, Filebeat « as a service » | ressources déclarées dans `gitops/manifests` (charts infra) |
| 9 | `argocd` | `argo/argo-cd` 10.11.1 (v3.5.6) | argocd | GitOps : la seule chose qui écrit dans `gitops-*` | HTTP derrière Traefik (`server.insecure`), admin local (mot de passe bcrypt posé par le script), suivi par annotation, réconciliation 60 s |

## Ce que le chart GitLab 10 ne fournit plus (et ce qu'on fournit)

| Dépendance | Dans le lab | Values |
|---|---|---|
| PostgreSQL ≥ 16 | `Cluster` CNPG `gitlab-pg` (17, 1 instance, extensions `pg_trgm`/`btree_gist` créées au bootstrap) | `global.psql.*`, Secret `gitlab-pg-app` généré par CNPG |
| Redis | StatefulSet `gitlab-redis` (7, `noeviction`, AOF) | `global.redis.*`, Secret `gitlab-redis-secret` |
| S3 | RustFS, un bucket par usage, une seule connexion | `global.appConfig.object_store`, `registry.storage`, `toolbox.backups` |

Retiré par rapport aux défauts : Gateway API/Envoy (Ingress Traefik), cert-manager embarqué,
nginx-ingress, Prometheus, KAS, gitlab-shell (SSH), runner embarqué, exporter. Mémoire au repos ≈ 3,3 Gi.

## Vérifications après `up`

```bash
uv run scripts/platform.py status
curl -s -o /dev/null -w '%{http_code}\n' http://gitlab.127-0-0-1.sslip.io/-/readiness   # 200
curl -s -o /dev/null -w '%{http_code}\n' http://registry.127-0-0-1.sslip.io/v2/          # 401 (registre up, auth exigée)
curl -s -o /dev/null -w '%{http_code}\n' http://argocd.127-0-0-1.sslip.io/healthz        # 200
kubectl -n ci-jobs get resourcequota ci-jobs-quota
```

Identifiants : GitLab `root` / `Gitops-Lab-2026!`, Argo CD `admin` / `Gitops-Lab-2026!`, RustFS
`rustfsadmin` / `.lab/secrets.json` (`rustfs-secret-key`).

## Bonnes pratiques mises en avant (et ce qui changerait en production)

- **Un composant = un dossier**, values commentées ligne par ligne, versions épinglées : la montée de
  version est un diff lisible.
- **Pod Security Admission** sur tous les namespaces ; seuls `ci-jobs` (kaniko en root dans son
  conteneur) et `logging` (hostPath) ont une exception explicite et justifiée.
- **Aucun privilège** pour construire des images (kaniko), aucun accès à l'API Kubernetes depuis la
  CI (la CI ne déploie pas : Argo CD le fait).
- Prod : TLS partout (ACME), SSO (OIDC) pour GitLab et Argo CD, admin Argo CD désactivé, HA (replicas,
  Redis HA, PostgreSQL 3 instances avec sauvegardes CNPG vers S3), secrets dans un coffre
  (Vault + External Secrets) plutôt qu'en variables CI.
