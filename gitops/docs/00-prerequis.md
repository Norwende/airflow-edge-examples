# 00 — Prérequis

## Machine

| Élément | Valeur retenue | Pourquoi |
|---|---|---|
| Rancher Desktop (Windows 11, WSL2) | k3s **v1.34**, moteur **dockerd** (cri-dockerd), Traefik activé | un seul nœud, Ingress fourni, `docker` depuis Windows parle au même démon que le kubelet (images locales visibles du cluster) |
| Mémoire WSL | `%USERPROFILE%\.wslconfig` → `[wsl2] memory=20GB` | GitLab (~4 Gi) + Argo CD + deux environnements (Elasticsearch, Kibana, PostgreSQL) + jobs CI ; sans ce fichier WSL prend 50 % de la RAM |
| Outils Windows | `kubectl`, `helm` ≥ 4, `docker` CLI, `git`, **`uv`** (exécute les scripts PEP 723), `node`/`npm` (optionnel : commitlint/semantic-release en local), `curl` | pas besoin de Python installé : `uv run scripts/<x>.py` télécharge l'interpréteur |
| Internet | oui | images, charts, PyPI, npm, DNS `sslip.io` |

## Réseau : une URL par service, la même partout

Domaine du lab : **`127-0-0-1.sslip.io`** (DNS public qui répond `127.0.0.1` à tout sous-domaine).
Depuis Windows et depuis le nœud, `127.0.0.1:80/443` tombe sur Traefik (ports publiés par Rancher
Desktop). Depuis un **pod**, `127.0.0.1` est le pod lui-même : l'override CoreDNS
`platform/01-dns/coredns-custom.yaml` fait répondre la ClusterIP de Traefik pour toute la zone
(`uv run scripts/platform.py up dns`). Résultat : GitLab Runner, Argo CD, kaniko et le navigateur
utilisent exactement la même URL `http://gitlab.127-0-0-1.sslip.io`.

## Registre d'images : HTTP pour kaniko, HTTPS vérifié pour dockerd

- kaniko (jobs CI) pousse en HTTP sur `registry.127-0-0-1.sslip.io` (`--insecure`).
- dockerd (kubelet qui tire l'image, `docker pull` depuis Windows) exige HTTPS : Traefik sert aussi
  le registre en TLS avec un certificat signé par la **CA du lab** (cert-manager, ClusterIssuer
  `lab-ca`), et dockerd approuve cette CA via `/etc/docker/certs.d/registry.127-0-0-1.sslip.io/ca.crt`,
  déposé par un script de provisioning Rancher Desktop
  (`%LOCALAPPDATA%\rancher-desktop\provisioning\docker-registry-ca.start`, rendu par
  `scripts/platform.py up certmanager`). Un nouveau script n'est pris en compte qu'au redémarrage de
  Rancher Desktop. Rien n'est affaibli : TLS reste vérifié, contre cette CA.
- Alternative (moins propre) : déclarer le registre dans `insecure-registries` de dockerd.

## Dépôts Helm

```bash
helm repo add jetstack https://charts.jetstack.io
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo add rustfs https://charts.rustfs.com
helm repo add gitlab https://charts.gitlab.io
helm repo add elastic https://helm.elastic.co
helm repo add argo https://argoproj.github.io/argo-helm
helm repo update
```

(`scripts/platform.py` le fait tout seul, `--force-update`.)
