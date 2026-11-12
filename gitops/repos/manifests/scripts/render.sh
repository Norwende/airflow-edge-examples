#!/bin/sh
# Lint et rendu de tous les charts, pour chaque env, exactement comme Argo CD :
#   -f charts/<kind>/<chart>/envs/<env>/values.yaml
#   --set-string image.repository=…,image.tag=…  (lus dans envs/<env>/version.yaml)
# Sortie : ${RENDER_DIR:-rendered}/<env>/<kind>-<chart>.yaml
# Utilisé par le job CI helm-lint ; lançable en local (POSIX sh + helm).
set -eu

out="${RENDER_DIR:-rendered}"
rm -rf "$out"
failed=0

# Lit image.<champ> dans un version.yaml (format minimal validé par
# scripts/validate_versions.py) : clé indentée de 2 espaces sous image:.
read_image_field() {
  awk -v field="$2" '
    /^[^ #]/ { in_image = ($0 ~ /^image:[[:space:]]*(#.*)?$/) ; next }
    in_image && $0 ~ "^  " field ":" {
      sub("^  " field ":[[:space:]]*", "")
      sub("[[:space:]]+#.*$", "")
      gsub(/^["\047]|["\047]$/, "")
      print
      exit
    }
  ' "$1"
}

for chart in charts/*/*/; do
  chart="${chart%/}"
  [ -f "$chart/Chart.yaml" ] || continue
  name="$(basename "$chart")"
  kind="$(basename "$(dirname "$chart")")"
  for envdir in "$chart"/envs/*/; do
    env="$(basename "$envdir")"
    version_file="$chart/envs/$env/version.yaml"
    repo="$(read_image_field "$version_file" repository)"
    tag="$(read_image_field "$version_file" tag)"
    if [ -z "$repo" ] || [ -z "$tag" ]; then
      echo "ERREUR $version_file : image.repository/image.tag introuvables" >&2
      failed=1
      continue
    fi
    echo "==> $kind/$name [$env] $repo:$tag"
    mkdir -p "$out/$env"
    set -- -f "$chart/envs/$env/values.yaml" --set-string "image.repository=$repo,image.tag=$tag"
    if ! helm lint --strict "$chart" --namespace "gitops-$env" "$@"; then
      failed=1
      continue
    fi
    if ! helm template "$name" "$chart" --namespace "gitops-$env" "$@" > "$out/$env/$kind-$name.yaml"; then
      failed=1
    fi
  done
done

if [ "$failed" -ne 0 ]; then
  echo "Échec du lint ou du rendu (voir ci-dessus)" >&2
  exit 1
fi
echo "Rendus disponibles dans $out/"
