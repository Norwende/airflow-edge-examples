#!/usr/bin/env python3
"""Valide les fichiers version.yaml du dépôt gitops/manifests (stdlib uniquement).

Règles (CONTRACT.md §4) pour chaque charts/*/*/envs/*/version.yaml :
  - bloc ``image`` avec ``repository`` et ``tag`` : chaînes non vides ;
  - ``tag`` toujours entre guillemets (sinon 1.10 deviendrait le flottant 1.1) ;
  - ``source`` (optionnel) : mapping de scalaires ;
  - aucune autre clé de premier niveau ;
  - chaque chart doit avoir values.yaml ET version.yaml pour chaque env.

Le format de version.yaml est volontairement minimal : mappings imbriqués de
scalaires, indentation de 2 espaces, commentaires. Le parseur ci-dessous refuse
tout le reste (listes, multi-lignes, ancres...) au lieu de deviner.

Vérifie aussi les manifests argocd/*.yaml : apiVersion/kind/metadata.name, et
pour les ApplicationSets les règles du contrat (goTemplate, missingkey=error,
pas de .path.basename). Si PyYAML est disponible (en CI :
``uv run --no-project --with pyyaml==6.0.3 scripts/validate_versions.py``), chaque
fichier argocd/*.yaml est en plus entièrement parsé et chaque version.yaml est
recoupé avec un vrai parseur YAML ; sans PyYAML, seul le contrôle stdlib s'applique.

Usage : validate_versions.py [RACINE_DU_DEPOT]   (code retour 1 si erreur)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

KEY_RE = re.compile(r"^(?P<indent> *)(?P<key>[A-Za-z0-9_.-]+):(?:\s+(?P<value>.*?))?\s*$")
ALLOWED_TOP = {"image", "source"}
ALLOWED_IMAGE = {"repository", "tag"}


class ParseError(Exception):
    """Erreur de syntaxe dans un version.yaml."""


def strip_comment(line: str) -> str:
    """Retire un commentaire de fin de ligne (hors chaîne entre guillemets)."""
    quote: str | None = None
    for i, ch in enumerate(line):
        if quote:
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
        elif ch == "#" and (i == 0 or line[i - 1] in " \t"):
            return line[:i].rstrip()
    return line.rstrip()


def parse_scalar(raw: str) -> tuple[object, bool]:
    """Retourne (valeur, était_entre_guillemets)."""
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ("'", '"'):
        inner = raw[1:-1]
        if raw[0] == '"' and ("\\" in inner or '"' in inner):
            raise ParseError(f"échappements non supportés dans {raw!r}")
        if raw[0] == "'":
            inner = inner.replace("''", "'")
        return inner, True
    if raw[:1] in ("[", "{", "&", "*", "!", "|", ">", "-", "@", "`"):
        raise ParseError(f"syntaxe YAML non supportée : {raw!r}")
    if raw in ("null", "~", ""):
        return None, False
    if raw in ("true", "false"):
        return raw == "true", False
    if re.fullmatch(r"[-+]?[0-9]+", raw):
        return int(raw), False
    if re.fullmatch(r"[-+]?([0-9]*\.[0-9]+|[0-9]+\.)([eE][-+]?[0-9]+)?", raw):
        return float(raw), False
    return raw, False


def parse_minimal_yaml(text: str) -> tuple[dict, dict]:
    """Parse des mappings imbriqués (indentation 2). Retourne (données, quoted)."""
    root: dict = {}
    quoted: dict[str, bool] = {}
    stack: list[tuple[int, dict, str]] = [(-1, root, "")]
    for lineno, line in enumerate(text.splitlines(), start=1):
        if "\t" in line:
            raise ParseError(f"ligne {lineno} : tabulation interdite")
        content = strip_comment(line)
        if not content.strip():
            continue
        if content.strip() == "---" and lineno == 1:
            continue
        m = KEY_RE.match(content)
        if not m:
            raise ParseError(f"ligne {lineno} : syntaxe non supportée : {line.strip()!r}")
        indent = len(m.group("indent"))
        if indent % 2:
            raise ParseError(f"ligne {lineno} : indentation impaire")
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent_indent, parent, prefix = stack[-1]
        if indent != parent_indent + 2 and not (parent_indent == -1 and indent == 0):
            raise ParseError(f"ligne {lineno} : indentation inattendue")
        key = m.group("key")
        if key in parent:
            raise ParseError(f"ligne {lineno} : clé dupliquée {key!r}")
        path = f"{prefix}.{key}" if prefix else key
        value = m.group("value")
        if value is None:
            child: dict = {}
            parent[key] = child
            stack.append((indent, child, path))
        else:
            parent[key], quoted[path] = parse_scalar(value)
    return root, quoted


def check_version_file(path: Path) -> list[str]:
    errors: list[str] = []
    try:
        data, quoted = parse_minimal_yaml(path.read_text(encoding="utf-8"))
    except (ParseError, UnicodeDecodeError) as exc:
        return [f"{path}: {exc}"]

    extra = set(data) - ALLOWED_TOP
    if extra:
        errors.append(f"{path}: clés de premier niveau inattendues : {sorted(extra)}")

    image = data.get("image")
    if not isinstance(image, dict):
        return [*errors, f"{path}: bloc 'image' manquant ou invalide"]
    extra = set(image) - ALLOWED_IMAGE
    if extra:
        errors.append(f"{path}: clés inattendues dans image : {sorted(extra)}")
    for key in ("repository", "tag"):
        value = image.get(key)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{path}: image.{key} doit être une chaîne non vide (trouvé {value!r})")
    if "tag" in image and not quoted.get("image.tag", False):
        errors.append(f"{path}: image.tag doit être entre guillemets (ex. tag: \"1.2.3\")")
    repo = image.get("repository")
    if isinstance(repo, str) and (":" in repo.rsplit("/", 1)[-1] or "@" in repo):
        errors.append(f"{path}: image.repository ne doit contenir ni tag ni digest ({repo!r})")
    tag = image.get("tag")
    if isinstance(tag, str) and not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
        errors.append(f"{path}: image.tag n'est pas un tag d'image valide ({tag!r})")

    source = data.get("source")
    if source is not None:
        if not isinstance(source, dict) or any(isinstance(v, dict) for v in source.values()):
            errors.append(f"{path}: 'source' doit être un mapping de scalaires")
    return errors


def check_layout(root: Path) -> list[str]:
    """Chaque chart a envs/<env>/{values,version}.yaml pour chaque env présent."""
    errors: list[str] = []
    charts = sorted(p for p in root.glob("charts/*/*") if (p / "Chart.yaml").is_file())
    if not charts:
        errors.append(f"{root}: aucun chart trouvé sous charts/*/*")
    for chart in charts:
        envs = sorted(p for p in (chart / "envs").glob("*") if p.is_dir())
        if not envs:
            errors.append(f"{chart}: aucun dossier envs/<env>")
        for env in envs:
            for name in ("values.yaml", "version.yaml"):
                if not (env / name).is_file():
                    errors.append(f"{env}: {name} manquant")
    return errors


def check_argocd(root: Path) -> list[str]:
    """Contrôle léger des manifests Argo CD (la syntaxe complète : yamllint)."""
    errors: list[str] = []
    files = sorted((root / "argocd").glob("*.yaml"))
    if not files:
        return [f"{root}/argocd: aucun manifest"]
    for path in files:
        text = path.read_text(encoding="utf-8")
        if "\t" in text:
            errors.append(f"{path}: tabulation interdite")
        for field in ("apiVersion", "kind"):
            if not re.search(rf"^{field}: \S+", text, re.M):
                errors.append(f"{path}: champ {field} absent")
        if not re.search(r"^metadata:\n(?:  .*\n)*?  name: \S+", text, re.M):
            errors.append(f"{path}: metadata.name absent")
        code = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))
        if "kind: ApplicationSet" in code:
            if ".path.basename" in code:
                errors.append(f"{path}: .path.basename vaut le nom de l'env avec le générateur files ; utiliser .path.segments")
            for needle in ("goTemplate: true", "missingkey=error", "{{ .image.repository }}", "{{ .image.tag }}"):
                if needle not in code:
                    errors.append(f"{path}: {needle!r} attendu")
            if code.count("{{") != code.count("}}"):
                errors.append(f"{path}: accolades de template déséquilibrées")
    return errors


def check_with_pyyaml(root: Path, version_files: list[Path]) -> list[str]:
    """Parse complet avec PyYAML si présent (sinon : rien, le contrôle stdlib suffit)."""
    try:
        import yaml  # type: ignore[import-untyped]  # noqa: PLC0415
    except ImportError:
        print("INFO PyYAML absent : parse complet des manifests argocd/ ignoré")
        return []
    errors: list[str] = []
    for path in sorted((root / "argocd").glob("*.yaml")):
        try:
            docs = [d for d in yaml.safe_load_all(path.read_text(encoding="utf-8")) if d is not None]
        except yaml.YAMLError as exc:
            errors.append(f"{path}: YAML invalide : {exc}")
            continue
        for doc in docs:
            if not isinstance(doc, dict) or not doc.get("kind") or not (doc.get("metadata") or {}).get("name"):
                errors.append(f"{path}: document sans kind/metadata.name")
            elif doc["kind"] == "ApplicationSet":
                spec = doc.get("spec") or {}
                if spec.get("goTemplate") is not True or "missingkey=error" not in (spec.get("goTemplateOptions") or []):
                    errors.append(f"{path}: goTemplate/missingkey=error requis")
    for path in version_files:
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            errors.append(f"{path}: YAML invalide : {exc}")
            continue
        image = (data or {}).get("image") or {}
        for key in ("repository", "tag"):
            if not isinstance(image.get(key), str) or not image[key]:
                errors.append(f"{path}: (PyYAML) image.{key} n'est pas une chaîne non vide")
    return errors


def main(argv: list[str]) -> int:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    root = Path(argv[1] if len(argv) > 1 else ".").resolve()
    files = sorted(root.glob("charts/*/*/envs/*/version.yaml"))
    errors = check_layout(root) + check_argocd(root) + check_with_pyyaml(root, files)
    if not files:
        errors.append(f"{root}: aucun version.yaml trouvé")
    for path in files:
        errs = check_version_file(path)
        errors.extend(errs)
        if not errs:
            print(f"OK   {path.relative_to(root).as_posix()}")
    for err in errors:
        print(f"ERREUR {err}", file=sys.stderr)
    print(f"{len(files)} version.yaml vérifiés, {len(errors)} erreur(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
