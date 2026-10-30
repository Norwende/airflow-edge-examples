# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Superpose la solution de docs/09 au repo, ou la retire (bibliothèque standard uniquement).

uv run archive/solution/apply.py            # copie overlay/ sur le repo (crée les dossiers)
uv run archive/solution/apply.py --check    # compare overlay/ et le repo, octet par octet (code 1 si écart)
uv run archive/solution/apply.py --revert   # remet base/ et supprime les fichiers propres à la solution

overlay/ : fichiers de la solution aux chemins du repo (nouveaux + versions modifiées des fichiers de base).
base/    : versions « énoncé » des fichiers de base que la solution modifie.
Copie octet par octet : fins de ligne (CRLF/LF) et encodage conservés.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent  # archive/solution/ -> racine du repo
OVERLAY = HERE / "overlay"
BASE = HERE / "base"


def files_of(tree: Path) -> list[Path]:
    """Chemins relatifs des fichiers de `tree` (hors __pycache__ et caches)."""
    if not tree.is_dir():
        sys.exit(f"Dossier introuvable : {tree}")
    return sorted(
        p.relative_to(tree)
        for p in tree.rglob("*")
        if p.is_file() and "__pycache__" not in p.parts and ".pytest_cache" not in p.parts
    )


def target(rel: Path) -> Path:
    """Chemin dans le repo ; refuse tout ce qui sortirait du repo ou viserait archive/solution."""
    dest = (ROOT / rel).resolve()
    if not dest.is_relative_to(ROOT) or dest.is_relative_to(HERE):
        sys.exit(f"Chemin refusé (hors du repo) : {rel}")
    return dest


def copy_tree(tree: Path, label: str) -> None:
    for rel in files_of(tree):
        dest = target(rel)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(tree / rel, dest)
        print(f"{label:<9} {rel.as_posix()}")


def apply() -> None:
    copy_tree(OVERLAY, "copié")
    print("\nSolution appliquée. Retour à l'énoncé : uv run archive/solution/apply.py --revert")


def check() -> int:
    counts = {"identique": 0, "différent": 0, "absent": 0}
    for rel in files_of(OVERLAY):
        dest = target(rel)
        if not dest.is_file():
            state = "absent"
        elif dest.read_bytes() == (OVERLAY / rel).read_bytes():
            state = "identique"
        else:
            state = "différent"
        counts[state] += 1
        print(f"{state:<9} {rel.as_posix()}")
    print("\n" + ", ".join(f"{n} {state}" for state, n in counts.items()))
    return 0 if counts["différent"] == 0 and counts["absent"] == 0 else 1


def remove_empty_dirs(start: Path) -> None:
    """Remonte depuis `start` et supprime les dossiers vides (ou ne contenant que __pycache__)."""
    d = start
    while d != ROOT and d.is_relative_to(ROOT) and d.is_dir():
        for cache in d.glob("__pycache__"):
            shutil.rmtree(cache)
            print(f"{'supprimé':<9} {cache.relative_to(ROOT).as_posix()}/")
        if any(d.iterdir()):
            return
        d.rmdir()
        print(f"{'supprimé':<9} {d.relative_to(ROOT).as_posix()}/")
        d = d.parent


def revert() -> None:
    copy_tree(BASE, "restauré")
    base_files = set(files_of(BASE))
    parents: set[Path] = set()
    for rel in files_of(OVERLAY):
        if rel in base_files:
            continue
        dest = target(rel)
        if dest.is_file():
            dest.unlink()
            print(f"{'supprimé':<9} {rel.as_posix()}")
        parents.add(dest.parent)
    # les plus profonds d'abord, pour vider les arborescences (ex. teams/team-iot/k8s puis teams/team-iot)
    for d in sorted(parents, key=lambda p: len(p.parts), reverse=True):
        remove_empty_dirs(d)
    print("\nÉnoncé restauré. Réappliquer la solution : uv run archive/solution/apply.py")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--check", action="store_true", help="comparer overlay/ et le repo, sans rien écrire")
    group.add_argument("--revert", action="store_true", help="revenir à l'énoncé (base/)")
    args = parser.parse_args()
    if args.check:
        return check()
    if args.revert:
        revert()
    else:
        apply()
    return 0


if __name__ == "__main__":
    sys.exit(main())
