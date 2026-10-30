# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Construit les images locales (dockerd de Rancher Desktop => visibles par k3s).

uv run scripts/build.py                     # image centrale + toutes les équipes présentes
uv run scripts/build.py --central           # image centrale seulement
uv run scripts/build.py --team team-data    # une équipe (répétable)
"""

import argparse

from _common import CENTRAL_IMAGE, ROOT, TEAMS, info, ok, require, run, step


def build_central() -> None:
    step(f"Image centrale {CENTRAL_IMAGE}")
    run(["docker", "build", "-t", CENTRAL_IMAGE, str(ROOT / "platform" / "central-image")])
    ok(CENTRAL_IMAGE)


def build_team(team: str) -> None:
    context = ROOT / "teams" / team
    image = f"airflow-edge/worker-{team}:dev"
    step(f"Image worker {image}")
    if not (context / "Dockerfile").exists():
        info(f"{context / 'Dockerfile'} absent : ignoré")
        return
    run(["docker", "build", "-t", image, str(context)])
    ok(image)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--central", action="store_true", help="image centrale")
    parser.add_argument("--team", action="append", choices=TEAMS, help="image d'une équipe (répétable)")
    args = parser.parse_args()
    require("docker")

    everything = not args.central and not args.team
    if args.central or everything:
        build_central()
    for team in args.team or (TEAMS if everything else []):
        build_team(team)


if __name__ == "__main__":
    main()
