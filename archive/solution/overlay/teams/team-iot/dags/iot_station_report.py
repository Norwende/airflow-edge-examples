"""
### iot_station_report — consommateur de l'**alias** `air_stations` (team-iot, docs/09 étape 7)

`publish` (dans `iot_ingest_measurements`) attache à l'alias `air_stations` un asset **par station**
(`air_station_st-01`, ...) créé à l'exécution : ce DAG est planifié par l'alias, sans connaître la
liste des stations.

Observation à consigner (3.3.2) : un DAG planifié par `Asset.ref` n'a pas de
`triggering_asset_events` (docs/05 §1) ; la tâche affiche **les deux** sources (`triggering_asset_events`
et `inlet_events[alias]`) pour voir ce qu'il en est pour un alias.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from iot_common import AIR_STATIONS, TEAM_DEFAULT_ARGS, TEAM_TAGS


@dag(
    dag_id="iot_station_report",
    schedule=AIR_STATIONS,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    default_args=TEAM_DEFAULT_ARGS,
    tags=[*TEAM_TAGS, "assets", "alias"],
    doc_md=__doc__,
)
def iot_station_report():
    @task(inlets=[AIR_STATIONS])
    def show_events(**context) -> dict:
        """Affiche ce qu'Airflow fournit au run : événements déclencheurs et `inlet_events` de l'alias."""
        triggering = context.get("triggering_asset_events") or {}
        found: dict[str, list] = {"triggering": [], "inlet": []}
        print(f"triggering_asset_events : {len(triggering)} entrée(s)")
        for asset, events in triggering.items():
            for e in events:
                print(f"  [triggering] {asset} → {e.asset.name} @ {e.timestamp} extra={e.extra}")
                found["triggering"].append({"asset": e.asset.name, "extra": e.extra})
        try:
            inlet = list(context["inlet_events"][AIR_STATIONS])
        except Exception as exc:  # observation : on veut voir l'erreur, pas échouer
            print(f"inlet_events[air_stations] indisponible : {exc!r}")
            inlet = []
        print(f"inlet_events[air_stations] : {len(inlet)} événement(s)")
        for e in inlet[-10:]:
            print(f"  [inlet] {e.asset.name} @ {e.timestamp} extra={e.extra}")
            found["inlet"].append({"asset": e.asset.name, "extra": e.extra})
        print("observation : triggering_asset_events", "VIDE" if not found["triggering"] else "renseigné")
        return found

    show_events()


iot_station_report()
