"""
### iot_reconcile — expression d'assets `&` / `|` (team-iot, docs/09 étape 7)

`schedule=(AIR_MEASUREMENTS & AIR_QUARANTINE) | Asset.ref(name="air_manual_reconcile")` :
démarre quand **les deux** assets ont été mis à jour depuis le dernier run (mesures publiées **et**
station mise en quarantaine), **ou** dès que `air_manual_reconcile` est mis à jour
(DAG manuel `iot_manual_reconcile`). `GET /api/v2/dags/iot_reconcile/details` → `asset_expression`.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from iot_common import AIR_MEASUREMENTS, AIR_QUARANTINE, TEAM_DEFAULT_ARGS, TEAM_TAGS

AIR_MANUAL_RECONCILE_REF = Asset.ref(name="air_manual_reconcile")


@dag(
    dag_id="iot_reconcile",
    schedule=(AIR_MEASUREMENTS & AIR_QUARANTINE) | AIR_MANUAL_RECONCILE_REF,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    default_args=TEAM_DEFAULT_ARGS,
    tags=[*TEAM_TAGS, "assets"],
    doc_md=__doc__,
)
def iot_reconcile():
    @task
    def which_assets(**context) -> list[str]:
        """Liste les assets (et leurs `extra`) qui ont déclenché ce run."""
        triggering = context.get("triggering_asset_events") or {}
        names = []
        for asset, events in triggering.items():
            for e in events:
                print(f"{asset} → {e.asset.name} @ {e.timestamp} extra={e.extra}")
                names.append(e.asset.name)
        if not names:
            print("triggering_asset_events vide (run manuel, ou déclenché via Asset.ref ?)")
        return sorted(set(names))

    which_assets()


iot_reconcile()
