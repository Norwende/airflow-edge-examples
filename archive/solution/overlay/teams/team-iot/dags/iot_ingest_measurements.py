"""
### iot_ingest_measurements — ingestion événementielle des mesures de qualité de l'air (team-iot)

docs/09, étapes 3 à 7 et 11. Chaque CSV déposé dans `s3://datalake/air/inbox/<station>/` déclenche
un run, **sans cron ni sensor** : le trigger `S3NewObjectTrigger` (triggerer, centre) émet un
événement sur l'asset surveillé `air_inbox`, le payload porte la clé du fichier.

**Chaîne**

1. `new_keys` : union des `extra["payload"]["keys"]` de tous les `triggering_asset_events[air_inbox]`
   **et** d'un listing de `air/inbox/` (fichiers en attente) ; en run manuel avec `reprocess_prefix`,
   liste `air/processed/<prefix>` à la place ; filtre `params.stations`.
2. `make_tmp_prefix` (**setup**) crée `air/tmp/<run_id>/.keep` ; `drop_tmp_prefix` (**teardown**)
   le supprime toujours, sans faire échouer le run.
3. `has_work` (`@task.short_circuit`) : aucun fichier → tout l'aval est *skipped* (sauf le teardown).
4. `parse_file` mappé par **clé de fichier** (`expand_kwargs`, `map_index_template`), pool `team-iot`,
   4 en parallèle au plus : rejette les lignes négatives, stats numpy, dérive (moyenne pm25 > 45 ;
   IsolationForest en information seulement).
5. `quality_gate` (`@task.branch`) : dérive → `hitl_decide`, sinon `publish`.
6. `hitl_decide` (HITL, 30 min, défaut *quarantine*) : **calibrate** → `apply_calibration` (offset saisi) ;
   **quarantine** → `incident_note` (commentaire saisi) → `quarantine_station` (asset `air_quarantine`) ;
   **ignore** → `publish`.
7. `publish` : parquet `air/measurements/ds=<date>/<station>-<HHMM>.parquet`, déplacement
   `air/inbox/` → `air/processed/`, événements `air_measurements` (+ `extra`) et, via l'alias
   `air_stations`, un asset **par station** (`air_station_<station>`).
8. `report` (`ALL_DONE_MIN_ONE_SUCCESS`) : résumé ; échoue si une tâche amont a échoué (sinon le run
   serait *success* puisque `report` est la feuille, et `on_failure_callback` ne partirait pas).

**Paramètres** (formulaire *Trigger*) : `stations`, `reprocess_prefix`, `dry_run`.
`dry_run=true` : **rien n'est écrit** (ni marqueur tmp, ni parquet, ni déplacement, ni quarantaine) ;
`publish` lève `AirflowSkipException` : **aucun événement d'asset** n'est émis.
Pour provoquer un échec de `publish` (étape 5) : `conf={"fail_publish": true}`.

**Alertes** : `on_failure_callback` = `SmtpNotifier` (Mailpit, `smtp_default`) + `MinioAlertNotifier`
(`air/alerts/<dag_id>-<run_id>.json`) ; **Deadline Alert** : 10 min après la mise en file du run,
`iot_callbacks.notify_deadline` (module des plugins centraux, exécuté par le triggerer).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.providers.standard.operators.hitl import HITLEntryOperator
from airflow.sdk import (
    Asset,
    AsyncCallback,
    DeadlineAlert,
    DeadlineReference,
    Metadata,
    Param,
    TriggerRule,
    dag,
    task,
)

from iot_common import (
    AIR_INBOX,
    AIR_MEASUREMENTS,
    AIR_QUARANTINE,
    AIR_STATIONS,
    ALERTS_PREFIX,
    INBOX_PREFIX,
    MAIL_FROM,
    MAIL_TO,
    PROCESSED_PREFIX,
    QUARANTINE_PREFIX,
    TEAM_DEFAULT_ARGS,
    TEAM_TAGS,
    TMP_PREFIX,
    AirHITLBranchOperator,
    HitlLinkSmtpNotifier,
    MinioAlertNotifier,
)

FAILURE_MAIL = SmtpNotifier(
    from_email=MAIL_FROM,
    to=MAIL_TO,
    smtp_conn_id="smtp_default",
    subject="[team-iot] échec de {{ dag.dag_id }}",
    html_content="<p>Le run <code>{{ run_id }}</code> de <b>{{ dag.dag_id }}</b> a échoué.</p>",
)

HITL_BODY = """\
Dérive détectée sur au moins une station :

{% for s in ti.xcom_pull(task_ids='quality_gate', key='drift_summary') or [] -%}
- **{{ s.station }}** (`{{ s.key }}`) : moyenne pm25 = {{ s.mean_pm25 }} — {{ s.reason }}
{% endfor %}
- **calibrate** : appliquer l'offset saisi au pm25 des stations en dérive, puis publier ;
- **quarantine** : déplacer leurs fichiers dans `air/quarantine/` (commentaire demandé ensuite) ;
- **ignore** : publier tel quel.

Run : `{{ run_id }}`. Sans réponse sous 30 min : *quarantine*.
"""


def _drifting(parsed: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [p for p in parsed if p["drift"]]


@dag(
    dag_id="iot_ingest_measurements",
    schedule=[AIR_INBOX],
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=4,
    default_args=TEAM_DEFAULT_ARGS,
    tags=[*TEAM_TAGS, "assets", "hitl", "event-driven"],
    doc_md=__doc__,
    params={
        "stations": Param(
            [],
            type="array",
            items={"type": "string"},
            title="Stations",
            description_md="Ne traiter que ces stations (vide = toutes). Ex. `st-01`.",
        ),
        "reprocess_prefix": Param(
            None,
            type=["null", "string"],
            title="Retraiter un préfixe",
            description_md=(
                "Run **manuel** : retraite `air/processed/<prefix>` (ex. `st-02/` ou `st-02/20261017`) "
                "au lieu de lire l'événement d'asset. Vide = comportement normal."
            ),
        ),
        "dry_run": Param(
            False,
            type="boolean",
            title="Dry run",
            description_md="Lire et contrôler sans **rien** écrire ni déplacer (aucun événement d'asset).",
        ),
    },
    on_failure_callback=[FAILURE_MAIL, MinioAlertNotifier()],
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=10),
        # chemin pointé (et non la fonction) : le module n'existe qu'au centre (plugins), pas sur le worker
        callback=AsyncCallback(
            "iot_callbacks.notify_deadline",
            kwargs={"alert_prefix": ALERTS_PREFIX, "mail_to": MAIL_TO, "mail_from": MAIL_FROM},
        ),
    ),
)
def iot_ingest_measurements():
    @task
    def new_keys(**context) -> list[dict[str, str]]:
        """
        Clés à traiter : union des payloads de **tous** les événements `air_inbox` et d'un listing
        de `air/inbox/` (rien n'est laissé de côté si des événements ont été fusionnés ou perdus) ;
        en run manuel avec `reprocess_prefix`, listing de `air/processed/<prefix>` à la place.
        """
        from iot_common import list_keys, station_of

        params = context["params"]
        if params.get("reprocess_prefix"):
            prefix = PROCESSED_PREFIX + params["reprocess_prefix"]
            keys = list_keys(prefix, ".csv")
            print(f"retraitement de {prefix} : {len(keys)} fichiers")
        else:
            try:
                events = context["triggering_asset_events"][AIR_INBOX]
            except KeyError:  # run manuel sans événement
                events = []
            keys = []
            for e in events:
                print(f"événement {e.asset.name} @ {e.timestamp} : {e.extra}")
                payload = (e.extra or {}).get("payload") or {}
                keys += payload.get("keys") or ([payload["key"]] if "key" in payload else [])
            pending = list_keys(INBOX_PREFIX, ".csv")
            print(f"{len(keys)} clé(s) dans les événements, {len(pending)} en attente dans {INBOX_PREFIX}")
            keys += pending
        keys = sorted(dict.fromkeys(k for k in keys if k.endswith(".csv")))  # dédoublonnage
        if params.get("stations"):
            keys = [k for k in keys if station_of(k) in params["stations"]]
        print(f"{len(keys)} fichier(s) : {keys}")
        return [{"key": k} for k in keys]

    @task
    def make_tmp_prefix(**context) -> str:
        """Setup : marqueur `air/tmp/<run_id>/.keep` (zone de travail du run)."""
        from iot_common import put_json

        prefix = f"{TMP_PREFIX}{context['run_id']}/"
        if context["params"]["dry_run"]:
            print(f"dry_run : {prefix} non créé")
        else:
            put_json(f"{prefix}.keep", {"run_id": context["run_id"]})
        return prefix

    @task
    def drop_tmp_prefix(**context) -> None:
        """Teardown : supprime `air/tmp/<run_id>/` ; n'échoue jamais (le run ne doit pas en dépendre)."""
        from iot_common import delete_prefix

        prefix = f"{TMP_PREFIX}{context['run_id']}/"
        try:
            print(f"{delete_prefix(prefix)} objet(s) supprimé(s) sous {prefix}")
        except Exception as exc:  # teardown best effort
            print(f"nettoyage de {prefix} impossible : {exc!r}")

    @task.short_circuit
    def has_work(keys: list[dict[str, str]]) -> bool:
        """Court-circuit : rien à faire → tout l'aval (hors teardown) passe en *skipped*."""
        return len(list(keys)) > 0

    @task(map_index_template="{{ task.op_kwargs['key'] }}", max_active_tis_per_dag=4, pool="team-iot")
    def parse_file(key: str) -> dict[str, Any]:
        """Lit un CSV, rejette les lignes à concentration négative, stats numpy, détection de dérive."""
        import numpy as np  # import local : règle D3

        from iot_common import POLLUTANTS, drift_detected, read_csv_rows, station_of

        rows = read_csv_rows(key)
        clean = [r for r in rows if all((r[c] is not None and r[c] >= 0) for c in POLLUTANTS)]
        rejected = len(rows) - len(clean)
        stats = {c: round(float(np.mean([r[c] for r in clean])), 2) if clean else None for c in POLLUTANTS}
        drift = drift_detected(clean)
        print(f"{key} : {len(rows)} lignes, {rejected} rejetée(s), moyennes {stats}, dérive {drift}")
        return {
            "key": key,
            "station": station_of(key),
            "rows": len(clean),
            "rejected": rejected,
            "mean_pm25": stats["pm25"],
            "means": stats,
            "drift": bool(drift["drift"]),
            "reason": drift["reason"],
            "rows_payload": clean,  # volontairement gros : > 64 Kio avec --hours élevé (étape 12)
        }

    @task.branch
    def quality_gate(parsed: list[dict[str, Any]], **context) -> str:
        """Une station dérive → décision humaine ; sinon publication directe."""
        drifting = _drifting(list(parsed))
        summary = [{k: p[k] for k in ("key", "station", "mean_pm25", "reason")} for p in drifting]
        context["ti"].xcom_push(key="drift_summary", value=summary)  # petit XCom pour le corps HITL
        print(f"stations en dérive : {summary}")
        return "hitl_decide" if drifting else "publish"

    hitl_decide = AirHITLBranchOperator(
        task_id="hitl_decide",
        subject="Station suspecte : que faire ?",
        body=HITL_BODY,
        options=["calibrate", "quarantine", "ignore"],
        options_mapping={
            "calibrate": "apply_calibration",
            "quarantine": "incident_note",
            "ignore": "publish",
        },
        params={
            "offset": Param(
                0.0,
                type="number",
                minimum=-50,
                maximum=50,
                title="Offset pm25",
                description_md="Ajouté au `pm25` des stations en dérive si **calibrate** (ex. `-40`).",
            )
        },
        defaults="quarantine",
        response_timeout=timedelta(minutes=30),
        # id = identifiant du gestionnaire d'auth (FAB : id numérique, 1 = admin) ;
        # un login ici -> 403 "User ... is not a respondent for the task" à la réponse.
        assigned_users=[{"id": "1", "name": "admin"}],
        notifiers=[
            HitlLinkSmtpNotifier(
                from_email=MAIL_FROM,
                to=MAIL_TO,
                smtp_conn_id="smtp_default",
                subject="[team-iot] décision attendue : {{ dag.dag_id }}",
                html_content=(
                    "<p>Dérive détectée, run <code>{{ run_id }}</code> : "
                    "{{ ti.xcom_pull(task_ids='quality_gate', key='drift_summary') }}</p>"
                ),
            )
        ],
    )

    @task
    def apply_calibration(parsed: list[dict[str, Any]], **context) -> dict[str, Any]:
        """Applique l'offset saisi dans `hitl_decide` au pm25 des stations en dérive."""
        ti = context["ti"]
        answer = ti.xcom_pull(task_ids="hitl_decide", key="hitl_response") or {}
        offset = float(answer.get("params_input", {}).get("offset", 0.0))
        print(f"réponse HITL : {answer}")
        calibrated: dict[str, list[dict[str, Any]]] = {}
        for p in _drifting(list(parsed)):
            rows = [{**r, "pm25": round(max(r["pm25"] + offset, 0.0), 2)} for r in p["rows_payload"]]
            calibrated[p["key"]] = rows
            mean = round(sum(r["pm25"] for r in rows) / len(rows), 2) if rows else None
            print(f"{p['station']} : offset {offset:+} → moyenne pm25 {p['mean_pm25']} → {mean}")
        return {"offset": offset, "rows": calibrated}

    incident_note = HITLEntryOperator(
        task_id="incident_note",
        subject="Commentaire d'incident",
        body=(
            "Les stations en dérive vont être mises en **quarantaine** : "
            "{{ ti.xcom_pull(task_ids='quality_gate', key='drift_summary') | map(attribute='station') "
            "| join(', ') }}. Décrire l'incident (part dans l'`extra` de l'événement `air_quarantine`)."
        ),
        params={"incident": Param("", type="string", title="Incident")},
        response_timeout=timedelta(minutes=30),
        # id = identifiant du gestionnaire d'auth (FAB : id numérique, 1 = admin) ;
        # un login ici -> 403 "User ... is not a respondent for the task" à la réponse.
        assigned_users=[{"id": "1", "name": "admin"}],
    )

    @task(outlets=[AIR_QUARANTINE])
    def quarantine_station(parsed: list[dict[str, Any]], **context):
        """Déplace les fichiers des stations en dérive vers `air/quarantine/` ; commentaire en `extra`."""
        from iot_common import move_object

        note = context["ti"].xcom_pull(task_ids="incident_note") or {}
        incident = (note.get("params_input") or {}).get("incident", "")
        drifting = _drifting(list(parsed))
        moved = []
        for p in drifting:
            dst = (
                QUARANTINE_PREFIX + p["key"].split("/", 2)[2]
            )  # air/inbox/<st>/<f> → air/quarantine/<st>/<f>
            if context["params"]["dry_run"]:
                print(f"dry_run : {p['key']} resterait en place (→ {dst})")
            else:
                move_object(p["key"], dst)
            moved.append(p["key"])
        stations = sorted({p["station"] for p in drifting})
        yield Metadata(AIR_QUARANTINE, {"stations": stations, "keys": moved, "incident": incident})
        return moved

    @task(outlets=[AIR_MEASUREMENTS, AIR_STATIONS], trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def publish(parsed: list[dict[str, Any]], **context) -> dict[str, Any]:
        """Parquet validé par fichier, inbox → processed, événements d'asset (dont un par station)."""
        from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

        from iot_common import measurement_key, move_object, stamp_of, write_parquet

        ti, params = context["ti"], context["params"]
        if context["dag_run"].conf.get("fail_publish"):
            raise AirflowFailException("échec volontaire (conf.fail_publish) : tester les notifications")
        if params["dry_run"]:
            raise AirflowSkipException("dry_run : aucune écriture, aucun événement d'asset")

        calibration = ti.xcom_pull(task_ids="apply_calibration") or {}
        calibrated = calibration.get("rows", {})
        quarantined = set(ti.xcom_pull(task_ids="quarantine_station") or [])
        to_publish = [p for p in parsed if p["key"] not in quarantined]
        if not to_publish:
            raise AirflowSkipException("tout est en quarantaine : rien à publier")

        written, per_station = [], {}
        for p in to_publish:
            rows = calibrated.get(p["key"], p["rows_payload"])
            out_key = measurement_key(p["station"], stamp_of(p["key"]))
            write_parquet(rows, out_key)
            if p["key"].startswith(INBOX_PREFIX):  # un retraitement lit déjà air/processed/
                move_object(p["key"], PROCESSED_PREFIX + p["key"][len(INBOX_PREFIX) :])
            written.append(out_key)
            st = per_station.setdefault(p["station"], {"rows": 0, "pm25_sum": 0.0})
            st["rows"] += len(rows)
            st["pm25_sum"] += sum(r["pm25"] for r in rows)

        outlet_events = context["outlet_events"]
        total = sum(s["rows"] for s in per_station.values())
        summary = {
            "files": len(written),
            "rows": total,
            "stations": sorted(per_station),
            "keys": written,
            "calibration_offset": calibration.get("offset"),
        }
        outlet_events[AIR_MEASUREMENTS].extra = summary
        for station, s in sorted(per_station.items()):
            mean = round(s["pm25_sum"] / s["rows"], 2) if s["rows"] else None
            outlet_events[AIR_STATIONS].add(
                Asset(name=f"air_station_{station}", uri=f"s3://datalake/air/measurements/{station}"),
                extra={"rows": s["rows"], "mean_pm25": mean},
            )
        print(f"publié : {summary}")
        return summary

    @task(trigger_rule=TriggerRule.ALL_DONE_MIN_ONE_SUCCESS)
    def report(**context) -> None:
        """Résumé du run ; échoue si une tâche a échoué (pour que le run, et ses alertes, échouent)."""
        from airflow.sdk.exceptions import AirflowFailException

        ti = context["ti"]
        published = ti.xcom_pull(task_ids="publish")
        quarantined = ti.xcom_pull(task_ids="quarantine_station")
        decision = ti.xcom_pull(task_ids="hitl_decide", key="hitl_response")
        print(f"publié : {published}")
        print(f"quarantaine : {quarantined}")
        print(f"décision HITL : {decision}")
        states = ti.get_task_states(dag_id=ti.dag_id, run_ids=[ti.run_id]).get(ti.run_id, {})
        print(f"états : {states}")
        failed = sorted(t for t, s in states.items() if str(s) in ("failed", "upstream_failed"))
        if failed:
            raise AirflowFailException(f"tâches en échec : {failed}")

    keys = new_keys()
    tmp = make_tmp_prefix()
    drop = drop_tmp_prefix().as_teardown(setups=tmp)
    gate_ok = has_work(keys)
    parsed = parse_file.partial().expand_kwargs(keys)
    gate = quality_gate(parsed)
    calib = apply_calibration(parsed)
    quarantined = quarantine_station(parsed)
    published = publish(parsed)

    keys >> tmp >> gate_ok >> parsed
    gate >> [hitl_decide, published]
    hitl_decide >> [calib, incident_note, published]
    incident_note >> quarantined
    [calib, quarantined] >> published
    [published, quarantined] >> drop
    [gate, published] >> report()


iot_ingest_measurements()
