"""
Plugin UI (Airflow 3.1+, `external_views`) : Mailpit et la console MinIO depuis le menu Airflow,
et une vue contextuelle de run vers ses fichiers temporaires (docs/09, étape 14).

Chargé par l'api-server depuis le `plugins_folder` (`/opt/airflow/plugins` dans l'image officielle).
Vérifier : `airflow plugins` ou **Admin → Plugins**.

- `url_route` présent → la page s'affiche dans une **iframe** de l'UI (Mailpit l'accepte) ;
- sans `url_route` → simple **lien externe** (la console MinIO refuse probablement l'iframe :
  `X-Frame-Options` / CSP) ;
- `destination: "dag_run"` → onglet sur la page d'un run ; `{RUN_ID}` / `{DAG_ID}` sont substitués.
"""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin


class AirViewsPlugin(AirflowPlugin):
    name = "air_views"
    external_views = [
        {
            "name": "Mailpit",
            "href": "http://mailpit.localhost",
            "url_route": "mailpit",  # iframe
            "destination": "nav",
            "category": "browse",
            "nav_top_level": True,
        },
        {
            "name": "MinIO Console",
            "href": "http://minio-console.localhost",  # pas de url_route : lien externe
            "destination": "nav",
            "category": "browse",
        },
        {
            "name": "Fichiers temporaires du run",
            "href": "http://minio-console.localhost/browser/datalake/air%2Ftmp%2F{RUN_ID}",
            "destination": "dag_run",
        },
    ]
