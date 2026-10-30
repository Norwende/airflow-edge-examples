"""
Trigger d'événement `S3NewObjectTrigger` : un événement par **nouvel** objet sous un préfixe S3.

SYNC : ce fichier existe en DEUX exemplaires qui doivent rester IDENTIQUES (même nom de module) :
`teams/team-iot/dags/iot_triggers.py` (source de vérité) et `platform/central-image/plugins/iot_triggers.py`.

Pourquoi deux copies ? (docs/09, étape 3)

- le **dag-processor** et le **worker** importent la classe en parsant le DAG : le dossier du bundle
  `team-iot` est dans leur `sys.path` → copie du bundle ;
- le **triggerer** ne charge pas les bundles : il ré-importe la classe par son classpath
  (`iot_triggers.S3NewObjectTrigger`, issu de `serialize()`) → copie dans le `plugins_folder`
  central (`/opt/airflow/plugins`, ajouté au `sys.path`). Au centre, le dag-processor voit aussi
  le dossier plugins : la copie trouvée en premier gagne, d'où l'obligation d'identité.

Règle D3 : aucun import d'équipe au niveau module (boto3 est importé dans `_list`). Le triggerer a
boto3 (provider Amazon de l'image officielle) et lit la connexion par `Connection.async_get`
(Task SDK, version asynchrone : on est dans la boucle asyncio du triggerer).

Payload : **un** `TriggerEvent` par cycle de poll, `{"keys": [...], "count": n, "sizes": [...],
"bucket": ...}` (toujours une liste, même pour un seul fichier). Observé en 3.3.2 : plusieurs
événements émis à quelques ms d'intervalle ne donnent qu'**un** run auquel un seul événement est
attaché, les autres sont perdus ; d'où l'événement groupé.

Limite assumée : l'état « déjà vu » est **en mémoire**. Au (re)démarrage du trigger, l'existant
n'est pas émis. Le DAG ne se fie donc pas qu'au payload : `new_keys` ajoute un listing de
`air/inbox/` (tout fichier en attente est traité au prochain run) et déplace chaque fichier traité
vers `air/processed/` (idempotence).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseEventTrigger, TriggerEvent


class S3NewObjectTrigger(BaseEventTrigger):
    """Liste `s3://<bucket>/<prefix>` toutes les `poke_interval` s et émet les clés nouvelles."""

    def __init__(
        self,
        prefix: str,
        poke_interval: float = 15.0,
        bucket: str = "datalake",
        aws_conn_id: str = "minio_s3",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.prefix = prefix
        self.poke_interval = poke_interval
        self.bucket = bucket
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        # classpath = "iot_triggers.S3NewObjectTrigger" : le module doit exister sous ce nom au triggerer
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {
                "prefix": self.prefix,
                "poke_interval": self.poke_interval,
                "bucket": self.bucket,
                "aws_conn_id": self.aws_conn_id,
            },
        )

    def _client(self, conn: Any) -> Any:
        """Client boto3 vers MinIO (endpoint_url dans l'extra de la connexion)."""
        import boto3  # import local : règle D3

        extra = conn.extra_dejson
        return boto3.client(
            "s3",
            endpoint_url=extra.get("endpoint_url"),
            region_name=extra.get("region_name", "us-east-1"),
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
        )

    def _list(self, client: Any) -> dict[str, int]:
        """Listing synchrone (boto3) → {clé: taille} ; appelé via asyncio.to_thread."""
        pages = client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self.prefix)
        return {
            o["Key"]: int(o["Size"])
            for page in pages
            for o in page.get("Contents", [])
            if not o["Key"].endswith("/")
        }

    async def run(self) -> AsyncIterator[TriggerEvent]:
        from airflow.sdk import Connection

        conn = await Connection.async_get(self.aws_conn_id)
        client = await asyncio.to_thread(self._client, conn)
        seen = await asyncio.to_thread(self._list, client)  # état initial : l'existant n'est pas « nouveau »
        self.log.info("s3://%s/%s : %d objets déjà présents (ignorés)", self.bucket, self.prefix, len(seen))
        while True:
            await asyncio.sleep(self.poke_interval)
            try:
                now = await asyncio.to_thread(self._list, client)
            except Exception:  # MinIO indisponible : on réessaie au prochain poll sans perdre l'état
                self.log.exception("listing s3://%s/%s en échec", self.bucket, self.prefix)
                continue
            new = sorted(now.keys() - seen.keys())
            if new:
                # UN SEUL événement par cycle, portant toutes les clés nouvelles : observé en 3.3.2,
                # 3 événements émis à 20 ms d'intervalle → un seul run, avec un seul événement attaché,
                # et les deux autres jamais consommés.
                self.log.info(
                    "%d nouvel(s) objet(s) sous s3://%s/%s : %s", len(new), self.bucket, self.prefix, new
                )
                yield TriggerEvent(
                    {"keys": new, "count": len(new), "sizes": [now[k] for k in new], "bucket": self.bucket}
                )
            seen = now
