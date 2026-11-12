"""Attend que le PostgreSQL de `DATABASE_URL` accepte les connexions (service CI du job `test`).

Le service `postgres:17-alpine` démarre en même temps que le job : sans cette attente, les tests
d'intégration peuvent partir avant la fin de l'initialisation de la base.
"""

from __future__ import annotations

import os
import sys
import time

import psycopg

TIMEOUT_S = int(os.environ.get("WAIT_FOR_POSTGRES_TIMEOUT", "90"))


def main() -> int:
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("DATABASE_URL absente : rien à attendre.")
        return 0
    deadline = time.monotonic() + TIMEOUT_S
    attempt = 0
    while True:
        attempt += 1
        try:
            with psycopg.connect(url, connect_timeout=2) as conn:
                conn.execute("SELECT 1")
        except psycopg.OperationalError as exc:
            if time.monotonic() > deadline:
                print(f"PostgreSQL injoignable après {TIMEOUT_S} s : {exc}", file=sys.stderr)
                return 1
            time.sleep(1)
        else:
            print(f"PostgreSQL prêt (tentative {attempt}).")
            return 0


if __name__ == "__main__":
    sys.exit(main())
