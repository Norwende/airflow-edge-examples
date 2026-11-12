"""Accès PostgreSQL : pool de connexions psycopg 3 et initialisation du schéma."""

from psycopg_pool import ConnectionPool

# Pas d'Alembic : le schéma tient en une table, créée au démarrage si elle n'existe pas.
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS notes (
    id          uuid        PRIMARY KEY,
    title       text        NOT NULL,
    content     text        NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now()
)
"""
# Clé arbitraire du verrou consultatif qui sérialise l'init du schéma entre réplicas : deux
# CREATE TABLE IF NOT EXISTS simultanés peuvent sinon échouer (violation d'unicité sur pg_type).
SCHEMA_LOCK_KEY = 0x6E6F746573  # "notes"

POOL_MIN_SIZE = 1
POOL_MAX_SIZE = 5
# Délai maximal d'attente de la base au démarrage (au-delà, le pod redémarre : CrashLoopBackOff).
POOL_OPEN_TIMEOUT_S = 30.0


def open_pool(conninfo: str) -> ConnectionPool:
    """Ouvre le pool et attend qu'au moins une connexion soit établie.

    ``check_connection`` valide chaque connexion avant de la prêter : une connexion morte
    (redémarrage ou bascule de l'instance CNPG) est remplacée au lieu de faire échouer la requête.
    """
    pool = ConnectionPool(
        conninfo,
        min_size=POOL_MIN_SIZE,
        max_size=POOL_MAX_SIZE,
        open=False,
        check=ConnectionPool.check_connection,
        name="notes-api",
    )
    try:
        pool.open(wait=True, timeout=POOL_OPEN_TIMEOUT_S)
    except Exception:
        pool.close()
        raise
    return pool


def init_schema(pool: ConnectionPool) -> None:
    """Crée la table ``notes`` si elle est absente (idempotent, sûr avec plusieurs réplicas)."""
    with pool.connection() as conn, conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_LOCK_KEY,))
        conn.execute(SCHEMA_SQL)
