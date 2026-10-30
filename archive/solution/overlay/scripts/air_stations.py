# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3"]
# ///
"""Simulateur de stations de qualité de l'air (exercice docs/09, étape 2).

Dépose un CSV par station et par heure dans MinIO : s3://datalake/air/inbox/<station>/<AAAAMMJJTHHMM>.csv
(12 lignes = un échantillon toutes les 5 min ; colonnes ts,station,lat,lon,pm25,pm10,no2,o3,temp).

uv run scripts/air_stations.py                                  # 3 stations, l'heure courante
uv run scripts/air_stations.py --stations 3 --hours 2 --negative st-01 --drift st-02
uv run scripts/air_stations.py --start 2026-10-16T00:00 --hours 24 --seed 42
uv run scripts/air_stations.py --dry-run                        # affiche sans écrire
"""

import argparse
import csv
import io
import random
import sys
from datetime import UTC, datetime, timedelta

from scenario import s3_client  # MinIO via l'Ingress (Host minio.localhost injecté avant SigV4)

BUCKET = "datalake"
COLUMNS = ["ts", "station", "lat", "lon", "pm25", "pm10", "no2", "o3", "temp"]
# Coordonnées fixes (carte Elasticsearch de l'étape 9) : st-01 Paris, st-02 Lyon, st-03 Marseille...
CITIES = [
    ("Paris", 48.8566, 2.3522),
    ("Lyon", 45.7640, 4.8357),
    ("Marseille", 43.2965, 5.3698),
    ("Toulouse", 43.6047, 1.4442),
    ("Nice", 43.7102, 7.2620),
    ("Nantes", 47.2184, -1.5536),
    ("Strasbourg", 48.5734, 7.7521),
    ("Bordeaux", 44.8378, -0.5792),
    ("Lille", 50.6292, 3.0573),
    ("Rennes", 48.1173, -1.6778),
]
SAMPLES_PER_HOUR = 12  # toutes les 5 minutes
DRIFT_PM25 = 40.0  # dérive simulée d'un capteur (-> HITL)


def station_ids(n: int) -> list[str]:
    if not 1 <= n <= len(CITIES):
        sys.exit(f"--stations doit être entre 1 et {len(CITIES)}")
    return [f"st-{i:02d}" for i in range(1, n + 1)]


def hour_rows(
    rng: random.Random,
    station: str,
    hour: datetime,
    *,
    negative: bool,
    drift: bool,
    samples: int = SAMPLES_PER_HOUR,
) -> list[dict]:
    """`samples` lignes réparties sur l'heure (12 = toutes les 5 min ; 3600 = une par seconde,
    utile pour dépasser le seuil XCom de 64 Kio à l'étape 12 de docs/09)."""
    _, lat, lon = CITIES[int(station[3:]) - 1]
    rows = []
    step = timedelta(seconds=3600 / samples)
    for i in range(samples):
        pm25 = rng.uniform(5, 35)
        rows.append(
            {
                "ts": (hour + step * i).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "station": station,
                "lat": lat,
                "lon": lon,
                "pm25": round(pm25 + (DRIFT_PM25 if drift else 0.0), 1),
                "pm10": round(max(pm25 + rng.uniform(5, 15), rng.uniform(10, 50)), 1),
                "no2": round(rng.uniform(10, 60), 1),
                "o3": round(rng.uniform(20, 90), 1),
                "temp": round(rng.uniform(5, 25), 1),
            }
        )
    if negative:
        rows[rng.randrange(samples)]["pm25"] = -5.0  # valeur impossible : ligne à rejeter
    return rows


def to_csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode()


def parse_start(value: str | None) -> datetime:
    if value is None:
        return datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start = datetime.fromisoformat(value.replace("Z", "+00:00"))
    start = start.replace(tzinfo=UTC) if start.tzinfo is None else start.astimezone(UTC)
    return start.replace(minute=0, second=0, microsecond=0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--stations", type=int, default=3, help="nombre de stations (st-01..)")
    parser.add_argument("--hours", type=int, default=1, help="nombre d'heures (un fichier par heure)")
    parser.add_argument("--start", help="première heure, ISO UTC (défaut : heure courante tronquée)")
    parser.add_argument("--negative", metavar="STATION", help="une ligne pm25=-5 par fichier de STATION")
    parser.add_argument("--drift", metavar="STATION", help=f"pm25 +{DRIFT_PM25:g} sur tous les fichiers")
    parser.add_argument(
        "--samples",
        type=int,
        default=SAMPLES_PER_HOUR,
        help="lignes par fichier (défaut 12 ; 3600 pour dépasser le seuil XCom de 64 Kio, étape 12)",
    )
    parser.add_argument("--prefix", default="air/inbox", help="préfixe S3 (défaut air/inbox)")
    parser.add_argument("--seed", type=int, help="graine aléatoire (valeurs reproductibles)")
    parser.add_argument("--dry-run", action="store_true", help="n'écrit rien, affiche les clés et un extrait")
    args = parser.parse_args()

    stations = station_ids(args.stations)
    for opt in ("negative", "drift"):
        if getattr(args, opt) and getattr(args, opt) not in stations:
            sys.exit(f"--{opt} {getattr(args, opt)} : station inconnue ({', '.join(stations)})")
    if args.hours < 1:
        sys.exit("--hours doit être >= 1")
    if not 1 <= args.samples <= 3600:
        sys.exit("--samples doit être entre 1 et 3600")

    rng = random.Random(args.seed)
    start = parse_start(args.start)
    client = None if args.dry_run else s3_client()
    prefix = args.prefix.strip("/")

    for h in range(args.hours):
        hour = start + timedelta(hours=h)
        for station in stations:
            neg, drift = station == args.negative, station == args.drift
            rows = hour_rows(rng, station, hour, negative=neg, drift=drift, samples=args.samples)
            key = f"{prefix}/{station}/{hour:%Y%m%dT%H%M}.csv"
            body = to_csv(rows)
            mean = sum(r["pm25"] for r in rows) / len(rows)
            if client:
                client.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="text/csv")
            tag = " (dry-run)" if args.dry_run else ""
            print(f"s3://{BUCKET}/{key}  {len(rows)} lignes  pm25 moyen={mean:.1f}{tag}")
            if args.dry_run and h == 0 and station == stations[0]:
                print("  " + body.decode().splitlines()[0] + "\n  " + body.decode().splitlines()[1])


if __name__ == "__main__":
    main()
