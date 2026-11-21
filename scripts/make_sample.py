"""Write a synthetic sample in the exact Tankerkoenig file format.

Used for CI and tests only. It is NOT real data and no result in the README comes from it.
Usage: python3 scripts/make_sample.py OUT_DIR [DAYS] [STATIONS]
"""
import csv
import datetime as dt
import gzip
import math
import pathlib
import random
import sys
import uuid

CITIES = {  # name: (plz prefix, lat, lon)
    "Essen": ("45", 51.455, 7.012), "Dortmund": ("44", 51.514, 7.465),
    "Duesseldorf": ("40", 51.227, 6.773), "Koeln": ("50", 50.938, 6.960),
    "Berlin": ("10", 52.520, 13.405), "Muenchen": ("80", 48.137, 11.575),
    "Hamburg": ("20", 53.551, 9.993), "Leipzig": ("04", 51.340, 12.375),
}
BRANDS = ["ARAL", "Shell", "ESSO", "JET", "TotalEnergies", "AVIA", "Raiffeisen", "freie Tankstelle"]
LEADERS = {"ARAL", "Shell"}
# intraday markup in EUR on top of the day's base: high early morning, low in the evening
CURVE = [0.10, 0.10, 0.10, 0.10, 0.10, 0.11, 0.12, 0.10, 0.07, 0.08, 0.06, 0.05,
         0.07, 0.05, 0.04, 0.05, 0.03, 0.02, 0.00, 0.01, 0.00, 0.03, 0.08, 0.10]


def tz(ts: dt.datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S") + "+01"


def main() -> None:
    out = pathlib.Path(sys.argv[1])
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 21
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 160
    rng = random.Random(7)
    start = dt.date(2026, 1, 5)
    (out / "prices").mkdir(parents=True, exist_ok=True)
    (out / "stations").mkdir(parents=True, exist_ok=True)

    stations = []
    for i in range(n):
        city = list(CITIES)[i % len(CITIES)]
        plz, lat, lon = CITIES[city]
        brand = rng.choice(BRANDS)
        stations.append(dict(
            uuid=str(uuid.UUID(int=rng.getrandbits(128))), name=f"{brand} {city} {i}", brand=brand,
            street="Hauptstr.", house_number=str(i), post_code=f"{plz}{rng.randint(100, 999)}", city=city,
            latitude=round(lat + rng.gauss(0, 0.03), 6), longitude=round(lon + rng.gauss(0, 0.04), 6),
            first_active="1970-01-01 01:00:00+01", openingtimes_json="{}",
            offset={f: rng.gauss(0, 0.02) for f in ("diesel", "e5", "e10")},
            lag=0 if brand in LEADERS else rng.randint(4, 40)))
    for d in (start, start + dt.timedelta(days - 1)):
        with gzip.open(out / "stations" / f"{d}.csv.gz", "wt", newline="") as f:
            w = csv.writer(f)
            w.writerow(["uuid", "name", "brand", "street", "house_number", "post_code", "city",
                        "latitude", "longitude", "first_active", "openingtimes_json"])
            for s in stations:
                w.writerow([s[k] for k in ("uuid", "name", "brand", "street", "house_number", "post_code",
                                           "city", "latitude", "longitude", "first_active", "openingtimes_json")])

    base = {"diesel": 1.62, "e5": 1.74, "e10": 1.68}
    last = {}
    for k in range(days):
        day = start + dt.timedelta(k)
        for f in base:
            base[f] += rng.gauss(0, 0.008)
        rows = []
        for s in stations:
            for h in range(24):
                minute = (5 + s["lag"] + rng.randint(0, 3)) % 60
                ts = dt.datetime.combine(day, dt.time(h, minute, rng.randint(0, 59)))
                prices = {f: round(base[f] + CURVE[h] + s["offset"][f] + rng.gauss(0, 0.004), 2) - 0.001
                          for f in base}
                prev = last.get(s["uuid"])
                changes = {f: int(prev is None or abs(prev[f] - prices[f]) > 1e-9) for f in base}
                if any(changes.values()):
                    rows.append((ts, s["uuid"], prices, changes))
                    last[s["uuid"]] = prices
        rows.sort(key=lambda r: r[0])
        with gzip.open(out / "prices" / f"{day}.csv.gz", "wt", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["date", "station_uuid", "diesel", "e5", "e10", "dieselchange", "e5change", "e10change"])
            for ts, su, p, c in rows:
                w.writerow([tz(ts), su, f"{p['diesel']:.3f}", f"{p['e5']:.3f}", f"{p['e10']:.3f}",
                            c["diesel"], c["e5"], c["e10"]])
    print(f"sample written: {days} days, {n} stations -> {out}")


if __name__ == "__main__":
    main()
