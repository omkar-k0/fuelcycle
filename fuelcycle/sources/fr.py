"""French government fuel price archive -> fuelcycle's canonical raw format.

Source: prix-carburants.gouv.fr open data, yearly archives at
https://donnees.roulez-eco.fr/opendata/annee/<YYYY> (Licence Ouverte / Open Licence 2.0).
Each archive holds one XML file: <pdv> per station, one <prix nom=".." maj=".." valeur=".."/>
per price record.

Canonical raw format = the Tankerkoenig daily CSV layout (date, station_uuid, diesel, e5, e10,
dieselchange, e5change, e10change), so bronze, silver, gold and the evals don't care where the
data came from. Each French price record becomes one row with one fuel changed.

Fuel mapping: Gazole -> diesel, SP95 -> e5, E10 -> e10. SP98, E85 and GPLc are counted
and left out (there is no matching column).

Usage: python -m fuelcycle.sources.fr data/fr/2025.zip data/fr/2026.zip --out data/raw \
           --start 2025-11-01 --end 2026-10-31
"""
import argparse
import json
import pathlib
import shutil
import tempfile
import xml.etree.ElementTree as ET
import zipfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

FUEL_MAP = {"Gazole": "diesel", "SP95": "e5", "E10": "e10"}
BATCH = 500_000


def _price(v: str | None) -> float | None:
    if v is None or v == "":
        return None
    try:
        x = float(v.replace(",", "."))
    except ValueError:
        return None
    return x / 1000 if x > 100 else x  # older archives store millièmes (1799 = 1.799 EUR)


def parse(zips: list[pathlib.Path], tmp: pathlib.Path) -> dict:
    """Stream the XML files into Parquet parts (events) and one stations table."""
    events, stations, stats = [], {}, {"records": 0, "kept": 0, "dropped_fuels": {}}
    part = 0

    def flush():
        nonlocal part, events
        if events:
            cols = list(zip(*events))
            pq.write_table(pa.table({"station_uuid": cols[0], "fuel": cols[1], "maj": cols[2], "price_raw": cols[3],
                                     "price": cols[4]}), tmp / f"events_{part:04d}.parquet")
            part += 1
            events = []

    for z in zips:
        with zipfile.ZipFile(z) as zf:
            name = next(n for n in zf.namelist() if n.lower().endswith(".xml"))
            with zf.open(name) as fh:
                for _, el in ET.iterparse(fh, events=("end",)):
                    if el.tag != "pdv":
                        continue
                    sid = el.get("id")
                    lat, lon = el.get("latitude"), el.get("longitude")
                    stations[sid] = {
                        "uuid": sid, "post_code": el.get("cp"), "city": (el.findtext("ville") or "").strip(),
                        "street": (el.findtext("adresse") or "").strip(), "station_type": el.get("pop"),
                        "latitude": float(lat) / 100000 if lat not in (None, "") else None,
                        "longitude": float(lon) / 100000 if lon not in (None, "") else None,
                    }
                    for p in el.iter("prix"):
                        stats["records"] += 1
                        fuel = FUEL_MAP.get(p.get("nom"))
                        if fuel is None:
                            k = p.get("nom") or "?"
                            stats["dropped_fuels"][k] = stats["dropped_fuels"].get(k, 0) + 1
                            continue
                        raw = p.get("valeur")
                        events.append((sid, fuel, p.get("maj"), raw, _price(raw)))
                        stats["kept"] += 1
                        if len(events) >= BATCH:
                            flush()
                    el.clear()
    flush()
    pq.write_table(pa.Table.from_pylist(list(stations.values())), tmp / "stations.parquet")
    stats["stations"] = len(stations)
    return stats


def write_raw(tmp: pathlib.Path, out: pathlib.Path, start: str, end: str) -> int:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'Europe/Paris'")
    stage = tmp / "by_day"
    # maj is local French time without offset ("2025-11-03T07:53:00" or with a space).
    # We attach the Europe/Paris offset so the timestamp is unambiguous downstream.
    con.execute(f"""
        COPY (
          WITH e AS (
            SELECT station_uuid, fuel, maj, price_raw, price,
                   TRY_CAST(replace(maj, 'T', ' ') AS TIMESTAMP) AS ts_local
            FROM read_parquet('{tmp.as_posix()}/events_*.parquet')
          )
          SELECT
            coalesce(cast(cast(ts_local AS DATE) AS VARCHAR), 'unparsed')     AS file_date,
            coalesce(strftime(timezone('Europe/Paris', ts_local), '%Y-%m-%d %H:%M:%S%z'), maj) AS "date",
            station_uuid,
            CASE WHEN fuel = 'diesel' THEN coalesce(cast(price AS VARCHAR), price_raw) END AS diesel,
            CASE WHEN fuel = 'e5'     THEN coalesce(cast(price AS VARCHAR), price_raw) END AS e5,
            CASE WHEN fuel = 'e10'    THEN coalesce(cast(price AS VARCHAR), price_raw) END AS e10,
            (fuel = 'diesel')::INT AS dieselchange,
            (fuel = 'e5')::INT     AS e5change,
            (fuel = 'e10')::INT    AS e10change
          FROM e
          WHERE ts_local IS NULL OR cast(ts_local AS DATE) BETWEEN DATE '{start}' AND DATE '{end}'
          ORDER BY file_date, "date"
        ) TO '{stage.as_posix()}' (FORMAT csv, HEADER, PARTITION_BY (file_date), COMPRESSION gzip)
    """)
    (out / "prices").mkdir(parents=True, exist_ok=True)
    (out / "stations").mkdir(parents=True, exist_ok=True)
    n = 0
    for d in sorted(stage.glob("file_date=*")):
        day = d.name.split("=", 1)[1]
        # rows whose timestamp could not be read are kept; the DQ gate quarantines them
        dst = out / "prices" / (f"{start}.unparsed.csv.gz" if day == "unparsed" else f"{day}.csv.gz")
        files = sorted(d.glob("*.csv.gz"))
        if len(files) == 1:
            shutil.move(files[0], dst)
        else:
            src = "[" + ",".join(f"'{f.as_posix()}'" for f in files) + "]"
            con.execute(f"COPY (SELECT * FROM read_csv({src}, header=true, all_varchar=true)) "
                        f"TO '{dst.as_posix()}' (FORMAT csv, HEADER, COMPRESSION gzip)")
        n += 1
    con.execute(f"""
        COPY (SELECT uuid, NULL AS name, NULL AS brand, street, NULL AS house_number, post_code, city,
                     latitude, longitude, NULL AS first_active, NULL AS openingtimes_json, station_type
              FROM read_parquet('{(tmp / "stations.parquet").as_posix()}'))
        TO '{(out / "stations" / f"{end}.csv.gz").as_posix()}' (FORMAT csv, HEADER, COMPRESSION gzip)
    """)
    return n


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("zips", nargs="+", type=pathlib.Path)
    p.add_argument("--out", type=pathlib.Path, default=pathlib.Path("data/raw"))
    p.add_argument("--start", default="2025-11-01")
    p.add_argument("--end", default="2026-10-31")
    a = p.parse_args()
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="fuelcycle_fr_"))
    try:
        stats = parse(a.zips, tmp)
        stats["days_written"] = write_raw(tmp, a.out, a.start, a.end)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    (a.out / "_source_fr.json").write_text(json.dumps(stats, indent=1))
    print(json.dumps(stats, indent=1))


if __name__ == "__main__":
    main()
