"""Raw Tankerkoenig csv.gz -> bronze Parquet, one partition per file date.

Bronze keeps every source row. Values are read as text first and cast with TRY_CAST,
so a malformed row never crashes the load; it reaches silver, where the DQ gate decides.
Re-running is safe: a file whose checksum is already in the manifest is skipped.
"""
import argparse
import hashlib
import json
import pathlib
import time

import duckdb


def sha256(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


PRICES_SQL = """
COPY (
  SELECT
    CAST('{day}' AS DATE)                         AS file_date,
    '{name}'                                      AS source_file,
    row_number() OVER ()                          AS source_row,
    "date"                                        AS ts_raw,
    TRY_CAST("date" AS TIMESTAMPTZ)               AS ts,
    station_uuid,
    TRY_CAST(diesel AS DOUBLE)                    AS diesel,
    TRY_CAST(e5 AS DOUBLE)                        AS e5,
    TRY_CAST(e10 AS DOUBLE)                       AS e10,
    TRY_CAST(dieselchange AS INTEGER)             AS dieselchange,
    TRY_CAST(e5change AS INTEGER)                 AS e5change,
    TRY_CAST(e10change AS INTEGER)                AS e10change
  FROM read_csv('{src}', header=true, all_varchar=true)
) TO '{dst}' (FORMAT parquet, COMPRESSION zstd)
"""

STATIONS_SQL = """
COPY (
  SELECT CAST('{day}' AS DATE) AS snapshot_date, uuid AS station_uuid, name, brand, street,
         house_number, post_code, city,
         TRY_CAST(latitude AS DOUBLE) AS latitude, TRY_CAST(longitude AS DOUBLE) AS longitude,
         {station_type} AS station_type
  FROM read_csv('{src}', header=true, all_varchar=true)
) TO '{dst}' (FORMAT parquet, COMPRESSION zstd)
"""


def ingest(raw: pathlib.Path, bronze: pathlib.Path) -> dict:
    manifest_path = bronze / "_manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    con = duckdb.connect()
    stats = {"loaded": 0, "skipped": 0, "rows": 0}
    t0 = time.time()
    for kind, sql, part in (("prices", PRICES_SQL, "file_date"), ("stations", STATIONS_SQL, "snapshot_date")):
        for src in sorted((raw / kind).glob("*.csv.gz")):
            day = src.name[:10]
            stem = src.name[: -len(".csv.gz")]
            key = f"{kind}/{src.name}"
            digest = sha256(src)
            dst = bronze / kind / f"{part}={day}" / f"{stem}.parquet"
            if manifest.get(key, {}).get("sha256") == digest and dst.exists():
                stats["skipped"] += 1
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            cols = con.execute(f"SELECT * FROM read_csv('{src.as_posix()}', header=true, all_varchar=true) LIMIT 0").description
            cols = [c[0] for c in cols]
            if kind == "prices" and cols[:2] != ["date", "station_uuid"]:
                raise ValueError(f"{src} does not look like a price file (header {cols[:3]}); refusing to load it")
            station_type = "station_type" if "station_type" in cols else "NULL"
            con.execute(sql.format(day=day, name=src.name, src=src.as_posix(), dst=dst.as_posix(),
                                   station_type=station_type))
            rows = con.execute(f"SELECT count(*) FROM '{dst.as_posix()}'").fetchone()[0]
            manifest[key] = {"sha256": digest, "rows": rows, "bronze": dst.relative_to(bronze).as_posix()}
            stats["loaded"] += 1
            stats["rows"] += rows
    manifest_path.write_text(json.dumps(manifest, indent=1, sort_keys=True))
    stats["seconds"] = round(time.time() - t0, 1)
    return stats


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--bronze", default="data/bronze")
    a = p.parse_args()
    print(ingest(pathlib.Path(a.raw), pathlib.Path(a.bronze)))


if __name__ == "__main__":
    main()
