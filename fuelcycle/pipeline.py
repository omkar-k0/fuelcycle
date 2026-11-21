"""Run the whole pipeline: ingest, then dbt month by month (keeps memory flat), then the rest.

python -m fuelcycle.pipeline --raw data/raw
"""
import argparse
import datetime as dt
import json
import os
import pathlib
import subprocess
import time

from fuelcycle import ingest

ROOT = pathlib.Path(__file__).resolve().parent.parent


def dbt(args: list[str], env: dict) -> None:
    subprocess.run(["dbt", *args, "--profiles-dir", "."], cwd=ROOT / "dbt", env=env, check=True)


def months(raw: pathlib.Path) -> list[str]:
    days = sorted(f.name[:10] for f in (raw / "prices").glob("*.csv.gz"))
    first, last = dt.date.fromisoformat(days[0]).replace(day=1), dt.date.fromisoformat(days[-1])
    out, m = [], first
    while m <= last:
        out.append(m.isoformat())
        m = (m.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--bronze", default="data/bronze")
    p.add_argument("--db", default="data/warehouse.duckdb")
    p.add_argument("--full-refresh", action="store_true")
    p.add_argument("--vars", default="{}", help='extra dbt vars as JSON, e.g. \'{"lf_follow_minutes": 30}\'')
    a = p.parse_args()
    env = {**os.environ, "FUELCYCLE_BRONZE": str(pathlib.Path(a.bronze).resolve()),
           "FUELCYCLE_DB": str(pathlib.Path(a.db).resolve())}
    extra = json.loads(a.vars)
    timings = {}
    t = time.time()
    timings["ingest"] = ingest.ingest(pathlib.Path(a.raw), pathlib.Path(a.bronze))
    timings["ingest_seconds"] = round(time.time() - t, 1)

    t = time.time()
    dbt(["run", "-s", "staging"], env)
    for i, m in enumerate(months(pathlib.Path(a.raw))):
        flags = ["--full-refresh"] if (a.full_refresh and i == 0) else []
        print(f"== month {m}", flush=True)
        dbt(["run", "-s", "silver_price_events", "gold_area_hour", "--vars", json.dumps({**extra, "month": m}), *flags], env)
    timings["incremental_seconds"] = round(time.time() - t, 1)

    t = time.time()
    dbt(["build", "--exclude", "silver_price_events", "gold_area_hour", "--vars", json.dumps(extra)], env)
    dbt(["test", "-s", "silver_price_events", "gold_area_hour", "--vars", json.dumps(extra)], env)
    timings["marts_and_tests_seconds"] = round(time.time() - t, 1)

    out = ROOT / "results" / "pipeline_timings.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(timings, indent=1))
    print(timings)


if __name__ == "__main__":
    main()
