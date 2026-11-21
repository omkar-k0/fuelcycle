"""Score the DQ gate with fault injection.

1. Copy N days of raw data twice: once untouched (clean) and once with injected faults.
2. Run ingest + the dbt silver layer on both.
3. An injected fault counts as caught if its event is flagged or quarantined; we also check
   whether the gate gave the right reason. A false alarm is an event that was accepted in the
   clean run but not in the injected run, without being an injected fault itself.
   Events the gate already rejects in the clean run are real-world anomalies with unknown truth
   and are reported separately (background rate), not scored.
"""
import argparse
import json
import os
import pathlib
import shutil
import subprocess

import duckdb

from fuelcycle import faults, ingest

ROOT = pathlib.Path(__file__).resolve().parent.parent


def run_silver(raw: pathlib.Path, work: pathlib.Path) -> pathlib.Path:
    bronze, db = work / "bronze", work / "eval.duckdb"
    ingest.ingest(raw, bronze)
    env = {**os.environ, "FUELCYCLE_BRONZE": str(bronze.resolve()), "FUELCYCLE_DB": str(db.resolve())}
    for args in (["run", "--full-refresh", "-s", "+silver_price_events"], ["test", "-s", "assert_bronze_reconciles"]):
        subprocess.run(["dbt", *args, "--profiles-dir", ".", "--quiet"], cwd=ROOT / "dbt", env=env, check=True)
    return db


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--work", default="data/eval")
    p.add_argument("--days", type=int, default=14, help="number of days, spread evenly over the data")
    p.add_argument("--rate", type=float, default=0.002, help="share of rows that get a fault")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="results/dq_eval.json")
    a = p.parse_args()

    raw, work = pathlib.Path(a.raw), pathlib.Path(a.work)
    all_days = sorted(f.name[:10] for f in (raw / "prices").glob("*.csv.gz"))
    step = max(1, len(all_days) // a.days)
    days = all_days[::step][: a.days]
    shutil.rmtree(work, ignore_errors=True)

    clean_raw = work / "clean_raw"
    (clean_raw / "prices").mkdir(parents=True)
    shutil.copytree(raw / "stations", clean_raw / "stations")
    for d in days:
        shutil.copy(raw / "prices" / f"{d}.csv.gz", clean_raw / "prices" / f"{d}.csv.gz")
    labels = faults.inject(raw, work / "faulty_raw", days, a.rate, a.seed)

    clean_db = run_silver(clean_raw, work / "clean")
    faulty_db = run_silver(work / "faulty_raw", work / "faulty")

    con = duckdb.connect()
    con.execute(f"attach '{clean_db}' as c (read_only)")
    con.execute(f"attach '{faulty_db}' as f (read_only)")
    con.register("labels_df", __import__("pandas").DataFrame(labels))
    con.execute("create table labels as select * from labels_df")

    # Clean events are matched across runs by their content, because injected rows shift row numbers.
    con.execute("""
        create table joined as
        with fz as (select *, row_number() over (partition by station_uuid, fuel, ts_raw, price order by source_row) k
                    from f.silver_price_events),
             cz as (select *, row_number() over (partition by station_uuid, fuel, ts_raw, price order by source_row) k
                    from c.silver_price_events)
        select fz.event_id, fz.dq_status, fz.dq_reasons, l.fault, cz.dq_status as clean_status
        from fz
        left join labels l using (event_id)
        left join cz on l.fault is null and cz.station_uuid = fz.station_uuid and cz.fuel = fz.fuel
                    and cz.ts_raw = fz.ts_raw and cz.price is not distinct from fz.price and cz.k = fz.k
    """)
    per_fault = con.sql("""
        select fault,
               count(*)                                                as injected,
               count_if(dq_status <> 'accepted')                       as caught,
               round(count_if(dq_status <> 'accepted') / count(*), 4)  as recall,
               round(count_if(list_contains(dq_reasons, fault)) / count(*), 4) as right_reason
        from joined where fault is not null and fault <> 'collateral'
        group by fault order by fault
    """).df()
    totals = con.sql("""
        select count_if(fault is not null and fault <> 'collateral')                               as injected,
               count_if(fault is not null and fault <> 'collateral' and dq_status <> 'accepted')  as caught,
               count_if(fault is null and clean_status = 'accepted' and dq_status <> 'accepted')  as false_alarms,
               count_if(fault is null and clean_status = 'accepted')                               as clean_events,
               count_if(fault is null and clean_status is not null and clean_status <> 'accepted') as background_rejected,
               count_if(fault is null and clean_status is null)                                    as unmatched
        from joined
    """).df().iloc[0].to_dict()
    totals = {k: int(v) for k, v in totals.items()}
    totals["recall"] = round(totals["caught"] / totals["injected"], 4)
    totals["precision"] = round(totals["caught"] / (totals["caught"] + totals["false_alarms"]), 4)
    totals["false_alarm_rate"] = round(totals["false_alarms"] / totals["clean_events"], 6)
    totals["days"] = days
    status_clean = con.sql("select dq_status, count(*) n from c.silver_price_events group by 1").df()
    result = {"totals": totals, "per_fault": per_fault.to_dict(orient="records"),
              "clean_run_status": status_clean.to_dict(orient="records"),
              "settings": {"rate": a.rate, "seed": a.seed}}
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1, default=str))
    print(per_fault.to_string(index=False))
    print({k: v for k, v in totals.items() if k != "days"})


if __name__ == "__main__":
    main()
