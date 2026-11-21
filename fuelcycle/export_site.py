"""Export the small JSON files the demo page reads. Only aggregates leave the warehouse."""
import argparse
import json
import pathlib

import duckdb
import pandas as pd


def rows(con, sql: str) -> list[dict]:
    try:
        df = con.execute(sql).df()
    except duckdb.CatalogException:  # mart not built for this market (e.g. hourly marts for France)
        return []
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = df[c].dt.strftime("%Y-%m-%dT%H:%M")
    return json.loads(df.to_json(orient="records", date_format="iso"))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/warehouse.duckdb")
    p.add_argument("--results", default="results")
    p.add_argument("--out", default="site/data")
    p.add_argument("--label", default="real", help="'real' or 'synthetic sample' (shown on the page)")
    a = p.parse_args()
    con = duckdb.connect(a.db, read_only=True)
    res = pathlib.Path(a.results)
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    def load(name):
        f = res / name
        return json.loads(f.read_text()) if f.exists() else None

    data = {
        "label": a.label,
        "overview": rows(con, """
            select min(file_date) as first_day, max(file_date) as last_day, count(*) as events,
                   count(distinct station_uuid) as stations,
                   count_if(dq_status = 'accepted') as accepted, count_if(dq_status = 'flagged') as flagged,
                   count_if(dq_status = 'quarantined') as quarantined
            from silver_price_events""")[0],
        "bronze_rows": con.execute("select count(*) from stg_prices").fetchone()[0],
        "dq_reasons": rows(con, """
            select dq_status, reason, sum(n_events) as n from dq_daily_report
            where reason <> 'none' group by all order by n desc"""),
        "daily_national": rows(con, """
            select day, fuel, price_mean as price, price_p10, price_p90, n_stations
            from gold_area_day where level = 'country' order by day, fuel"""),
        "cities_latest": rows(con, """
            with last as (select max(day) d from gold_area_day)
            select area as city, fuel, price_mean as price, n_stations
            from gold_area_day, last where level = 'city' and day = last.d and n_stations >= 10
            order by fuel, price"""),
        "change_timing": rows(con, "select * from gold_change_timing order by fuel, hour"),
        "cycle": rows(con, """
            select fuel, hour, round(avg(cents_vs_day_avg), 2) as cents
            from gold_daily_cycle group by all order by fuel, hour"""),
        "cycle_by_month": rows(con, """
            select month, fuel, hour, round(avg(cents_vs_day_avg), 2) as cents
            from gold_daily_cycle group by all order by month, fuel, hour"""),
        "cheapest": rows(con, """
            select area, fuel, cheapest_hour, cents_saved_vs_day_avg, confidence, decision, min_stations
            from gold_cheapest_hour where level in ('city', 'country') order by area, fuel"""),
        "leader_by_brand": rows(con, """
            select coalesce(nullif(trim(brand), ''),
                            case station_type when 'A' then 'Motorway stations' when 'R' then 'Road stations' end,
                            'unbranded') as brand, count(*) as stations,
                   count_if(role = 'leader') as leaders, count_if(role = 'follower') as followers,
                   count_if(role = 'mixed') as mixed,
                   round(median(lead_share), 3) as median_lead_share,
                   round(median(median_neighbour_lag_min), 1) as median_follow_lag_min
            from gold_leader_follower where role in ('leader', 'follower', 'mixed')
            group by 1 having count(*) >= 20 order by stations desc limit 15"""),
        "dq_eval": load("dq_eval.json"),
        "forecast_eval": load("forecast_eval.json"),
        "timings": load("pipeline_timings.json"),
    }
    bt = res / "forecast_eval.backtest.parquet"
    if bt.exists():
        data["forecast_example"] = rows(con, f"""
            select day + interval 1 day as day, price as today, price_next as actual, p10, p50, p90, decision
            from '{bt.as_posix()}' where series = 'country|ALL|e10' order by day""")
    (out / "fuelcycle.json").write_text(json.dumps(data, default=str))
    print(f"wrote {out / 'fuelcycle.json'} ({(out / 'fuelcycle.json').stat().st_size / 1e3:.0f} kB)")


if __name__ == "__main__":
    main()
