{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='day',
          enabled=var('hourly_marts')) }}
-- Nationwide hourly price: the price in force at the middle of every hour, per station and fuel
-- (ASOF join), averaged over all stations. Feeds the intraday-cycle mart. One month per run;
-- Area-level prices are daily (gold_area_day): an hourly grid for every city and region needed
-- ~2 GB of spill per month on a 3 GB machine for no extra information where prices move daily.
with bounds as (
    select {{ run_start() }} as t0, {{ run_end() }} as t1
),
ev as (
    {{ usable_events() }}
      and ts_local >= (select t0 from bounds) - interval {{ var('stale_days') }} day
      and ts_local <  (select t1 from bounds)
),
hours as (
    select unnest(generate_series(t0, t1 - interval 1 hour, interval 1 hour)) + interval 30 minute as hour_mid
    from bounds
),
station_fuels as (select distinct station_uuid, fuel from ev),
grid as (select * from station_fuels cross join hours),
priced as (
    select g.station_uuid, g.fuel, g.hour_mid, e.price
    from grid g
    asof join ev e
      on g.station_uuid = e.station_uuid and g.fuel = e.fuel and e.ts_local <= g.hour_mid
    where g.hour_mid - e.ts_local <= interval {{ var('stale_days') }} day
),
rolled as (
    select fuel, hour_mid, 'country' as level, 'ALL' as area,
           count(*) as n_stations, avg(price) as price_mean,
           quantile_cont(price, 0.1) as price_p10, median(price) as price_p50,
           quantile_cont(price, 0.9) as price_p90
    from priced
    group by fuel, hour_mid
)
select cast(hour_mid - interval 30 minute as timestamp) as hour_ts,
       cast(hour_mid as date)                          as day,
       level, area, fuel, n_stations,
       round(price_mean, 4) as price_mean, round(price_p10, 4) as price_p10,
       round(price_p50, 4) as price_p50, round(price_p90, 4) as price_p90
from rolled
