{{ config(enabled=var('hourly_marts')) }}
-- The intraday price cycle: how far each hour sits above or below that day's average,
-- nationwide, per fuel, by weekday and hour, per month (so you can see if the pattern is stable).
with base as (
    select hour_ts, day, fuel, price_mean,
           price_mean - avg(price_mean) over (partition by fuel, day) as dev
    from {{ ref('gold_area_hour') }}
    where level = 'country'
)
select date_trunc('month', day)::date as month,
       fuel,
       isodow(day)                    as weekday,
       hour(hour_ts)                  as hour,
       count(*)                       as n_days,
       round(avg(dev) * 100, 2)       as cents_vs_day_avg,
       round(stddev_samp(dev) * 100, 2) as cents_sd
from base
group by all
