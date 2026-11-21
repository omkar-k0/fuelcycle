{{ config(enabled=var('hourly_marts')) }}
-- "When in the day is it cheapest to fill up?" nationwide, per fuel, over the last N days of data.
-- The mart only answers when the evidence is strong enough; otherwise it says so.
with recent as (
    select * from {{ ref('gold_area_hour') }}
    where day > (select max(day) from {{ ref('gold_area_hour') }}) - {{ var('cheapest_window_days') }}
),
daily as (
    select *,
           price_mean - min(price_mean) over (partition by level, area, fuel, day) as above_min,
           price_mean - avg(price_mean) over (partition by level, area, fuel, day) as vs_avg
    from recent
),
by_hour as (
    select level, area, fuel, hour(hour_ts) as hour,
           count(distinct day)            as n_days,
           min(n_stations)                as min_stations,
           avg(vs_avg)                    as avg_vs_day_avg,
           avg((above_min <= 0.01)::int)  as share_days_within_1ct_of_min
    from daily
    group by all
),
best as (
    select *, row_number() over (partition by level, area, fuel order by avg_vs_day_avg) as rk
    from by_hour
)
select level, area, fuel,
       hour                                       as cheapest_hour,
       n_days, min_stations,
       round(-avg_vs_day_avg * 100, 1)            as cents_saved_vs_day_avg,
       round(share_days_within_1ct_of_min, 2)     as confidence,
       case when n_days < {{ var('cheapest_min_days') }} then 'not enough days'
            when min_stations < {{ var('cheapest_min_stations') }} then 'not enough stations'
            when share_days_within_1ct_of_min < {{ var('cheapest_min_confidence') }} then 'pattern not stable'
            else 'answer' end                     as decision
from best
where rk = 1
