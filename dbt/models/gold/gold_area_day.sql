{{ config(materialized='table') }}
-- Daily price per area and fuel: the price in force at 12:00 at every station (ASOF join),
-- rolled up to country, 2-digit postcode region (French departement / German PLZ zone) and
-- larger cities. Daily grain fits markets where stations change price about once a day.
with ev as (
    {{ usable_events() }}
),
bounds as (
    select date_trunc('day', min(ts_local)) as t0, date_trunc('day', max(ts_local)) as t1 from ev
),
days as (
    select unnest(generate_series(t0, t1, interval 1 day)) + interval 12 hour as noon from bounds
),
station_fuels as (select distinct station_uuid, fuel from ev),
priced as (
    select g.station_uuid, g.fuel, g.noon, e.price
    from (select * from station_fuels cross join days) g
    asof join ev e
      on g.station_uuid = e.station_uuid and g.fuel = e.fuel and e.ts_local <= g.noon
    where g.noon - e.ts_local <= interval {{ var('stale_days') }} day
),
big_cities as (
    select city from {{ ref('stg_stations') }}
    where city is not null group by city having count(*) >= {{ var('city_min_stations') }}
),
labelled as (
    select p.*, s.plz_region, case when b.city is not null then s.city end as city
    from priced p
    join {{ ref('stg_stations') }} s using (station_uuid)
    left join big_cities b on b.city = s.city
)
select cast(noon as date) as day,
       case when grouping(plz_region) = 0 then 'region'
            when grouping(city) = 0 then 'city' else 'country' end as level,
       coalesce(plz_region, city, 'ALL')                            as area,
       fuel,
       count(*)                                                     as n_stations,
       round(avg(price), 4)                                         as price_mean,
       round(quantile_cont(price, 0.1), 4)                          as price_p10,
       round(median(price), 4)                                      as price_p50,
       round(quantile_cont(price, 0.9), 4)                          as price_p90
from labelled
group by grouping sets ((noon, fuel), (noon, fuel, plz_region), (noon, fuel, city))
having not (grouping(city) = 0 and city is null)
   and not (grouping(plz_region) = 0 and plz_region is null)
