{{ config(enabled=var('hourly_marts')) }}
-- gold_area_hour must not extend past the last day that has price events.
select max(hour_ts) as last_hour
from {{ ref('gold_area_hour') }}
having max(hour_ts) >= (select date_trunc('day', max(ts_local)) + interval 1 day from {{ ref('silver_price_events') }})
