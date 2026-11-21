-- When in the day do stations change prices, and in which direction?
-- Built from the events themselves, so it is cheap at any scale.
with moves as (
    select fuel, ts_local, price,
           price - lag(price) over (partition by station_uuid, fuel order by ts_local) as delta
    from ({{ usable_events() }}) e
)
select fuel,
       hour(ts_local)                              as hour,
       count(*)                                    as changes,
       round(count(*) / sum(count(*)) over (partition by fuel), 4) as share_of_changes,
       round(avg((delta > 0)::int), 3)             as share_up,
       round(avg(delta) * 100, 2)                  as avg_change_ct,
       round(avg(abs(delta)) * 100, 2)             as avg_abs_change_ct
from moves
where delta is not null and delta <> 0
group by fuel, hour(ts_local)
order by fuel, hour
