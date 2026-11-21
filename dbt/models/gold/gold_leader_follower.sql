-- Who moves first? For neighbouring stations (fuel = lf_fuel) (within lf_radius_km), take each price move of
-- station A and find the next move of neighbour B in the same direction (ASOF join).
-- A station is a "leader" if neighbours mostly follow it, a "follower" if it mostly follows them.
with moves as (
    select station_uuid, fuel, ts_local, price,
           sign(price - lag(price) over (partition by station_uuid, fuel order by ts_local)) as dir,
           lead(ts_local) over (partition by station_uuid, fuel order by ts_local)           as next_ts
    from ({{ usable_events() }}) e
    where fuel = '{{ var("lf_fuel") }}'
      and ts_local > (select max(ts_local) from {{ ref('silver_price_events') }}) - interval {{ var('lf_window_days') }} day
    qualify dir is not null and dir <> 0
),
st as (select station_uuid, brand, station_type, city, latitude, longitude from {{ ref('stg_stations') }} where latitude is not null),
pairs as (
    select a.station_uuid as a_uuid, b.station_uuid as b_uuid
    from st a join st b
      on a.station_uuid <> b.station_uuid
     and abs(a.latitude - b.latitude) < {{ var('lf_radius_km') }} / 111.0
     and 2 * 6371 * asin(sqrt(pow(sin(radians(b.latitude - a.latitude) / 2), 2)
         + cos(radians(a.latitude)) * cos(radians(b.latitude)) * pow(sin(radians(b.longitude - a.longitude) / 2), 2)))
         <= {{ var('lf_radius_km') }}
),
a_moves as (
    select p.a_uuid, p.b_uuid, m.ts_local, m.dir, m.next_ts
    from pairs p join moves m on m.station_uuid = p.a_uuid
),
b_moves as (
    select station_uuid as b_uuid, ts_local as b_ts, dir as b_dir from moves
),
matched as (
    -- next move of B strictly after A's move. It only counts as following if it is in the same
    -- direction, within lf_follow_minutes, and before A moves again.
    select a.a_uuid, a.b_uuid, a.ts_local, a.dir, a.next_ts, b.b_ts, b.b_dir
    from a_moves a
    asof left join b_moves b on a.b_uuid = b.b_uuid and b.b_ts > a.ts_local
),
pair_stats as (
    select a_uuid, b_uuid,
           count(*) as a_moves,
           count_if(b_dir = dir and b_ts - ts_local <= interval {{ var('lf_follow_minutes') }} minute and (next_ts is null or b_ts < next_ts)) as followed,
           median(epoch(b_ts - ts_local) / 60) filter (
               where b_dir = dir and b_ts - ts_local <= interval {{ var('lf_follow_minutes') }} minute and (next_ts is null or b_ts < next_ts)) as median_lag_min
    from matched group by all
),
station_stats as (
    select s.station_uuid,
           sum(l.a_moves)                                    as moves,
           sum(l.followed)                                   as moves_followed_by_neighbours,
           (select sum(f.followed) from pair_stats f where f.b_uuid = s.station_uuid) as moves_following_neighbours,
           count(l.b_uuid)                                   as n_neighbours,
           median(l.median_lag_min)                          as median_neighbour_lag_min
    from st s
    left join pair_stats l on l.a_uuid = s.station_uuid
    group by s.station_uuid
)
select x.station_uuid, st.brand, st.station_type, st.city, x.n_neighbours, x.moves,
       x.moves_followed_by_neighbours, x.moves_following_neighbours,
       round(x.median_neighbour_lag_min, 1) as median_neighbour_lag_min,
       round(x.moves_followed_by_neighbours
             / nullif(x.moves_followed_by_neighbours + x.moves_following_neighbours, 0), 3) as lead_share,
       case when x.n_neighbours = 0 then 'no neighbours'
            when coalesce(x.moves, 0) < {{ var('lf_min_moves') }} then 'not enough moves'
            when x.moves_followed_by_neighbours > 2 * x.moves_following_neighbours then 'leader'
            when x.moves_following_neighbours > 2 * x.moves_followed_by_neighbours then 'follower'
            else 'mixed' end as role
from station_stats x
join st using (station_uuid)
