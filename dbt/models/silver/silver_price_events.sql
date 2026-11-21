{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='file_date') }}
-- The data-quality gate. Every fuel price event from bronze lands here exactly once with a
-- dq_status: accepted, flagged (kept but not used by gold) or quarantined (with reason codes).
-- Nothing is dropped silently; tests/assert_bronze_reconciles.sql checks the row count.
with src as (
    select * from {{ ref('stg_prices') }}
    {% if var('month') %}
    where file_date >= date '{{ var("month") }}'
      and file_date <  date '{{ var("month") }}' + interval 1 month
    {% endif %}
),

unpivoted as (
    select file_date, source_file, source_row, ts_raw, ts, station_uuid,
           fuel, price, change_flag
    from src
    unpivot include nulls ((price, change_flag) for fuel in (
        (diesel, dieselchange) as 'diesel',
        (e5, e5change)         as 'e5',
        (e10, e10change)       as 'e10'))
),

-- change_flag 0 = unchanged, 2 = fuel removed: not price events. 1 = changed, 3 = new.
-- A NULL flag is a malformed row and goes to the gate, not to the bin.
events as (
    select *,
           (ts at time zone 'Europe/Berlin')                       as ts_local,
           source_file || ':' || source_row || ':' || fuel         as event_id
    from unpivoted
    where change_flag is null or change_flag in (1, 3)
),

hard_rules as (
    select e.*,
        list_filter([
            case when e.ts is null or e.price is null or e.change_flag is null
                 or e.station_uuid is null then 'unparseable' end,
            case when e.price <= 0 then 'nonpositive_price' end,
            case when e.price > 0 and (e.price < {{ var('price_min') }} or e.price > {{ var('price_max') }})
                 then 'price_out_of_range' end,
            case when e.station_uuid is not null and s.station_uuid is null then 'unknown_station' end,
            case when e.ts is not null and cast(e.ts_local as date) <> e.file_date
                 then 'timestamp_outside_file_day' end,
            case when row_number() over (
                    partition by e.station_uuid, e.fuel, e.ts, e.price order by e.source_row) > 1
                 then 'duplicate' end
        ], lambda x: x is not null) as hard_reasons,
        s.plz_region,
        s.station_type
    from events e
    left join {{ ref('stg_stations') }} s using (station_uuid)
),

-- Soft rules only look at rows that passed the hard rules.
clean as (
    select *,
        lag(price)     over w as prev_price,
        lead(price)    over w as next_price,
        lead(ts_local) over w as next_ts
    from hard_rules
    where len(hard_reasons) = 0
    window w as (partition by station_uuid, fuel order by ts_local, source_row)
),

-- Peer group = same day, fuel, postcode region AND station type.
-- Two lessons from the first real runs:
--  * motorway stations are expensive by design; comparing them with ordinary stations made
--    81% of all flags motorway stations. Peers now share the station type.
--  * grouping by hour as well left many events with one or two peers, so the median was the
--    event itself and injected outliers went unseen (recall 69%). Peers are now the whole day,
--    and with fewer than outlier_min_peers the rule abstains instead of guessing.
regional as (
    select file_date, fuel, plz_region, station_type,
           count(distinct station_uuid)       as n_peers,
           median(price)                      as reg_median,
           median(abs(price - median_price))  as reg_mad
    from (select *, median(price) over (partition by file_date, fuel, plz_region, station_type) as median_price
          from clean)
    group by all
),

soft_rules as (
    select c.event_id,
        list_filter([
            case when c.prev_price is not null and c.next_price is not null
                  and abs(c.price / c.prev_price - 1) > {{ var('spike_pct') }}
                  and abs(c.next_price / c.prev_price - 1) < 0.01
                  and c.next_ts - c.ts_local <= interval {{ var('spike_revert_minutes') }} minute
                 then 'spike_reversal' end,
            case when r.reg_median is not null and r.n_peers >= {{ var('outlier_min_peers') }}
                  and abs(c.price - r.reg_median) > greatest({{ var('outlier_abs') }}, {{ var('outlier_mad') }} * r.reg_mad)
                 then 'regional_outlier' end
        ], lambda x: x is not null) as soft_reasons
    from clean c
    left join regional r
      on r.file_date = c.file_date and r.fuel = c.fuel
     and r.plz_region is not distinct from c.plz_region
     and r.station_type is not distinct from c.station_type
)

select
    h.event_id,
    h.file_date,
    h.source_file,
    h.source_row,
    h.ts_raw,
    h.ts_local,
    h.station_uuid,
    h.fuel,
    h.price,
    h.change_flag,
    case when len(h.hard_reasons) > 0 then 'quarantined'
         when len(coalesce(s.soft_reasons, [])) > 0 then 'flagged'
         else 'accepted' end                                   as dq_status,
    list_concat(h.hard_reasons, coalesce(s.soft_reasons, [])) as dq_reasons
from hard_rules h
left join soft_rules s using (event_id)
