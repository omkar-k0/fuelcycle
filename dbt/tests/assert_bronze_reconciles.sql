-- Every bronze fuel cell that is a price event (flag 1, 3 or NULL) must appear in silver exactly once.
with bronze as (
    select file_date,
           count_if(dieselchange is null or dieselchange in (1, 3))
         + count_if(e5change is null or e5change in (1, 3))
         + count_if(e10change is null or e10change in (1, 3)) as n
    from {{ ref('stg_prices') }}
    group by 1
),
silver as (
    select file_date, count(*) as n, count(distinct event_id) as n_ids
    from {{ ref('silver_price_events') }}
    group by 1
)
select b.file_date, b.n as bronze_events, s.n as silver_events, s.n_ids
from bronze b
left join silver s using (file_date)
where s.n is null or s.n <> b.n or s.n_ids <> s.n
