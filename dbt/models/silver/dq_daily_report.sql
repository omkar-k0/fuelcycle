-- One row per file day: how many events were accepted, flagged and quarantined, and why.
with exploded as (
    select file_date, dq_status, unnest(case when len(dq_reasons) = 0 then ['none'] else dq_reasons end) as reason
    from {{ ref('silver_price_events') }}
)
select file_date, dq_status, reason, count(*) as n_events
from exploded
group by all
order by file_date, dq_status, reason
