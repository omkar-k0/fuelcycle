{{ config(materialized='view') }}
select * from {{ ref('silver_price_events') }} where dq_status = 'quarantined'
