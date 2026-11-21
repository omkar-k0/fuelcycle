{# Time window the current run covers. With --vars month the run covers that month only. #}
{% macro run_start() -%}
  {%- if var('month') -%} timestamp '{{ var("month") }}'
  {%- else -%} (select date_trunc('day', min(ts_local)) from {{ ref('silver_price_events') }}) {%- endif -%}
{%- endmacro %}
{% macro run_end() -%}
  {#- never past the last day with data, or the ASOF join would carry stale prices forward -#}
  {%- set data_end -%}(select date_trunc('day', max(ts_local)) + interval 1 day from {{ ref('silver_price_events') }}){%- endset -%}
  {%- if var('month') -%} least(timestamp '{{ var("month") }}' + interval 1 month, {{ data_end }})
  {%- else -%} {{ data_end }} {%- endif -%}
{%- endmacro %}
{% macro usable_events() -%}
  select station_uuid, fuel, ts_local, price
  from {{ ref('silver_price_events') }}
  where dq_status = 'accepted' {% if var('gold_include_flagged') %} or dq_status = 'flagged' {% endif %}
{%- endmacro %}
