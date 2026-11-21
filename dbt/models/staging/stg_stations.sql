-- Latest known master data per station, with a cleaned city name and the 2-digit postcode region.
with ranked as (
    select *,
           row_number() over (partition by station_uuid order by snapshot_date desc) as rn
    from read_parquet('{{ var("bronze_path") }}/stations/*/*.parquet', hive_partitioning = true, union_by_name = true)
    where station_uuid is not null
)
select
    station_uuid,
    name,
    brand,
    nullif(trim(post_code), '')                                   as post_code,
    left(trim(post_code), 2)                                      as plz_region,
    -- upper-case, drop anything after a comma or bracket: "Essen (Ruhr)" -> "ESSEN",
    -- hyphenated names survive ("SAINT-DENIS-LES-BOURG", "Essen-Kray" stays its own place)
    nullif(upper(trim(regexp_replace(trim(city), '\s*[,(].*$', ''))), '') as city,
    station_type,
    latitude,
    longitude
from ranked
where rn = 1
