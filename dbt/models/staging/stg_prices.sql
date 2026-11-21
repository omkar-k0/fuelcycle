-- One row per source row, straight from bronze. No filtering here.
select
    file_date,
    source_file,
    source_row,
    ts_raw,
    ts,
    station_uuid,
    diesel, e5, e10,
    dieselchange, e5change, e10change
from read_parquet('{{ var("bronze_path") }}/prices/*/*.parquet', hive_partitioning = true, union_by_name = true)
