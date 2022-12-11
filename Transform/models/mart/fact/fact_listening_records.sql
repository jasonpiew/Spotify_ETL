with source_table as(
    select * from {{ source('staging_spotify', 'stg_spotify_project') }}
)
,date_table as(
    select * from {{ ref('dim_date') }}
)
,tracks_album_table as(
    select * from {{ ref('dim_tracks_album') }}
)
,track_config_table as(
    select * from {{ ref('dim_tracks_config') }}
)

,define_table as(
select distinct {{ dbt_utils.surrogate_key(['source_table.date_of_the_day', 'source_table.time_of_the_day', 'source_table.id']) }} as listening_records_id,
date_table.time_id,
tracks_album_table.track_album_id_key,
device_platform,
ms,
track_config_table.config_id as reason_start,
track_config_table_reason_end.config_id as reason_end,
shuffle,
skipped
from source_table
join date_table
on (source_table.time_of_the_day = date_table.time_of_the_day
and source_table.date_of_the_day = date_table.date_of_the_day)
join tracks_album_table
on tracks_album_table.track_url = source_table.track_url
join track_config_table
on source_table.reason_start = track_config_table.tracks_config
join track_config_table as track_config_table_reason_end
on source_table.reason_end = track_config_table_reason_end.tracks_config
)
,
remove_duplicate as(
select *,
row_number()over(partition by listening_records_id) as rn
from define_table
)

select listening_records_id,
time_id,
track_album_id_key,
device_platform,
ms,
reason_start,
reason_end,
shuffle,
skipped
from
remove_duplicate
where rn = 1