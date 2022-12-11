{# 
    in this file,
    we will reference the base staging stg_spotify_project to get the
    source
 #}
with filter_column_and_create_unique_key_tracks as(
select
distinct track_url as track_url,
track_name as track_name
from {{ source('staging_spotify', 'stg_spotify_project') }}
)
,tracks_table as(
    select * from filter_column_and_create_unique_key_tracks
 )
, source_table as(
    select * from {{ source('staging_spotify', 'stg_spotify_project') }}
)
, album_table as(
    select * from {{ ref('dim_albums') }}
)
select 
distinct {{ dbt_utils.surrogate_key(['tracks_table.track_url', 'album_table.album_id_key']) }} as 
track_album_id_key,
tracks_table.track_url as track_url,
tracks_table.track_name,
album_table.album_id_key
from tracks_table
left join source_table
on tracks_table.track_url = source_table.track_url
left join album_table
on album_table.album_name = source_table.album_name