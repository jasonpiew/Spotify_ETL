{# 
    we can references view from staging as a table too
    , in this file, we define mart_source that locates the stg_spotify_project source
    in the database
 #}

with filter_column_and_create_unique_key_albums as(
select
distinct album_name as album_name,
rank()over(order by album_name) as album_id --unique key
from {{ source('staging_spotify', 'stg_spotify_project') }}
)
,album_table as(
    select * from filter_column_and_create_unique_key_albums
)
,artist_table as(
    select * from {{ ref('dim_artists') }}
)
,source_table as(
     select * from {{ source('staging_spotify', 'stg_spotify_project') }}
)
,join_artist_id as(
    select distinct {{ dbt_utils.surrogate_key(['artist_table.artist_id', 'album_table.album_name']) }} as album_id_key,
    album_table.album_name,
    album_table.album_id,
    artist_table.artist_id
    from album_table
    left join source_table
    on album_table.album_name = source_table.album_name
    left join artist_table
    on artist_table.artist_name = source_table.artist_name 
)
,final as (
    select * from join_artist_id
)
select * from final