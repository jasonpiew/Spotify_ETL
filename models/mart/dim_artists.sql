{# 
    we can references view from staging as a table too
    , in this file, we define mart_source that locates the stg_spotify_project source
    in the database
 #}

with filter_column_and_create_unique_key_artists as(
select 
distinct artist_name as artist_name,
rank()over(order by artist_name) as artist_id --unique key
from {{ source('staging_spotify', 'stg_spotify_project') }}
)

,final as (
    select * from filter_column_and_create_unique_key_artists
)
select * from final