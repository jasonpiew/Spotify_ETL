{{config(primary_key='platform_id')}}


with source_table as(
    select 
    distinct device_platform,
    dense_rank()over(order by device_platform) as platform_id
     from {{ source('staging_spotify', 'stg_spotify_project') }}
)
,final as(
    select * from source_table
)
select * from final