{# 
    contains an information about how the tracks started and ended
 #}
with source_table as(
    select 
    reason_start as tracks_config
    from {{ source('staging_spotify', 'stg_spotify_project') }}
    union all
    select 
    reason_end
    from {{ source('staging_spotify', 'stg_spotify_project') }}
)
,final as(
    select distinct tracks_config,
    rank()over(order by tracks_config) as config_id
    from source_table
)
select * from final