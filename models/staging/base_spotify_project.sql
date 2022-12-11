{# getting data from the source
this column will be used for all of the interfaces sources #}

with getting_sources_spotify as(
    select *,
    {{ dbt_utils.surrogate_key(['index', 'ts']) }} as pr_keys
    from {{ source('raw_spotify', 'spotify_project') }}
)
,final as(
    select *
    from getting_sources_spotify
)

select *
from final