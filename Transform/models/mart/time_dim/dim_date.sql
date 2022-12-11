{# 
    In this source, we will reference a time as its own dimension table
 #}
with source_table as(
    select * from {{ source('staging_spotify', 'stg_spotify_project') }}
 )
 , time_dimension_table as(
    select distinct {{ dbt_utils.surrogate_key(['date_of_the_day', 'time_of_the_day']) }} as time_id,
    date_of_the_day,
    time_of_the_day,
    date_trunc('hour',time_of_the_day) as hour,
    extract(month from date_of_the_day) as month,
    extract(quarter from date_of_the_day) as quarter,
    extract(year from date_of_the_day) as year,
    extract(week from date_of_the_day) as week
    from source_table
 )
, final as(
    select * from time_dimension_table
)
select * from final