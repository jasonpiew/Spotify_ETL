{# 
    In stg_spotify project
    we retrieve all the column needed from the source
    and giving the naming convention alongside transformation AKA data type change
 #}

 with renaming_column_and_casting_type as (
    select
        username,
        ts::date as date_of_the_day,
        ts::timestamp::time as time_of_the_day,
        index as id,
        split_part(platform,' ',1) AS device_platform,
        ms_played * INTERVAL '1 ms' AS MS,
        conn_country as user_country,
        ip_addr_decrypted as ip_address,
        user_agent_decrypted,
        master_metadata_track_name as track_name,
        master_metadata_album_artist_name as artist_name,
        master_metadata_album_album_name as album_name,
        split_part(spotify_track_uri,':',3) AS track_url,
        reason_start,
        reason_end,
        shuffle,
        skipped,
        offline
    from {{ ref('base_spotify_project') }}
    where ms_played != 0
 )

 ,final as(
    select * from renaming_column_and_casting_type
 )

 select * from final
 where track_name is not null
 and album_name is not null

