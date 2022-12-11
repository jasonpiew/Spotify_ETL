{{
  config(
    materialized='incremental'
      )
}}

select *
from {{ ref('testdotdot') }}
{% if is_incremental() %}

where id > (select max(id) from {{this}})
{% endif %}


{# {{
    config(
        materialized='incremental'
    )
}} #}
{# 
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where event_time > (select max(event_time) from {{ this }})

{% endif %} #}