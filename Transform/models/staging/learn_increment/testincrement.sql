{{
  config(
    materialized='incremental'
      )
}}

select *
from {{ ref('timestamp') }}
where now > (select max(now) from {{this}})
