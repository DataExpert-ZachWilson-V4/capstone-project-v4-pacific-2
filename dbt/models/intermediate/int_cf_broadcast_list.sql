{{ config (
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['broadcast_timestamp']
) }}
WITH
  audience as (
    SELECT
      broadcast_timestamp,
      broadcast_audience
    FROM
      {{ source ('userbase_staging', 'int_cf_broadcast_audience') }}
  ),
  broadcast_list AS (
    SELECT
      broadcast_timestamp,
      broadcast_message,
      total_recipients,
      total_opened,
      total_clicked,
      failed_to_deliver
    FROM
      {{ source ('userbase_staging', 'stg_cf_bcast_list_incremental') }}
  ),
  combined AS (
    SELECT
      bl.broadcast_timestamp,
      bl.broadcast_message,
      bl.total_recipients,
      bl.total_opened,
      bl.total_clicked,
      bl.failed_to_deliver,
      a.broadcast_audience
    FROM
      broadcast_list bl
      LEFT JOIN audience a ON bl.broadcast_timestamp = a.broadcast_timestamp
  ),
  existing_data AS (
    SELECT
      *
    FROM
      {{ this }}
  )
SELECT
  c.*
FROM
  combined c
  LEFT JOIN existing_data ed ON c.broadcast_timestamp = ed.broadcast_timestamp 
  
{% if is_incremental () %}
WHERE ed.broadcast_timestamp IS NULL
{% endif %}