{{config (
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['broadcast_id']
) }}
WITH
  segment AS (
    SELECT
      broadcast_id,
      broadcast_timestamp,
      parameters_name,
      parameters_operation,
      CASE
        WHEN parameters_type = 'segment' THEN element
      END AS segment_id,
      CASE
        WHEN parameters_type = 'custom'
        OR parameters_type = 'sequence' THEN element
      END AS value
    FROM
      {{ source ('userbase_staging', 'stg_cf_user_filters_incremental') }}
  ),
  segment_list AS (
    SELECT *
    FROM
      {{ source ('userbase_staging', 'stg_cf_segment_list_incremental') }}
  ),
  combined AS (
    SELECT
      s.broadcast_id,
      s.broadcast_timestamp,
      COALESCE(s.parameters_name, 'segment') AS parameters_name,
      s.parameters_operation,
      COALESCE(s.value, sl.segment_name) AS value,
    FROM
      segment s
      LEFT JOIN segment_list sl ON s.segment_id = sl.segment_id
  ),
  transformed AS (
    SELECT
      broadcast_id,
      broadcast_timestamp,
      STRING_AGG(CONCAT(parameters_name," ",parameters_operation," ",value," ")) AS broadcast_audience
    FROM
      combined
    GROUP BY
      broadcast_id,
      broadcast_timestamp
  ),
  existing_data AS (
    SELECT
      broadcast_id,
      broadcast_timestamp,
      broadcast_audience
    FROM
      {{ this }}
  )
SELECT
  t.broadcast_id,
  t.broadcast_timestamp,
  t.broadcast_audience
FROM
  transformed t
  LEFT JOIN existing_data e ON t.broadcast_id = e.broadcast_id 
  
{% if is_incremental () %}
WHERE e.broadcast_id is null 
{% endif %}