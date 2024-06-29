{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['broadcast_id', 'broadcast_timestamp'],
    post_hook=[
    "TRUNCATE TABLE blended-setup.userbase_staging.cf_user_filters"
    ]
) }}
WITH
    source AS (
        SELECT
            broadcast_id,
            broadcast_timestamp,
            operation,
            valid,
            parameters
        FROM
            {{ source ('userbase_staging', 'cf_user_filters') }}
    ),
    transformed_filters AS (
        SELECT
            s.broadcast_id,
            s.broadcast_timestamp,
            s.operation,
            s.valid,
            s.parameters.name AS parameters_name,
            s.parameters.operation AS parameters_operation,
            s.parameters.type AS parameters_type,
            element
        FROM
            source s
            CROSS JOIN UNNEST(s.parameters.values.list) AS segment_id
    ),
    existing_filters AS (
        SELECT
            broadcast_id,
            broadcast_timestamp
        FROM
            {{ this }}
    )
SELECT
    tf.broadcast_id,
    tf.broadcast_timestamp,
    tf.operation,
    tf.valid,
    tf.parameters_name,
    tf.parameters_operation,
    tf.parameters_type,
    tf.element
FROM
    transformed_filters tf
    LEFT JOIN existing_filters ef ON tf.broadcast_id = ef.broadcast_id
    AND tf.broadcast_timestamp = ef.broadcast_timestamp 
    
{% if is_incremental () %}
WHERE ef.broadcast_id is null 
{% endif %}