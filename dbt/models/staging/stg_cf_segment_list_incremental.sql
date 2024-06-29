{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['segment_id'],
    post_hook = [
        "TRUNCATE TABLE blended-setup.userbase_staging.cf_segment_list"
    ]
) }}
WITH
    new_data AS (
        SELECT
            segment_id,
            segment_name,
            last_updated_date
        FROM
            {{ source ('userbase_staging', 'cf_segment_list') }}
    ),
    existing_data AS (
        SELECT
            segment_id,
            segment_name,
            last_updated_date
        FROM
            {{ this }}
    )
SELECT
    nd.segment_id,
    nd.segment_name,
    nd.last_updated_date
FROM
    new_data nd
    LEFT JOIN existing_data ed ON nd.segment_id = ed.segment_id 
    
{% if is_incremental () %}
WHERE
    ed.segment_id IS NULL
    OR ed.segment_name != nd.segment_name 
{% endif %}