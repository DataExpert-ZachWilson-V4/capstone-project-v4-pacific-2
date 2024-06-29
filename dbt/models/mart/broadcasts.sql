{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['broadcast_timestamp']
) }}

WITH new_data AS(
    SELECT *
    FROM {{ source ('userbase_staging', 'int_cf_broadcast_list') }}
),
old_data AS(
    SELECT *
    FROM {{ this }}
)

SELECT
    nd.broadcast_timestamp,
    nd.broadcast_message,
    nd.total_recipients,
    nd.total_opened,
    nd.total_clicked,
    nd.failed_to_deliver,
    nd.broadcast_audience
FROM new_data nd 
FULL OUTER JOIN old_data od 
ON nd.broadcast_timestamp=od.broadcast_timestamp

{% if is_incremental() %}
WHERE od.broadcast_timestamp IS NULL
{% endif %}