{{ config (
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'broadcast_timestamp',
    post_hook = [
        "TRUNCATE TABLE blended-setup.userbase_staging.cf_broadcasts_list"
    ]
) }}
WITH
    source AS (
        SELECT
            broadcast_timestamp,
            content AS broadcast_message,
            CASE
                WHEN CAST(REPLACE(progress, '%', '') AS INTEGER) = 100 THEN 'delivered'
                WHEN CAST(REPLACE(progress, '%', '') AS INTEGER) < 100 THEN 'in progress'
            END AS broadcast_progress,
            total_users AS total_recipients,
            read AS total_opened,
            clicked AS total_clicked,
            failed AS failed_to_deliver,
            CURRENT_DATETIME () AS last_updated_dt
        FROM
            {{ source ('userbase_staging', 'cf_broadcasts_list') }}
    ),
    existing_data AS (
        SELECT
            broadcast_timestamp,
            total_opened,
            total_clicked,
            failed_to_deliver
        FROM
            {{ this }}
    )
SELECT
    s.*
FROM
    source s
    LEFT JOIN existing_data ed ON s.broadcast_timestamp = ed.broadcast_timestamp 
    
{% if is_incremental () %}
WHERE
    ed.broadcast_timestamp IS NULL
    OR (
        s.total_opened != ed.total_opened
        OR s.total_clicked != ed.total_clicked
        OR s.failed_to_deliver != ed.failed_to_deliver
    ) 
{% endif %}
