{{ config (
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['chatfuel_user_id', 'status'],
    post_hook = [
        "TRUNCATE TABLE blended-setup.userbase_staging.cf_users"
    ]
) }}
WITH
    source AS (
        SELECT
            last_seen,
            chatfuel_user_id,
            COALESCE(first_name, instagram_name, instagram_handle) AS first_name,
            last_name,
            signed_up,
            status,
            timezone,
            gender,
            last_updated_date,
            instagram_name,
            instagram_handle,
            CASE
                WHEN instagram_name IS NOT NULL
                OR instagram_handle IS NOT NULL THEN 'instagram_user'
                ELSE 'fb_user'
            END AS platform,
            ROW_NUMBER() OVER (
                PARTITION BY
                    chatfuel_user_id,
                    first_name
                ORDER BY
                    last_updated_date DESC
            ) AS rn
        FROM
            {{ source ('userbase_staging', 'cf_users') }}
    ),
    filtered_source AS (
        SELECT
            last_seen,
            chatfuel_user_id,
            first_name,
            last_name,
            signed_up,
            status,
            timezone,
            gender,
            last_updated_date,
            instagram_name,
            instagram_handle,
            platform
        FROM
            source
        WHERE
            rn = 1
    ),
    existing_data AS (
        SELECT
            last_seen,
            chatfuel_user_id,
            first_name,
            last_name,
            signed_up,
            status,
            timezone,
            gender,
            last_updated_date,
            instagram_name,
            instagram_handle,
            platform
        FROM
            {{ this }}
    )
SELECT
    s.last_seen,
    s.chatfuel_user_id,
    s.first_name,
    s.last_name,
    s.signed_up,
    s.status,
    s.timezone,
    s.gender,
    s.last_updated_date,
    s.instagram_name,
    s.instagram_handle,
    s.platform
FROM
    filtered_source s
    FULL OUTER JOIN existing_data ed ON s.chatfuel_user_id = ed.chatfuel_user_id

{% if is_incremental () %}
WHERE
    ed.chatfuel_user_id IS NULL
    OR s.chatfuel_user_id = ed.chatfuel_user_id and ed.status != s.status 
{% endif %}

