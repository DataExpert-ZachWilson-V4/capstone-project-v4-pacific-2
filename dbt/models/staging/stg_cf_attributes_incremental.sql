{{ config (
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['chatfuel_user_id', 'attribute_name'],
    post_hook = [
        "TRUNCATE TABLE blended-setup.userbase_staging.cf_attributes"
    ]
) }}
WITH
    source AS (
        SELECT
            chatfuel_user_id,
            attribute_name,
            attribute_value,
            last_updated_date,
            ROW_NUMBER() OVER (
                PARTITION BY
                    chatfuel_user_id,
                    attribute_name
                ORDER BY
                    last_updated_date DESC
            ) AS rn
        FROM
            {{ source ('userbase_staging', 'cf_attributes') }}
    ),
    existing_data AS (
        SELECT
            chatfuel_user_id,
            attribute_name,
            attribute_value,
            last_updated_date
        FROM
            {{ this }}
    ),
    filtered_source AS (
        SELECT
            chatfuel_user_id,
            attribute_name,
            attribute_value,
            last_updated_date
        FROM
            source
        WHERE
            rn = 1
    )
SELECT
    fs.*
FROM
    filtered_source fs
    LEFT JOIN existing_data ed ON fs.chatfuel_user_id = ed.chatfuel_user_id
    AND fs.attribute_name = ed.attribute_name
    AND fs.attribute_value = ed.attribute_value 
    
{% if is_incremental () %}
WHERE
    ed.chatfuel_user_id IS NULL
    OR ed.attribute_value != fs.attribute_value 
{% endif %}
