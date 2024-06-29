{{ config (
    materialized = 'table',
    post_hook = [
        "TRUNCATE TABLE blended-setup.userbase_staging.cf_broadcast_recipients"
    ]
) }}
WITH
    new_broadcasts AS (
        SELECT
            b.chatfuel_user_id,
            'received_broadcast' AS attribute_name,
            CAST(b.broadcast_id AS STRING) AS attribute_value,
            CURRENT_TIMESTAMP() AS last_updated_date
        FROM
            {{ source ('userbase_staging', 'cf_broadcast_recipients') }} b
    )
SELECT
    chatfuel_user_id,
    attribute_name,
    attribute_value,
    last_updated_date
FROM
    {{ source ('userbase_staging', 'stg_cf_attributes_incremental') }}
UNION ALL
SELECT
    nb.chatfuel_user_id,
    nb.attribute_name,
    nb.attribute_value,
    nb.last_updated_date
FROM
    new_broadcasts nb
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ source ('userbase_staging', 'stg_cf_attributes_incremental') }} ai
    WHERE
        ai.chatfuel_user_id = nb.chatfuel_user_id
        AND ai.attribute_name = nb.attribute_name
        AND ai.attribute_value = nb.attribute_value
)
