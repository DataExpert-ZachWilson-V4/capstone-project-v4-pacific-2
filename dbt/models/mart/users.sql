{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date']
) }}

WITH new_data AS(
    SELECT 
    CAST(date as DATE) as date,
    new_subscribers,
    active_users,
    blocked,
    unsubscribed,
    phone_numbers,
    emails,
    lead,
    hot_lead,
    advocate,
    hero,
    support,
    on_the_fence,
    oppose
    FROM {{ source ('userbase_staging', 'int_cf_users_combined') }}
),
old_data AS(
    SELECT 
    CAST(date as DATE) as date,
    new_subscribers,
    active_users,
    blocked,
    unsubscribed,
    phone_numbers,
    emails,
    lead,
    hot_lead,
    advocate,
    hero,
    support,
    on_the_fence,
    oppose
    FROM {{ this }}
)

SELECT
    nd.date,
    nd.new_subscribers,
    nd.active_users,
    nd.blocked,
    nd.unsubscribed,
    nd.phone_numbers,
    nd.emails,
    nd.lead,
    nd.hot_lead,
    nd.advocate,
    nd.hero,
    nd.support,
    nd.on_the_fence,
    nd.oppose
FROM new_data nd 
FULL OUTER JOIN old_data od 
ON nd.date=od.date

{% if is_incremental() %}
WHERE od.date is null
{% endif %}
