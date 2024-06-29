{{config (materialized = 'view')}}

WITH
    no_unsubs AS (
        SELECT
            *
        FROM
            {{source ('userbase_staging', 'int_cf_attributes')}} main
        WHERE NOT EXISTS (
            SELECT 1
            FROM
                {{ source ('userbase_staging', 'int_cf_attributes') }} sub
            WHERE sub.chatfuel_user_id = main.chatfuel_user_id
                AND sub.attribute_name = 'unsubscribed'
                AND sub.attribute_value = 'yes'
        )
    ),
    analysis_on_no_unsubs AS (
        SELECT
            current_date('US/Pacific')-1 AS date,
            COUNT(
                CASE
                    WHEN attribute_name = 'bcast_start_rate'
                    AND CAST(CAST(attribute_value as decimal) as integer) >= 1 THEN chatfuel_user_id
                END
            ) AS started_read_broadcast,
            COUNT(
                CASE
                    WHEN attribute_name = 'bcast_read_rate'
                    AND CAST(CAST(attribute_value as decimal) as integer) >= 1 THEN chatfuel_user_id
                END
            ) AS read_broadcast_till_the_end,
            COUNT(
                CASE
                    WHEN attribute_name = 'phone_number'
                    AND NOT REGEXP_CONTAINS (attribute_value, r'[a-zA-Z]') THEN chatfuel_user_id
                END
            ) AS phone_numbers,
            COUNT(
                CASE
                    WHEN attribute_name = 'email'
                    AND REGEXP_CONTAINS (attribute_value, r'@') THEN chatfuel_user_id
                END
            ) AS emails,
            COUNT(
                CASE
                    WHEN attribute_name = 'segment'
                    AND attribute_value = 'lead' THEN chatfuel_user_id
                END
            ) AS lead,
            COUNT(
                CASE
                    WHEN attribute_name = 'segment'
                    AND attribute_value = 'hot_lead' THEN chatfuel_user_id
                END
            ) AS hot_lead,
            COUNT(
                CASE
                    WHEN attribute_name = 'segment'
                    AND attribute_value = 'advocate' THEN chatfuel_user_id
                END
            ) AS advocate,
            COUNT(
                CASE
                    WHEN attribute_name = 'segment'
                    AND attribute_value = 'hero' THEN chatfuel_user_id
                END
            ) AS hero,
            COUNT(
                CASE
                    WHEN attribute_name = 'research_answer'
                    AND attribute_value = 'support' THEN chatfuel_user_id
                END
            ) AS support,
            COUNT(
                CASE
                    WHEN attribute_name = 'research_answer'
                    AND attribute_value = 'on_the_fence' THEN chatfuel_user_id
                END
            ) AS on_the_fence,
            COUNT(
                CASE
                    WHEN attribute_name = 'research_answer'
                    AND attribute_value = 'oppose' THEN chatfuel_user_id
                END
            ) AS oppose
        FROM
            no_unsubs
        WHERE
            CAST(last_updated_date AS DATE) = current_date('US/Pacific')-1
    ),
    unsubs AS (
        SELECT
            current_date('US/Pacific')-1 AS date,
            COUNT(
                CASE
                    WHEN attribute_name = 'unsubscribed'
                    AND attribute_value = 'yes' THEN chatfuel_user_id
                END
            ) AS unsubscribed
        FROM
            {{source ('userbase_staging', 'int_cf_attributes')}}
        WHERE
            CAST(last_updated_date AS DATE) = current_date('US/Pacific')-1
    ),
    combined_pt1 AS (
        SELECT
            aou.date,
            CAST(aou.started_read_broadcast as INT64) + CAST(aou.read_broadcast_till_the_end as INT64) AS active_users,
            aou.phone_numbers,
            aou.emails,
            aou.lead,
            aou.hot_lead,
            aou.advocate,
            aou.hero,
            aou.support,
            aou.on_the_fence,
            aou.oppose,
            u.unsubscribed
        FROM
            analysis_on_no_unsubs aou
            FULL OUTER JOIN unsubs u ON aou.date = u.date
    ),
    new_subscribers AS (
        SELECT
            current_date('US/Pacific')-1 AS date,
            COUNT(
                distinct chatfuel_user_id
            ) AS new_subscribers
        FROM
            {{source ('userbase_staging', 'stg_cf_users_incremental')}}
        WHERE
            DATE(PARSE_TIMESTAMP ('%Y-%m-%d %H:%M:%S', signed_up)) = current_date('US/Pacific')-1
            AND status = 'reachable'
    ),
    blocked AS (
        SELECT
            current_date('US/Pacific')-1 AS date,
            count(distinct chatfuel_user_id) AS blocked
        FROM
            {{source ('userbase_staging', 'stg_cf_users_incremental')}}
        WHERE
            CAST(CAST(last_updated_date AS TIMESTAMP) AS DATE) = current_date('US/Pacific')-1
            AND status = 'blocked'
    ),
    combined_pt2 AS (
        SELECT
            ns.date,
            ns.new_subscribers,
            b.blocked
        FROM
            new_subscribers ns
            FULL OUTER JOIN blocked b ON ns.date = b.date
    )
SELECT
    CAST(c1.date AS STRING) as date,
    c2.new_subscribers,
    c1.active_users,
    c2.blocked,
    c1.unsubscribed,
    c1.phone_numbers,
    c1.emails,
    c1.lead,
    c1.hot_lead,
    c1.advocate,
    c1.hero,
    c1.support,
    c1.on_the_fence,
    c1.oppose
FROM
    combined_pt1 c1
    FULL OUTER JOIN combined_pt2 c2 ON c1.date = c2.date
