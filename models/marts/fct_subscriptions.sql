{{
    config(
        materialized='table'
    )
}}

with subscriptions as (

    select * from {{ ref('stg_stripe__subscriptions') }}

),

users as (

    select user_id, region, signup_source from {{ ref('stg_app__users') }}

)

select
    s.subscription_id,
    s.user_id,
    s.plan,
    s.mrr,
    s.plan_mrr,
    s.started_at,
    s.ended_at,
    s.change_type,
    s.is_active,
    u.region,
    u.signup_source,

    -- Duration
    case
        when s.ended_at is not null
        then extract(epoch from (s.ended_at - s.started_at)) / 86400.0
    end as duration_days,

    date_trunc('month', s.started_at)::date as started_month,
    date_trunc('month', s.ended_at)::date as ended_month

from subscriptions s
left join users u on s.user_id = u.user_id
