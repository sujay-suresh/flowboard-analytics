{{
    config(
        materialized='table'
    )
}}

with users as (

    select * from {{ ref('stg_app__users') }}

),

events as (

    select
        user_id,
        event_timestamp

    from {{ ref('stg_app__user_events') }}

),

subscriptions as (

    select * from {{ ref('stg_stripe__subscriptions') }}

),

user_plan as (

    select distinct on (user_id)
        user_id,
        plan as current_plan

    from subscriptions
    order by user_id, started_at desc

),

-- Pre-compute days since signup for each event
event_days as (

    select
        e.user_id,
        extract(epoch from (e.event_timestamp - u.signed_up_at)) / 86400.0 as days_since_signup

    from events e
    inner join users u on e.user_id = u.user_id

),

-- For each user, determine which retention windows have activity
user_retention_activity as (

    select
        user_id,
        -- Day 1: activity on days 1-7
        max(case when days_since_signup >= 1 and days_since_signup < 8 then 1 else 0 end) as retained_day_1,
        -- Day 7: activity on days 7-14
        max(case when days_since_signup >= 7 and days_since_signup < 14 then 1 else 0 end) as retained_day_7,
        -- Day 14: activity on days 14-21
        max(case when days_since_signup >= 14 and days_since_signup < 21 then 1 else 0 end) as retained_day_14,
        -- Day 30: activity on days 30-37
        max(case when days_since_signup >= 30 and days_since_signup < 37 then 1 else 0 end) as retained_day_30,
        -- Day 60: activity on days 60-67
        max(case when days_since_signup >= 60 and days_since_signup < 67 then 1 else 0 end) as retained_day_60,
        -- Day 90: activity on days 90-97
        max(case when days_since_signup >= 90 and days_since_signup < 97 then 1 else 0 end) as retained_day_90

    from event_days
    group by user_id

),

-- Unpivot into one row per user per retention day
unpivoted as (

    select
        u.user_id,
        u.signup_week as cohort_week,
        u.region,
        u.signup_source,
        coalesce(up.current_plan, 'free') as plan_at_latest,
        periods.retention_day,
        case periods.retention_day
            when 1 then coalesce(ra.retained_day_1, 0)
            when 7 then coalesce(ra.retained_day_7, 0)
            when 14 then coalesce(ra.retained_day_14, 0)
            when 30 then coalesce(ra.retained_day_30, 0)
            when 60 then coalesce(ra.retained_day_60, 0)
            when 90 then coalesce(ra.retained_day_90, 0)
        end as is_retained

    from users u
    cross join (
        select unnest(array[1, 7, 14, 30, 60, 90]) as retention_day
    ) periods
    left join user_retention_activity ra on u.user_id = ra.user_id
    left join user_plan up on u.user_id = up.user_id

)

select * from unpivoted
