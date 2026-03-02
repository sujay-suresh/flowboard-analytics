{{
    config(
        materialized='table'
    )
}}

/*
    Accumulating snapshot fact table (Kimball pattern).
    One row per user, with timestamp columns for each funnel milestone.
    Rows are updated as users progress through milestones.
*/

with funnel as (

    select * from {{ ref('int_users_funnel_stages') }}

),

users as (

    select * from {{ ref('stg_app__users') }}

),

activation as (

    select * from {{ ref('int_users_activation_scored') }}

)

select
    u.user_id,
    u.region,
    u.signup_source,
    u.signup_week as cohort_week,
    u.signup_month as cohort_month,

    -- Milestone timestamps (accumulating snapshot columns)
    f.signed_up_at,
    f.created_first_board_at,
    f.invited_first_member_at,
    f.active_in_week_2_at,
    f.upgraded_to_paid_at,

    -- Lag times between milestones (in hours)
    extract(epoch from (f.created_first_board_at - f.signed_up_at)) / 3600.0
        as hours_to_first_board,
    extract(epoch from (f.invited_first_member_at - f.created_first_board_at)) / 3600.0
        as hours_board_to_invite,
    extract(epoch from (f.upgraded_to_paid_at - f.signed_up_at)) / 3600.0
        as hours_to_conversion,

    -- Completion flags
    f.created_first_board_at is not null as completed_board,
    f.invited_first_member_at is not null as completed_invite,
    f.active_in_week_2_at is not null as completed_week2,
    f.upgraded_to_paid_at is not null as completed_conversion,

    f.max_funnel_stage,

    -- Activation score
    a.activation_score,
    a.is_activated

from users u
left join funnel f on u.user_id = f.user_id
left join activation a on u.user_id = a.user_id
