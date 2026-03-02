{{
    config(
        materialized='table'
    )
}}

with users as (

    select * from {{ ref('stg_app__users') }}

),

funnel as (

    select * from {{ ref('int_users_funnel_stages') }}

),

activation as (

    select * from {{ ref('int_users_activation_scored') }}

),

current_plan as (

    select distinct on (user_id)
        user_id,
        plan as current_plan,
        mrr as current_mrr

    from {{ ref('stg_stripe__subscriptions') }}
    order by user_id, started_at desc

)

select
    u.user_id,
    u.email,
    u.full_name,
    u.region,
    u.signup_source,
    u.signed_up_at,
    u.signup_week,
    u.signup_month,
    u.initial_plan,
    coalesce(cp.current_plan, 'free') as current_plan,
    coalesce(cp.current_mrr, 0) as current_mrr,

    -- Funnel milestones
    f.created_first_board_at,
    f.invited_first_member_at,
    f.active_in_week_2_at,
    f.upgraded_to_paid_at,
    f.max_funnel_stage,

    -- Activation
    a.activation_score,
    a.is_activated,
    a.members_invited_week1,
    a.tasks_created_week1,
    a.active_days_week1,

    -- Derived
    case
        when f.max_funnel_stage = 5 then 'converted'
        when f.max_funnel_stage = 4 then 'retained_week2'
        when f.max_funnel_stage = 3 then 'invited_member'
        when f.max_funnel_stage = 2 then 'created_board'
        else 'signed_up_only'
    end as lifecycle_stage

from users u
left join funnel f on u.user_id = f.user_id
left join activation a on u.user_id = a.user_id
left join current_plan cp on u.user_id = cp.user_id
