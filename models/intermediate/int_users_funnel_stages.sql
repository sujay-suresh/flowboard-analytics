{{
    config(
        materialized='view'
    )
}}

with users as (

    select * from {{ ref('stg_app__users') }}

),

events as (

    select * from {{ ref('stg_app__user_events') }}

),

subscriptions as (

    select * from {{ ref('stg_stripe__subscriptions') }}

),

-- Milestone 1: signed_up (all users)
signed_up as (

    select
        user_id,
        signed_up_at as signed_up_at

    from users

),

-- Milestone 2: created_first_board
first_board as (

    select
        user_id,
        min(event_timestamp) as created_first_board_at

    from events
    where event_type = 'board_created'
    group by user_id

),

-- Milestone 3: invited_first_member
first_invite as (

    select
        user_id,
        min(event_timestamp) as invited_first_member_at

    from events
    where event_type = 'member_invited'
    group by user_id

),

-- Milestone 4: active_in_week_2 (at least 1 event in days 8-14)
week2_activity as (

    select
        e.user_id,
        min(e.event_timestamp) as active_in_week_2_at

    from events e
    inner join users u on e.user_id = u.user_id
    where e.event_timestamp >= u.signed_up_at + interval '7 days'
      and e.event_timestamp < u.signed_up_at + interval '14 days'
    group by e.user_id

),

-- Milestone 5: upgraded_to_paid
first_upgrade as (

    select
        user_id,
        min(started_at) as upgraded_to_paid_at

    from subscriptions
    where plan in ('pro', 'enterprise')
    group by user_id

),

funnel as (

    select
        su.user_id,
        su.signed_up_at,
        fb.created_first_board_at,
        fi.invited_first_member_at,
        w2.active_in_week_2_at,
        fu.upgraded_to_paid_at,

        -- Funnel stage reached
        case
            when fu.upgraded_to_paid_at is not null then 5
            when w2.active_in_week_2_at is not null then 4
            when fi.invited_first_member_at is not null then 3
            when fb.created_first_board_at is not null then 2
            else 1
        end as max_funnel_stage

    from signed_up su
    left join first_board fb on su.user_id = fb.user_id
    left join first_invite fi on su.user_id = fi.user_id
    left join week2_activity w2 on su.user_id = w2.user_id
    left join first_upgrade fu on su.user_id = fu.user_id

)

select * from funnel
