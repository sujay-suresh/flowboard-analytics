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

-- Week 1 activity (first 7 days)
week1_events as (

    select
        e.user_id,
        e.event_type,
        e.event_timestamp

    from events e
    inner join users u on e.user_id = u.user_id
    where e.event_timestamp < u.signed_up_at + interval '7 days'

),

-- Compute activation signals
activation_signals as (

    select
        u.user_id,

        -- Boards created in week 1
        coalesce(sum(case when w.event_type = 'board_created' then 1 else 0 end), 0)
            as boards_created_week1,

        -- Members invited in week 1
        coalesce(sum(case when w.event_type = 'member_invited' then 1 else 0 end), 0)
            as members_invited_week1,

        -- Tasks created in week 1
        coalesce(sum(case when w.event_type = 'task_created' then 1 else 0 end), 0)
            as tasks_created_week1,

        -- Total events in week 1
        coalesce(count(w.event_type), 0) as total_events_week1,

        -- Distinct active days in week 1
        coalesce(count(distinct w.event_timestamp::date), 0) as active_days_week1

    from users u
    left join week1_events w on u.user_id = w.user_id
    group by u.user_id

),

-- Score activation (weighted)
scored as (

    select
        user_id,
        boards_created_week1,
        members_invited_week1,
        tasks_created_week1,
        total_events_week1,
        active_days_week1,

        -- Activation score (0-100 scale)
        least(100, (
            -- Board created (20 pts)
            case when boards_created_week1 >= 1 then 20 else 0 end
            -- Members invited (40 pts — the key activation signal)
            + case
                when members_invited_week1 >= 3 then 40
                when members_invited_week1 >= 2 then 30
                when members_invited_week1 >= 1 then 15
                else 0
              end
            -- Tasks created (20 pts)
            + case
                when tasks_created_week1 >= 5 then 20
                when tasks_created_week1 >= 2 then 10
                else 0
              end
            -- Active days (20 pts)
            + case
                when active_days_week1 >= 5 then 20
                when active_days_week1 >= 3 then 10
                else 0
              end
        )) as activation_score,

        -- Binary: is user "activated"? (invited >=2 members in week 1)
        case when members_invited_week1 >= 2 then true else false end as is_activated

    from activation_signals

)

select * from scored
