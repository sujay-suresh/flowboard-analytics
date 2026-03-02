-- Custom test: No events should exist from users before their signup date.

select
    e.event_id,
    e.user_id,
    e.event_timestamp,
    u.signed_up_at

from {{ ref('fct_user_events') }} e
inner join {{ ref('dim_users') }} u on e.user_id = u.user_id

where e.event_timestamp < u.signed_up_at
