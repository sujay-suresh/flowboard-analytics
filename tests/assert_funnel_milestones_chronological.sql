-- Custom test: Core funnel milestones must be chronologically ordered.
-- Board creation and invites can happen independently, but all milestones must be after signup
-- and upgrades must be after signup.

select
    user_id,
    signed_up_at,
    created_first_board_at,
    invited_first_member_at,
    active_in_week_2_at,
    upgraded_to_paid_at

from {{ ref('fct_user_funnel_snapshot') }}

where
    -- Board must be after signup
    (created_first_board_at is not null and created_first_board_at < signed_up_at)
    -- Invite must be after signup
    or (invited_first_member_at is not null and invited_first_member_at < signed_up_at)
    -- Week 2 activity must be after signup
    or (active_in_week_2_at is not null and active_in_week_2_at < signed_up_at)
    -- Upgrade must be after signup
    or (upgraded_to_paid_at is not null and upgraded_to_paid_at < signed_up_at)
