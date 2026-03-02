-- Custom test: Retention flags must be 0 or 1 (boolean integers).

select
    user_id,
    retention_day,
    is_retained

from {{ ref('int_users_retention_flags') }}

where is_retained not in (0, 1)
