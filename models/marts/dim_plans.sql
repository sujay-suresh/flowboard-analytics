{{
    config(
        materialized='table'
    )
}}

select
    plan_name,
    monthly_price,
    annual_price,
    max_users,
    has_gantt,
    has_automations,
    has_custom_fields,
    has_reporting,
    has_api_access,
    has_sso,
    has_guest_access

from (
    values
        ('free',       0.00,    0.00,     5, false, false, false, false, false, false, false),
        ('pro',       29.00,  290.00,    50, true,  true,  true,  true,  false, false, false),
        ('enterprise', 99.00, 990.00, 10000, true,  true,  true,  true,  true,  true,  true)
) as plans(
    plan_name, monthly_price, annual_price, max_users,
    has_gantt, has_automations, has_custom_fields, has_reporting,
    has_api_access, has_sso, has_guest_access
)
