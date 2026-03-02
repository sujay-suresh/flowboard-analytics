{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert'
    )
}}

with events as (

    select * from {{ ref('stg_app__user_events') }}

),

users as (

    select user_id, region, signup_source from {{ ref('stg_app__users') }}

)

select
    e.event_id,
    e.user_id,
    e.event_type,
    e.event_timestamp,
    e.event_date,
    e.event_hour,
    e.event_properties,
    u.region,
    u.signup_source

from events e
left join users u on e.user_id = u.user_id

{% if is_incremental() %}
where e.event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}
