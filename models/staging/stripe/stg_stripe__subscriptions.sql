with source as (

    select * from {{ source('stripe', 'raw_subscriptions') }}

),

renamed as (

    select
        subscription_id,
        user_id,
        plan,
        mrr,
        started_at,
        ended_at,
        change_type,
        case
            when plan = 'free' then 0
            when plan = 'pro' then 29.00
            when plan = 'enterprise' then 99.00
        end as plan_mrr,
        case
            when ended_at is null then true
            else false
        end as is_active

    from source

)

select * from renamed
