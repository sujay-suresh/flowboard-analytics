with source as (

    select * from {{ source('app', 'raw_users') }}

),

renamed as (

    select
        user_id,
        email,
        full_name,
        region,
        signup_source,
        signed_up_at,
        initial_plan,
        date_trunc('week', signed_up_at)::date as signup_week,
        date_trunc('month', signed_up_at)::date as signup_month

    from source

)

select * from renamed
