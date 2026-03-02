with source as (

    select * from {{ source('app', 'raw_feature_usage') }}

),

renamed as (

    select
        usage_id,
        user_id,
        feature_name,
        used_at,
        duration_seconds,
        used_at::date as usage_date

    from source

)

select * from renamed
