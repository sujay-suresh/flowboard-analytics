with source as (

    select * from {{ source('intercom', 'raw_conversations') }}

),

renamed as (

    select
        conversation_id,
        user_id,
        topic,
        status,
        created_at,
        resolved_at,
        messages_count,
        first_response_minutes,
        case
            when resolved_at is not null
            then extract(epoch from (resolved_at - created_at)) / 3600.0
        end as resolution_hours

    from source

)

select * from renamed
