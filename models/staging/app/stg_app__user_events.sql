with source as (

    select * from {{ source('app', 'raw_user_events') }}

),

renamed as (

    select
        event_id,
        user_id,
        event_type,
        event_timestamp,
        event_properties,
        event_timestamp::date as event_date,
        date_trunc('hour', event_timestamp) as event_hour

    from source

)

select * from renamed
