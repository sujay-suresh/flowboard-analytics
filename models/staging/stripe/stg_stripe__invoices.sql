with source as (

    select * from {{ source('stripe', 'raw_invoices') }}

),

renamed as (

    select
        invoice_id,
        subscription_id,
        user_id,
        amount,
        currency,
        status,
        issued_at,
        issued_at::date as invoice_date,
        date_trunc('month', issued_at)::date as invoice_month

    from source

)

select * from renamed
