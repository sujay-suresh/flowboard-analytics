{{
    config(
        materialized='table'
    )
}}

with date_spine as (

    select
        d::date as date_day

    from generate_series('2024-09-01'::date, '2026-03-31'::date, '1 day'::interval) d

)

select
    date_day,
    extract(year from date_day)::int as year_number,
    extract(month from date_day)::int as month_number,
    extract(day from date_day)::int as day_number,
    extract(dow from date_day)::int as day_of_week,
    extract(week from date_day)::int as week_number,
    extract(quarter from date_day)::int as quarter_number,
    to_char(date_day, 'YYYY-MM') as year_month,
    to_char(date_day, 'Day') as day_name,
    to_char(date_day, 'Month') as month_name,
    date_trunc('week', date_day)::date as week_start_date,
    date_trunc('month', date_day)::date as month_start_date,
    date_trunc('quarter', date_day)::date as quarter_start_date,
    case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend

from date_spine
