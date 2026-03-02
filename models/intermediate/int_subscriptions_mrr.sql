{{
    config(
        materialized='view'
    )
}}

with subscriptions as (

    select * from {{ ref('stg_stripe__subscriptions') }}

),

-- Generate a month series for the data range
months as (

    select generate_series(
        '2024-09-01'::date,
        '2026-02-01'::date,
        '1 month'::interval
    )::date as report_month

),

-- Determine MRR changes per month
mrr_changes as (

    select
        date_trunc('month', started_at)::date as change_month,
        user_id,
        subscription_id,
        plan,
        change_type,
        mrr,
        case
            when change_type = 'new' and plan != 'free' then mrr
            when change_type = 'expansion' then mrr
            else 0
        end as mrr_added,
        case
            when change_type = 'churn' then
                coalesce(
                    (select s2.mrr
                     from {{ ref('stg_stripe__subscriptions') }} s2
                     where s2.user_id = subscriptions.user_id
                       and s2.ended_at = subscriptions.started_at
                     limit 1),
                    0
                )
            else 0
        end as mrr_lost

    from subscriptions
    where change_type != 'new' or plan != 'free'

),

-- Monthly MRR waterfall
monthly_mrr as (

    select
        m.report_month,

        -- New MRR: first paid subscription for a user in this month
        coalesce(sum(case
            when mc.change_type = 'new' and mc.plan != 'free'
            then mc.mrr_added
        end), 0) as new_mrr,

        -- Expansion MRR
        coalesce(sum(case
            when mc.change_type = 'expansion'
            then mc.mrr_added
        end), 0) as expansion_mrr,

        -- Contraction MRR (downgrades — not used in our data but included for completeness)
        0::numeric as contraction_mrr,

        -- Churn MRR
        coalesce(sum(case
            when mc.change_type = 'churn'
            then mc.mrr_lost
        end), 0) as churn_mrr

    from months m
    left join mrr_changes mc
        on mc.change_month = m.report_month
    group by m.report_month

),

-- Running total
mrr_waterfall as (

    select
        report_month,
        new_mrr,
        expansion_mrr,
        contraction_mrr,
        churn_mrr,
        new_mrr + expansion_mrr - contraction_mrr - churn_mrr as net_new_mrr,
        sum(new_mrr + expansion_mrr - contraction_mrr - churn_mrr)
            over (order by report_month) as ending_mrr

    from monthly_mrr

)

select * from mrr_waterfall
