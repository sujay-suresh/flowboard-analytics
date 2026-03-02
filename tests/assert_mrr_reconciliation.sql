-- Custom test: MRR reconciliation — sum of net changes should equal ending MRR.
-- For each month, ending_mrr should equal cumulative sum of net_new_mrr.

with mrr_check as (

    select
        report_month,
        net_new_mrr,
        ending_mrr,
        sum(net_new_mrr) over (order by report_month) as expected_ending_mrr

    from {{ ref('int_subscriptions_mrr') }}

)

select
    report_month,
    ending_mrr,
    expected_ending_mrr,
    abs(ending_mrr - expected_ending_mrr) as difference

from mrr_check

where abs(ending_mrr - expected_ending_mrr) > 0.01
