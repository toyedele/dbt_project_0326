with source as (
    select * from {{ ref('stg_transactions__refunds') }}
),

final as (
    select
        refund_id,
        payment_id,
        order_id,
        refund_amount,
        currency,
        reason,
        refunded_at::date as refunded_date,
        date_trunc('month', refunded_date) as refunded_month,
        date_trunc('quarter', refunded_date) as refunded_quarter
    from source
)

select * from final