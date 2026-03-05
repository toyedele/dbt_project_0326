with source as (
    select * from {{ ref('stg_transactions__payments') }}
),

final as (
    select
        payment_id,
        order_id,
        attempt_number,
        lower(payment_provider) as payment_provider,
        payment_method,
        payment_status,
        amount,
        currency,
        created_at,
        processed_at,
        iff(processed_at >= created_at, processed_at, created_at)::date as processed_date,
        date_trunc('month',   processed_date) as processed_month,
        date_trunc('quarter', processed_date) as processed_quarter,
        failure_reason
    from source
)

select * from final