-- when currency is null, assumption is that it's in USD as base currency is USD
with source as (
    select * from {{ ref('stg_transactions__orders') }}
),

final as (
    select
        order_id,
        customer_id,
        case when customer_id is not null then 'identified' else 'anonymous' end as client_type,
        iff(order_created_at >= order_updated_at, order_created_at, order_updated_at)::date as revenue_date,
        date_trunc('month',   revenue_date) as revenue_month,
        date_trunc('quarter', revenue_date) as revenue_quarter,
        lower(order_status) as order_status,
        coalesce(currency, 'USD') as currency,
        order_total,
        shipping_amount,
        discount_amount,
        order_total - shipping_amount - ifnull(discount_amount, 0)  as net_revenue
    from source
)

select * from final