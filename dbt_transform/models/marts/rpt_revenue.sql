with fx_rates as (
    select * from {{ ref('tr_finance__fx_rates')}}
),

payments as (
    select * from {{ ref('tr_finance__payments') }}
),

refunds as (
    select * from {{ ref('tr_finance__refunds') }}
),

orders as (
    select * from {{ ref('tr_sales__orders') }}
),

items as (
    select * from {{ ref('tr_sales__order_items') }}
),

marketing_clicks as (
    select * from {{ ref('tr_marketing__clicks') }}
),

customers as (
    select * from {{ ref('tr_marketing__customers') }}
),

final as (
    select
        o.revenue_date,
        o.revenue_month,
        o.revenue_quarter,
        o.order_id as order_id,
        o.customer_id,
        o.client_type,
        o.order_status,
        o.currency,
        o.net_revenue,
        case
            when o.currency = 'USD' then o.net_revenue
            when fx_order.rate is null or fx_order.rate = 0 then null
            else round((o.net_revenue / fx_order.rate), 2)
        end as normalised_revenue,
        i.product_sku as product_id,
        i.product_name as product_name,
        c.country,
        c.marketing_opt_in,
        p.payment_provider,
        p.payment_status,
        p.amount as payment_amount,
        case
            when p.currency = 'USD' then p.amount
            when fx_payment.rate is null or fx_payment.rate = 0 then null
            else round((p.amount / fx_payment.rate), 2)
        end as normalised_payment_amount,
        p.currency as payment_currency,
        p.processed_date,
        p.processed_month,
        p.processed_quarter,
        r.refund_amount,
        case
            when r.currency = 'USD' then r.refund_amount
            when fx_refund.rate is null or fx_refund.rate = 0 then null
            else round((r.refund_amount / fx_refund.rate), 2)
        end as normalised_refund_amount,
        r.currency as refund_currency,
        r.reason as refund_reason,
        r.refunded_date,
        r.refunded_month,
        r.refunded_quarter
    from orders o
    inner join items i on o.order_id = i.order_id
    inner join payments p on o.order_id = p.order_id and p.processed_date >= o.revenue_date
    left join refunds r on o.order_id = r.order_id
    left join customers c on o.customer_id = c.customer_id
    left join fx_rates fx_order
        on o.revenue_date = fx_order.calendar_date
        and o.currency = fx_order.quote_currency
        and fx_order.base_currency = 'USD'
    left join fx_rates fx_payment
        on p.processed_date = fx_payment.calendar_date
        and p.currency = fx_payment.quote_currency
        and fx_payment.base_currency = 'USD'
    left join fx_rates fx_refund
        on r.refunded_date = fx_refund.calendar_date
        and r.currency = fx_refund.quote_currency
        and fx_refund.base_currency = 'USD'
)

select * from final
