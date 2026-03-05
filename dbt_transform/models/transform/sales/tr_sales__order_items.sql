with source as (
    select * from {{ ref('stg_product__order_items') }}
),

final as (
    select
        order_item_id,
        order_id,
        product_sku,
        product_name,
        quantity,
        unit_price,
        line_discount,
        quantity * (unit_price + ifnull(line_discount, 0)) as revenue,
        case when quantity < 0 then true else false end as reversals
    from source
)

select * from final