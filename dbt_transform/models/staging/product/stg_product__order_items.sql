with source as (
    select * from {{ source('raw', 'order_items') }}
),

deduped as (
    select
        order_item_id,
        order_id,
        product_sku,
        product_name,
        quantity::number as quantity,
        unit_price::float as unit_price,
        line_discount::float as line_discount,
        _source_file,
        _loaded_at
    from source
    qualify row_number() over (partition by order_item_id order by _row_hash desc) = 1
)

select * from deduped
