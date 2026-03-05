with source as (
    select * from {{ source('raw', 'orders') }}
),

deduped as (
    select
        order_id,
        customer_id,
        order_created_at::timestamp as order_created_at,
        try_to_timestamp(order_updated_at, 'MM-DD-YYYY HH24:MI:SS') as order_updated_at,
        status as order_status,
        currency,
        order_total::float as order_total,
        shipping_amount::float as shipping_amount,
        discount_amount::float as discount_amount,
        _source_file,
        _loaded_at
    from source
    qualify row_number() over (partition by order_id order by _row_hash desc) = 1
)

select * from deduped
