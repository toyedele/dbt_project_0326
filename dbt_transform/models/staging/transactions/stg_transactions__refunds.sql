with source as (
    select * from {{ source('raw', 'refunds') }}
),

deduped as (
    select
        refund_id,
        payment_id,
        order_id,
        refund_amount::float as refund_amount,
        currency,
        reason,
        try_to_timestamp(refunded_at, 'DD-MM-YYYY HH24:MI') as refunded_at,
        _source_file,
        _loaded_at
    from source
    qualify row_number() over (partition by refund_id order by _row_hash desc) = 1
)

select * from deduped
