with source as (
    select * from {{ source('raw', 'payments') }}
),

deduped as (
    select
        payment_id,
        order_id,
        attempt_number::number as attempt_number,
        payment_provider,
        payment_method,
        status as payment_status,
        amount::float as amount,
        currency,
        try_to_timestamp(created_at, 'YYYY/MM/DD HH24:MI') as created_at,
        processed_at::timestamp as processed_at,
        failure_reason,
        _source_file,
        _loaded_at
    from source
    qualify row_number() over (partition by payment_id order by _row_hash desc) = 1
)

select * from deduped
