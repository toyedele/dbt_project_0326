with source as (
    select * from {{ source('raw', 'customers') }}
),

deduped as (
    select
        customer_id,
        email,
        full_name,
        country,
        created_at::timestamp as created_at,
        try_to_timestamp(updated_at, 'DD/MM/YYYY HH24:MI') as updated_at,
        marketing_opt_in,
        _source_file,
        _loaded_at
    from source
    qualify row_number() over (partition by customer_id order by _row_hash desc) = 1
)

select * from deduped
