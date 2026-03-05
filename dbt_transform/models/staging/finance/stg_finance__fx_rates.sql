with source as (
    select * from {{ source('raw', 'fx_rates') }}
),

deduped as (
    select
        date::date as calendar_date,
        base_currency,
        quote_currency,
        rate::float as rate,
        source,
        _source_file,
        _loaded_at
    from source
    qualify row_number() over (partition by date order by _row_hash desc) = 1
)

select * from deduped
