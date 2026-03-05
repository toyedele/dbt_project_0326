with source as (
    select * from {{ source('raw', 'marketing_clicks') }}
),

deduped as (
    select
        click_id,
        customer_id,
        anonymous_id,
        channel,
        clicked_at::timestamp as clicked_at,
        campaign,
        utm_source,
        utm_medium,
        _source_file,
        _loaded_at
    from source
    qualify row_number() over (partition by click_id order by _row_hash desc) = 1
)

select * from deduped
