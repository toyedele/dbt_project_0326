with source as (
    select * from {{ ref('stg_events__marketing_clicks') }}
),

final as (
    select
        click_id,
        customer_id,
        anonymous_id,
        case when customer_id is not null then 'identified' else 'anonymous' end as client_type,
        channel,
        clicked_at,
        campaign,
        utm_source,
        utm_medium
    from source
)

select * from final