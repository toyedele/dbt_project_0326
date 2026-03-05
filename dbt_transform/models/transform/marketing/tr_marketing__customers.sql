{{
    config(
        materialized = 'incremental',
        unique_key = 'customer_id',
        incremental_strategy = 'merge',
        merge_update_columns = ['email', 'country', 'marketing_opt_in'],
    )
}}

with source as (
    select * from {{ ref('stg_users__customers') }}
),

final as (
    select
        customer_id,
        email,
        full_name,
        country,
        marketing_opt_in::boolean as marketing_opt_in
    from source
)

select * from final