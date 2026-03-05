-- Assumption Rate is for 1 USD to quote_currency.
-- if quote_currency is USD then rate should be 1 but this isn't the case hence the exclusion.

with source as (
    select * from {{ ref('stg_finance__fx_rates') }}
),

final as (
    select
        calendar_date,
        base_currency,
        quote_currency,
        rate
    from source
    where quote_currency != 'USD' 
)

select * from final