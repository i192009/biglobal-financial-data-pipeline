{{ config(severity='warn') }}
select * from {{ ref('mart_portfolio_exposure') }} where holding_quantity>0 and (missing_price or stale_price)
