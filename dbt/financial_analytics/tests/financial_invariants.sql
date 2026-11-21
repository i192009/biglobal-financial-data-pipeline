select 'cash_balance' as invariant,r.id from {{ source('raw','raw_account_balances') }} r
left join {{ ref('int_reconciliation_inputs') }} c on c.account_id=r.id
where not r.deleted and (r.data->>'cash_minor')::numeric<>coalesce(c.ledger_cash_minor,0)
union all
select 'missing_fee',e.id from {{ ref('stg_executions') }} e left join {{ ref('stg_fees') }} f on f.execution_id=e.id
where f.id is null or f.amount_minor<>e.fee_minor or f.reversed<>e.reversed
union all
select 'execution_arithmetic',id from {{ ref('stg_executions') }} where quantity<=0 or price_minor<=0 or notional_minor<>quantity::numeric*price_minor or fee_minor<0
union all
select 'position_balance',p.id from {{ source('raw','raw_positions') }} p left join (
 select account_id,instrument_id,sum(quantity_movement) as quantity from {{ ref('int_position_movements') }} group by 1,2
) m on m.account_id=p.data->>'account_id' and m.instrument_id=p.data->>'instrument_id'
where not p.deleted and (p.data->>'quantity')::numeric<>coalesce(m.quantity,0)
union all
select 'invalid_sequence',id from {{ ref('int_account_cash_movements') }} where cash_balance_minor<0
union all
select 'invalid_reversal',id from {{ ref('fct_transaction_reversals') }} where amount_minor+original_amount_minor<>0
union all
select 'cost_basis_coverage',e.id from {{ ref('int_deduplicated_executions') }} e left join {{ ref('int_cost_basis') }} b using(id) where b.id is null
union all
select 'orphan_instrument',e.id from {{ ref('stg_executions') }} e left join {{ source('raw','raw_instruments') }} i on i.id=e.instrument_id and not i.deleted where i.id is null
union all
select 'invalid_timestamp',id from {{ ref('stg_executions') }} where occurred_at<'2000-01-01'::timestamptz or occurred_at>now()+interval '1 day'
union all
select 'invalid_position_value',account_id from {{ ref('fct_daily_positions') }} where holding_quantity<0 or cost_minor<0 or market_value_minor<>holding_quantity::numeric*price_minor
