{{ config(materialized='table', indexes=[{'columns': ['id'], 'unique': true}, {'columns': ['account_id', 'instrument_id', 'sequence_id']}]) }}
-- Perpetual weighted average, buy fees capitalized; sell fees reduce proceeds.
-- NUMERIC carries fractional minor units deterministically, no binary floats.
with recursive ordered as (
 select *,row_number() over(partition by account_id,instrument_id order by sequence_id) as rn
 from {{ ref('int_deduplicated_executions') }}
), basis as (
 select o.*,quantity::bigint as holding_quantity,
 (notional_minor+fee_minor)::numeric(38,12) as cost_minor,
 0::numeric(38,12) as realized_pnl_minor
 from ordered o where rn=1 and side='buy'
 union all
 select o.*,
 b.holding_quantity+case o.side when 'buy' then o.quantity else -o.quantity end,
 (case when o.side='buy' then b.cost_minor+o.notional_minor+o.fee_minor
       when b.holding_quantity=o.quantity then 0
       else b.cost_minor*(b.holding_quantity-o.quantity)/nullif(b.holding_quantity,0) end)::numeric(38,12),
 (case when o.side='sell' then o.notional_minor-o.fee_minor-b.cost_minor*o.quantity/nullif(b.holding_quantity,0)
       else 0 end)::numeric(38,12)
 from basis b join ordered o on o.account_id=b.account_id and o.instrument_id=b.instrument_id and o.rn=b.rn+1
)
select * from basis
