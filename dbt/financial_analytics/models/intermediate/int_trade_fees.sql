select execution_id,sum(amount_minor) as fee_minor from {{ ref('stg_fees') }} where not reversed group by execution_id
