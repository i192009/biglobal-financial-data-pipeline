select * from {{ source('operations','reconciliation_results') }} where status in ('FAIL','WARNING','INCONCLUSIVE')
