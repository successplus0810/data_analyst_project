select item_idnt, sum(clm_qty)  clm_qty, avg(clm_rate) clm_rate
, listagg(distinct CLM_REF_NUM,', ') ref_num
from COLES.STI_WIP_CS.CLAIM_DETAILS_SUMMED_ALL
where item_idnt in ('{}')
and (clm_code like '%PS%' or clm_code like '%BS%' or clm_code like '%COLOTHER%')
and clm_code not ilike '%man%'
and clm_start between '{}'::date and '{}'::date
and clm_end between '{}'::date and '{}'::date
GROUP BY item_idnt
ORDER BY item_idnt