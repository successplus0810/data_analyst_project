SELECT DISTINCT CLM_CODE, CLM_DESC, CLM_START, CLM_END
FROM COLES.STI_WIP_CS.CLAIM_DETAILS_SUMMED_all
WHERE (upper(clm_code) LIKE '%PS%' OR  upper(clm_code) LIKE '%BS%'  OR  upper(clm_code) LIKE '%COLOTHER%')
AND (clm_desc LIKE '%COLS%' OR CLM_DESC LIKE '%Coles Online%')
AND LOWER(CLM_DESC) NOT LIKE '%rain%'
AND CLM_END >='2021-5-1'
AND item_idnt in  ({})
ORDER BY clm_end