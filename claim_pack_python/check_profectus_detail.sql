select distinct  FILE_NAME , EFFECTIVE_START_DATE ,EFFECTIVE_END_DATE
from coles.PVN_CLAIMS.CT_CS_SCAN_PROF_DETAIL_MERGED_2
where PRODUCT_NUMBER in ({}) and sales_date between '{}' and '{}'