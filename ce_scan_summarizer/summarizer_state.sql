SELECT * from
(with temp as
(
SELECT * FROM TABLE(COLES.STI_WIP_CE.SUM_CE_PROMO_2022(STRTOK_TO_ARRAY('{},',','),'{}',DATE('{}'::DATE,'YYYY-MM-DD'),DATE('{}'::DATE,'YYYY-MM-DD'),round({},2)::FLOAT))
-----------------------------------------------------------------------item--------promo-----start-------------------end---------------promo price---
)
select rday_dt, rsku_id, ritem_desc, rstate, round(rf_norm_sell_price_amt,2) rf_norm_sell_price_amt, rprm_price, round(rtotal_sales_amt_mod,2) as rtotal_sales_amt_mod , rqty, round(rf_actual_sell_price_amt_mod,2)
as rf_actual_sell_price_amt_mod, round(rtotal_sales_amt_non,2) as rtotal_sales_amt_non, rqty_non, round(rf_actual_sell_price_amt_mod_non,2) as rf_actual_sell_price_amt_mod_non, 
round(rtotal_sales_amt_mod_promo,2) as rtotal_sales_amt_mod_promo, rqty_promo, round(rf_actual_sell_price_amt_mod_1,2) as rf_actual_sell_price_amt_mod_1, {} AS SCAN_RATE
from temp
where RQTY_PROMO != 0
and rstate IN ({})
order by rsku_id,rday_dt,rstate
);