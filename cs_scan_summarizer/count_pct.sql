SELECT SKU_ID ITEM_IDNT, mode(round(F_NORM_SELL_PRC_GST_INC_AMT,2)) normal_price
FROM coles.STI_WIP_CS.SALES_WIP_STATE 
WHERE sku_id IN ('{}')
AND day_dt BETWEEN '{}' AND '{}'
GROUP BY SKU_ID