import json
import pandas as pd
import snowflake.connector as sf
import os
import xlwings as xw
from xlwings.constants import DeleteShiftDirection
import datetime

##########################'

config_coles = r"config.json"
config_coles_clean = r"config2.json"

file_sql_summ = r"summarizer.sql"
file_sql_cd_ref = r"cd_ref.sql"
file_sql_dept = r"dept.sql"
file_sql_gst = r"gst.sql"
file_sql_cd_ref_listagg = r"cd_ref_listagg.sql"
file_sql_cd_ref_listagg_item = r"cd_ref_listagg_item.sql"

os.chdir('D:\\python\\cs_scan_summarizer')

path_excel = r"CS_SCAN_Vendorname_Analyst_Date.xlsx"
path_import_item = 'item_import.xlsx'
# vendor_name = (input('Input vendor name : ')).upper()
# analyst_name = (input('Input analyst name. Example: CT. Your answer is ')).upper()
# date_batch = input('Input date batch. Example: 20230207. Your answer is ')

vendor_name = 'a'
analyst_name = 'a'
date_batch = 'a'

path_export_final = 'CS_SCAN_'+vendor_name+'_'+analyst_name+'_'+date_batch+'.xlsx'
path_export_final_xlsb = 'CS_SCAN_'+vendor_name+'_'+analyst_name+'_'+date_batch+'.xlsb'

def set_up(config):
    """Set up connection to SnowFlake"""
    config = json.loads(open(config).read())
    account = config['snowflake']['account']
    user = config['snowflake']['user']
    warehouse = config['snowflake']['warehouse']
    role = config['snowflake']['role']
    database = config['snowflake']['database']
    schema = config['snowflake']['schema']
    password = config['snowflake']['password']
    auth = config['snowflake']['authenticator']

    conn = sf.connect(user=user, password=password, account=account, authenticator=auth,
                      warehouse=warehouse, role=role, database=database, schema=schema)

    cursor = conn.cursor()
    return cursor
def connect_sql(cursor,file_sql,item_code,start_date = '',end_date='',var_1='',var_2 = '',var_3 = ''):
    try:
        cursor.execute((open(file_sql).read()).format(item_code,start_date,end_date,var_1,var_2,var_3))
        all_rows = cursor.fetchall()
        field_names = [i[0] for i in cursor.description]
    finally:
        # conn.close()
        pass
    df = pd.DataFrame(all_rows)
    try:
        df.columns = field_names
    except ValueError:
        return pd.DataFrame(columns=field_names)
    return df
   

def item_gst(i):
    df = pd.read_excel(path_import_item,sheet_name=str(i))
    df['ITEM_IDNT'] = df['ITEM_IDNT'].astype(str)
    df['ITEM_IDNT'] = df['ITEM_IDNT'].str.strip() 
    item_unique = df['ITEM_IDNT'].drop_duplicates().tolist()
    item_unique = "','".join(item_unique)
    df['STATE'] = df['STATE'].str.strip()
    clm_start = df['CLM_START'][0]
    clm_end = df['CLM_END'][0]
    df_gst = connect_sql(cursor,file_sql=file_sql_gst, item_code=item_unique)
    gst = df_gst['CML_COST_GST_RATE_PCT'][0]
    gst = int(gst)
    claim_number = f'{i}_{gst}'
    dept = df_gst['DEPT_IDNT'][0]
    supp_num = df_gst['SUPP_IDNT'][0]
    supp_desc = df_gst['SUPP_DESC'][0]
    vendor_num = df_gst['VENDOR_NUM'][0]
    item_list_dict = df.set_index(['ITEM_IDNT','STATE'])[['RRP','SCANRATE']].to_dict('index')
    for key,value in item_list_dict.items():
        if gst == 10:
            item_list_dict[key] = [item_list_dict[key]['RRP']/1.1] + [item_list_dict[key]['SCANRATE']]
        else:
            item_list_dict[key] = [item_list_dict[key]['RRP']] + [item_list_dict[key]['SCANRATE']]   
    return supp_num,supp_desc,vendor_num,claim_number,gst,clm_start,clm_end,dept,item_unique,item_list_dict

def writer_excel(data,remove,number_sheet,path_export_final):
    # data = list_data, remove = list_remove,number_sheet= str(index_promo)+'_'+str(gst),path_export_final=path_export_final
    #select sheet
    sheet_df_mapping = {number_sheet: data}
    sheet_df_remove  = {number_sheet: remove}
    # Open Excel in background
    with xw.App(visible=False) as app:
        wb = app.books.open(path_export_final)
        # List of current worksheet names
        current_sheets = [sheet.name for sheet in wb.sheets]
        # Iterate over sheet/df mapping
        # If sheet already exist, overwrite current cotent. Else, add new sheet
        print('start copy data')
        for sheet_name in sheet_df_mapping.keys():
            if sheet_name in current_sheets:
                for df_data in data :
                    wb.sheets(sheet_name).range(df_data['cell_export']).options(index=False,header=False).value = df_data['df']
            else:
                'Name of sheet cannot be found in Excel file, please check again'
        print('done copy data')
        print('start delete rows')
        for sheet_name in sheet_df_remove.keys():
            if sheet_name in current_sheets:
                for df_remove in remove :
                    # wb.sheets(sheet_name).range(df_cell['cell_export']).options(index=False,header=False).value = df_cell['df']
                    length_start = df_remove['length_start'] + df_remove['count_df']
                    range_length_to_remove = str(length_start)+':'+ str(df_remove['length_end'])
                    wb.sheets(sheet_name).range(range_length_to_remove).api.Delete(DeleteShiftDirection.xlShiftUp)
            else:
                'Name of sheet cannot be found in Excel file, please check again'
        print('done delete rows')
        wb.save(path_export_final)
    return None

def fill_summary_sheet(summary_index_list,path_export_final):
    print('Start fill summary sheet')
    with xw.App(visible=False) as app:
        wb_from = app.books.open(path_export_final)
        summary_index = 1
        for index in summary_index_list:
            wb_from.sheets['Vendor Summary'].range('B'+str(summary_index+10)).value = index
            summary_index += 1
        length_start = summary_index + 10
        range_length_to_remove = str(length_start)+':'+ str(30)
        print(range_length_to_remove)
        wb_from.sheets('Vendor Summary').range(range_length_to_remove).api.Delete(DeleteShiftDirection.xlShiftUp)         
        wb_from.save(path_export_final)
    return 'Done fill summary sheet' 

def create_worksheet(index_promo,gst,path_export_final):
    # Open Excel in background
    with xw.App(visible=False) as app:
        if index_promo == 1:
            wb_from = app.books.open(path_excel)
        else :
            wb_from = app.books.open(path_export_final)
        ws_from = wb_from.sheets['template']
        ws_from.copy(before=ws_from, name=str(index_promo)+'_'+str(gst))
        wb_from.save(path_export_final)
    return 'Done create worksheet'     

def remove_sheet_change_xlsb(sheet_name,path_export_final,path_export_final_xlsb):
    print('Start delete sheet & change to xlsb')
    with xw.App(visible=False) as app:
        wb = app.books.open(path_export_final)                
        wb.sheets[sheet_name].delete()
        wb.save(path_export_final_xlsb)
    try:
        os.remove(path_export_final)
    except Exception as e:
        print(e)
    return print('Done delete sheet & change to xlsb')

# item_code=0,var_1=0,var_2=0,var_3=0,var_4=0

def df_sales_data(item_list_dict_gsted):
    i = 0
    for key,value in item_list_dict_gsted.items():
        print(key,value)
        item_code,state = key
        df_each_item = connect_sql(cursor,file_sql = file_sql_summ ,item_code = item_code,start_date = clm_start,end_date =clm_end,var_1=value[0],var_2=value[1],var_3 = state)
        if i == 0:
            df_merge = df_each_item
        else :
            df_merge = pd.concat([df_merge, df_each_item], ignore_index=True)
        i+=1
    df_merge['ELI_CLAIM'] = df_merge.RQTY_PROMO * df_merge.SCAN_RATE
    df_merge= df_merge.sort_values(by=['RSKU_ID','RDAY_DT','RSTATE'], ascending=True)
    return df_merge

def product_state_summary(df_sales,df_state_ref):
    print('Start product_state_summary')
    list_data = []
    list_remove = []
    # Find distict item_code and state
    # writer_excel(df,cell_export,length_start,count_df,length_end,number_sheet,path_export_final)
    df_temp =df_sales.drop_duplicates(['RSKU_ID','RITEM_DESC','RSTATE'])[['RSKU_ID','RITEM_DESC','RSTATE']]
    df_temp_2 = pd.merge(df_temp,df_state_ref,left_on=['RSKU_ID','RSTATE'],right_on=['ITEM_IDNT','CLM_STATE'], how='left')
    # print(df_ref)
    df_final = df_temp_2[['RSKU_ID','RITEM_DESC','RSTATE','REF_NUM','CLM_QTY','CLM_RATE']]
    df_sku_desc = df_final[['RSKU_ID','RITEM_DESC']]
    df_state = df_final[['RSTATE']]
    df_ref = df_final[['REF_NUM','CLM_QTY','CLM_RATE']]
    df_ref.insert(1,"REF_DESC",'')
    # Calculate number of rows
    number_rows_state = len(df_ref)

    dict_data_sku = {'df':df_sku_desc,'cell_export':'B121'}
    dict_data_state = {'df':df_state,'cell_export':'E121'}
    dict_data_remove = {'df':df_ref,'cell_export':'M121'}
    dict_remove = {'count_df':number_rows_state,'length_start':121,'length_end':601}
    list_data.append(dict_data_sku)
    list_data.append(dict_data_state)
    list_data.append(dict_data_remove)
    list_remove.append(dict_remove)
    print('Done product_state_summary')
    return list_data,list_remove


def product_summary(df_sales,df_item_ref):
    print('Start product_summary')
    list_data = []
    list_remove = []
    df_product =df_sales.drop_duplicates(['RSKU_ID','RITEM_DESC'])[['RSKU_ID','RITEM_DESC']]
    df_temp = pd.merge(df_product,df_item_ref,left_on=['RSKU_ID'],right_on=['ITEM_IDNT'], how='left')
    # df_final = df_temp[['RSKU_ID','RITEM_DESC','REF_NUM']]
    df_product_1 = df_temp[['RSKU_ID','RITEM_DESC']]
    df_ref_1 = df_temp[['REF_NUM']]
    number_rows_sales = len(df_product)
    # writer_excel(df = df_product,path_export_final = path_export_final, cell_export = 'B20',number_sheet = number_sheet,length_start=20 , count_df=number_rows_sales, length_end=116)
    dict_data_sku = {'df':df_product_1,'cell_export':'B20'}
    dict_data_ref = {'df':df_ref_1,'cell_export':'L20'}
    dict_remove = {'count_df':number_rows_sales,'length_start':20,'length_end':116}
    list_data.append(dict_data_sku)
    list_data.append(dict_data_ref)
    list_remove.append(dict_remove)
    print('Done product_summary')
    return list_data , list_remove

def cd_ref(df_sales):
    print('Start cd ref')
    list_data = []
    list_remove = []
    df_ref = connect_sql(cursor,file_sql_cd_ref ,item_code = item_unique, start_date= clm_start , end_date= clm_end, var_1 = clm_start, var_2 = clm_end)
    df_ref_groupby = df_ref.groupby('CLM_REF_NUM').agg({'CLM_PRODUCT':'sum'}).sort_values(by='CLM_PRODUCT', ascending=True).reset_index()
    df_sales_daily = pd.concat([df_sales, df_ref_groupby], axis=1 )
    print('Done cd ref')
    print('start state ref')
    df_state_ref = connect_sql(cursor, file_sql_cd_ref_listagg ,item_code = item_unique, start_date= clm_start , end_date= clm_end, var_1 = clm_start, var_2 = clm_end)
    print('done state ref')
    print('start item ref')
    df_item_ref = connect_sql(cursor, file_sql_cd_ref_listagg_item ,item_code = item_unique, start_date= clm_start , end_date= clm_end, var_1 = clm_start, var_2 = clm_end)
    print('done item ref')
    # writer_excel(df = df_sales, cell_export = 'B174',number_sheet= str(index_promo)+'_'+str(gst),length_start=174 ,count_df=len(df_sales), length_end=10174,path_export_final=path_export_final)
    dict_data = {'df':df_sales_daily,'cell_export':'B606'}
    dict_remove = {'count_df':len(df_sales),'length_start':606,'length_end':20606}
    list_data.append(dict_data)
    list_remove.append(dict_remove)
    return df_item_ref,df_state_ref,df_ref,df_sales_daily,list_data,list_remove

# MAIN
print('START')
cursor = set_up(config = config_coles)
excel_file = pd.ExcelFile(path_import_item)
count_sheets_excel_file = len(excel_file.sheet_names)
summary_index_list =[]
for i in range(1,count_sheets_excel_file+1):
    supp_num,supp_desc,vendor_num,claim_number,gst,clm_start,clm_end,dept,item_unique,item_list_dict = item_gst(i)
    df_sales = df_sales_data(item_list_dict)
    df_item_ref,df_state_ref,df_ref,df_sales_daily,list_data_sales,list_remove_sales  = cd_ref(df_sales)
    if df_ref.empty:
        prmt_id = ''
        prmt_name = ''
    else:
        prmt_id = df_ref['PRMTN_COMP_IDNT'][0]
        prmt_name = df_ref['PRMTN_COMP_NAME'][0]
    dict_data_dept = {'df':dept,'cell_export':'F8'}
    dict_data_supp_num = {'df':supp_num,'cell_export':'E8'}
    dict_data_supp_desc = {'df':supp_desc,'cell_export':'C8'}
    dict_data_vendor_num = {'df':vendor_num,'cell_export':'D8'}
    dict_data_claim_number = {'df': claim_number,'cell_export':'B16'}
    dict_data_prmt_id = {'df':prmt_id,'cell_export':'B12'}
    dict_data_prmt_name = {'df':prmt_name,'cell_export':'C12'}
    list_data_state,list_remove_state = product_state_summary(df_sales,df_state_ref)
    list_data_product ,list_remove_product = product_summary(df_sales,df_item_ref)
    list_data = list_data_sales + list_data_state + list_data_product + [dict_data_dept] + [dict_data_prmt_id] + [dict_data_prmt_name] +  [dict_data_supp_num] + [dict_data_supp_desc] + [dict_data_vendor_num] + [dict_data_claim_number]
    list_remove = list_remove_sales + list_remove_state + list_remove_product
    create_worksheet(i,gst,path_export_final)
    writer_excel(list_data,list_remove,claim_number,path_export_final)
    summary_index_list.append(claim_number)
fill_summary_sheet(summary_index_list,path_export_final=path_export_final) 
remove_sheet_change_xlsb(sheet_name = 'template',path_export_final=path_export_final ,path_export_final_xlsb = path_export_final_xlsb)
print('END')
 

# if __name__ == '__main__':
#     main()