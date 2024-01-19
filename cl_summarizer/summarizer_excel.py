import json
import pandas as pd
import snowflake.connector as sf
import os
import xlwings as xw
from xlwings.constants import DeleteShiftDirection
import datetime
import math
pd.options.mode.chained_assignment = None

##########################
analyst_name = 'TA'
date_batch = '20230101'
#########################

number_sheet_ap = 'AP'
number_sheet_pd = 'Promo Detail'
number_sheet_cd = 'Claims Detail'

config_coles = r"config.json"

file_sql_pd = r"pd.sql"
file_sql_cd = r"cd.sql"
file_sql_ap = r"ap.sql"
file_sql_dept = r"dept.sql"
file_sql_gst = r"gst.sql"
file_sql_summ = r"summarizer.sql"
# file_sql_cd_ref = r"cd_ref.sql"
file_sql_summarizer_state_single = r"summarizer_state_single.sql"
file_sql_summarizer_state_bundle = r"summarizer_state_bundle.sql"
file_sql_summarizer_national_single = r"summarizer_national_single.sql"
file_sql_summarizer_national_bundle = r"summarizer_national_bundle.sql"
file_sql_cd_national = r"cd_national.sql"
file_sql_cd_state = r"cd_state.sql"
file_sql_check_category_name = r"category_name.sql"
file_sql_check_category_id= r"category_id.sql"
file_sql_get_ven_id_name = r"get_ven_id_name.sql"


path_excel = 'CL_SCAN_Vendorname_Analyst_Date.xlsx'
path_import_item = 'item_import_1.xlsx'







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
def connect_sql(cursor,file_sql,scan,item_code,start_date = '',end_date='',brandid = '', uom = 0,var_1 = '', var_2 ='',var_3 = ''):
    try:
        cursor.execute((open(file_sql).read()).format(scan,item_code,start_date,end_date,brandid,uom,var_1,var_2,var_3))
        all_rows = cursor.fetchall()
        field_names = [i[0] for i in cursor.description]
    finally:
        # conn.close()
        pass
    df = pd.DataFrame(all_rows)
    try:
        df.columns = field_names
    except ValueError:
        return pd.DataFrame(columns = field_names)
    return df


def df_sales_data(cursor,i):
    df_excel = pd.read_excel(path_import_item,sheet_name=str(i))
    df_excel = df_excel.drop_duplicates()
    item_list = df_excel['ITEMID'].drop_duplicates().tolist()
    brand_list = df_excel['BRANDID'].drop_duplicates().tolist()
    state_list = df_excel['SUBSTATE'].drop_duplicates().tolist()
    item_list = convert_to_input_sql(item_list)
    brand_list = convert_to_input_sql(brand_list)
    state_list = convert_to_input_sql(state_list)
    classify_state = df_excel['CLASSIFY_STATE'][0]
    classify_single = df_excel['RRP'][0]
    startdate = df_excel['STARTDATE'][0]
    enddate = df_excel['ENDDATE'][0]
    try:
        df_excel['RRP'] = (df_excel['RRP']/1.1).round(2)
    except:
        df_excel['BUNDLE_PRICE'] = (df_excel['BUNDLE_PRICE']/1.1).round(2)
    print(df_excel)
    # return 0
    if classify_state.lower() == 'state':
        print('state')
        cursor.execute("TRUNCATE TABLE COLES.LIQUORLAND.CT_LIQUOR_20210101_20221031_RAW_SALES_TEMP_AUTO")
        cursor.execute(("INSERT INTO COLES.LIQUORLAND.CT_LIQUOR_20210101_20221031_RAW_SALES_TEMP_AUTO  SELECT DATE1, ITEMIDSKU, UNITOFMEASURE, ITEMNAME, STOREBRANDID, STATE, SALETRANSACTIONNUMBER, SALETRANSACTIONLINENUMBER, AVERAGEITEMSELLPRICE, TOTALLINESALE, ITEMQUANTITY, DIMPROMOPRICEADVDISC1ID, CLASSIFICATION FROM COLES.LIQUORLAND.RAW_SALES WHERE ITEMIDSKU IN ({}) AND DATE1 BETWEEN '{}' AND '{}' AND TRIM(STOREBRANDID) IN ({}) AND STATE IN ({})").format(item_list,startdate,enddate,brand_list,state_list))
        if math.isnan(classify_single):
            print('bundle')
            print(item_list,brand_list,state_list)
            j = 0 
            item_list_dict = df_excel.set_index(['ITEMID','BRANDID','UOM','SUBSTATE'])[['STARTDATE','ENDDATE','BUNDLE_QTY','BUNDLE_PRICE','DEAL']].to_dict('index')
            for key,value in item_list_dict.items():
                itemid,brandid,uom,state = key
                b_qty = value['BUNDLE_QTY']
                b_price = value['BUNDLE_PRICE']
                scan = value['DEAL']
                df_each_item = connect_sql(cursor=cursor,file_sql = file_sql_summarizer_state_bundle ,scan=scan,item_code = itemid,start_date = startdate,end_date=enddate,brandid=brandid,uom=uom,var_1 = b_qty,var_2 = b_price,var_3 = state)
                df_each_cd = connect_sql(cursor=cursor,file_sql = file_sql_cd_state ,scan=startdate,item_code = enddate,start_date = startdate,end_date=enddate,brandid=itemid,uom=brandid,var_1 = state,var_2 = uom,var_3 = scan)
                if j == 0:
                    df_merge = df_each_item
                    cd_ref = df_each_cd
                else :
                    df_merge = pd.concat([df_merge, df_each_item], ignore_index=True)
                    cd_ref = pd.concat([cd_ref, df_each_cd], ignore_index=True)        
                j+=1
            df_merge['ELI_CLAIM'] = df_merge.PROMO_QTY * df_merge.SCAN
        else:
            print('single')
            print(item_list,brand_list)
            j = 0 
            item_list_dict = df_excel.set_index(['ITEMID','BRANDID','UOM','SUBSTATE'])[['STARTDATE','ENDDATE','RRP','DEAL']].to_dict('index')
            for key,value in item_list_dict.items():
                itemid,brandid,uom,state = key
                rrp = value['RRP']
                scan = value['DEAL']
                df_each_item = connect_sql(cursor=cursor,file_sql = file_sql_summarizer_state_single ,scan=scan,item_code = itemid,start_date = startdate,end_date=enddate,brandid=brandid,uom=uom,var_1 = rrp,var_2 = state)
                df_each_cd = connect_sql(cursor=cursor,file_sql = file_sql_cd_state ,scan=startdate,item_code = enddate,start_date = startdate,end_date=enddate,brandid=itemid,uom=brandid,var_1 = state,var_2 = uom,var_3 = scan)
                if j == 0:
                    df_merge = df_each_item
                    cd_ref = df_each_cd
                else :
                    df_merge = pd.concat([df_merge, df_each_item], ignore_index=True)
                    cd_ref = pd.concat([cd_ref, df_each_cd], ignore_index=True)        
                j+=1
            df_merge['ELI_CLAIM'] = df_merge.ITEMQUANTITY_PROMO * df_merge.SCAN
        print(df_merge,cd_ref)
    else :
        print('national')
        # print(("CREATE OR REPLACE TEMPORARY TABLE COLES.LIQUORLAND.CT_LIQUOR_20210101_20221031_RAW_SALES_TEMP_AUTO AS SELECT DATE1, ITEMIDSKU, UNITOFMEASURE, ITEMNAME, STOREBRANDID, STATE, SALETRANSACTIONNUMBER, SALETRANSACTIONLINENUMBER, AVERAGEITEMSELLPRICE, TOTALLINESALE, ITEMQUANTITY, DIMPROMOPRICEADVDISC1ID, CLASSIFICATION FROM COLES.LIQUORLAND.CT_LIQUOR_20210101_20221031_RAW_SALES_1 WHERE ITEMIDSKU IN ({}) AND DATE1 BETWEEN '{}' AND '{}' AND TRIM(STOREBRANDID) IN ({})").format(item_list,startdate,enddate,brand_list))
        cursor.execute("TRUNCATE TABLE COLES.LIQUORLAND.CT_LIQUOR_20210101_20221031_RAW_SALES_TEMP_AUTO")
        cursor.execute(("INSERT INTO COLES.LIQUORLAND.CT_LIQUOR_20210101_20221031_RAW_SALES_TEMP_AUTO  SELECT DATE1, ITEMIDSKU, UNITOFMEASURE, ITEMNAME, STOREBRANDID, STATE, SALETRANSACTIONNUMBER, SALETRANSACTIONLINENUMBER, AVERAGEITEMSELLPRICE, TOTALLINESALE, ITEMQUANTITY, DIMPROMOPRICEADVDISC1ID, CLASSIFICATION FROM COLES.LIQUORLAND.RAW_SALES WHERE ITEMIDSKU IN ({}) AND DATE1 BETWEEN '{}' AND '{}' AND TRIM(STOREBRANDID) IN ({})").format(item_list,startdate,enddate,brand_list))
        if math.isnan(classify_single):
            print('bundle')
            j = 0 
            item_list_dict = df_excel.set_index(['ITEMID','BRANDID','UOM'])[['STARTDATE','ENDDATE','BUNDLE_QTY','BUNDLE_PRICE','DEAL']].to_dict('index')
            for key,value in item_list_dict.items():
                itemid,brandid,uom= key
                b_qty = value['BUNDLE_QTY']
                b_price = value['BUNDLE_PRICE']
                scan = value['DEAL']
                df_each_item = connect_sql(cursor=cursor,file_sql = file_sql_summarizer_national_bundle ,scan=scan,item_code = itemid,start_date = startdate,end_date=enddate,brandid=brandid,uom=uom,var_1 = b_qty,var_2 = b_price)
                df_each_cd = connect_sql(cursor=cursor,file_sql = file_sql_cd_national ,scan=startdate,item_code = enddate,start_date = startdate,end_date=enddate,brandid=itemid,uom=brandid,var_1 = uom,var_2 = scan)
                if j == 0:
                    df_merge = df_each_item
                    cd_ref = df_each_cd
                else :
                    df_merge = pd.concat([df_merge, df_each_item], ignore_index=True)
                    cd_ref = pd.concat([cd_ref, df_each_cd], ignore_index=True)        
                j+=1
            df_merge['ELI_CLAIM'] = df_merge.PROMO_QTY * df_merge.SCAN
        else:
            print('single')
            print(item_list,brand_list)
            j = 0 
            item_list_dict = df_excel.set_index(['ITEMID','BRANDID','UOM'])[['STARTDATE','ENDDATE','RRP','DEAL']].to_dict('index')
            for key,value in item_list_dict.items():
                itemid,brandid,uom = key
                rrp = value['RRP']
                scan = value['DEAL']
                print(rrp,scan)
                df_each_item = connect_sql(cursor=cursor,file_sql = file_sql_summarizer_national_single ,scan=scan,item_code = itemid,start_date = startdate,end_date=enddate,brandid=brandid,uom=uom,var_1 = rrp)
                df_each_cd = connect_sql(cursor=cursor,file_sql = file_sql_cd_national ,scan=startdate,item_code = enddate,start_date = startdate,end_date=enddate,brandid=itemid,uom=brandid,var_1 = uom,var_2 = scan)
                if j == 0:
                    df_merge = df_each_item
                    cd_ref = df_each_cd
                else :
                    df_merge = pd.concat([df_merge, df_each_item], ignore_index=True)
                    cd_ref = pd.concat([cd_ref, df_each_cd], ignore_index=True)        
                j+=1
            df_merge['ELI_CLAIM'] = df_merge.ITEMQUANTITY_PROMO * df_merge.SCAN
    try :
        cd_ref['REBATENO'] = cd_ref['REBATENO'].str.zfill(8)
    except:
        pass
    return df_merge,cd_ref



current_dir = os.getcwd()

def convert_to_input_sql(num_list):
    num_list_final = ''
    # print('SUPP LIST',supp_num_list)
    for num_list in num_list:
        num_list_final = num_list_final + "'" + str(num_list) + "',"
    return num_list_final[:-1]

def convert_to_input_function(num_list):
    num_list_final = ''
    # print('SUPP LIST',supp_num_list)
    for num_list in num_list:
        num_list_final = num_list_final + str(num_list) + ','
    return num_list_final[:-1]

def get_info(df_splited):
    supp_num_list = list(df_splited['VENDOR_NUMBER'].drop_duplicates())
    item_list = list(df_splited['ITEMID'].drop_duplicates())

    supp_num_list_final = convert_to_input_sql(num_list = supp_num_list)
    item_list_final = convert_to_input_sql(num_list = item_list)
    item_input_function = convert_to_input_function(num_list = item_list)
    return supp_num_list_final,item_list_final,item_input_function

def writer_excel(data,remove,number_sheet,path_export_final):
    # data = list_data, remove = list_remove,number_sheet= str(index_promo)+'_'+str(gst),path_export_final=path_export_final
    #select sheet
    sheet_df_mapping = {number_sheet: data}
    sheet_df_remove  = {number_sheet: remove}
    print('sheet_df_mapping',sheet_df_mapping)
    print('sheet_df_remove',sheet_df_remove)
    # Open Excel in background
    with xw.App(visible=False) as app:
        wb = app.books.open(path_export_final)
        # List of current worksheet names
        current_sheets = [sheet.name for sheet in wb.sheets]
        # Iterate over sheet/df mapping
        # If sheet already exist, overwrite current cotent. Else, add new sheet
        for sheet_name in sheet_df_mapping.keys():
            print(number_sheet)
            print(sheet_name)
            if sheet_name in current_sheets:
                for df_data in data :
                    print(df_data)
                    wb.sheets(sheet_name).range(df_data['cell_export']).options(index=False,header=False).value = df_data['df']
            else:
                'Name of sheet cannot be found in Excel file, please check again'
        for sheet_name in sheet_df_remove.keys():
            if sheet_name in current_sheets:
                for df_remove in remove :
                    # wb.sheets(sheet_name).range(df_cell['cell_export']).options(index=False,header=False).value = df_cell['df']
                    length_start = df_remove['length_start'] + df_remove['count_df']
                    range_length_to_remove = str(length_start)+':'+ str(df_remove['length_end'])
                    wb.sheets(sheet_name).range(range_length_to_remove).api.Delete(DeleteShiftDirection.xlShiftUp)
            else:
                'Name of sheet cannot be found in Excel file, please check again'
        wb.save(path_export_final)
    return None

def fill_summary_sheet(supp_desc,summary_index_list,path_export_final,vendor_num):
    with xw.App(visible=False) as app:
        wb_from = app.books.open(path_export_final)
        print('start AP sheet' )
        wb_from.sheets.add('AP',after= wb_from.sheets['template'])
        wb_from.sheets['AP'].range('A2').value = vendor_num 
        print('Done AP sheet' )
        print('Start fill summary sheet')
        summary_index = 1
        for i in range(1,summary_index_list):
            wb_from.sheets['Supplier Summary'].range('B'+str(summary_index+7)).value = i
            wb_from.sheets['Supplier Summary'].range('D'+str(summary_index+7)).value = supp_desc
            summary_index += 1
            wb_from.sheets['Supplier Summary'].range('B'+str(summary_index+7)+':N'+str(summary_index+7)).clear_contents()
            wb_from.sheets['Supplier Summary'].range('B'+str(summary_index+7)+':N'+str(summary_index+7)).clear_formats()
            summary_index += 1
            i += 1
        length_start = summary_index + 7
        print('length_start',length_start)
        range_length_to_remove = str(length_start -1)+':'+ str(38)
        wb_from.sheets('Supplier Summary').range(range_length_to_remove).api.Delete(DeleteShiftDirection.xlShiftUp)  
        print('Done fill summary sheet')
        wb_from.save(path_export_final)
    return None

def create_worksheet(index_promo,path_export_final):
    # Open Excel in background
    with xw.App(visible=False) as app:
        if index_promo == '1':
            wb_from = app.books.open(path_excel)
        else :
            wb_from = app.books.open(path_export_final)
        ws_from = wb_from.sheets['template']
        ws_from.copy(before=ws_from, name=str(index_promo))
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

def df_sales_data_cd(df_merge, cd_ref):
    list_data = []
    list_remove = []
    if cd_ref.empty == False:
        cd_ref_sales = cd_ref.groupby('REBATENO')['CLM_VAL'].apply('sum').reset_index(name = 'CLM_VAL')
    else:
        cd_ref_sales = pd.DataFrame(columns = ['REBATENO','CLM_VAL'])
    df_merge = df_merge.sort_values(by=['BRANDID','ITEMIDSKU','ITEMNAME','UOM_QTY','DATE1','STATE']).reset_index(drop= True)
    df_sales = pd.concat([df_merge,cd_ref_sales],axis=1)
    dict_data_sales = {'df':df_sales,'cell_export':'B606'}
    dict_remove_sales = {'count_df':len(df_sales),'length_start':606,'length_end':20606}
    list_data.append(dict_data_sales)
    list_remove.append(dict_remove_sales)
    return list_data,list_remove

def product_state_summary(df_merge,cd_ref):
    print('Start product_state_summary')
    list_data = []
    list_remove = []
    print(cd_ref)
    if cd_ref.empty == False:
        cd_ref_state = cd_ref.groupby(['BRANDID','ITEMID','UOM_QTY','STATE']).agg({'REBATENO':(lambda x: ', '.join(sorted(x.unique()))),'CLM_QTY':'sum','REBATE_ENTITLEMENT_NUM':'mean'}).reset_index()
    else:
        cd_ref_state = pd.DataFrame(columns = ['BRANDID','ITEMID','UOM_QTY','STATE','REBATENO','CLM_QTY','REBATE_ENTITLEMENT_NUM'])
    print(cd_ref_state)
    df_state = df_merge[['ITEMIDSKU','ITEMNAME','BRANDID','UOM_QTY','STATE']].drop_duplicates().sort_values(by=['ITEMIDSKU','ITEMNAME','BRANDID','UOM_QTY','STATE'])
    df_state.insert(2,'BLANK','')
    df_state_cd = pd.merge(df_state,cd_ref_state,left_on= ['ITEMIDSKU','BRANDID','UOM_QTY','STATE'],right_on= ['ITEMID','BRANDID','UOM_QTY','STATE'] ,how = 'left')
    df_state_cd_item = df_state_cd[['ITEMIDSKU','ITEMNAME','BLANK','BRANDID','UOM_QTY','STATE']]
    df_state_cd_rebateno = df_state_cd[['REBATENO','CLM_QTY','REBATE_ENTITLEMENT_NUM']]
    # Calculate number of rows
    number_rows_state = len(df_state_cd_item)
    dict_data_sku = {'df':df_state_cd_item,'cell_export':'B111'}
    dict_data_rebateno  = {'df':df_state_cd_rebateno,'cell_export':'O111'}
    dict_remove = {'count_df':number_rows_state,'length_start':111,'length_end':601}
    list_data.append(dict_data_sku)
    list_data.append(dict_data_rebateno)
    list_remove.append(dict_remove)
    print('Done product_state_summary')
    return list_data,list_remove

def product_summary(df_merge,cd_ref):
    print('Start product_summary')
    list_data = []
    list_remove = []
    cd_ref_item = cd_ref.groupby(['BRANDID','ITEMID','UOM_QTY']).agg({'REBATENO':(lambda x: ', '.join(sorted(x.unique())))}).reset_index()
    df_item = df_merge[['ITEMIDSKU','ITEMNAME','BRANDID','UOM_QTY']].drop_duplicates().sort_values(by=['ITEMIDSKU','ITEMNAME','BRANDID','UOM_QTY'])
    df_item.insert(2,'BLANK','')
    df_item_cd = pd.merge(df_item,cd_ref_item,left_on= ['ITEMIDSKU','BRANDID','UOM_QTY'],right_on= ['ITEMID','BRANDID','UOM_QTY'] ,how = 'left')
    df_item_cd = df_item_cd[['ITEMIDSKU','ITEMNAME','BLANK','BRANDID','UOM_QTY','REBATENO']]
    number_rows_sales = len(df_item_cd)
    # writer_excel(df = df_product,path_export_final = path_export_final, cell_export = 'B20',number_sheet = number_sheet,length_start=20 , count_df=number_rows_sales, length_end=116)
    dict_data = {'df':df_item_cd,'cell_export':'B8'}
    dict_remove = {'count_df':number_rows_sales,'length_start':8,'length_end':104}
    list_data.append(dict_data)
    list_remove.append(dict_remove)
    print('Done product_summary')
    return list_data , list_remove


# def remove_sheet_change_xlsb(sheet_name):
#     print('Start delete sheet & change to xlsb')
#     with xw.App(visible=False) as app:
#         wb = app.books.open(name_export)                
#         wb.sheets[sheet_name].delete()
#         wb.save(name_export_xlsb)
#     try:
#         os.remove(name_export)
#     except Exception as e:
#         print(e)
#     return print('Done delete sheet & change to xlsb')

def rebate_item(df_merge,cd_ref):
    itemid_name = df_merge[['ITEMIDSKU','ITEMNAME']].drop_duplicates()
    itemid_name = itemid_name['ITEMIDSKU'] + ' - ' +itemid_name['ITEMNAME']
    itemid_name = ', '.join(itemid_name)
    if cd_ref.empty:
        notes = f'PROMO - The vendor agreed to support scan for item {itemid_name} during this time. However according to our records, no funding has been charged. Please see sales data and email evidence for more information.'
    else:
        rebateno = cd_ref['REBATENO'].drop_duplicates().sort_values().tolist()
        rebateno = ', '.join(rebateno)
        notes = f'PROMO - The vendor agreed to support scan for item {itemid_name} during this time. Rebateno {rebateno} were raised in an attempt to claim for the scan funding due, however according to our records there were several promotional units sold that were missed from this invoice and did not receive the agreed scan funding. Please see sales data and email evidence for more information.'
    dict_data_notes = {'df': notes,'cell_export':'N4'}
    return dict_data_notes

def category_id(cursor,list_data_item):
    item_brandid_check_cat = list_data_item[0]['df'][['ITEMIDSKU','BRANDID']] 
    item_cat = item_brandid_check_cat['ITEMIDSKU'][0]
    brandid_cat = item_brandid_check_cat['BRANDID'][0]
    cat_name_check = connect_sql(cursor,file_sql_check_category_name,item_cat,'')['ITEMGROUP'][0]
    cat_id_check = connect_sql(cursor,file_sql_check_category_id,cat_name_check,brandid_cat)
    cat_id_check = cat_id_check['CATEGORY_ID'][0]
    dict_data_cat = {'df': cat_id_check,'cell_export':'C4'}
    return dict_data_cat


def main():
    os.chdir('D:\\python\\cl_summarizer')
    print('START')
    cursor = set_up(config = config_coles)
    excel_file = pd.ExcelFile(path_import_item)
    count_sheets_excel_file = len(excel_file.sheet_names)
    df_excel_get_item = pd.read_excel(path_import_item,sheet_name='1')
    df_excel_get_item = df_excel_get_item['ITEMID'].drop_duplicates()[0]
    ven_check = connect_sql(cursor,file_sql_get_ven_id_name,df_excel_get_item,'')
    supp_desc = ven_check['VEN_NAME'][0]
    vendor_num = ven_check['VEN_ID'][0]
    path_export_final_morethan20k = f'CL_SCAN_{supp_desc}_{analyst_name}_{date_batch}.xlsx'
    path_export_final_lessthan20k = f'CL_SCAN_{supp_desc}_{analyst_name}_{date_batch}_LESSTHAN20K.xlsx'
    path_export_final_morethan20k_xlsb = f'CL_SCAN_{supp_desc}_{analyst_name}_{date_batch}.xlsb'
    path_export_final_lessthan20k_xlsb = f'CL_SCAN_{supp_desc}_{analyst_name}_{date_batch}_LESSTHAN20K.xlsb'

    dict_classify = {}
    j = 1
    k = 1
    for i in range(1,count_sheets_excel_file+1):
        df_merge,cd_ref = df_sales_data(cursor,i)
        # print(df_merge)
        # df_Sales
        list_data_sales,list_remove_sales = df_sales_data_cd(df_merge,cd_ref=cd_ref)
        # return 0
        # df_state
        list_data_state,list_remove_state = product_state_summary(df_merge,cd_ref=cd_ref)
        # return 0 
        #df_item
        list_data_item,list_remove_item = product_summary(df_merge,cd_ref)
        # notes:
        dict_data_notes = rebate_item(df_merge,cd_ref)
        # category id 
        dict_data_category_id = category_id(cursor,list_data_item)
        # item
        dict_sheet_id_less = {'df': j,'cell_export':'B4'}
        dict_sheet_id_more = {'df': k,'cell_export':'B4'}
        #classify 20k
        sum_eli = df_merge['ELI_CLAIM'].sum()
        sum_claimed = cd_ref['CLM_VAL'].sum()
        list_data = list_data_sales  + list_data_state + list_data_item + [dict_data_notes] + [dict_data_category_id]
        list_remove =  list_remove_sales + list_remove_state + list_remove_item
        if float(sum_eli)- float(sum_claimed) > 20000:
            if 'MORETHAN20K' not in dict_classify.keys():
                dict_classify['MORETHAN20K'] =  [[list_data + [dict_sheet_id_less]] + [list_remove]]
            else: 
                dict_classify['MORETHAN20K'].append([list_data + [dict_sheet_id_less]] + [list_remove]) 
            j+=1
        else:
            if 'LESSTHAN20K' not in dict_classify.keys():
                dict_classify['LESSTHAN20K'] = [[list_data + [dict_sheet_id_more]] + [list_remove]]
            else: 
                dict_classify['LESSTHAN20K'].append([list_data + [dict_sheet_id_more]] + [list_remove])
            k += 1
        i+= 1
    for key in dict_classify:
        if key == 'LESSTHAN20K':
            i_1 = 1
            for df_element in dict_classify['LESSTHAN20K']:
                create_worksheet(index_promo=str(i_1),path_export_final=path_export_final_lessthan20k)
                writer_excel(data = df_element[0], remove = df_element[1],number_sheet= str(i_1),path_export_final=path_export_final_lessthan20k) 
                i_1+= 1
            fill_summary_sheet(supp_desc,summary_index_list = i_1,path_export_final =path_export_final_lessthan20k ,vendor_num = vendor_num)
            remove_sheet_change_xlsb('template',path_export_final_lessthan20k,path_export_final_lessthan20k_xlsb)
        else:
            i_2 = 1
            for df_element in dict_classify['MORETHAN20K']:
                create_worksheet(index_promo=str(i_2),path_export_final=path_export_final_morethan20k)
                writer_excel(data = df_element[0], remove = df_element[1],number_sheet= str(i_2),path_export_final=path_export_final_morethan20k) 
                i_2+= 1
            fill_summary_sheet(supp_desc,summary_index_list = i_2,path_export_final =path_export_final_morethan20k ,vendor_num = vendor_num)
            remove_sheet_change_xlsb('template',path_export_final_morethan20k,path_export_final_morethan20k_xlsb)
    print('ENDDDDD')

if __name__ == '__main__':
    main()
    