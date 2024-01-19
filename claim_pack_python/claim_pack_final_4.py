import json
import pandas as pd
import snowflake.connector as sf
import os
import xlwings as xw
from xlwings.constants import DeleteShiftDirection
import datetime
pd.options.mode.chained_assignment = None

current_dir = os.getcwd()
os.chdir("D:\\python\\claim_pack_python")
###### Analyst fill
folder_name = '202105'

###############################################
config_coles = r"config.json"

file_sql_claimpack = r"claim_pack_state.sql"
file_sql_summ = r"summarizer.sql"
file_sql_cd_ref = r"cd_ref.sql"
file_sql_cd_check_cole_online = r"cd_check_cole_online.sql"
file_sql_ven_stop_trading = r"check_ven_stop_trading.sql"
file_sql_cd_check_prgx = r"cd_check_prgx.sql"
file_sql_cd_ref_listagg = r"cd_ref_listagg.sql"
file_sql_cd_ref_listagg_item = r"cd_ref_listagg_item.sql"
file_sql_check_prof = r"check_profectus_detail.sql"

path_check_list = fr"D:\\python\\claim_pack_python\\claim_qty\\{folder_name}\\checklist.xlsx"
path_check_list_promo = fr"D:\\python\\claim_pack_python\\claim_qty\\{folder_name}\\check_list_promo.xlsx"

path_export = fr"D:\\python\\claim_pack_python\\claim_qty\\{folder_name}\\"
path_excel = r"CS_SCAN_Vendorname_Analyst_Date.xlsx"
path_dna = r"DNA.xlsx"



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
def connect_sql(cursor,file_sql,item_code=0,var_1=0,var_2=0,var_3=0,var_4=0):
    try:
        # cursor.execute((open(file_sql).read()))
        cursor.execute((open(file_sql).read()).format(item_code,var_1,var_2,var_3,var_4))
        all_rows = cursor.fetchall()
        field_names = [i[0] for i in cursor.description]
    finally:
        pass
        # conn.close()
    df = pd.DataFrame(all_rows)
    try:
        df.columns = field_names
    except ValueError:
        return pd.DataFrame([])
    return df

def convert_to_input_sql(num_list):
    num_list_final = ''
    # print('SUPP LIST',supp_num_list)
    for num_list in num_list:
        num_list_final = num_list_final + "'" + num_list + "',"
    return num_list_final[:-1]

def convert_to_input_function(num_list):
    num_list_final = ''
    # print('SUPP LIST',supp_num_list)
    for num_list in num_list:
        num_list_final = num_list_final + num_list + ','
    return num_list_final[:-1]

def get_info(df_splited):
    supp_num_list = list(df_splited['SUPPLIER'].drop_duplicates())
    item_list = list(df_splited['SKU_ID'].drop_duplicates())

    supp_num_list_final = convert_to_input_sql(num_list = supp_num_list)
    item_list_final = convert_to_input_sql(num_list = item_list)
    item_input_function = convert_to_input_function(num_list = item_list)
    return supp_num_list_final,item_list_final,item_input_function

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
        for sheet_name in sheet_df_mapping.keys():
            if sheet_name in current_sheets:
                for df_data in data :
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

def df_sales_data(cursor, item_list_dict_gsted,file_sql,start_date,end_date):
    i = 0
    for key,value in item_list_dict_gsted.items():
        print(key,value)
        df_each_item = connect_sql(cursor = cursor,file_sql = file_sql ,item_code = key,var_1 = start_date,var_2=end_date,var_3=value[0],var_4=value[1])
        if i == 0:
            df_merge = df_each_item
        else :
            df_merge = pd.concat([df_merge, df_each_item], ignore_index=True)
        i+=1
    df_merge['ELI_CLAIM'] = df_merge.RQTY_PROMO * df_merge.SCAN_RATE
    return df_merge

def product_state_summary(df,df_ref):
    print('Start product_state_summary')
    list_data = []
    list_remove = []
    # Find distict item_code and state
    # writer_excel(df,cell_export,length_start,count_df,length_end,number_sheet,path_export_final)
    df_temp =df.drop_duplicates(['RSKU_ID','RITEM_DESC','RSTATE'])[['RSKU_ID','RITEM_DESC','RSTATE']]
    df_temp_2 = pd.merge(df_temp,df_ref,left_on=['RSKU_ID','RSTATE'],right_on=['ITEM_IDNT','CLM_STATE'], how='left')
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

# def product_state_df(df_sales,df_ref):
#     df_ref_groupby = df_ref.groupby('CLM_REF_NUM').agg({'CLM_PRODUCT':'sum'}).sort_values(by='CLM_PRODUCT', ascending=True).reset_index()
#     ref_num = ', '.join(df_ref_groupby['CLM_REF_NUM'])
#     df_ref_groupby_qty = df_ref[['ITEM_IDNT','CLM_STATE','CLM_QTY','CLM_RATE']].groupby(['ITEM_IDNT','CLM_STATE']).agg({'CLM_QTY':'sum','CLM_RATE':'mean'}).sort_values(by=['ITEM_IDNT','CLM_STATE'], ascending=True).reset_index()
#     df_ref_groupby_qty.insert(1,"REF_NUM",ref_num)
#     df_temp = pd.merge(df_sales,df_ref_groupby_qty,left_on=['RSKU_ID','RSTATE'],right_on=['ITEM_IDNT','CLM_STATE'], how='left')
#     df_final = df_temp[['RSKU_ID','RITEM_DESC','RSTATE','REF_NUM','CLM_QTY','CLM_RATE']]
#     return df_final

def product_summary(df,df_item_ref):
    print('Start product_summary')
    list_data = []
    list_remove = []
    df_product =df.drop_duplicates(['RSKU_ID','RITEM_DESC'])[['RSKU_ID','RITEM_DESC']]
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

def cd_ref(prmt_id,cursor,file_sql,item_code,df_sales, file_sql_2,file_sql_3):
    print('Start cd ref')
    list_data = []
    list_remove = []
    df_ref = connect_sql(cursor=cursor,file_sql = file_sql ,item_code = item_code,var_1 = prmt_id)
    df_ref_groupby = df_ref.groupby('CLM_REF_NUM').agg({'CLM_PRODUCT':'sum'}).sort_values(by='CLM_PRODUCT', ascending=True).reset_index()
    df_sales_daily = pd.concat([df_sales, df_ref_groupby], axis=1 )
    print('Done cd ref')
    print('start state ref')
    df_state_ref = connect_sql(cursor=cursor,file_sql = file_sql_2 ,item_code = item_code,var_1 = prmt_id)
    print('done state ref')
    print('start item ref')
    df_item_ref = connect_sql(cursor=cursor,file_sql = file_sql_3 ,item_code = item_code,var_1 = prmt_id)
    print('done item ref')
    # writer_excel(df = df_sales, cell_export = 'B174',number_sheet= str(index_promo)+'_'+str(gst),length_start=174 ,count_df=len(df_sales), length_end=10174,path_export_final=path_export_final)
    dict_data = {'df':df_sales_daily,'cell_export':'B606'}
    dict_remove = {'count_df':len(df_sales),'length_start':606,'length_end':20606}
    list_data.append(dict_data)
    list_remove.append(dict_remove)
    return df_item_ref,df_state_ref,df_ref,df_sales_daily,list_data,list_remove
  
def main():
    cursor = set_up(config = config_coles)
    df_raw = connect_sql(cursor = cursor,file_sql = file_sql_claimpack,item_code=0,var_1=0,var_2=0,var_3=0,var_4=0)
    df_unique_supp = df_raw[['SUPPLIER','PRMTN_COMP_IDNT']].drop_duplicates().values.tolist()
    # Read DNA file
    df_dna = pd.read_excel(path_dna,sheet_name='DNA')
    list_dna = df_dna['DNA'].drop_duplicates().values.tolist()
    # Read vendor stop trading
    df_ven_stop_trading = connect_sql(cursor = cursor,file_sql = file_sql_ven_stop_trading)
    list_ven_stop_trading = df_ven_stop_trading['RMS_NUM'].drop_duplicates().values.tolist()
    # Create dictionary with supp_num key and list of promo_ids
    dict_sup_pro = {}
    i=0
    for list_sup in df_unique_supp:
        if i == 0:
            dict_sup_pro[list_sup[0]] = [list_sup[1]]
        else:
            if list_sup[0] in dict_sup_pro.keys():
                dict_sup_pro[list_sup[0]].append(list_sup[1])
            else:
                dict_sup_pro[list_sup[0]] = [list_sup[1]]
        i+=1

    j = 0
    for supp_num,list_pmt_id in dict_sup_pro.items():
        supp_num_convert = convert_to_input_sql(num_list=[supp_num])
        # To classify Check_column
        for pmt_id in list_pmt_id:
            df_splited = df_raw[(df_raw['SUPPLIER'] == supp_num) & (df_raw['PRMTN_COMP_IDNT'] == pmt_id)]   
            # Classify Check_column
            if int(supp_num) in list_dna:
                df_splited['CHECK_COLUMN'] = 'DNA'
            elif str(supp_num_convert) in list_ven_stop_trading:
                df_splited['CHECK_COLUMN'] = 'VENDOR STOP TRADING'
            elif df_splited['ELI_FINAL'].sum() < 100 :
                df_splited['CHECK_COLUMN'] = 'ELI_FINAL < 100'
            # elif df_splited['ELI2'].sum() <= 0 :
            #     df_splited['CHECK_COLUMN'] = 'ELI2 <= 0'
            # elif df_splited['ELI3'].sum() <= 0 :
            #     df_splited['CHECK_COLUMN'] = 'ELI3 <= 0'
            elif '1' not in df_splited['OVERLAP_PROMO'].unique().astype(str): 
                df_splited['CHECK_COLUMN'] = 'OVERLAP'
            elif df_splited['CLM_QTY'].sum() <= 0 :
                df_splited['CHECK_COLUMN'] = 'CLM_QTY <= 0'
            else:
                # if df_splited['SKU_ID'].count() == df_splited['SKU_ID'].drop_duplicates().count():
                    supp_num_list_final,item_list_final,item_input_function = get_info(df_splited = df_splited)
                    clm_end_check = df_splited['CLM_END'].unique()[0].astype(str)
                    clm_start_check = df_splited['CLM_START'].unique()[0].astype(str)
                    # print(clm_end_check)
                    clm_end_check_converted = datetime.datetime.strptime(clm_end_check,'%Y-%m-%dT%H:%M:%S.000%f')
                    # print(clm_end_check_converted)
                    clm_end_check_converted_2 = int(datetime.datetime.timestamp(clm_end_check_converted)) * 10**9
                    df_cd_check_cole_online = connect_sql(cursor = cursor,file_sql = file_sql_cd_check_cole_online,item_code=item_list_final)
                    df_cd_check_prgx = connect_sql(cursor = cursor,file_sql = file_sql_cd_check_prgx,item_code=item_list_final)
                    if df_cd_check_cole_online.empty:
                        flag_co = True
                    else:
                        df_list = df_cd_check_cole_online[['CLM_START','CLM_END']].values.tolist()
                        flag_co = True
                        for sub_list in df_list:
                            if (clm_end_check_converted_2 >= sub_list[0] and clm_end_check_converted_2 <= sub_list[1]):
                                flag_co = False
                            else:
                                pass
                    if flag_co :
                        if df_cd_check_prgx.empty:
                            flag_prgx = True
                        else:
                            df_list_prgx = df_cd_check_prgx[['CLM_START','CLM_END']].values.tolist()
                            flag_prgx = True
                            for sub_list in df_list_prgx:
                                if (clm_end_check_converted_2 >= sub_list[0] and clm_end_check_converted_2 <= sub_list[1]):
                                    flag_prgx = False
                                else:
                                    pass
                        if flag_prgx:
                            df_check_prof = connect_sql(cursor=cursor, file_sql=file_sql_check_prof,item_code=item_list_final, var_1 =  clm_start_check, var_2 = clm_end_check)
                            if df_check_prof.empty:
                                if df_splited[['SKU_ID','CLM_RATE','PROMO_PRICE']].drop_duplicates()['SKU_ID'].count() == df_splited['SKU_ID'].drop_duplicates().count():
                                    df_splited['CHECK_COLUMN'] = 'TO QA_NATIONAL'
                                else:
                                    df_splited['CHECK_COLUMN'] = 'TO QA_STATE'
                                # if df_splited['ELI_EXCLUDE_CO_BEFORE'].sum() < 100 :
                                #     df_splited['CHECK_COLUMN_EXCLUDE_CO_BEFORE'] = 'EXCLUDE_CO < 100'
                                # else:
                                #     df_splited['CHECK_COLUMN_EXCLUDE_CO_BEFORE'] = 'EXCLUDE_CO >= 100'
                            else:
                                df_splited['CHECK_COLUMN'] = 'PROFECTUS CLAIMED'
                        else:
                            df_splited['CHECK_COLUMN'] = 'PRGX'
                    else:
                        df_splited['CHECK_COLUMN'] = 'COLES ONLINE'
                # else:
                #     df_splited['CHECK_COLUMN'] = 'CHECK AGAIN'
            if j  == 0:
                df_raw_check = df_splited
            else:
                df_raw_check = pd.concat([df_raw_check, df_splited], ignore_index=True)
            j+=1
            print(df_splited)
    #Export checklist
    df_raw_check.to_excel(path_check_list,index=False)
    ###############################################################
    error_list =[]
    time_start = datetime.datetime.now()
    # Filter df_splited with condition , keep TO QA and PRGX
    df_raw_filter = df_raw_check[(df_raw_check['CHECK_COLUMN'] == 'TO QA_NATIONAL') | (df_raw_check['CHECK_COLUMN'] == 'PRGX')] 
    df_unique_supp_filter = df_raw_filter[['SUPPLIER','PRMTN_COMP_IDNT']].drop_duplicates().values.tolist()
    # Create dictionary with supp_num key and list of promo_ids after filter conditions, keep check again and to QA
    dict_sup_pro_filter = {}
    j=0
    for list_sup in df_unique_supp_filter:
        if j == 0:
            dict_sup_pro_filter[list_sup[0]] = [list_sup[1]]
        else:
            if list_sup[0] in dict_sup_pro_filter.keys():
                dict_sup_pro_filter[list_sup[0]].append(list_sup[1])
            else:
                dict_sup_pro_filter[list_sup[0]] = [list_sup[1]]
        j+=1

    check_list_promo_index = 1
    for supp_num,list_pmt_id in dict_sup_pro_filter.items():
        index_promo=1
        # summary_index = 1
        summary_index_list = []
        for pmt_id in list_pmt_id:
            print('-------------------------------------------------------------------------------------------------------------------------------------')
            check_list_promo = []
            df_splited_filter = df_raw_filter[(df_raw_filter['SUPPLIER'] == supp_num) & (df_raw_filter['PRMTN_COMP_IDNT'] == pmt_id)] 
            # get some important variable
            supp_num_list_final,item_list_final,item_input_function = get_info(df_splited = df_splited_filter)
            supp_desc = df_splited_filter['SUPP_DESC'].unique()[0].replace("/","")
            print('supp_desc',supp_desc)
            gst = df_splited_filter['CML_COST_GST_RATE_PCT'].unique()[0]
            dept = df_splited_filter['DEPT_IDNT'].unique()[0]
            prmt_id = df_splited_filter['PRMTN_COMP_IDNT'].unique()[0]
            prmt_name = df_splited_filter['PRMTN_COMP_NAME'].unique()[0] 
            try:
                paf_loc = df_splited_filter['PAF_LOCATION'].unique()[0]
            except Exception :
                paf_loc = '0'
            try:
                email_loc = df_splited_filter['EMAIL'].unique()[0]
            except Exception :
                email_loc = '0'
            vendor_num = df_splited_filter['VENDOR_NUM'].unique()[0] 
            clm_start = df_splited_filter['CLM_START'].unique()[0].astype(str)
            clm_end = df_splited_filter['CLM_END'].unique()[0].astype(str)
            clm_start_converted = datetime.datetime.strptime(clm_start,'%Y-%m-%dT%H:%M:%S.000%f').strftime('%d/%m/%Y')
            clm_end_converted = datetime.datetime.strptime(clm_end,'%Y-%m-%dT%H:%M:%S.000%f').strftime('%d/%m/%Y')
            #create path for excel and path_xlsb for excel
            path_export_final = path_export+'CS_SCAN_'+supp_desc+'_Analyst_date.xlsx'
            path_export_final_xlsb = path_export+'CS_SCAN_'+supp_desc+'_Analyst_date_'+supp_num+'.xlsb'
            create_worksheet(index_promo=index_promo,gst=gst,path_export_final=path_export_final)
            # create dictionary with keY sku_id and value is list rrp & scan
            if df_splited_filter[['PRM_PRICE','CLM_RATE']].drop_duplicates().count()['PRM_PRICE'] == 1 :
                item_list_dict = {}
                prm_price = df_splited_filter['PRM_PRICE'].unique()[0]
                clm_rate = df_splited_filter['CLM_RATE'].unique()[0]
                item_list_dict[item_input_function] = [prm_price] + [clm_rate]
            else:   
                item_list_dict = df_splited_filter.set_index('SKU_ID')[['PRM_PRICE','CLM_RATE']].to_dict('index')
                for key,value in item_list_dict.items():
                    item_list_dict[key] = [item_list_dict[key]['PRM_PRICE']] + [item_list_dict[key]['CLM_RATE']] 
            # To create excel file
            summary_index_list.append(str(index_promo)+'_'+str(gst))
            # writer_excel_without_remove_rows(df = df_splited,path = path_export_final,cell_export = 'A1',number_sheet=str(index_promo)+'_'+str(gst),path_export_final=path_export_final)   
            df_sales = df_sales_data(cursor = cursor,item_list_dict_gsted = item_list_dict,file_sql = file_sql_summ ,start_date = clm_start_converted,end_date = clm_end_converted)   
            # df_ref,df_sales = cd_ref(prmt_id=prmt_id,cursor = cursor,file_sql=file_sql_cd_ref,item_code=item_list_final,df_sales=df_sales)  
            df_item_ref,df_state_ref,df_ref,df_sales,list_data_sales,list_remove_sales  = cd_ref(prmt_id=prmt_id,cursor = cursor,file_sql=file_sql_cd_ref,item_code=item_list_final,df_sales=df_sales,file_sql_2 = file_sql_cd_ref_listagg, file_sql_3 = file_sql_cd_ref_listagg_item)  
            list_data_state,list_remove_state = product_state_summary(df = df_sales,df_ref=df_state_ref)
            list_data_product ,list_remove_product  = product_summary(df = df_sales, df_item_ref = df_item_ref)
            dict_data_dept = {'df':dept,'cell_export':'F8'}
            dict_data_prmt_id = {'df':prmt_id,'cell_export':'B12'}
            dict_data_prmt_name = {'df':prmt_name,'cell_export':'C12'}
            dict_data_paf_loc = {'df':paf_loc,'cell_export':'J4'}
            dict_data_email_loc = {'df':email_loc,'cell_export':'K4'}
            dict_data_supp_num = {'df':supp_num,'cell_export':'E8'}
            dict_data_supp_desc = {'df':supp_desc,'cell_export':'C8'}
            dict_data_vendor_num = {'df':vendor_num,'cell_export':'D8'}
            dict_data_claim_number = {'df':str(index_promo)+'_'+str(gst),'cell_export':'B16'}
            list_data = list_data_sales + list_data_state + list_data_product + [dict_data_dept] + [dict_data_prmt_id] + [dict_data_prmt_name] + [dict_data_paf_loc] + [dict_data_email_loc] + [dict_data_supp_num] + [dict_data_supp_desc] + [dict_data_vendor_num] + [dict_data_claim_number]
            list_remove = list_remove_sales + list_remove_state + list_remove_product
            #  Fill sheet Complete Daily Sales Data
            writer_excel(data = list_data, remove = list_remove,number_sheet= str(index_promo)+'_'+str(gst),path_export_final=path_export_final)  
            index_promo+=1
            check_list_promo_index += 1 
            #export check_list_promo
            with xw.App(visible=False) as app:
                print('check_list_promo_index')
                if check_list_promo_index == 2:
                    wb = app.books.open('check_list_promo.xlsx')
                else:
                    wb = app.books.open(path_check_list_promo)
                wb_sheet = wb.sheets['Sheet1']
                check_list_promo = [supp_num] + [supp_desc] +[prmt_id] + [clm_start] + [clm_end]+ [dept] + ['Done'] 
                print(check_list_promo)
                wb_sheet.range(f'A{check_list_promo_index}').value =  check_list_promo
                wb.save(path_check_list_promo)              
        # Fill sheet Vendor Summary
        fill_summary_sheet(summary_index_list= summary_index_list,path_export_final=path_export_final)         
        remove_sheet_change_xlsb(sheet_name = 'template',path_export_final=path_export_final ,path_export_final_xlsb = path_export_final_xlsb)  
        print('-------------------------------------------------------------------------------------------------------------------------------------')
    print(datetime.datetime.now() - time_start)  

if __name__ == '__main__':
    main()
    