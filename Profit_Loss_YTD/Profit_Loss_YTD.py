from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import date,datetime,timedelta
import psycopg2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import openpyxl
# pd.set_option('display.float_format', lambda x: '%.3f' % x)
########4###########################

def get_gl_details(zid,year,smonth,emonth):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""select glmst.zid, glmst.xacc,glheader.xyear, glheader.xper,SUM(gldetail.xprime)
                        FROM glmst
                        JOIN
                        gldetail
                        ON glmst.xacc = gldetail.xacc
                        JOIN
                        glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glmst.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND glheader.zid = '%s'
                        AND (glmst.xacctype = 'Income' OR glmst.xacctype = 'Expenditure')
                        AND glheader.xyear = '%s'
                        AND glheader.xper >= '%s'
                        AND glheader.xper <= '%s'
                        GROUP BY glmst.zid, glmst.xacc, glmst.xacctype, glmst.xhrc1, glmst.xhrc2, glheader.xyear, glheader.xper
                        ORDER BY glheader.xper ASC , glmst.xacctype"""%(zid,zid,zid,year,smonth,emonth),con = engine)
    return df

def get_gl_details_project(zid,project,year,smonth,emonth):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""select glmst.zid, glmst.xacc, glheader.xyear, glheader.xper,SUM(gldetail.xprime)
                        FROM glmst
                        JOIN
                        gldetail
                        ON glmst.xacc = gldetail.xacc
                        JOIN
                        glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glmst.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND glheader.zid = '%s'
                        AND gldetail.xproj = '%s'
                        AND (glmst.xacctype = 'Income' OR glmst.xacctype = 'Expenditure')
                        AND glheader.xyear = '%s'
                        AND glheader.xper >= '%s'
                        AND glheader.xper <= '%s'
                        GROUP BY glmst.zid, glmst.xacc, glmst.xacctype, glmst.xhrc1, glmst.xhrc2, glheader.xyear, glheader.xper
                        ORDER BY glheader.xper ASC , glmst.xacctype"""%(zid,zid,zid,project,year,smonth,emonth),con = engine)
    return df

def get_gl_details_bs(zid,year,emonth):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""select glmst.zid, glmst.xacc, glheader.xyear, glheader.xper,SUM(gldetail.xprime)
                        FROM glmst
                        JOIN
                        gldetail
                        ON glmst.xacc = gldetail.xacc
                        JOIN
                        glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glmst.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND glheader.zid = '%s'
                        AND (glmst.xacctype = 'Asset' OR glmst.xacctype = 'Liability')
                        AND glheader.xyear = '%s'
                        AND glheader.xper <= '%s'
                        GROUP BY glmst.zid, glmst.xacc, glheader.xyear, glheader.xper"""%(zid,zid,zid,year,emonth),con = engine)
    return df

def get_gl_details_bs_project(zid,project,year,emonth):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""select glmst.zid, glmst.xacc, glmst.xacctype, glmst.xhrc1, glmst.xhrc2, glmst.xaccusage, glheader.xyear, glheader.xper,SUM(gldetail.xprime)
                        FROM glmst
                        JOIN
                        gldetail
                        ON glmst.xacc = gldetail.xacc
                        JOIN
                        glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glmst.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND glheader.zid = '%s'
                        AND gldetail.xproj = '%s'
                        AND (glmst.xacctype = 'Asset' OR glmst.xacctype = 'Liability')
                        AND glheader.xyear = '%s'
                        AND glheader.xper <= '%s'
                        GROUP BY glmst.zid, glmst.xacc, glmst.xacctype, glmst.xhrc1, glmst.xhrc2, glheader.xyear, glheader.xper
                        ORDER BY glheader.xper ASC , glmst.xacctype"""%(zid,zid,zid,project,year,emonth),con = engine)
    return df

def get_gl_master(zid):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT xacc, xdesc, xacctype, xhrc1, xhrc2, xhrc3, xhrc4 FROM glmst WHERE glmst.zid = %s"""%(zid),con=engine)
    return df

def get_gl_details_ap_project(zid,project,year,xacc,emonth,sup_list):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    if isinstance(sup_list,tuple):
        df1 = pd.read_sql("""SELECT 'INTERNAL',SUM(gldetail.xprime)
                            FROM glmst
                            JOIN
                            gldetail
                            ON glmst.xacc = gldetail.xacc
                            JOIN
                            glheader
                            ON gldetail.xvoucher = glheader.xvoucher
                            WHERE glmst.zid = '%s'
                            AND gldetail.zid = '%s'
                            AND glheader.zid = '%s'
                            AND gldetail.xproj = '%s'
                            AND glmst.xacc = '%s' 
                            AND glheader.xyear = '%s'
                            AND glheader.xper <= '%s'
                            AND gldetail.xsub IN %s"""%(zid,zid,zid,project,xacc,year,emonth,sup_list),con = engine)
        df2 = pd.read_sql("""SELECT 'EXTERNAL',SUM(gldetail.xprime)
                            FROM glmst
                            JOIN
                            gldetail
                            ON glmst.xacc = gldetail.xacc
                            JOIN
                            glheader
                            ON gldetail.xvoucher = glheader.xvoucher
                            WHERE glmst.zid = '%s'
                            AND gldetail.zid = '%s'
                            AND glheader.zid = '%s'
                            AND gldetail.xproj = '%s'
                            AND glmst.xacc = '%s' 
                            AND glheader.xyear = '%s'
                            AND glheader.xper <= '%s'
                            AND gldetail.xsub NOT IN %s"""%(zid,zid,zid,project,xacc,year,emonth,sup_list),con = engine)
    else:
        df1 = pd.read_sql("""SELECT 'EXTERNAL',SUM(gldetail.xprime)
                            FROM glmst
                            JOIN
                            gldetail
                            ON glmst.xacc = gldetail.xacc
                            JOIN
                            glheader
                            ON gldetail.xvoucher = glheader.xvoucher
                            WHERE glmst.zid = '%s'
                            AND gldetail.zid = '%s'
                            AND glheader.zid = '%s'
                            AND gldetail.xproj = '%s'
                            AND glmst.xacc = '%s' 
                            AND glheader.xyear = '%s'
                            AND glheader.xper <= '%s'
                            AND gldetail.xsub != '%s'"""%(zid,zid,zid,project,xacc,year,emonth,sup_list),con = engine)
        df2 = pd.read_sql("""SELECT 'INTERNAL',SUM(gldetail.xprime)
                            FROM glmst
                            JOIN
                            gldetail
                            ON glmst.xacc = gldetail.xacc
                            JOIN
                            glheader
                            ON gldetail.xvoucher = glheader.xvoucher
                            WHERE glmst.zid = '%s'
                            AND gldetail.zid = '%s'
                            AND glheader.zid = '%s'
                            AND gldetail.xproj = '%s'
                            AND glmst.xacc = '%s' 
                            AND glheader.xyear = '%s'
                            AND glheader.xper <= '%s'
                            AND gldetail.xsub = '%s'"""%(zid,zid,zid,project,xacc,year,emonth,sup_list),con = engine)
    df = pd.concat([df1,df2],axis=0)
    return df

ap_dict =  {100000:['Karigor Ltd.','9030001',('SUP-000003','SUP-000004','SUP-000060','SUP-000061')],
            100001:['GULSHAN TRADING','09030001',('SUP-000001','SUP-000002','SUP-000003','SUP-000004','SUP-000010','SUP-000014','SUP-000020','SUP-000027','SUP-000049','SUP-000057')],
            100002:['Gulshan Chemical','09030001','SUP-000011'],
            100003:['Gulshan Thread Tape','09030001',('SUP-000001','SUP-000002','SUP-000010')],
            100004:['Gulshan Plastic','09030001','SUP-000001'],
            100005:['Zepto Chemicals','09030001',('SUP-000006','SUP-000011','SUP-000012','SUP-000016')],
            100006:['HMBR Grocery Shop','09030001',('SUP-000006','SUP-000003')],
            100007:['HMBR Paint Roller Co.','09030001',('SUP-000001','SUP-000005')],
            100008:['Steel Scrubber Co.','09030001','SUP-000010'],
            100009:['Gulshan Packaging','09030001','SUP-000002']}

    
income_statement_label = {'04-Cost of Goods Sold':'02-1-Cost of Revenue',
'0401-DIRECT EXPENSES':'07-1-Other Operating Expenses, Total',
'0401-PURCHASE':'07-1-Other Operating Expenses, Total',
'0501-OTHERS DIRECT EXPENSES':'07-1-Other Operating Expenses, Total',
'0601-OTHERS DIRECT EXPENSES':'07-1-Other Operating Expenses, Total',
'0631- Development Expenses':'07-1-Other Operating Expenses, Total',
'06-Office & Administrative Expenses':'03-1-Office & Administrative Expenses',
'0625-Property Tax & Others':'09-1-Income Tax & VAT',
'0629- HMBR VAT & Tax Expenses':'09-1-Income Tax & VAT',
'0629-VAT & Tax Expenses':'09-1-Income Tax & VAT',
'0630- Bank Interest & Charges':'08-1-Interest Expense',
'0630-Bank Interest & Charges':'08-1-Interest Expense',
'0631-Other Expenses':'07-1-Other Operating Expenses, Total',
'0633-Interest-Loan':'08-1-Interest Expense',
'0636-Depreciation':'05-1-Depreciation/Amortization',
'07-Sales & Distribution Expenses':'04-1-Sales & Distribution Expenses',
'SALES & DISTRIBUTION EXPENSES':'04-1-Sales & Distribution Expenses',
'0701-MRP-Discount' : '04-2-MRP Discount',
'0702-Discount-Expense' : '04-3-Discount Expense',
'08-Revenue':'01-1-Revenue',
'14-Purchase Return':'06-1-Unusual Expenses (Income)',
'15-Sales Return':'06-1-Unusual Expenses (Income)',
'':'06-1-Unusual Expenses (Income)',
'Profit/Loss':'10-1-Net Income'}

income_label = pd.DataFrame(income_statement_label.items(),columns = ['xhrc4','Income Statement'])

balance_sheet_label = {
'0101-CASH & CASH EQUIVALENT':'01-3-Cash',
'0102-BANK BALANCE':'01-3-Cash',
'0103-ACCOUNTS RECEIVABLE':'02-1-Accounts Receivable',
'ACCOUNTS RECEIVABLE':'02-1-Accounts Receivable',
'0104-PREPAID EXPENSES':'04-1-Prepaid Expenses',
'0105-ADVANCE ACCOUNTS':'04-1-Prepaid Expenses',
'0106-STOCK IN HAND':'03-1-Inventories',
'02-OTHER ASSET':'05-1-Other Assets',
'0201-DEFFERED CAPITAL EXPENDITURE':'05-1-Other Assets',
'0203-LOAN TO OTHERS CONCERN':'05-1-Other Assets',
'0204-SECURITY DEPOSIT':'05-1-Other Assets',
'0205-LOAN TO OTHERS CONCERN':'05-1-Other Assets',
'0206-Other Investment':'05-1-Other Assets',
'0301-Lab Equipment':'06-1-Property, Plant & Equipment',
'0301-Office Equipment':'06-1-Property, Plant & Equipment',
'0302-Corporate Office Equipments':'06-1-Property, Plant & Equipment',
'0303-Furniture & Fixture':'06-1-Property, Plant & Equipment',
'0303-Lab Decoration':'06-1-Property, Plant & Equipment',
'0304-Trading Vehicles':'06-1-Property, Plant & Equipment',
'0305-Private Vehicles':'06-1-Property, Plant & Equipment',
'0305-Plants & Machinery':'06-1-Property, Plant & Equipment',
'0306- Plants & Machinery':'06-1-Property, Plant & Equipment',
'0307-Intangible Asset':'07-1-Goodwill & Intangible Asset',
'0308-Land & Building':'06-1-Property, Plant & Equipment',
'0901-Accrued Expenses':'09-1-Accrued Liabilities',
'0902-Income Tax Payable':'09-1-Accrued Liabilities',
'0903-Accounts Payable':'08-1-Accounts Payable',
'0904-Money Agent Liability':'10-1-Other Short Term Liabilities',
'0904-Reconciliation Liability':'10-1-Other Short Term Liabilities',
'0905-C & F Liability':'10-1-Other Short Term Liabilities',
'0906-Others Liability':'10-1-Other Short Term Liabilities',
'INTERNATIONAL PURCHASE TAX & COMMISSION':'10-1-Other Short Term Liabilities',
'1001-Short Term Bank Loan':'11-1-Debt',
'1002-Short Term Loan':'11-1-Debt',
'11-Reserve & Fund':'12-1-Other Long Term Liabilities',
'1202-Long Term Bank Loan':'11-1-Debt',
'13-Owners Equity':'13-1-Total Shareholders Equity'}

balance_label = pd.DataFrame(balance_sheet_label.items(),columns = ['xhrc4','Balance Sheet'])


### define business Id and date time year list for comparison (separate if project)
zid_list_hmbr = [100002,100003,100005,100006,100007,100008,100009]
# zid_list_fixit = [100000,100001,100002,100003]
zid_trade = 100001
zid_plastic = 100004
zid_karigor = 100000

project_trade = 'GULSHAN TRADING'

project_plastic = 'Gulshan Plastic'

project_karigor = 'Karigor Ltd.'

##### call SQL once and get the main data once into a dataframe (get the year and month as an integer)
start_year = int(input('input year like 2022------  '))


start_month = int(input('input from month eg: if january then 1------'))
end_month = int(input('input end month eg: if january then 1------  '))
  #need to be change if month change

### make a 3 year list
year_list = []
new_year = 0
for i in range(5):
    new_year = start_year - i
    year_list.append(new_year)
year_list.reverse()
    
#create master dataframe

    # in order for a proper debug we are going to do sum tests on each part of the project algorithm loop to find our why the merge is not working
    #that is exactly what is not working becuase the data behaves until then. 
main_data_dict_pl = {}
for i in zid_list_hmbr:
    df_master = get_gl_master(i)
    df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
#     df_main = get_gl_details(i,start_year,start_month,end_month)
#     df_main = df_main.groupby(['xacc'])['sum'].sum().reset_index().round(1).rename(columns={'sum':start_year})
    for item,idx in enumerate(year_list):
        df = get_gl_details(i,idx,start_month,end_month)
        df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
        if item == 0:
#             df_new = df_main.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
            df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
        else:
            df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    main_data_dict_pl[i] = df_new.merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

# df_trade = get_gl_details_project(zid_trade,project_trade,start_year,start_month,end_month)
# df_trade = df_trade.groupby(['xacc'])['sum'].sum().reset_index().round(1).rename(columns={'sum':start_year})
df_master = get_gl_master(zid_trade)
df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
for item,idx in enumerate(year_list):
    df = get_gl_details_project(zid_trade,project_trade,idx,start_month,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
main_data_dict_pl[zid_trade] = df_new.merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

# df_plastic = get_gl_details_project(zid_plastic,project_plastic,start_year,start_month,end_month)
# df_plastic = df_plastic.groupby(['xacc'])['sum'].sum().reset_index().round(1).rename(columns={'sum':start_year})
df_master = get_gl_master(zid_plastic)
df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
for item,idx in enumerate(year_list):
    df = get_gl_details_project(zid_plastic,project_plastic,idx,start_month,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
main_data_dict_pl[zid_plastic] = df_new.merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

df_master = get_gl_master(zid_karigor)
df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
for item,idx in enumerate(year_list):
    df = get_gl_details_project(zid_karigor,project_karigor,idx,start_month,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    print(df['sum'].sum(),'profit & loss')
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    print('kargor work is done')
main_data_dict_pl[zid_karigor] = df_new.merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

#create master dataframe



main_data_dict_bs = {}
for i in zid_list_hmbr:
    df_master = get_gl_master(i)
    df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
    for item,idx in enumerate(year_list):
        df = get_gl_details_bs(i,idx,end_month)
        df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
        if item == 0:
            df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':idx})
        else:
            df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':idx})
        main_data_dict_bs[i] = df_new.merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

# df_trade_bs = get_gl_details_bs_project(zid_trade,project_trade,start_year,start_month,end_month)
# df_trade_bs = df_trade_bs.groupby(['xacc','xdesc'])['sum'].sum().reset_index().round(1).rename(columns={'sum':start_year})
df_master = get_gl_master(zid_trade)
df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
for item,idx in enumerate(year_list):
    df = get_gl_details_bs_project(zid_trade,project_trade,idx,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
# df_mst = get_gl_master(i)
# df_new = df_new.merge(df_mst,on='xacc',how='left')
main_data_dict_bs[zid_trade] = df_new.merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

# df_plastic_bs = get_gl_details_bs_project(zid_plastic,project_plastic,start_year,start_month,end_month)
# df_plastic_bs = df_plastic_bs.groupby(['xacc','xdesc'])['sum'].sum().reset_index().round(1).rename(columns={'sum':start_year})
df_master = get_gl_master(zid_plastic)
df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
for item,idx in enumerate(year_list):
    df = get_gl_details_bs_project(zid_plastic,project_plastic,idx,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
# df_mst = get_gl_master(i)
# df_new = df_new.merge(df_mst,on='xacc',how='left')
main_data_dict_bs[zid_plastic] = df_new.merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

df_master = get_gl_master(zid_karigor)
df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
for item,idx in enumerate(year_list):
    df = get_gl_details_bs_project(zid_karigor,project_karigor,idx,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
# df_mst = get_gl_master(i)
# df_new = df_new.merge(df_mst,on='xacc',how='left')
main_data_dict_bs[zid_karigor] = df_new.merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

ap_final_dict = {}

data_ap = {'AP_TYPE':['INTERNAL','EXTERNAL']}

for k,v in ap_dict.items():
    df_ap = pd.DataFrame(data_ap)
    for item,idx in enumerate(year_list):
        zid = k
        project = v[0]
        acc = v[1]
        sup_list = v[2]

        df_1 = get_gl_details_ap_project(zid,project,idx,acc,end_month,sup_list).round(1).rename(columns={'?column?':'AP_TYPE','sum':idx}).fillna(0)

        df_ap = df_ap.merge(df_1,on='AP_TYPE',how='left')
        ap_final_dict[k] = df_ap

level_1_dict = {}
for key in main_data_dict_pl:
    level_1_dict[key] = main_data_dict_pl[key].groupby(['xacctype'])[[i for i in year_list]].sum().reset_index().round(1)
    level_1_dict[key].loc[len(level_1_dict[key].index),:]=level_1_dict[key].sum(axis=0,numeric_only = True)
    level_1_dict[key].at[len(level_1_dict[key].index)-1,'xacctype'] = 'Profit/Loss'
    ## we can add new ratios right here!
    
level_2_dict = {}
for key in main_data_dict_pl:
    level_2_dict[key] = main_data_dict_pl[key].groupby(['xhrc1'])[[i for i in year_list]].sum().reset_index().round(1)
    level_2_dict[key].loc[len(level_2_dict[key].index),:]=level_2_dict[key].sum(axis=0,numeric_only = True)
    level_2_dict[key].at[len(level_2_dict[key].index)-1,'xhrc1'] = 'Profit/Loss'
    
level_3_dict = {}
for key in main_data_dict_pl:
    level_3_dict[key] = main_data_dict_pl[key].groupby(['xhrc2'])[[i for i in year_list]].sum().reset_index().round(1)
    level_3_dict[key].loc[len(level_3_dict[key].index),:]=level_3_dict[key].sum(axis=0,numeric_only = True)
    level_3_dict[key].at[len(level_3_dict[key].index)-1,'xhrc2'] = 'Profit/Loss'
    

############



level_4_dict = {}
income_s_dict = {}
for key in main_data_dict_pl:
    print(key)
    level_4_dict[key] = main_data_dict_pl[key].groupby(['xhrc4'])[[i for i in year_list]].sum().reset_index().round(1)
    level_4_dict[key].loc[len(level_4_dict[key].index),:]=level_4_dict[key].sum(axis=0,numeric_only = True)
    level_4_dict[key].at[len(level_4_dict[key].index)-1,'xhrc4'] = 'Profit/Loss'
    df_i = level_4_dict[key].merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left').sort_values('Income Statement').set_index('Income Statement').reset_index()
    df_i = df_i.groupby(['Income Statement']).sum().reset_index()    
    if ~df_i['Income Statement'].isin(['06-1-Unusual Expenses (Income)']).any():
        df_i.loc[len(df_i.index)] = ['06-1-Unusual Expenses (Income)',0,0,0,0,0]
    df_i.loc[len(df_i.index)] = ['02-2-Gross Profit','-','-','-','-','-']
    df_i.loc[len(df_i.index)] = ['07-2-EBIT','-','-','-','-','-']
    df_i.loc[len(df_i.index)] = ['08-2-EBT','-','-','-','-','-']
    df_i = df_i.set_index('Income Statement')
    df_i.loc['02-2-Gross Profit'] = df_i.loc['01-1-Revenue']+df_i.loc['02-1-Cost of Revenue']
    try:
        df_i.loc['07-2-EBIT'] = df_i.loc['02-2-Gross Profit'] + df_i.loc['03-1-Office & Administrative Expenses'] + df_i.loc['04-1-Sales & Distribution Expenses'] + df_i.loc['05-1-Depreciation/Amortization'] + df_i.loc['06-1-Unusual Expenses (Income)'] + df_i.loc['07-1-Other Operating Expenses, Total'] + df_i.loc['04-2-MRP Discount'] + df_i.loc['04-3-Discount Expense']
    except Exception as e:
        df_i.loc['07-2-EBIT'] = df_i.loc['02-2-Gross Profit'] + df_i.loc['03-1-Office & Administrative Expenses'] + df_i.loc['04-1-Sales & Distribution Expenses'] + df_i.loc['05-1-Depreciation/Amortization'] + df_i.loc['06-1-Unusual Expenses (Income)'] + df_i.loc['07-1-Other Operating Expenses, Total'] + df_i.loc['04-2-MRP Discount']
        print(e)
    df_i.loc['08-2-EBT'] = df_i.loc['07-2-EBIT'] + df_i.loc['08-1-Interest Expense']
    df_i = df_i.sort_index().reset_index()
    income_s_dict[key] = df_i



### balance sheet

level_4_dict_bs = {}
balance_s_dict = {}

for key in main_data_dict_bs:
    level_4_dict_bs[key] = main_data_dict_bs[key].groupby(['xhrc4'])[[i for i in year_list]].sum().reset_index().round(1)
    level_4_dict_bs[key].loc[len(level_4_dict_bs[key].index),:]=level_4_dict_bs[key].sum(axis=0,numeric_only = True)
    level_4_dict_bs[key].at[len(level_4_dict_bs[key].index)-1,'xhrc4'] = 'Balance'
    df_b = level_4_dict_bs[key].merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left').sort_values('Balance Sheet').set_index('Balance Sheet').reset_index().drop(['xhrc4'],axis=1)
    df_b = df_b.groupby(['Balance Sheet']).sum().reset_index()
    df1 = ap_final_dict[key][ap_final_dict[key]['AP_TYPE']=='EXTERNAL'].rename(columns={'AP_TYPE':'Balance Sheet'})
    df_b = df_b.append(df1).reset_index().drop(['index'],axis=1)
    df_b.loc[len(df_b.index)] = ['01-1-Assets','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['01-2-Current Assets','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['04-2-Total Current Assets','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['04-3-Non-Current Assets','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['07-2-Total Non-Current Assets','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['07-3-Current Liabilities','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['11-2-Total Current Liabilities','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['11-4-Non-Current Liabilities','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['12-2-Total Non-Current Liabilities','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['13-2-Retained Earnings','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['13-3-Balance Check','-','-','-','-','-']
    df_b = df_b.set_index('Balance Sheet')
    try:
        df_b.loc['04-2-Total Current Assets'] = df_b.loc['01-3-Cash']+df_b.loc['02-1-Accounts Receivable']+df_b.loc['03-1-Inventories']+df_b.loc['04-1-Prepaid Expenses']
        df_b.loc['07-2-Total Non-Current Assets'] = df_b.loc['05-1-Other Assets']+df_b.loc['06-1-Property, Plant & Equipment']+df_b.loc['07-1-Goodwill & Intangible Asset']
        df_b.loc['11-2-Total Current Liabilities'] = df_b.loc['08-1-Accounts Payable']+df_b.loc['09-1-Accrued Liabilities']+df_b.loc['10-1-Other Short Term Liabilities']+df_b.loc['11-1-Debt']
        df_b.loc['12-2-Total Non-Current Liabilities'] = df_b.loc['12-1-Other Long Term Liabilities']
        df1 = income_s_dict[key].set_index('Income Statement')
        df_b.loc['13-2-Retained Earnings'] = df1.loc['10-1-Net Income']
        df_b.loc['13-3-Balance Check'] = df_b.loc['04-2-Total Current Assets'] + df_b.loc['07-2-Total Non-Current Assets'] + df_b.loc['11-2-Total Current Liabilities'] + df_b.loc['12-2-Total Non-Current Liabilities'] + df_b.loc['13-1-Total Shareholders Equity'] + df_b.loc['13-2-Retained Earnings']
    except Exception as e:
        print(e)
        pass
    df_b = df_b.sort_index().reset_index().round(0)
    balance_s_dict[key] = df_b


#cash flow statement
cashflow_s_dict = {}
for key in income_s_dict:
    print(key)
    df_i2= income_s_dict[key].set_index('Income Statement').replace('-',0)
    df_b2 = balance_s_dict[key].set_index('Balance Sheet').replace('-',0)
    df_b22 = df_b2
    #create a temporary dataframe which caluclates the difference between the 2 years
    df_b2 = df_b2.diff(axis=1).fillna(0)
    
    df2 = pd.DataFrame(columns=balance_s_dict[key].columns).rename(columns={'Balance Sheet':'Description'})
    ##operating cashflow
    df2.loc[len(df2.index)] = ['01-Operating Activities','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['02-Net Income','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['03-Depreciation and amortization','-','-','-','-','-']
#     df2.loc[len(df2.index)] = ['04-Increase/Decrease in Current Asset','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['04-1-Accounts Receivable','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['04-2-Inventories','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['04-3-Prepaid Expenses','-','-','-','-','-']

#     df2.loc[len(df2.index)] = ['05-Increase/Decrease in Current Liabilities','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['05-1-Accounts Payable','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['05-2-Accrued Liabilities','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['05-3-Other Short Term Liabilities','-','-','-','-','-']
    
    df2.loc[len(df2.index)] = ['06-Other operating cash flow adjustments',0,0,0,0,0]
    df2.loc[len(df2.index)] = ['07-Total Operating Cashflow','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['08','-','-','-','-','-']
    
    #investing cashflow
    df2.loc[len(df2.index)] = ['09-Investing Cashflow','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['10-Capital asset acquisitions/disposal','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['11-Other investing cash flows',0,0,0,0,0]
    df2.loc[len(df2.index)] = ['12-Total Investing Cashflow','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['13','-','-','-','-','-']

    #financing cashflow
    df2.loc[len(df2.index)] = ['14-Financing Cashflow','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['15-Increase/Decrease in Debt','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['16-Increase/Decrease in Equity','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['16-1-Increase/Decrease in Retained Earning','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['17-Other financing cash flows','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['18-Total Financing Cashflow','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['19','-','-','-','-','-']
    
    ##change in cash calculations
    df2.loc[len(df2.index)] = ['20-Year Opening Cash','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['21-Change in Cash','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['22-Year Ending Cash','-','-','-','-','-']
    df2 = df2.set_index('Description')
    
    try:
        #operating cashflow calculations
        df2.loc['02-Net Income'] = df_i2.loc['10-1-Net Income']
        df2.loc['03-Depreciation and amortization'] = df_i2.loc['05-1-Depreciation/Amortization']

        df2.loc['04-1-Accounts Receivable'] = df_b2.loc['02-1-Accounts Receivable']
        df2.loc['04-2-Inventories'] = df_b2.loc['03-1-Inventories']
        df2.loc['04-3-Prepaid Expenses'] = df_b2.loc['04-1-Prepaid Expenses']

        df2.loc['05-1-Accounts Payable'] = df_b2.loc['08-1-Accounts Payable']
        df2.loc['05-2-Accrued Liabilities'] = df_b2.loc['09-1-Accrued Liabilities']
        df2.loc['05-3-Other Short Term Liabilities'] = df_b2.loc['10-1-Other Short Term Liabilities']
    
        df2.loc['07-Total Operating Cashflow'] = df2.loc['02-Net Income'] + df2.loc['03-Depreciation and amortization'] + df2.loc['04-1-Accounts Receivable'] + df2.loc['04-2-Inventories'] + df2.loc['04-3-Prepaid Expenses'] + df2.loc['05-1-Accounts Payable'] + df2.loc['05-2-Accrued Liabilities'] + df2.loc['05-3-Other Short Term Liabilities']
    except Exception as e:
        print(e)
        pass
    
    #investing cashflow calculations
    df2.loc['10-Capital asset acquisitions/disposal'] = df_b2.loc['07-2-Total Non-Current Assets']
    df2.loc['12-Total Investing Cashflow'] = df2.loc['10-Capital asset acquisitions/disposal'] + df2.loc['11-Other investing cash flows']
    
    #financing cashflow calculations
    df2.loc['15-Increase/Decrease in Debt'] = df_b2.loc['11-1-Debt']
    df2.loc['16-Increase/Decrease in Equity'] = df_b2.loc['13-1-Total Shareholders Equity']
    df2.loc['16-1-Increase/Decrease in Retained Earning'] = df_b2.loc['13-2-Retained Earnings']
    df2.loc['17-Other financing cash flows'] = df_b2.loc['12-2-Total Non-Current Liabilities']
    df2.loc['18-Total Financing Cashflow'] = df2.loc['15-Increase/Decrease in Debt'] + df2.loc['16-Increase/Decrease in Equity'] + df2.loc['16-1-Increase/Decrease in Retained Earning'] + df2.loc['17-Other financing cash flows']
    
    ##change in cash calculations
    try:
        df2.loc['20-Year Opening Cash'] = df_b22.loc['01-3-Cash'].shift(periods=1,axis=0)
        df2.loc['21-Change in Cash'] = -(df2.loc['07-Total Operating Cashflow'] + df2.loc['12-Total Investing Cashflow'] + df2.loc['18-Total Financing Cashflow'] - df2.loc['02-Net Income'] - df2.loc['03-Depreciation and amortization'])
        df2.loc['22-Year Ending Cash'] = df2.loc['20-Year Opening Cash'] + df2.loc['21-Change in Cash']
    except Exception as e:
        print(e)
        pass
    
    cashflow_s_dict[key] = df2.sort_index().reset_index().fillna(0)


statement_3_dict = {}
for key in income_s_dict:
    print(key)
    df_i3 = income_s_dict[key].rename(columns={'Income Statement':'Description'})
    df_b3 = balance_s_dict[key].rename(columns={'Balance Sheet':'Description'})
    df_c = cashflow_s_dict[key]
    
    df12 = pd.concat([df_i3,df_b3,df_c]).reset_index(drop=True)
    daysinyear = 365
    #ratios
    df12.loc[len(df12.index)] = ['Ratios','-','-','-','-','-']
    df12.loc[len(df12.index)] = ['COGS Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Gross Profit Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Operating Profit','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Net Profit Ratio','-','-','-','-','-']  

    ##coverages
    df12.loc[len(df12.index)] = ['Tax Coverage','-','-','-','-','-']
    df12.loc[len(df12.index)] = ['Interest Coverage','-','-','-','-','-'] 

    #expense ratios
    df12.loc[len(df12.index)] = ['OAE Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['S&D Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Deprication Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Unusual Expense Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Other Operating Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Interest Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Tax Ratio','-','-','-','-','-'] 

    #efficiency ratios
    df12.loc[len(df12.index)] = ['Quick Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Quick Ratio Adjusted','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Current Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Current Ratio Adjusted','-','-','-','-','-'] 

    #asset ratios
    df12.loc[len(df12.index)] = ['Total Asset Turnover Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Net Asset Turnover Ratio','-','-','-','-','-'] 

    #working capital days
    df12.loc[len(df12.index)] = ['Inventory Turnover','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Inventory Days','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Accounts Receivable Turnover','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Accounts Receivable Days','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Accounts Payable Turnover','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Accounts Payable Turnover*','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Accounts Payable Days','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Accounts Payable Days*','-','-','-','-','-'] 

    #other ratios
    df12.loc[len(df12.index)] = ['PP&E Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Working Capital Turnover','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Working Capital Turnover*','-','-','-','-','-'] 

    #debt ratios
    df12.loc[len(df12.index)] = ['Cash Turnover','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Debt/Equity','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Debt/Capital','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Debt/TNW','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Total Liabilities/Equity','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Total Liabilities/Equity*','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Total Assets to Equity','-','-','-','-','-'] 


    df12.loc[len(df12.index)] = ['Debt/EBITDA','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Capital Structure Impact','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Acid Test','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Acid Test*','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['ROE','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['ROA','-','-','-','-','-'] 

    df12 = df12.set_index('Description').replace(0,np.nan)
    #ratio calculation
    try:
        ##profitability ratios
        df12.loc['COGS Ratio'] = df12.loc['02-1-Cost of Revenue']*100/df12.loc['01-1-Revenue']
        df12.loc['Gross Profit Ratio'] = df12.loc['02-2-Gross Profit']*100/df12.loc['01-1-Revenue']
        df12.loc['Operating Profit'] = df12.loc['07-2-EBIT']*100/df12.loc['01-1-Revenue']
        df12.loc['Net Profit Ratio'] = df12.loc['10-1-Net Income']*100/df12.loc['01-1-Revenue']

        ##coverages
        df12.loc['Tax Coverage'] = df12.loc['09-1-Income Tax & VAT']*100/df12.loc['08-2-EBT']
        df12.loc['Interest Coverage'] = df12.loc['08-1-Interest Expense']*100/df12.loc['07-2-EBIT']

        #expense ratios
        df12.loc['OAE Ratio'] = df12.loc['03-1-Office & Administrative Expenses']*100/df12.loc['01-1-Revenue']
        df12.loc['S&D Ratio'] = df12.loc['04-1-Sales & Distribution Expenses']*100/df12.loc['01-1-Revenue']
        df12.loc['Deprication Ratio'] = df12.loc['05-1-Depreciation/Amortization']*100/df12.loc['01-1-Revenue']
        df12.loc['Unusual Expense Ratio'] = df12.loc['06-1-Unusual Expenses (Income)']*100/df12.loc['01-1-Revenue']
        df12.loc['Other Operating Ratio'] = df12.loc['07-1-Other Operating Expenses, Total']*100/df12.loc['01-1-Revenue']
        df12.loc['Interest Ratio'] = df12.loc['08-1-Interest Expense']*100/df12.loc['01-1-Revenue']
        df12.loc['Tax Ratio'] = df12.loc['09-1-Income Tax & VAT']*100/df12.loc['01-1-Revenue']

        #efficiency ratios
        df12.loc['Quick Ratio'] = df12.loc['04-2-Total Current Assets']/df12.loc['11-2-Total Current Liabilities']
        df12.loc['Quick Ratio Adjusted'] = df12.loc['04-2-Total Current Assets']/(df12.loc['11-2-Total Current Liabilities']-df12.loc['EXTERNAL'])
        df12.loc['Current Ratio'] = df12.loc['04-2-Total Current Assets']/df12.loc['11-2-Total Current Liabilities']
        df12.loc['Current Ratio Adjusted'] = df12.loc['04-2-Total Current Assets']/(df12.loc['11-2-Total Current Liabilities']-df12.loc['EXTERNAL'])

        #asset ratios
        df12.loc['Total Asset Turnover Ratio'] = df12.loc['01-1-Revenue']/(df12.loc['04-2-Total Current Assets']+df12.loc['07-2-Total Non-Current Assets'])
        df12.loc['Net Asset Turnover Ratio'] = df12.loc['01-1-Revenue']/(df12.loc['04-2-Total Current Assets']+df12.loc['07-2-Total Non-Current Assets']+df12.loc['11-2-Total Current Liabilities']+df12.loc['12-2-Total Non-Current Liabilities'])

        #working capital days
        df12.loc['Inventory Turnover'] = df12.loc['02-1-Cost of Revenue']/df12.loc['03-1-Inventories']
        df12.loc['Inventory Days'] = df12.loc['03-1-Inventories']*daysinyear/df12.loc['02-1-Cost of Revenue']
        df12.loc['Accounts Receivable Turnover'] = df12.loc['01-1-Revenue']/df12.loc['02-1-Accounts Receivable']
        df12.loc['Accounts Receivable Days'] = df12.loc['02-1-Accounts Receivable']*daysinyear/df12.loc['01-1-Revenue']
        df12.loc['Accounts Payable Turnover'] = df12.loc['02-1-Cost of Revenue']/df12.loc['08-1-Accounts Payable']
        df12.loc['Accounts Payable Turnover*'] = df12.loc['02-1-Cost of Revenue']/df12.loc['EXTERNAL']
        df12.loc['Accounts Payable Days'] = df12.loc['08-1-Accounts Payable']*daysinyear/df12.loc['02-1-Cost of Revenue']
        df12.loc['Accounts Payable Days*'] = df12.loc['EXTERNAL']*daysinyear/df12.loc['02-1-Cost of Revenue']

        #other ratios
        df12.loc['PP&E Ratio'] = df12.loc['06-1-Property, Plant & Equipment']/df12.loc['01-1-Revenue']
        df12.loc['Working Capital Turnover'] = df12.loc['01-1-Revenue']/(df12.loc['02-1-Accounts Receivable']+df12.loc['03-1-Inventories']+df12.loc['08-1-Accounts Payable'])
        df12.loc['Working Capital Turnover*'] = df12.loc['01-1-Revenue']/(df12.loc['02-1-Accounts Receivable']+df12.loc['03-1-Inventories']+df12.loc['EXTERNAL'])

        total_debt = df12.loc['11-1-Debt'] + df12.loc['10-1-Other Short Term Liabilities'] + df12.loc['12-1-Other Long Term Liabilities']
        #debt ratios
        df12.loc['Cash Turnover'] = df12.loc['01-1-Revenue']/df12.loc['01-3-Cash']
        df12.loc['Debt/Equity'] = total_debt/(df12.loc['13-1-Total Shareholders Equity'])
        df12.loc['Debt/Capital'] = total_debt/(df12.loc['13-1-Total Shareholders Equity']-total_debt)
        df12.loc['Debt/TNW'] = total_debt/(df12.loc['07-2-Total Non-Current Assets']-df12.loc['07-1-Goodwill & Intangible Asset'])
        df12.loc['Total Liabilities/Equity'] = (df12.loc['11-2-Total Current Liabilities']+df12.loc['12-2-Total Non-Current Liabilities'])/(df12.loc['13-1-Total Shareholders Equity']-total_debt)
        df12.loc['Total Liabilities/Equity*'] = (df12.loc['11-2-Total Current Liabilities']-df12.loc['EXTERNAL']+df12.loc['12-2-Total Non-Current Liabilities'])/(df12.loc['13-1-Total Shareholders Equity']-total_debt)
        df12.loc['Total Assets to Equity'] = (df12.loc['04-2-Total Current Assets']+df12.loc['07-2-Total Non-Current Assets'])/df12.loc['13-1-Total Shareholders Equity']


        df12.loc['Debt/EBITDA'] = total_debt/(df12.loc['07-2-EBIT']+df12.loc['05-1-Depreciation/Amortization'])
        df12.loc['Capital Structure Impact'] = df12.loc['08-2-EBT']/df12.loc['07-2-EBIT']
        df12.loc['Acid Test'] = (df12.loc['04-2-Total Current Assets']-df12.loc['03-1-Inventories'])/df12.loc['11-2-Total Current Liabilities']
        df12.loc['Acid Test*'] =(df12.loc['04-2-Total Current Assets']-df12.loc['03-1-Inventories'])/(df12.loc['11-2-Total Current Liabilities']-df12.loc['EXTERNAL'])
        df12.loc['ROE'] = df12.loc['10-1-Net Income']/df12.loc['13-1-Total Shareholders Equity']
        df12.loc['ROA'] = df12.loc['10-1-Net Income']/(df12.loc['04-2-Total Current Assets']+df12.loc['07-2-Total Non-Current Assets'])
    except:
        pass
    
    statement_3_dict[key] = (df12*-1).round(3).reset_index().fillna(0)
######                 
zid_dict = {100000:'Karigor',100001:'Trading',100002:'Chemical',100003:'Thread Tape',100004:'Plastic',100005:'Zepto',100006:'Grocery',100007:'Paint Roller',100008:'Scrubber',100009:'Packaging'}

# take income of Trading, Karigor, Zepto & Grocery for the 3 years in 3 different dataframes

pl_data_income = main_data_dict_pl
income_dict = {}
for key in pl_data_income:
    df = pl_data_income[key]
    for i in year_list:
        income_dict[key] = [df[df['xacctype'] == 'Income'].sum()[i] for i in year_list]
income_df = pd.DataFrame.from_dict(income_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
income_df['Name'] = income_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
income_df = income_df[new_cols]
income_df.loc[len(income_df.index),:] = income_df.sum(axis=0,numeric_only=True)
income_df.at[len(income_df.index)-1,'Name'] = 'Total'

pl_data_COGS = main_data_dict_pl
COGS_dict = {}
for key in pl_data_COGS:
    df = pl_data_COGS[key]
    for i in year_list:
        if key != 100000:
            COGS_dict[key] = [df[df['xacc'] == '04010020'][i][df.loc[df['xacc']=='04010020'].index[0]] for i in year_list]
        else:
            COGS_dict[key] = [df[df['xacc'] == '4010020'][i][df.loc[df['xacc']=='4010020'].index[0]] for i in year_list]
COGS_df = pd.DataFrame.from_dict(COGS_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
COGS_df['Name'] = COGS_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
COGS_df = COGS_df[new_cols]
COGS_df.loc[len(COGS_df.index),:] = COGS_df.sum(axis=0,numeric_only=True)
COGS_df.at[len(COGS_df.index)-1,'Name'] = 'Total'


pl_data_expense = main_data_dict_pl
expense_dict = {}
for key in pl_data_expense:
    df = pl_data_expense[key]
    for i in year_list:
        expense_dict[key] = [df[(df['xacc'] != '04010020') & (df['xacctype'] == 'Expenditure')].sum()[i] for i in year_list]
expense_df = pd.DataFrame.from_dict(expense_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
expense_df['Name'] = expense_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
expense_df = expense_df[new_cols]
expense_df.loc[len(expense_df.index),:] = expense_df.sum(axis=0,numeric_only=True)
expense_df.at[len(expense_df.index)-1,'Name'] = 'Total'

pl_data_profit = main_data_dict_pl
profit_dict = {}
for key in pl_data_profit:
    df = pl_data_profit[key]
    for i in year_list:
        profit_dict[key] = [df.sum()[i] for i in year_list]
profit_df = pd.DataFrame.from_dict(profit_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
profit_df['Name'] = profit_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
profit_df = profit_df[new_cols]
profit_df.loc[len(profit_df.index),:] = profit_df.sum(axis=0,numeric_only=True)
profit_df.at[len(profit_df.index)-1,'Name'] = 'Total'

## taxes should be separated according to VAT and income tax. Also I think now the structure is even more different
pl_data_EBITDA = level_3_dict
EBITDA_dict = {}
for key in pl_data_EBITDA:
    df = pl_data_EBITDA[key]
    for i in year_list:
        EBITDA_dict[key] = [df[(df['xhrc2']!='0625-Property Tax & Others') & (df['xhrc2']!='0604-City Corporation Tax') & (df['xhrc2']!='0629- HMBR VAT & Tax Expenses') & (df['xhrc2']!='0630- Bank Interest & Charges') & (df['xhrc2']!='0633-Interest-Loan') & (df['xhrc2']!='0636-Depreciation') & (df['xhrc2']!='Profit/Loss')].sum()[i] for i in year_list]
EBITDA_df = pd.DataFrame.from_dict(EBITDA_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
EBITDA_df['Name'] = EBITDA_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
EBITDA_df = EBITDA_df[new_cols]
EBITDA_df.loc[len(EBITDA_df.index),:] = EBITDA_df.sum(axis=0,numeric_only=True)
EBITDA_df.at[len(EBITDA_df.index)-1,'Name'] = 'Total'

pl_data_tax = level_3_dict
tax_dict = {}
for key in pl_data_tax:
    df = pl_data_tax[key]
    for i in year_list:
        tax_dict[key] = [df[(df['xhrc2']=='0625-Property Tax & Others') | (df['xhrc2']=='0604-City Corporation Tax') | (df['xhrc2']=='0629- HMBR VAT & Tax Expenses')].sum()[i] for i in year_list]
tax_df = pd.DataFrame.from_dict(tax_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
tax_df['Name'] = tax_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
tax_df = tax_df[new_cols]
tax_df.loc[len(tax_df.index),:] = tax_df.sum(axis=0,numeric_only=True)
tax_df.at[len(tax_df.index)-1,'Name'] = 'Total'

pl_data_interest = level_3_dict
interest_dict = {}
for key in pl_data_interest:
    df = pl_data_interest[key]
    for i in year_list:
        interest_dict[key] = [df[(df['xhrc2']=='0630- Bank Interest & Charges') | (df['xhrc2']=='0633-Interest-Loan')].sum()[i] for i in year_list] ### here 
interest_df = pd.DataFrame.from_dict(interest_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
interest_df['Name'] = interest_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
interest_df = interest_df[new_cols]
interest_df.loc[len(interest_df.index),:] = interest_df.sum(axis=0,numeric_only=True)
interest_df.at[len(interest_df.index)-1,'Name'] = 'Total'


# pl_data_asset = main_data_dict_bs
# asset_dict = {}
# for key in pl_data_asset:
#     df = pl_data_asset[key]
#     for i in year_list:
#         asset_dict[key] = [df[df['xacctype'] == 'Asset'].sum()[i].round(0) for i in year_list]
# asset_df = pd.DataFrame.from_dict(asset_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
# asset_df['Name'] = asset_df['index'].map(zid_dict)
# new_cols = ['index','Name']+[i for i in year_list] 
# asset_df = asset_df[new_cols]
# asset_df[str(year_list[1])+'-'+str(year_list[2])] = asset_df[year_list[1]] - asset_df[year_list[2]]
# asset_df[str(year_list[0])+'-'+str(year_list[1])] = asset_df[year_list[0]] - asset_df[year_list[1]]
# asset_df.loc[len(asset_df.index),:] = asset_df.sum(axis=0,numeric_only=True)
# asset_df.at[len(asset_df.index)-1,'Name'] = 'Total'

# pl_data_liable = main_data_dict_bs
# liable_dict = {}
# for key in pl_data_liable:
#     df = pl_data_liable[key]
#     for i in year_list:
#         liable_dict[key] = [df[df['xacctype'] == 'Liability'].sum()[i].round(0) for i in year_list]
# liable_df = pd.DataFrame.from_dict(liable_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
# liable_df['Name'] = liable_df['index'].map(zid_dict)
# new_cols = ['index','Name']+[i for i in year_list] 
# liable_df = liable_df[new_cols]
# liable_df[str(year_list[1])+'-'+str(year_list[2])] = liable_df[year_list[2]] - liable_df[year_list[1]]
# liable_df[str(year_list[0])+'-'+str(year_list[1])] = liable_df[year_list[1]] - liable_df[year_list[0]]
# liable_df.loc[len(liable_df.index),:] = liable_df.sum(axis=0,numeric_only=True)
# liable_df.at[len(liable_df.index)-1,'Name'] = 'Total'

##New code addition by director on 19112022 regarding ap ar and inv
pl_data_apari = main_data_dict_bs
apari_dict = {}
for key in pl_data_apari:
    if key != 100000:
        df = pl_data_apari[key]
        apari_dict[key] = df[(df['xacc'] == '09030001')|(df['xacc'] == '01030001')|(df['xacc'] == '01060003')|(df['xacc'] == '01060001')]
        apari_dict[key]['Business'] = key
    apari_df = pd.concat([apari_dict[key] for key in apari_dict],axis=0)
    apari_df['Name'] = apari_df['Business'].map(zid_dict)

###Profit & loss
hmbr_pl = main_data_dict_pl[100001]
karigor_pl = main_data_dict_pl[100000]
chemical_pl = main_data_dict_pl[100002]
thread_pl = main_data_dict_pl[100003]
plastic_pl = main_data_dict_pl[100004]
zepto_pl = main_data_dict_pl[100005]
grocery_pl = main_data_dict_pl[100006]
paint_pl = main_data_dict_pl[100007]
steel_pl = main_data_dict_pl[100008]
packaging_pl = main_data_dict_pl[100009]
### Blance Sheet
hmbr_bs = main_data_dict_bs[100001]
karigor_bs = main_data_dict_bs[100000]
chemical_bs = main_data_dict_bs[100002]
thread_bs = main_data_dict_bs[100003]
plastic_bs = main_data_dict_bs[100004]
zepto_bs = main_data_dict_bs[100005]
grocery_bs = main_data_dict_bs[100006]
paint_bs = main_data_dict_bs[100007]
steel_bs = main_data_dict_bs[100008]
packaging_bs = main_data_dict_bs[100009]

### all balance sheet together
all_bs = pd.concat(main_data_dict_bs,axis=0)

### Summery Details
hmbr_summery = level_1_dict[100001]
karigor_summery = level_1_dict[100000]
chemical_summery = level_1_dict[100002]
thread_summery = level_1_dict[100003]
plastic_summery = level_1_dict[100004]
zepto_summery = level_1_dict[100005]
grocery_summery = level_1_dict[100006]
paint_summery = level_1_dict[100007]
steel_summery = level_1_dict[100008]
packaging_summery = level_1_dict[100009]

##lvl 4
hmbr_summery_lvl_4 = level_4_dict[100001]
karigor_summery_lvl_4 = level_4_dict[100000]
chemical_summery_lvl_4 = level_4_dict[100002]
thread_summery_lvl_4 = level_4_dict[100003]
plastic_summery_lvl_4 = level_4_dict[100004]
zepto_summery_lvl_4 = level_4_dict[100005]
grocery_summery_lvl_4 = level_4_dict[100006]
paint_summery_lvl_4 = level_4_dict[100007]
steel_summery_lvl_4 = level_4_dict[100008]
packaging_summery_lvl_4 = level_4_dict[100009]

all_lvl_4 = pd.concat(level_4_dict,axis = 0)

hmbr_summery_lvl_4_bs = level_4_dict_bs[100001]
karigor_summery_lvl_4_bs = level_4_dict_bs[100000]
chemical_summery_lvl_4_bs = level_4_dict_bs[100002]
thread_summery_lvl_4_bs = level_4_dict_bs[100003]
plastic_summery_lvl_4_bs = level_4_dict_bs[100004]
zepto_summery_lvl_4_bs = level_4_dict_bs[100005]
grocery_summery_lvl_4_bs = level_4_dict_bs[100006]
paint_summery_lvl_4_bs = level_4_dict_bs[100007]
steel_summery_lvl_4_bs = level_4_dict_bs[100008]
packaging_summery_lvl_4_bs = level_4_dict_bs[100009]

all_lvl_4_bs = pd.concat(level_4_dict_bs,axis = 0)

hmbr_summery_ap_final_dict = ap_final_dict[100001]
karigor_summery_ap_final_dict = ap_final_dict[100000]
chemical_summery_ap_final_dict = ap_final_dict[100002]
thread_summery_ap_final_dict = ap_final_dict[100003]
plastic_summery_ap_final_dict = ap_final_dict[100004]
zepto_summery_ap_final_dict = ap_final_dict[100005]
grocery_summery_ap_final_dict = ap_final_dict[100006]
paint_summery_ap_final_dict = ap_final_dict[100007]
steel_summery_ap_final_dict = ap_final_dict[100008]
packaging_summery_ap_final_dict = ap_final_dict[100009]

all_ap_final_dict = pd.concat(ap_final_dict,axis=0)

hmbr_summery_statements = statement_3_dict[100001]
karigor_summery_statements = statement_3_dict[100000]
chemical_summery_statements = statement_3_dict[100002]
thread_summery_statements = statement_3_dict[100003]
plastic_summery_statements = statement_3_dict[100004]
zepto_summery_statements = statement_3_dict[100005]
grocery_summery_statements = statement_3_dict[100006]
paint_summery_statements = statement_3_dict[100007]
steel_summery_statements = statement_3_dict[100008]
packaging_summery_statements = statement_3_dict[100009]

with pd.ExcelWriter('level_4.xlsx') as writer:  
    hmbr_summery_lvl_4.to_excel(writer, sheet_name='100001')
    karigor_summery_lvl_4.to_excel(writer, sheet_name='100000')
    chemical_summery_lvl_4.to_excel(writer, sheet_name='100002')
    thread_summery_lvl_4.to_excel(writer, sheet_name='100003')
    plastic_summery_lvl_4.to_excel(writer, sheet_name='100004')
    zepto_summery_lvl_4.to_excel(writer, sheet_name='100005')
    grocery_summery_lvl_4.to_excel(writer, sheet_name='100006')
    paint_summery_lvl_4.to_excel(writer, sheet_name='100007')
    steel_summery_lvl_4.to_excel(writer, sheet_name='100008')
    packaging_summery_lvl_4.to_excel(writer, sheet_name='100009')

###Excel File Generate
profit_excel = f'p&l{start_year}_{start_month}_{end_month}.xlsx'
balance_excel = f'b&l{start_year}_{start_month}_{end_month}.xlsx'
details_excel = f'profitLossDetail{start_year}_{start_month}_{end_month}.xlsx'
lvl_4_details_excel = f'level_4{start_year}_{start_month}_{end_month}.xlsx'
lvl_4_bs_details_excel = f'level_4_bs{start_year}_{start_month}_{end_month}.xlsx'
ap_final_dict_excel = f'ap_final_dict{start_year}_{start_month}_{end_month}.xlsx'
statement_3_dict_excel = f'statement_3_dict{start_year}_{start_month}_{end_month}.xlsx'
with pd.ExcelWriter(profit_excel) as writer:  
    hmbr_pl.to_excel(writer, sheet_name='100001')
    karigor_pl.to_excel(writer, sheet_name='100000')
    chemical_pl.to_excel(writer, sheet_name='100002')
    thread_pl.to_excel(writer, sheet_name='100003')
    plastic_pl.to_excel(writer, sheet_name='100004')
    zepto_pl.to_excel(writer, sheet_name='100005')
    grocery_pl.to_excel(writer, sheet_name='100006')
    paint_pl.to_excel(writer, sheet_name='100007')
    steel_pl.to_excel(writer, sheet_name='100008')
    packaging_pl.to_excel(writer, sheet_name='100009')

with pd.ExcelWriter(balance_excel) as writer:  
    hmbr_bs.to_excel(writer, sheet_name='100001')
    karigor_bs.to_excel(writer, sheet_name='100000')
    chemical_bs.to_excel(writer, sheet_name='100002')
    thread_bs.to_excel(writer, sheet_name='100003')
    plastic_bs.to_excel(writer, sheet_name='100004')
    zepto_bs.to_excel(writer, sheet_name='100005')
    grocery_bs.to_excel(writer, sheet_name='100006')
    paint_bs.to_excel(writer, sheet_name='100007')
    steel_bs.to_excel(writer, sheet_name='100008')
    packaging_bs.to_excel(writer, sheet_name='100009')
    all_bs.to_excel(writer, sheet_name='all_bs')

# income_df COGS_df expense_df, profit_df asset_df liable_df
with pd.ExcelWriter(details_excel) as writer:  
    income_df.to_excel(writer, sheet_name='income')
    COGS_df.to_excel(writer, sheet_name='COGS')
    expense_df.to_excel(writer, sheet_name='expense')
    profit_df.to_excel(writer, sheet_name='profit')
    # asset_df.to_excel(writer, sheet_name='asset')
    # liable_df.to_excel(writer, sheet_name='liable')
    apari_df.to_excel(writer,sheet_name='apari')
    EBITDA_df.to_excel(writer,sheet_name='EBITDA')
    interest_df.to_excel(writer,sheet_name='interest')
    tax_df.to_excel(writer,sheet_name='tax')
# income_df COGS_df expense_df, profit_df asset_df liable_df
#lvl-4
with pd.ExcelWriter(lvl_4_details_excel) as writer:  
    hmbr_summery_lvl_4.to_excel(writer, sheet_name='100001')
    karigor_summery_lvl_4.to_excel(writer, sheet_name='100000')
    chemical_summery_lvl_4.to_excel(writer, sheet_name='100002')
    thread_summery_lvl_4.to_excel(writer, sheet_name='100003')
    plastic_summery_lvl_4.to_excel(writer, sheet_name='100004')
    zepto_summery_lvl_4.to_excel(writer, sheet_name='100005')
    grocery_summery_lvl_4.to_excel(writer, sheet_name='100006')
    paint_summery_lvl_4.to_excel(writer, sheet_name='100007')
    steel_summery_lvl_4.to_excel(writer, sheet_name='100008')
    packaging_summery_lvl_4.to_excel(writer, sheet_name='100009')
    all_lvl_4.to_excel(writer,sheet_name='all_lvl_4')
#lvl4-bs
with pd.ExcelWriter(lvl_4_bs_details_excel) as writer:  
    hmbr_summery_lvl_4_bs.to_excel(writer, sheet_name='100001')
    karigor_summery_lvl_4_bs.to_excel(writer, sheet_name='100000')
    chemical_summery_lvl_4_bs.to_excel(writer, sheet_name='100002')
    thread_summery_lvl_4_bs.to_excel(writer, sheet_name='100003')
    plastic_summery_lvl_4_bs.to_excel(writer, sheet_name='100004')
    zepto_summery_lvl_4_bs.to_excel(writer, sheet_name='100005')
    grocery_summery_lvl_4_bs.to_excel(writer, sheet_name='100006')
    paint_summery_lvl_4_bs.to_excel(writer, sheet_name='100007')
    steel_summery_lvl_4_bs.to_excel(writer, sheet_name='100008')
    packaging_summery_lvl_4_bs.to_excel(writer, sheet_name='100009')
    all_lvl_4_bs.to_excel(writer,sheet_name='all_lvl_4_bs')

with pd.ExcelWriter(ap_final_dict_excel) as writer:
    hmbr_summery_ap_final_dict.to_excel(writer,sheet_name='100001')
    karigor_summery_ap_final_dict.to_excel(writer,sheet_name='100000')
    chemical_summery_ap_final_dict.to_excel(writer,sheet_name='100002')
    thread_summery_ap_final_dict.to_excel(writer,sheet_name='100003')
    plastic_summery_ap_final_dict.to_excel(writer,sheet_name='100004')
    zepto_summery_ap_final_dict.to_excel(writer,sheet_name='100005')
    grocery_summery_ap_final_dict.to_excel(writer,sheet_name='100006')
    paint_summery_ap_final_dict.to_excel(writer,sheet_name='100007')
    steel_summery_ap_final_dict.to_excel(writer,sheet_name='100008')
    packaging_summery_ap_final_dict.to_excel(writer,sheet_name='100009')
    all_ap_final_dict.to_excel(writer,sheet_name='all_ap_final_dict')

with pd.ExcelWriter(statement_3_dict_excel) as writer:
    hmbr_summery_statements.to_excel(writer,sheet_name='100001')
    karigor_summery_statements.to_excel(writer,sheet_name='100000')
    chemical_summery_statements.to_excel(writer,sheet_name='100002')
    thread_summery_statements.to_excel(writer,sheet_name='100003')
    plastic_summery_statements.to_excel(writer,sheet_name='100004')
    zepto_summery_statements.to_excel(writer,sheet_name='100005')
    grocery_summery_statements.to_excel(writer,sheet_name='100006')
    paint_summery_statements.to_excel(writer,sheet_name='100007')
    steel_summery_statements.to_excel(writer,sheet_name='100008')
    packaging_summery_statements.to_excel(writer,sheet_name='100009')

# ###Email    
me = "XXXXXX@gmail.com"
you = ["XXXXXXm"]


msg = MIMEMultipart('alternative')
msg['Subject'] = f"profit & loss HMBR .year: {start_year} month from {start_month} to {end_month}"
msg['From'] = me
msg['To'] = ", ".join(you)

HEADER = '''
<html>
    <head>
    </head>
    <body>
'''
FOOTER = '''
    </body>
</html>
'''
# income_df COGS_df expense_df, profit_df asset_df liable_df
with open('profitLoss.html','w') as f:
    f.write(HEADER)
    f.write('HMBR Details')
    f.write(hmbr_summery.to_html(classes='df_summery'))
    f.write('Karigor Details')
    f.write(karigor_summery.to_html(classes='df_summery1'))
    f.write('Chemical Details')
    f.write(chemical_summery.to_html(classes='df_summery2'))
    f.write('Thread Tape Details')
    f.write(thread_summery.to_html(classes='df_summery3'))
    f.write('Plastic Details')
    f.write(plastic_summery.to_html(classes='df_summery4'))
    f.write('Zepto Details')
    f.write(zepto_summery.to_html(classes='df_summery5'))
    f.write('Grocery Details')
    f.write(grocery_summery.to_html(classes='df_summery6'))
    f.write('Paint Roller Details')
    f.write(paint_summery.to_html(classes='df_summery7'))
    f.write('Steel Scrubber Details')
    f.write(steel_summery.to_html(classes='df_summery8'))
    f.write('Packaging Details')
    f.write(packaging_summery.to_html(classes='df_summery9'))
    f.write('Cost of good sold details')
    f.write(COGS_df.to_html(classes='df_summery10'))
    f.write('Income Details')
    f.write(income_df.to_html(classes='df_summery11'))
    f.write('Expense details')
    f.write(expense_df.to_html(classes='df_summery12'))
    f.write('Profit Details')
    f.write(profit_df.to_html(classes='df_summery13'))
    # f.write('Asset Details')
    # f.write(asset_df.to_html(classes='df_summery14'))
    # f.write('Liability Details')
    # f.write(liable_df.to_html(classes='df_summery15'))
    f.write(FOOTER)

filename = "profitLoss.html"
f = open(filename)
attachment = MIMEText(f.read(),'html')
msg.attach(attachment)

part1 = MIMEBase('application', "octet-stream")
part1.set_payload(open(profit_excel, "rb").read())
encoders.encode_base64(part1)
part1.add_header('Content-Disposition', 'attachment; filename="profit.xlsx"')
msg.attach(part1)

part2 = MIMEBase('application', "octet-stream")
part2.set_payload(open(balance_excel, "rb").read())
encoders.encode_base64(part2)
part2.add_header('Content-Disposition', 'attachment; filename="balance.xlsx"')
msg.attach(part2)

part3 = MIMEBase('application', "octet-stream")
part3.set_payload(open(details_excel, "rb").read())
encoders.encode_base64(part3)
part3.add_header('Content-Disposition', 'attachment; filename="profitLossDetail.xlsx"')
msg.attach(part3)

part4 = MIMEBase('application', "octet-stream")
part4.set_payload(open(lvl_4_details_excel, "rb").read())
encoders.encode_base64(part4)
part4.add_header('Content-Disposition', 'attachment; filename="lvl_3.xlsx"')
msg.attach(part4)

part5 = MIMEBase('application', "octet-stream")
part5.set_payload(open(lvl_4_bs_details_excel, "rb").read())
encoders.encode_base64(part5)
part5.add_header('Content-Disposition', 'attachment; filename="lvl_3bs_.xlsx"')
msg.attach(part5)

part6 = MIMEBase('application', "octet-stream")
part6.set_payload(open(ap_final_dict_excel, "rb").read())
encoders.encode_base64(part6)
part6.add_header('Content-Disposition', 'attachment; filename="ap_final_dict_.xlsx"')
msg.attach(part6)

part7 = MIMEBase('application', "octet-stream")
part7.set_payload(open(statement_3_dict_excel, "rb").read())
encoders.encode_base64(part7)
part7.add_header('Content-Disposition', 'attachment; filename="statement_3_dict_.xlsx"')
msg.attach(part7)

username = 'XXXXXX@gmail.com'
password = 'XXXXXX'


s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(me,you,msg.as_string())
s.quit()
