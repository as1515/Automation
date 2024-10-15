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
import sys, os
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
        df1 = pd.read_sql("""SELECT 'INTERNAL',glheader.xyear,glheader.xper,SUM(gldetail.xprime)
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
                            AND gldetail.xsub IN %s
                            GROUP BY glheader.xyear,glheader.xper"""%(zid,zid,zid,project,xacc,year,emonth,sup_list),con = engine)
        df2 = pd.read_sql("""SELECT 'EXTERNAL',glheader.xyear,glheader.xper,SUM(gldetail.xprime)
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
                            AND gldetail.xsub NOT IN %s
                            GROUP BY glheader.xyear,glheader.xper"""%(zid,zid,zid,project,xacc,year,emonth,sup_list),con = engine)
    else:
        df1 = pd.read_sql("""SELECT 'EXTERNAL',glheader.xyear,glheader.xper,SUM(gldetail.xprime)
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
                            AND gldetail.xsub != '%s'
                            GROUP BY glheader.xyear,glheader.xper"""%(zid,zid,zid,project,xacc,year,emonth,sup_list),con = engine)
        df2 = pd.read_sql("""SELECT 'INTERNAL',glheader.xyear,glheader.xper,SUM(gldetail.xprime)
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
                            AND gldetail.xsub = '%s'
                            GROUP BY glheader.xyear,glheader.xper"""%(zid,zid,zid,project,xacc,year,emonth,sup_list),con = engine)
    df = pd.concat([df1,df2],axis=0)
    return df

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


### define business Id and date time year list for comparison (separate if project)
zid_list_hmbr = {100000: 'Karigor Ltd.', 100001: 'GULSHAN TRADING', 100002:'',100003: '', 100004: 'Gulshan Plastic' ,100005: '',100006: '',100007: '',100008: '',100009: ''}
# zid_list_fixit = 100000,100001,100002,100003]
##### call SQL once and get the main data once into a dataframe (get the year and month as an integer)
current_year = int(input('input year___________  '))
current_end_month = int(input('input month___________'))


current_start_month = 1

last_year = current_year - 1
start_month = 1
end_month = 12

ap_final_dict = {}

data_ap = {'AP_TYPE':['INTERNAL','EXTERNAL']}

for k,v in ap_dict.items():
    print(k)
    df_ap = pd.DataFrame(data_ap)

    zid = k
    project = v[0]
    acc = v[1]
    sup_list = v[2]

    df_1 = get_gl_details_ap_project(zid,project,current_year,acc,current_end_month,sup_list).rename(columns={'?column?':'AP_TYPE'}).round(1)
    df_1['balance'] = df_1.groupby(['AP_TYPE','xyear'])['sum'].cumsum()
    df_1 = df_1.pivot(index=['AP_TYPE'],columns=['xyear','xper'],values='balance')
    
    df_2 = get_gl_details_ap_project(zid,project,last_year,acc,end_month,sup_list).rename(columns={'?column?':'AP_TYPE'}).round(1)
    df_2['balance'] = df_2.groupby(['AP_TYPE','xyear'])['sum'].cumsum()
    df_2 = df_2.pivot(index=['AP_TYPE'],columns=['xyear','xper'],values='balance')
    
    df_ap = df_ap.merge(df_1,on='AP_TYPE',how='left').merge(df_2,on='AP_TYPE',how='left').set_index('AP_TYPE')
    df_ap = df_ap.reindex(sorted(df_ap.columns), axis=1)
    ap_final_dict[k] = df_ap.reset_index().drop([(current_year,0),(last_year,0)],axis=1)


main_data_dict_pl = {}
for i,p in zid_list_hmbr.items():
    if p == '':
        print(i)
        df_master = get_gl_master(i)
        df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]

        df = get_gl_details(i,current_year,current_start_month,current_end_month)
        df2 = get_gl_details(i,last_year,start_month,end_month)
        df = df.append(df2)
        df = df.pivot_table(['sum'],index=['xacc'],columns=['xyear','xper'],aggfunc='sum').reset_index()

        df_new = df.merge(df_master[['xacc','xdesc','xacctype','xhrc1','xhrc2','xhrc3','xhrc4']],on=['xacc'],how='right').merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left')
    else:
        print(p)
        df_master = get_gl_master(i)
        df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]

        df = get_gl_details_project(i,p,current_year,current_start_month,current_end_month)
        df2 = get_gl_details_project(i,p,last_year,start_month,end_month)
        df = df.append(df2)
        df = df.pivot_table(['sum'],index=['xacc'],columns=['xyear','xper'],aggfunc='sum').reset_index()

        df_new = df.merge(df_master[['xacc','xdesc','xacctype','xhrc1','xhrc2','xhrc3','xhrc4']],on=['xacc'],how='right').merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left')
    main_data_dict_pl[i] = df_new.sort_values(['xacctype'],ascending=True).fillna(0)
    
main_data_dict_bs = {}
for i,p in zid_list_hmbr.items():
    if p == '':
        print(i)
        df_master = get_gl_master(i)
        df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]

        df = get_gl_details_bs(i,current_year,current_end_month)

        for item,m in enumerate(range(df['xper'].max()+1)):
            df_m = df[df['xper']<=m].groupby(['xacc'])['sum'].sum().reset_index().round(1)
            if item == 0:
                df_new_c = df_master.merge(df_m[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':(current_year,m)})
            else:
                df_new_c = df_new_c.merge(df_m[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':(current_year,m)})

        df = get_gl_details_bs(i,last_year,end_month)

        for item,m in enumerate(range(df['xper'].max()+1)):
            df_m = df[df['xper']<=m].groupby(['xacc'])['sum'].sum().reset_index().round(1)
            if item == 0:
                df_new = df_master.merge(df_m[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':(last_year,m)})
            else:
                df_new = df_new.merge(df_m[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':(last_year,m)})
        df_l = df_new_c.merge(df_new,on=['xacc'],how='left')
        df_l = df_l.drop(['xdesc_y','xacctype_y','xhrc1_y','xhrc2_y','xhrc3_y','xhrc4_y',(current_year,0),(last_year,0)],axis=1).rename(columns={'xdesc_x':'xdesc','xacctype_x':'xacctype','xhrc1_x':'xhrc1','xhrc2_x':'xhrc2','xhrc3_x':'xhrc3','xhrc4_x':'xhrc4'})
        main_data_dict_bs[i] = df_l.merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left')
    else:
        print(i)
        print(p)
        df_master = get_gl_master(i)
        df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]

        df = get_gl_details_bs_project(i,p,current_year,current_end_month)

        for item,m in enumerate(range(df['xper'].max()+1)):
            df_m = df[df['xper']<=m].groupby(['xacc'])['sum'].sum().reset_index().round(1)
            if item == 0:
                df_new_c = df_master.merge(df_m[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':(current_year,m)})
            else:
                df_new_c = df_new_c.merge(df_m[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':(current_year,m)})

        df = get_gl_details_bs_project(i,p,last_year,end_month)

        for item,m in enumerate(range(df['xper'].max()+1)):
            df_m = df[df['xper']<=m].groupby(['xacc'])['sum'].sum().reset_index().round(1)
            if item == 0:
                df_new = df_master.merge(df_m[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':(last_year,m)})
            else:
                df_new = df_new.merge(df_m[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':(last_year,m)})
        df_l = df_new_c.merge(df_new,on=['xacc'],how='left')
        df_l = df_l.drop(['xdesc_y','xacctype_y','xhrc1_y','xhrc2_y','xhrc3_y','xhrc4_y',(current_year,0),(last_year,0)],axis=1).rename(columns={'xdesc_x':'xdesc','xacctype_x':'xacctype','xhrc1_x':'xhrc1','xhrc2_x':'xhrc2','xhrc3_x':'xhrc3','xhrc4_x':'xhrc4'})
        main_data_dict_bs[i] = df_l.merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left')


############ income statement

level_4_dict = {}
income_s_dict = {}
for key in main_data_dict_pl:
    print(key)
    level_4_dict[key] = main_data_dict_pl[key].groupby(['Income Statement']).sum().reset_index().round(1)
    level_4_dict[key].loc[len(level_4_dict[key].index),:]=level_4_dict[key].sum(axis=0,numeric_only = True)
    level_4_dict[key].at[len(level_4_dict[key].index)-1,'Income Statement'] = '10-1-Net Income'
    df_i = level_4_dict[key]
    
    if ~df_i['Income Statement'].isin(['06-1-Unusual Expenses (Income)']).any():
        new = ['06-1-Unusual Expenses (Income)']
        df_i = df_i.append(pd.Series(new, index=df_i.columns[:len(new)]), ignore_index=True)
    new = ['02-2-Gross Profit']
    df_i = df_i.append(pd.Series(new, index=df_i.columns[:len(new)]), ignore_index=True)
    new = ['07-2-EBIT']
    df_i = df_i.append(pd.Series(new, index=df_i.columns[:len(new)]), ignore_index=True)
    new = ['08-2-EBT']
    df_i = df_i.append(pd.Series(new, index=df_i.columns[:len(new)]), ignore_index=True)
    
    df_i.columns = [i[1:] for i in df_i.columns]
    df_i = df_i.rename(columns={'ncome Statement':'Income Statement'})
    
    df_i = df_i.set_index('Income Statement').fillna(0)
    try:
        df_i.loc['02-2-Gross Profit'] = df_i.loc['01-1-Revenue']+df_i.loc['02-1-Cost of Revenue']
        try:
            df_i.loc['07-2-EBIT'] = df_i.loc['02-2-Gross Profit'] + df_i.loc['03-1-Office & Administrative Expenses'] + df_i.loc['04-1-Sales & Distribution Expenses'] + df_i.loc['05-1-Depreciation/Amortization'] + df_i.loc['06-1-Unusual Expenses (Income)'] + df_i.loc['07-1-Other Operating Expenses, Total'] + df_i.loc['04-2-MRP Discount'] + df_i.loc['04-3-Discount Expense']
        except Exception as e:
            df_i.loc['07-2-EBIT'] = df_i.loc['02-2-Gross Profit'] + df_i.loc['03-1-Office & Administrative Expenses'] + df_i.loc['04-1-Sales & Distribution Expenses'] + df_i.loc['05-1-Depreciation/Amortization'] + df_i.loc['06-1-Unusual Expenses (Income)'] + df_i.loc['07-1-Other Operating Expenses, Total'] + df_i.loc['04-2-MRP Discount']
            print(e)
        df_i.loc['08-2-EBT'] = df_i.loc['07-2-EBIT'] + df_i.loc['08-1-Interest Expense']
        
        cols = [i]
        df_i = df_i.sort_index().reset_index()
        income_s_dict[key] = df_i
    except Exception as e:
        print(e)
        pass


##### balance sheet
level_4_dict_bs = {}
balance_s_dict = {}

for key in main_data_dict_bs:
    print(key)
    level_4_dict_bs[key] = main_data_dict_bs[key].groupby(['Balance Sheet']).sum().reset_index().round(1)
    level_4_dict_bs[key].loc[len(level_4_dict_bs[key].index),:]=level_4_dict_bs[key].sum(axis=0,numeric_only = True)
    level_4_dict_bs[key].at[len(level_4_dict_bs[key].index)-1,'Balance Sheet'] = 'Balance'
    
#     df1 = ap_final_dict[key][ap_final_dict[key]['AP_TYPE']=='EXTERNAL'].rename(columns={'AP_TYPE':'Balance Sheet'})
    
#     df_b = df_b.append(df1).reset_index().drop(['index'],axis=1)
    df_b = level_4_dict_bs[key]
    new = ['01-1-Assets']
    df_b = df_b.append(pd.Series(new, index=df_b.columns[:len(new)]), ignore_index=True)
    new = ['01-2-Current Assets']
    df_b = df_b.append(pd.Series(new, index=df_b.columns[:len(new)]), ignore_index=True)
    new = ['04-2-Total Current Assets']
    df_b = df_b.append(pd.Series(new, index=df_b.columns[:len(new)]), ignore_index=True)
    new = ['04-3-Non-Current Assets']
    df_b = df_b.append(pd.Series(new, index=df_b.columns[:len(new)]), ignore_index=True)
    new = ['07-2-Total Non-Current Assets']
    df_b = df_b.append(pd.Series(new, index=df_b.columns[:len(new)]), ignore_index=True)
    new = ['07-3-Current Liabilities']
    df_b = df_b.append(pd.Series(new, index=df_b.columns[:len(new)]), ignore_index=True)
    new = ['11-2-Total Current Liabilities']
    df_b = df_b.append(pd.Series(new, index=df_b.columns[:len(new)]), ignore_index=True)
    new = ['11-4-Non-Current Liabilities']
    df_b = df_b.append(pd.Series(new, index=df_b.columns[:len(new)]), ignore_index=True)
    new = ['12-2-Total Non-Current Liabilities']
    df_b = df_b.append(pd.Series(new, index=df_b.columns[:len(new)]), ignore_index=True)
    new = ['13-2-Retained Earnings']
    df_b = df_b.append(pd.Series(new, index=df_b.columns[:len(new)]), ignore_index=True)
    new = ['13-3-Balance Check']
    df_b = df_b.append(pd.Series(new, index=df_b.columns[:len(new)]), ignore_index=True)
    
    
    df_b = df_b.set_index('Balance Sheet').fillna(0)
    df_b = df_b[sorted(df_b.columns)] 
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


######cashflow statement

cashflow_s_dict = {}
for key in income_s_dict:
    print(key)
    df_i2= income_s_dict[key].set_index('Income Statement').replace('-',0)
    df_b2 = balance_s_dict[key].set_index('Balance Sheet').replace('-',0)
    df_b22 = df_b2
    #create a temporary dataframe which caluclates the difference between the 2 years
    df_b2 = df_b2.diff(axis=1).fillna(0)
    
    df2 = pd.DataFrame(columns=balance_s_dict[key].columns).rename(columns={'Balance Sheet':'Description'})
    
    new = ['01-Operating Activities']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)
    new = ['02-Net Income']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)
    new = ['03-Depreciation and amortization']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)
    new = ['04-1-Accounts Receivable']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)
    new = ['04-2-Inventories']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)
    new = ['04-3-Prepaid Expenses']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)

    new = ['05-1-Accounts Payable']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)
    new = ['05-2-Accrued Liabilities']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)
    new = ['05-3-Other Short Term Liabilities']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)
    new = ['06-Other operating cash flow adjustments']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['07-Total Operating Cashflow']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['08']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    
    #investing cashflow
    new = ['09-Investing Cashflow']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['10-Capital asset acquisitions/disposal']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['11-Other investing cash flows']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['12-Total Investing Cashflow']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['13']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           

    #financing cashflow
    new = ['14-Financing Cashflow']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['15-Increase/Decrease in Debt']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['16-Increase/Decrease in Equity']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['16-1-Increase/Decrease in Retained Earning']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['17-Other financing cash flows']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['18-Total Financing Cashflow']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['19']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    
    ##change in cash calculations
    new = ['20-Year Opening Cash']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['21-Change in Cash']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)                           
    new = ['22-Year Ending Cash']
    df2 = df2.append(pd.Series(new, index=df2.columns[:len(new)]), ignore_index=True)
    
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
        
        df2.loc['06-Other operating cash flow adjustments'] = 0
        df2.loc['07-Total Operating Cashflow'] = df2.loc['02-Net Income'] + df2.loc['03-Depreciation and amortization'] + df2.loc['04-1-Accounts Receivable'] + df2.loc['04-2-Inventories'] + df2.loc['04-3-Prepaid Expenses'] + df2.loc['05-1-Accounts Payable'] + df2.loc['05-2-Accrued Liabilities'] + df2.loc['05-3-Other Short Term Liabilities']
        
        #investing cashflow calculations
        df2.loc['10-Capital asset acquisitions/disposal'] = df_b2.loc['07-2-Total Non-Current Assets']
        df2.loc['11-Other investing cash flows'] = 0
        df2.loc['12-Total Investing Cashflow'] = df2.loc['10-Capital asset acquisitions/disposal'] + df2.loc['11-Other investing cash flows']

        #financing cashflow calculations
        df2.loc['15-Increase/Decrease in Debt'] = df_b2.loc['11-1-Debt']
        df2.loc['16-Increase/Decrease in Equity'] = df_b2.loc['13-1-Total Shareholders Equity']
        df2.loc['16-1-Increase/Decrease in Retained Earning'] = df_b2.loc['13-2-Retained Earnings']
        df2.loc['17-Other financing cash flows'] = df_b2.loc['12-2-Total Non-Current Liabilities']
        df2.loc['18-Total Financing Cashflow'] = df2.loc['15-Increase/Decrease in Debt'] + df2.loc['16-Increase/Decrease in Equity'] + df2.loc['16-1-Increase/Decrease in Retained Earning'] + df2.loc['17-Other financing cash flows']
        
        ##change in cash calculations
        df2.loc['20-Year Opening Cash'] = df_b22.loc['01-3-Cash'].shift(periods=1,axis=0)
        df2.loc['21-Change in Cash'] = -(df2.loc['07-Total Operating Cashflow'] + df2.loc['12-Total Investing Cashflow'] + df2.loc['18-Total Financing Cashflow'] - df2.loc['16-1-Increase/Decrease in Retained Earning'] - df2.loc['03-Depreciation and amortization'])
        df2.loc['22-Year Ending Cash'] = df2.loc['20-Year Opening Cash'] + df2.loc['21-Change in Cash']
    except Exception as e:
        print(e)
        pass
    
    cashflow_s_dict[key] = df2.sort_index().reset_index().fillna('-')

##### statement 3 dict

statement_3_dict = {}
for key in income_s_dict:
    print(key)
    df_i3 = income_s_dict[key].rename(columns={'Income Statement':'Description'})
    df_b3 = balance_s_dict[key].rename(columns={'Balance Sheet':'Description'})
    df_ap3 = ap_final_dict[key].rename(columns={'AP_TYPE':'Description'})
    df_c = cashflow_s_dict[key]
    
    df12 = pd.concat([df_i3,df_b3,df_ap3,df_c]).reset_index(drop=True)
    daysinyear = 365
    #ratios
    new = ['Ratios']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['COGS Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Gross Profit Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Operating Profit']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Net Profit Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)

    ##coverages
    new = ['Tax Coverage']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Interest Coverage']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)

    #expense ratios
    new = ['OAE Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['S&D Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Deprication Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Unusual Expense Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Other Operating Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Interest Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Tax Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)

    #efficiency ratios
    new = ['Quick Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Quick Ratio Adjusted']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Current Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Current Ratio Adjusted']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)

    #asset ratios
    new = ['Total Asset Turnover Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Net Asset Turnover Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)

    #working capital days
    new = ['Inventory Turnover']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Inventory Days']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Accounts Receivable Turnover']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Accounts Receivable Days']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Accounts Payable Turnover']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Accounts Payable Turnover*']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Accounts Payable Days']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Accounts Payable Days*']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)

    #other ratios
    new = ['PP&E Ratio']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Working Capital Turnover']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Working Capital Turnover*']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)

    #debt ratios
    new = ['Cash Turnover']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Debt/Equity']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Debt/Capital']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Debt/TNW']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Total Liabilities/Equity']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Total Liabilities/Equity*']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Total Assets to Equity']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)


    new = ['Debt/EBITDA']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Capital Structure Impact']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Acid Test']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['Acid Test*']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['ROE']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)
    new = ['ROA']
    df12 = df12.append(pd.Series(new, index=df12.columns[:len(new)]), ignore_index=True)

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
    except Exception as e:
        print(e)
        pass
    
    statement_3_dict[key] = (df12*-1).round(3).reset_index().fillna(0)


    ###input must be current year and analysis end month (i have coded the input already)
    ### no html 
    ### provide the following in excel sheets : main_data_dict_pl[key],main_data_dict_bs[key],statement_3_dict[key] (key is business Id so each zid in different sheets)

# main_df_dict_list
with pd.ExcelWriter('main_data_pl.xlsx') as writer:
        for i in main_data_dict_pl:
            try:
                main_data_dict_pl[i].to_excel(writer, sheet_name=f"{i}")
            except ValueError as e:
                print (e)


# main_data_dict_bs 
with pd.ExcelWriter('main_data_bs.xlsx') as writer:
        for i in main_data_dict_bs:
            try:
                main_data_dict_bs[i].to_excel(writer, sheet_name=f"{i}")
            except ValueError as e:
                print (e)

# statement_3
with pd.ExcelWriter('statement_3.xlsx') as writer:
        for i in statement_3_dict:
            try:
                statement_3_dict[i].to_excel(writer, sheet_name=f"{i}")
            except ValueError as e:
                print (e)

# Email Part.

def send_mail(subject, bodyText, attachment, recipient = ['XXXXXX@gmail.com']):
	me = "XXXXXX@gmail.com"
	you = recipient
	msg = MIMEMultipart('alternative')
	msg['Subject'] = subject
	msg['From'] = me
	msg['To'] = ", ".join(you)
	text = bodyText
#### if no attachment file provide in send mail argument then attachment part will ignore
	if not attachment: 
		part1 = MIMEText(text, "plain")
		msg.attach(part1)
	else:
		part1 = MIMEText(text, "plain")
		msg.attach(part1)

		for i in range (0, len(attachment)):
			part2 = MIMEBase('application', "octet-stream")
			part2.set_payload(open(attachment[i], "rb").read())
			encoders.encode_base64(part2)
			part2.add_header(f'Content-Disposition', 'attachment; filename="{}"'.format(attachment[i]))
			msg.attach(part2)

	username = 'XXXXXX'
	password = 'XXXXXX'

	s = smtplib.SMTP('smtp.gmail.com:587')
	s.starttls()
	s.login(username, password)
	s.sendmail(me,you,msg.as_string())
	s.quit()



attachment=['main_data_pl.xlsx', 'main_data_bs.xlsx', 'statement_3.xlsx']
recipient = ['XXXXXX']
body_text = "please find the attachment"
subject =  f"Month Wise hmbr profit and loss {current_year} - {current_end_month}"

send_mail(subject, body_text , attachment, recipient)