# %%
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
import xlrd
from dateutil.relativedelta import relativedelta



# %%

def get_cus(zid):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost/da')
    df = pd.read_sql("""SELECT cacus.xcus,cacus.xshort,cacus.xadd2, cacus.xcity,cacus.xstate FROM cacus WHERE zid = '%s'"""%(zid),con=engine)
    return df

def get_sales(zid,year,month):
    year_month = str(year) +str(month)
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost/da')
    df = pd.read_sql("""SELECT imtrn.xcus,imtrn.xitem, imtrn.xyear, imtrn.xper, imtrn.xdate , imtrn.xqty, imtrn.xdoctype ,imtrn.xdocnum,  opdor.xdornum, opdor.xordernum, opddt.xrate , opddt.xlineamt, opdor.xdiscamt, opdor.xtotamt, opdor.xsp
                    FROM imtrn
                    JOIN opddt
                    ON imtrn.xdocnum = opddt.xdornum
                    AND imtrn.xitem = opddt.xitem
                    JOIN opdor
                    ON imtrn.xdocnum = opdor.xdornum
                    WHERE imtrn.zid = '%s'
                    AND opddt.zid = '%s'
                    AND opdor.zid = '%s'
                    AND CONCAT(imtrn.xyear,imtrn.xper) >= '%s'
                    AND imtrn.xdoctype = '%s'"""%(zid,zid,zid,year_month,'DO--'),con=engine)
    return df


# as there is no reference in opord and imtrn so we need at first get zepto opdor+imtrn sale and put xordernum from opdor then we create realationship with opord table later in dataframe
def zepto_get_sales_co(zid,  co_numbers):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost/da')
    df = pd.read_sql("""select opord.xordernum, sum (opodt.xlineamt) as total  from opord
                    inner join opodt
                    on opord.xordernum= opodt.xordernum
                    where opord.zid= %s
                    and opodt.zid = %s
                    and opord.xordernum in %s
                    group by opord.xordernum
                    """%(zid,zid,co_numbers),con=engine)

    return df


# %%



def get_return(zid,year,month):
    year_month = str(year) + str(month)
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost/da')
    df = pd.read_sql("""SELECT imtrn.xcus, imtrn.xitem, imtrn.xyear, imtrn.xper, imtrn.xdate,imtrn.xqty, opcdt.xrate, (opcdt.xrate*imtrn.xqty) as totamt, imtrn.xdoctype ,imtrn.xdocnum, opcrn.xemp
                        FROM imtrn 
                        JOIN opcdt
                        ON imtrn.xdocnum = opcdt.xcrnnum
                        AND imtrn.xitem = opcdt.xitem
                        JOIN opcrn
                        ON imtrn.xdocnum = opcrn.xcrnnum
                        WHERE imtrn.zid = '%s'
                        AND opcdt.zid = '%s'
                        AND opcrn.zid = '%s'
                        AND CONCAT(imtrn.xyear,imtrn.xper) >= '%s'
                        AND imtrn.xdoctype = '%s'"""%(zid,zid,zid,year_month,'SR--'),con=engine)
    return df

def get_acc_receivable(zid, proj, year, month):
    year_month = str(year) + str(month)
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost/da')
    df = pd.read_sql("""SELECT gldetail.xsub, SUM(gldetail.xprime) as AR
                        FROM glheader
                        JOIN gldetail
                        ON glheader.xvoucher = gldetail.xvoucher
                        JOIN cacus
                        ON gldetail.xsub = cacus.xcus
                        WHERE glheader.zid = '%s'
                        AND gldetail.zid = '%s'
                        AND cacus.zid = '%s'
                        AND gldetail.xproj = '%s'
                        AND gldetail.xvoucher NOT LIKE 'OB--%%'
                        AND CONCAT(glheader.xyear,glheader.xper) <= '%s'
                        GROUP BY gldetail.xsub"""%(zid,zid,zid,proj,year_month),con=engine)
    return df



# %%


def get_employee(zid):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost/da')
    df = pd.read_sql("""SELECT xemp,xname,xdept,xdesig,xstatusemp FROM prmst WHERE zid = '%s'"""%(zid),con=engine)
#     df = df[(df['xdept']=='Sales & Marketing')|(df['xdept']=='Marketing')|(df['xdept']=='Sales')]
#     df = df[df['xstatusemp']=='A-Active']
    return df
#find the different employee ID between zepto and hmbr using prmst of Both



# %%


df_overall_target = pd.read_excel('aws_target.xlsx', engine = 'openpyxl')
df_overall_target = df_overall_target[df_overall_target.columns[1:4]] ##discuss with sales on this excel issue
df_overall_target = df_overall_target.groupby(['Market'])['HMBR'].sum().reset_index().round(2).rename(columns={'Market':'xstate','HMBR':'Target'})
df_salesman_target = pd.read_excel('sw_target.xlsx', engine = 'openpyxl')
df_salesman_target = df_salesman_target[df_salesman_target.columns[0:3]] ##discuss with sales on this excel issue
df_salesman_target = df_salesman_target.rename(columns={'Employee Code':'xsp'})



# %%


####add zepto target sheet here####

#get all the customers involved for HMBR
zid_trading = 100001 
zid_karigor = 100000
zid_zepto = 100005
proj_trading = 'GULSHAN TRADING'
proj_zepto = 'Zepto Chemicals'
proj_karigor = 'Karigor Ltd.'



# %%



#when going to the live site we need to remove the time delta to reflect this month
this_datetime = datetime.now()
number_day = this_datetime.day

month_list_6 = [(this_datetime - relativedelta(months=i)).strftime('%Y/%m') for i in range(6)]
month_list_24 = [(this_datetime - relativedelta(months=i)).strftime('%Y/%m') for i in range(24)]

start_year = int(month_list_24[-1].split('/')[0])
start_month = int(month_list_24[-1].split('/')[1])
end_year = int(month_list_24[0].split('/')[0])
end_month = int(month_list_24[0].split('/')[1])
last_year =  int(month_list_24[1].split('/')[0])
last_month = int(month_list_24[1].split('/')[1])



# %%



#HMBR employee Data
df_emp_h = get_employee(zid_trading)
df_emp_h = df_emp_h.rename(columns={'xemp':'xsp'})
df_emp_h = df_emp_h[(df_emp_h['xdept']=='Sales & Marketing')|(df_emp_h['xdept']=='Marketing')|(df_emp_h['xdept']=='Sales')]
df_emp_h = df_emp_h[df_emp_h['xstatusemp']=='A-Active']



# %%



#karigor employee data
df_emp_k = get_employee(zid_karigor)
df_emp_k = df_emp_k.rename(columns={'xemp':'xsp'})



# %%



#Zepto Employee data
df_emp_z = get_employee(zid_zepto)
df_emp_z = df_emp_z.rename(columns={'xemp':'xsp'})
df_emp_z['businessId'] = np.where((df_emp_z['xdept']!= ''), 'Zepto', 'HMBR')
df_emp_z.loc[df_emp_z['xsp'].str.startswith('AD'),'businessId'] = 'Fixit'
df_emp_z.loc[df_emp_z['xsp'].str.startswith('EC'),'businessId'] = 'E-Commerce'
df_emp_z.loc[df_emp_z['xsp'].str.startswith('RD'),'businessId'] = 'Other'



# %%



#HMBR
#overall performance with this month total vs target, last 6 months overall and last 24 months all customers

#customer information is common for all months and held as the base dataframe
df_cus_h = get_cus(zid_trading).sort_values('xcus')
# for this month:
df_sales_h = get_sales(zid_trading,start_year,start_month)
df_return_h = get_return(zid_trading,start_year,start_month).rename(columns={'xemp':'xsp'})



# %%
df_sales_h.head(2)

# %%
df_sales_h = df_sales_h.drop(columns=['xordernum'])

# %%



#get this month and balance from last month
#cancel accounts from here and have an individual sheet where this is happening every 7 days with accounts payable
# df_acc_h = get_acc_receivable(zid_trading,proj_trading,end_year,end_month).rename(columns={'xsub':'xcus'})
# df_acc_h_l = get_acc_receivable(zid_trading,proj_trading,last_year,last_month).rename(columns={'xsub':'xcus'})

#grouping together the values as per market or xstate to make the final table

#final over all for hmbr customer wise
df_sales_g_h = df_sales_h.groupby(['xcus','xyear','xper','xsp'])['xlineamt'].sum().reset_index().round(2)
df_return_g_h = df_return_h.groupby(['xcus','xyear','xper','xsp'])['totamt'].sum().reset_index().round(2)
df_hmbr_g_h = df_cus_h.merge(df_sales_g_h[['xcus','xyear','xper','xsp','xlineamt']],on=['xcus'],how='left').merge(df_return_g_h[['xcus','xyear','xper','xsp','totamt']],on=['xcus','xyear','xper','xsp'],how='left').fillna(0)
df_hmbr_g_h['HMBR'] = df_hmbr_g_h['xlineamt'] - df_hmbr_g_h['totamt']
# df_hmbr_g_h['Net']
df_hmbr_g_h = df_hmbr_g_h.drop(columns=['xlineamt','totamt'])
df_hmbr_g_h['xyear'] = df_hmbr_g_h['xyear'].astype(np.int64)
df_hmbr_g_h['xper'] = df_hmbr_g_h['xper'].astype(np.int64)
df_hmbr_g_h['time_line'] = df_hmbr_g_h['xyear'].astype(str)+'/'+df_hmbr_g_h['xper'].astype(str)
df_hmbr_customer = pd.pivot_table(df_hmbr_g_h,values='HMBR', index=['xcus','xshort','xcity'],columns=['time_line'],aggfunc=np.sum).reset_index().drop(columns=['0/0']).fillna(0)
df_hmbr_salesman = pd.pivot_table(df_hmbr_g_h,values='HMBR', index=['xsp'],columns=['time_line'],aggfunc=np.sum).reset_index().drop(columns=['0/0']).fillna(0)
df_hmbr_salesman = df_hmbr_salesman.merge(df_emp_h[['xsp','xname']],on=['xsp'],how='left')


df_hmbr_overall = df_hmbr_g_h[(df_hmbr_g_h['xyear']==end_year) & (df_hmbr_g_h['xper']==end_month)]
df_hmbr_overall = df_hmbr_overall.groupby(['xstate'])['HMBR'].sum().reset_index().round(2)

df_hmbr_overall = df_hmbr_overall.merge(df_overall_target[['xstate','Target']],on=['xstate'],how='left')



# %%



#Zepto overall performace for HMBR 

# #Zepto
df_cus_z = get_cus(zid_zepto).sort_values('xcus')
df_sales_z = get_sales(zid_zepto,start_year,start_month).rename(columns={'xemp':'xsp'}).merge(df_emp_z[['xsp','businessId']],on=['xsp'],how='left')
df_sales_z

# %%
df_sales_z_h = df_sales_z[df_sales_z['businessId']=='HMBR']
df_return_z = get_return(zid_zepto,start_year,start_month).rename(columns={'xemp':'xsp'}).merge(df_emp_z[['xsp','businessId']],on=['xsp'],how='left')
df_return_z_h = df_return_z[df_return_z['businessId']=='HMBR']
# # #can use this later as well in the zepto section
# # df_acc_z = get_acc_receivable(zid_zepto,proj_zepto).rename(columns={'xsub':'xcus'})



# %%



# # #final for zepto(all) customer wise which can be converted to 
df_sales_g_z = df_sales_z.groupby(['xcus','xyear','xper','xsp','businessId', 'xdocnum','xordernum'])['xlineamt'].sum().reset_index().round(2)
df_return_g_z = df_return_z.groupby(['xcus','xyear','xper','xsp'])['totamt'].sum().reset_index().round(2)

df_sales_g_z

# %%
co_list = tuple(df_sales_g_z['xordernum'])


# %%
# the accurate sales come from opord.xtotamt or group by opodt.xlineamt. so we need to replace above xlineamt from opdor

df_sales_g_z_co =  zepto_get_sales_co(zid_zepto,co_list)
df_sales_g_z_co

# %%
# now merge with df_sales_g_z thus we can get actual amount 

df_sales_g_z = pd.merge(df_sales_g_z, df_sales_g_z_co, on = 'xordernum' , how = 'left')
df_sales_g_z

# %%
# now drop previous xlineamt from do and rename order total to xlineamt from co

df_sales_g_z = df_sales_g_z.drop(columns= ['xlineamt']).rename(columns={'total' : 'xlineamt' })
df_sales_g_z

# %%



# #final for all
df_zepto_g_z = df_cus_h.merge(df_sales_g_z[['xcus','xyear','xper','xsp','businessId','xlineamt']],on=['xcus'],how='left').merge(df_return_g_z[['xcus','xyear','xper','xsp','totamt']],on=['xcus','xyear','xper','xsp'],how='left').fillna(0)

#final for HMBR
# df_zepto_g_zh = df_zepto_g_z[df_zepto_g_z['businessId']=='HMBR']

df_zepto_g_z['Zepto'] = df_zepto_g_z['xlineamt'] - df_zepto_g_z['totamt']
df_zepto_g_z = df_zepto_g_z.drop(columns=['xlineamt','totamt'])
df_zepto_g_z['xyear'] = df_zepto_g_z['xyear'].astype(np.int64)
df_zepto_g_z['xper'] = df_zepto_g_z['xper'].astype(np.int64)
df_zepto_g_z['time_line'] = df_zepto_g_z['xyear'].astype(str)+'/'+df_zepto_g_z['xper'].astype(str)
df_zepto_customer = pd.pivot_table(df_zepto_g_z,values='Zepto', index=['xcus','xshort','xcity'],columns=['time_line'],aggfunc=np.sum).reset_index().drop(columns=['0/0']).fillna(0)
df_zepto_salesman = pd.pivot_table(df_zepto_g_z,values='Zepto', index=['xsp'],columns=['time_line'],aggfunc=np.sum).reset_index().drop(columns=['0/0']).fillna(0)
df_zepto_salesman = df_zepto_salesman.merge(df_emp_z[['xsp','xname']],on=['xsp'],how='left')
df_zepto_customer



# %%



#final for HMBR
df_zepto_g_zh = df_zepto_g_z[df_zepto_g_z['businessId']=='HMBR']

df_zepto_overall = df_zepto_g_zh[(df_zepto_g_z['xyear']==end_year) & (df_zepto_g_zh['xper']==end_month)]
remove_salesman_list = ['SA--000446', 'SA--000100', 'SA--000431', 'SA--000443', 'SA--000440', 'SA--000425', 'SA--000200', 'SA--000421', 'SA--000199', 'SA--000427', 'SA--000448', 'SA--000409', 'SA--000194', 'SA--000196', 'SA--000428', 'SA--000430']
df_zepto_overall = df_zepto_overall[~df_zepto_overall['xsp'].isin(remove_salesman_list)]
df_zepto_overall = df_zepto_overall.groupby(['xstate'])['Zepto'].sum().reset_index().round(2)



df_hmbr_overall = df_hmbr_overall.merge(df_zepto_overall[['xstate','Zepto']],on=['xstate'],how='left')
df_hmbr_overall



# %%



# df_zepto_overall = df_zepto_overall.merge(df_overall_target[['xstate','HMBR']],on=['xstate'],how='left')

# #karigor 

#customer information is common for all months and held as the base dataframe
df_cus_k = get_cus(zid_karigor).sort_values('xcus')
# for this month:
df_sales_k = get_sales(zid_karigor,start_year,start_month)
df_return_k = get_return(zid_karigor,start_year,start_month).rename(columns={'xemp':'xsp'})



# %%



#get this month and balance from last month
#cancel accounts from here and have an individual sheet where this is happening every 7 days with accounts payable
# df_acc_h = get_acc_receivable(zid_trading,proj_trading,end_year,end_month).rename(columns={'xsub':'xcus'})
# df_acc_h_l = get_acc_receivable(zid_trading,proj_trading,last_year,last_month).rename(columns={'xsub':'xcus'})

#grouping together the values as per market or xstate to make the final table

#final over all for hmbr customer wise
df_sales_g_k = df_sales_k.groupby(['xcus','xyear','xper','xsp'])['xlineamt'].sum().reset_index().round(2)
df_return_g_k = df_return_k.groupby(['xcus','xyear','xper','xsp'])['totamt'].sum().reset_index().round(2)
df_karigor_g_k = df_cus_h.merge(df_sales_g_k[['xcus','xyear','xper','xsp','xlineamt']],on=['xcus'],how='left').merge(df_return_g_k[['xcus','xyear','xper','xsp','totamt']],on=['xcus','xyear','xper','xsp'],how='left').fillna(0)
df_karigor_g_k['karigor'] = df_karigor_g_k['xlineamt'] - df_karigor_g_k['totamt']

df_karigor_g_k = df_karigor_g_k.drop(columns=['xlineamt','totamt'])
df_karigor_g_k['xyear'] = df_karigor_g_k['xyear'].astype(np.int64)
df_karigor_g_k['xper'] = df_karigor_g_k['xper'].astype(np.int64)
df_karigor_g_k['time_line'] = df_karigor_g_k['xyear'].astype(str)+'/'+df_karigor_g_k['xper'].astype(str)
df_karigor_customer = pd.pivot_table(df_karigor_g_k,values='karigor', index=['xcus','xshort','xcity'],columns=['time_line'],aggfunc=np.sum).reset_index().drop(columns=['0/0']).fillna(0)
df_karigor_salesman = pd.pivot_table(df_karigor_g_k,values='karigor', index=['xsp'],columns=['time_line'],aggfunc=np.sum).reset_index().drop(columns=['0/0']).fillna(0)
df_karigor_salesman = df_karigor_salesman.merge(df_emp_k[['xsp','xname']],on=['xsp'],how='left')


df_karigor_overall = df_karigor_g_k[(df_karigor_g_k['xyear']==end_year) & (df_karigor_g_k['xper']==end_month)]
df_karigor_overall = df_karigor_overall.groupby(['xstate'])['karigor'].sum().reset_index().round(2)

df_hmbr_overall = df_hmbr_overall.merge(df_karigor_overall[['xstate','karigor']],on=['xstate'],how='left').fillna(0)
df_hmbr_overall['Total_net_Sales'] = df_hmbr_overall['HMBR'] + df_hmbr_overall['Zepto'] + df_hmbr_overall['karigor']
df_hmbr_overall



# %%


df_hmbr_overall



# %%


df_hmbr_overall['Difference'] = df_hmbr_overall['Target'] - df_hmbr_overall['Total_net_Sales'] 
df_hmbr_overall['% Achievement'] = (df_hmbr_overall['Total_net_Sales'] / df_hmbr_overall['Target'])*100

df_hmbr_overall = df_hmbr_overall[['xstate','HMBR','karigor','Zepto','Total_net_Sales','Target','Difference','% Achievement']]

last_row = {'xstate':'Grand_Total',
            'HMBR':df_hmbr_overall['HMBR'].sum(),
            'karigor':df_hmbr_overall['karigor'].sum(),
            'Zepto':df_hmbr_overall['Zepto'].sum(),
            'Total_net_Sales':df_hmbr_overall['Total_net_Sales'].sum(),
            'Target':df_hmbr_overall['Target'].sum(),
            'Difference':df_hmbr_overall['Difference'].sum(),
            '% Achievement':(df_hmbr_overall['Total_net_Sales'].sum()/df_hmbr_overall['Target'].sum())*100
           }

df_hmbr_overall = df_hmbr_overall.append(last_row, ignore_index = True)

last_row = {
            'xstate':'',
            'HMBR':'Overall',
            'karigor':'',
            'Zepto':'',
            'Total_net_Sales':'Required %',
            'Target':'',
            'Difference':'',
            '% Achievement':(100/30.5)*number_day
           }

df_hmbr_overall = df_hmbr_overall.append(last_row, ignore_index = True)

last_row = {
            'xstate':'',
            'HMBR':'Overall',
            'karigor':'',
            'Zepto':'',
            'Total_net_Sales':'Gap %',
            'Target':'',
            'Difference':'',
            '% Achievement': df_hmbr_overall.loc[df_hmbr_overall['xstate']=='Grand_Total']['% Achievement'].values[0]- df_hmbr_overall.loc[df_hmbr_overall['Total_net_Sales']=='Required %']['% Achievement'].values[0]  
           }

df_hmbr_overall = df_hmbr_overall.append(last_row, ignore_index = True).fillna(0).round(2)

df_hmbr_sales_t = df_hmbr_salesman[['xsp',df_hmbr_salesman.columns[-2]]]
df_hmbr_sales_t = df_hmbr_sales_t.merge(df_emp_k[['xsp','xname']],on=['xsp'],how='left').merge(df_salesman_target[['xsp','Employee Name','Monthly Target']],on=['xsp'],how='left').fillna(0)

df_hmbr_sales_t = df_hmbr_salesman[['xsp',df_hmbr_salesman.columns[-2]]]
df_hmbr_sales_t = df_hmbr_sales_t.merge(df_emp_k[['xsp','xname']],on=['xsp'],how='left').merge(df_salesman_target[['xsp','Employee Name','Monthly Target']],on=['xsp'],how='left').fillna(0)
df_hmbr_sales_t = df_hmbr_sales_t[df_hmbr_sales_t['Employee Name']!=0]
df_hmbr_sales_t = df_hmbr_sales_t[['xsp','Employee Name',df_hmbr_salesman.columns[-2],'Monthly Target']]

df_hmbr_sales_t['% Achieved'] = (df_hmbr_sales_t[df_hmbr_salesman.columns[-2]]/df_hmbr_sales_t['Monthly Target'])*100
df_hmbr_sales_t = df_hmbr_sales_t.round(2)



# %%


df_emp_name=df_emp_h.iloc[:,0:2]



# %%


#df_hmbr_overall (Print HTML & as a sheet in HMBR Excel File)
#df_hmbr_sales_t (Print HTML $ as a sheet in HMBR Excel File)

# df_hmbr_customer (Save as sheet in HMBR Excel File)
# df_hmbr_salesman (Save as sheet in HMBR Excel File) also merge df_emp_h for salesman name

# df_karigor_customer (Save as sheet in HMBR Excel File)
# df_karigor_salesman (Save as sheet in HMBR Excel File) also merge df_emp_k for salesman name



# df_zepto_customer (Save as sheet in HMBR Excel File)
# df_zepto_salesman (Save as sheet in HMBR Excel File) also merge df_emp_k for salesman name



# %%


import xlsxwriter
DATA_DIR = "/"
options = {}
options['strings_to_formulas'] = False
options['strings_to_urls'] = False
writer = pd.ExcelWriter('SalesTarget.xlsx',engine='xlsxwriter',options=options)




df_hmbr_overall.to_excel(writer,"HmbrOverAllSummary", index=False)
df_hmbr_sales_t.to_excel(writer,"HmbrOverAllSaleSummary",index=False)
df_hmbr_customer.to_excel(writer,"HmbrCustomerWise", index=False)
df_hmbr_salesman.to_excel(writer,"HmbrSalesManWise", index=False)
df_karigor_customer.to_excel(writer,"KarigorCustomerWise", index=False)
df_karigor_salesman.to_excel(writer,"KarigorSalesManWise", index=False)
df_zepto_customer.to_excel(writer,"ZeptoCustomertWise", index=False)
df_zepto_salesman.to_excel(writer,"ZeptoSalesManWise", index=False)





writer.save()
writer.close()



import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pretty_html_table import build_table
import random


##################if 1 df then funcion will one#########
def get_dataFrame(): 
	return df_hmbr_overall
    


def send_mail(body):
    ##################if 1 df then parameter will one#########
	me = "XXXXXX@gmail.com"
	you = ["XXXXXX@gmail.com", "XXXXXX@gmail.com","XXXXXX@gmail.com","XXXXXX@gmail.com","XXXXXX@gmail.com", "XXXXXX@gmail.com", "XXXXXX@gmail.com"]
	#you = ["motiurrahman7770@gmail.com","shahalamhmbr@gmail.com"] "asaddat87@gmail.com",,
	msg = MIMEMultipart()
	msg['Subject'] = "HMBR Overall Target"
	msg['From'] = me
	msg['To'] = ", ".join(you)


    



	part = MIMEBase('application', "octet-stream")
	part.set_payload(open("SalesTarget.xlsx", "rb").read()) 
	encoders.encode_base64(part)
	part.add_header('Content-Disposition', 'attachment; filename="SalesTarget.xlsx"')
	msg.attach(part)
	
	
	
	
	body_content = body ##################if 1 df then body_content will one#########
	heading="<h2 style='color:red'> HMBR OVERALL SUMMARY </h2>"
	
	msg.attach(MIMEText(heading, "html"))
	msg.attach(MIMEText(body_content, "html"))
##################if 1 df then attach will one#########
	username = 'XXXXXX@gmail.com'
	password = 'XXXXXX'
	s = smtplib.SMTP('smtp.gmail.com:587')
	s.starttls()
	s.login(username, password)
	s.sendmail(me,you,msg.as_string())
	s.quit()
	
def send_html_table():
	data_table = get_dataFrame()
##################if 1 df then parameter will one#########
	
	themes=['blue_dark','grey_dark','orange_dark','green_dark','red_dark']
	themes=random.choice(themes)
	output = build_table(data_table
            , 'yellow_dark'
            , font_size='medium'
            , font_family='Open Sans, sans-serif'
            , text_align='left'


            ,conditions={
                '% Achievement': {
                    'min': 50,
                    'max': 60,
                    'min_color': 'red',
                    'max_color': 'green',
                }
            }
			, even_color='black'
			, even_bg_color='white')

	#output1 = build_table(data_table1, 'red_dark',font_size = 'small', font_family = 'arial',  text_align = 'left')##################if 1 df then output will one#########
	send_mail(output)##################if 1 df then parameter will one#########
	return "Mail sent successfully."


send_html_table()

	






