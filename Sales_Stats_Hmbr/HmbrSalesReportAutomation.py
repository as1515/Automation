from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import date,datetime,timedelta
import psycopg2
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import openpyxl

main_time = time.time()

def create_prmst(zid):
    start_time_prmst = time.time()
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df_prmst = pd.read_sql("select xemp,xname from prmst where zid = '%s'" % (zid), con = engine)
    end_time_prmst = time.time()
    print("--- %s seconds for create_prmst ---" % (end_time_prmst-start_time_prmst))
    return df_prmst

def create_cacus(zid):
    start_time_cacus= time.time()
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df_cacus = pd.read_sql("select zid,xcus,xshort,xadd2,xcity from cacus where zid = '%s'" % (zid), con = engine)
    end_time_cacus = time.time()
    print("--- %s seconds for create_cacus ---" % (end_time_cacus-start_time_cacus))
    return df_cacus

def create_caitem(zid):
    start_time_caitem=time.time()
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df_caitem = pd.read_sql("select zid,xitem,xdesc,xgitem,xstdprice,xsrate from caitem where zid = '%s'" % (zid), con = engine)
    end_time_caitem=time.time()
    print("--- %s seconds for create_caitem ---" % (end_time_caitem-start_time_caitem))
    return df_caitem

def create_opdor(zid):
    start_time_opdor=time.time()
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df_opdor = pd.read_sql("select xordernum,xdate,xcus,xdiv,xsp,xtotamt from opdor where zid = '%s'" % (zid), con = engine)
    end_time_opdor=time.time()
    print("--- %s seconds for create_opdor ---" % (end_time_opdor-start_time_opdor))
    return df_opdor

def create_opddt(zid):
    start_time_opddt=time.time()
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df_opddt = pd.read_sql("select zid,xordernum,xitem,xqty,xrate,xdisc,xdiscf,xlineamt,xdtwotax,xdtdisc,xdtcomm from opddt where zid = '%s'" % (zid), con = engine)
    end_time_opddt=time.time()
    print("--- %s seconds for create_opddt ---" % (end_time_opddt-start_time_opddt))
    return df_opddt

def create_opcrn(zid):
    start_opcrn=time.time()
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df_opcrn = pd.read_sql("select xcrnnum,xdate,xcus,xdisc,xdiscf,xglref,xordernum,xemp from opcrn where zid = '%s'" % (zid), con = engine)
    end_opddt=time.time()
    print("--- %s seconds for create_opcrn ---" % (end_opddt-start_opcrn))
    return df_opcrn

def create_opcdt(zid):
    start_opcdt= time.time()
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df_opcdt = pd.read_sql("select zid,xcrnnum,xitem,xqty,xdornum,xrate,xlineamt from opcdt where zid = '%s'" % (zid), con = engine)
    end_opcdt=time.time()
    print("--- %s seconds for create_opcdt ---" % (end_opcdt-start_opcdt))
    return df_opcdt

def create_rectreturn(zid):
    start_rectreturn=time.time()
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df_imtemptrn = pd.read_sql("select zid,ximtmptrn,xdate,xyear,xper,xcus,xemp,xarea,xtrnimf from imtemptrn where xstatustrn ='5-Confirmed' and zid = '%s'" % (zid), con = engine)
    df_imtemptdt = pd.read_sql("select ximtmptrn,xitem,xqtyord,xrate,xlineamt from imtemptdt where zid = '%s'" % (zid), con = engine)
    df_imtemptdt = df_imtemptdt.merge(df_imtemptrn,on='ximtmptrn',how='left')
    df_imtemptdt = df_imtemptdt[df_imtemptdt['xtrnimf']=='RECT']
    thisYear = datetime.now().year
    df_imtemptdt = df_imtemptdt[df_imtemptdt['xyear']==thisYear]
    end_rectreturn=time.time()
    print("--- %s seconds for create_rectreturn ---" % (end_rectreturn-start_rectreturn))
    return df_imtemptdt


start_main=time.time()
def create_mainsheet(zid):
    df_prmst = create_prmst(zid)
    df_cacus = create_cacus(zid)
    df_caitem = create_caitem(zid)
    df_opdor = create_opdor(zid)
    df_opddt = create_opddt(zid)
    df_opcdt = create_opcdt(zid)
    df_opcrn = create_opcrn(zid)
    df_main_sale = df_opddt.merge(df_opdor,on='xordernum',how='left')
    df_main_return = df_opcdt.merge(df_opcrn,on='xcrnnum',how='left')
    df_main_return = df_main_return.rename(columns={'zid':'zidreturn','xrate':'xratereturn','xlineamt':'xlineamtreturn','xdisc':'xdiscreturn','xdiscf':'xdiscfreturn','xcus':'xcusreturn','xdate':'xdatereturn','xqty':'xqtyreturn'})
    df_main = df_main_sale.merge(df_main_return,on=['xordernum','xitem'],how='left')
    df_main = df_main.merge(df_cacus,on='xcus',how='left')
    df_main = df_main.merge(df_caitem,on='xitem', how='left')
    df_prmst = df_prmst.rename(columns={'xemp':'xsp'})
    df_main = df_main.merge(df_prmst,on='xsp',how='left')
    df_main = df_main.fillna(value=0,axis=1)
    df_main['xfinallineamt']= df_main['xlineamt']-df_main['xlineamtreturn']
    df_main['xfinalqtydel']= df_main['xqty']-df_main['xqtyreturn']
    df_main['xfinalrate'] = df_main['xfinallineamt']/df_main['xfinalqtydel']
    df_main = df_main.drop(['zid_x','zid_y'],axis=1)
    thisYear = datetime.now().year
    df_main['xdate'] = pd.to_datetime(df_main['xdate'])
    df_main['Year'] = df_main['xdate'].dt.year
    df_main['Month'] = df_main['xdate'].dt.month
    df_main = df_main[df_main['Year']==thisYear]
    return df_main



zid = '100001'
df_main = create_mainsheet(zid)
end_main=time.time()
print("--- %s seconds for create_mainsheet ---" % (end_main-start_main))


#salesman wise product sales
start_time_df_salesmanwiseproductsales = time.time()
df_salesman_product = df_main.groupby(['xsp','xname','xitem','xdesc']).sum()[['xfinalqtydel']]
df_salesman_product = df_salesman_product.reset_index()
df_salesman_product = df_salesman_product[df_salesman_product['xname']!=0]
df_salesman_product['spname'] = df_salesman_product['xsp'] + ':-' + df_salesman_product['xname']
df_salesman_product['itemdesc'] = df_salesman_product['xitem'] + ':-' + df_salesman_product['xdesc']
df_salesman_product = df_salesman_product.pivot(index='spname',columns='itemdesc',values='xfinalqtydel')
df_salesman_product = df_salesman_product.reset_index()
df_salesman_product = df_salesman_product.rename(columns={'spname':'Salesman Name'})
df_salesman_product.loc['sum'] = df_salesman_product.sum(axis=0)
df_salesman_product = df_salesman_product.fillna(value=0,axis=1)
df_salesman_product.loc['sum','Salesman Name'] = 0
end_time_df_salesmanwiseproductsales = time.time()
print("--- %s seconds for salesman wise product sales ---" % (end_time_df_salesmanwiseproductsales-start_time_df_salesmanwiseproductsales))

#this month production.BS

this_month=time.time()
thisMonth = datetime.now().month

df_main_month = df_main[df_main['Month']==thisMonth]
df_salesman_product_month = df_main_month.groupby(['xsp','xname','xitem','xdesc']).sum()[['xfinalqtydel']]
df_salesman_product_month = df_salesman_product_month.reset_index()
df_salesman_product_month = df_salesman_product_month[df_salesman_product_month['xname']!=0]
df_salesman_product_month['spname'] = df_salesman_product_month['xsp'] + ':-' + df_salesman_product_month['xname']
df_salesman_product_month['itemdesc'] = df_salesman_product_month['xitem'] + ':-' + df_salesman_product_month['xdesc']
df_salesman_product_month = df_salesman_product_month.pivot(index='spname',columns='itemdesc',values='xfinalqtydel')
df_salesman_product_month = df_salesman_product_month.reset_index()
df_salesman_product_month = df_salesman_product_month.rename(columns={'spname':'Salesman Name'})
df_salesman_product_month.loc['sum'] = df_salesman_product_month.sum(axis=0)
df_salesman_product_month = df_salesman_product_month.fillna(value=0,axis=1)
df_salesman_product_month.loc['sum','Salesman Name'] = 0

end_this_month=time.time()

print("--- %s seconds for df_salesman_product ---" % (end_this_month-this_month))




#datewise product sales

date_wise=time.time()
df_datewise_product = df_main.groupby(['xdate','xitem','xdesc']).sum()[['xfinalqtydel']]
df_datewise_product = df_datewise_product.reset_index()
df_datewise_product['itemdesc'] = df_datewise_product['xitem'] + df_datewise_product['xdesc']
df_datewise_product = df_datewise_product.pivot(index='xdate',columns='itemdesc',values='xfinalqtydel')
df_datewise_product = df_datewise_product.reset_index()
df_datewise_product['Month'] = pd.DatetimeIndex(df_datewise_product['xdate']).month
monthList = list(set(df_datewise_product['Month'].tolist()))
monthDict = {1:'January',2:'February',3:'March',4:'April',5:'May',6:'June',7:'July',8:'August',9:'September',10:'October',11:'November',12:'December'}

# for m in monthList:
#     df_datewise_product = df_datewise_product.append(df_datewise_product[df_datewise_product['Month']==m].sum(numeric_only=True),ignore_index=True)
#     df_datewise_product.at[df_datewise_product.index[-1],'xdate'] = monthDict[m]

df_datewise_product = df_datewise_product.fillna(value=0,axis=1)

end_date_wise=time.time()
print("--- %s seconds for datewise product sales  ---" % (end_date_wise-date_wise ))


#create area wise product sales

area_wise=time.time()

df_areawise_product = df_main.groupby(['xdiv','xitem','xdesc']).sum()[['xfinalqtydel']]
df_areawise_product = df_areawise_product.reset_index()
df_areawise_product['itemdesc'] = df_areawise_product['xitem'] + ':-' + df_areawise_product['xdesc']
df_areawise_product = df_areawise_product.pivot(index='xdiv',columns='itemdesc',values='xfinalqtydel')
df_areawise_product = df_areawise_product.reset_index()
df_areawise_product = df_areawise_product.rename(columns={'xdiv':'Area'})
df_areawise_product.loc['sum'] = df_areawise_product.sum(axis=0)
df_areawise_product = df_areawise_product.fillna(value=0,axis=1)
df_areawise_product.loc['sum','Area'] = 0
end_area_wise=time.time()
print("--- %s seconds for area_wise product sales  ---" % (end_area_wise-area_wise ))

#Create Area-Customer wise product sales

customer_wise=time.time()
df_customer_area_product = df_main.groupby(['xdiv','xcus','xshort','xitem','xdesc']).sum()[['xfinalqtydel']]
df_customer_area_product = df_customer_area_product.reset_index()
df_customer_area_product = pd.pivot_table(df_customer_area_product,index=['xdiv','xcus','xshort'],columns=['xitem','xdesc'],aggfunc = np.sum)
df_customer_area_product = df_customer_area_product.fillna(value=0,axis=1)

infoDictMonthly = {}


thisDay = datetime.now().day
thisDayName = datetime.now().strftime("%A")
if thisDay == 2 and thisDayName == 'Saturday' :
    thisMonth = datetime.now().month - 1
elif thisDay == 1:
    thisMonth = datetime.now().month - 1
else:
    thisMonth = datetime.now().month


df_month = df_main[df_main['Month']==thisMonth]

infoDictMonthly['Month'] = thisMonth
end_customer_wise=time.time()

print("--- %s seconds for customer wise   ---" % (customer_wise-end_customer_wise ))


#salesman current month gross
s_man_gross=time.time()
gs = df_month.groupby(['xsp','xname']).sum()[['xfinallineamt']]

#highest grossing salesman
hgsList = list(gs['xfinallineamt'].idxmax())
hgsList.append(gs['xfinallineamt'].max())
infoDictMonthly['Salesman with Highest Gross Sales'] = hgsList

#Lowest grossing salesman
lgsList = list(gs['xfinallineamt'].idxmin())
lgsList.append(gs['xfinallineamt'].min())
infoDictMonthly['Salesman with Lowest Gross Sales'] = lgsList

#Area current Month Gross
ga = df_month.groupby(['xdiv']).sum()[['xfinallineamt']]

#Highest grossing Area
hgaList = [ga['xfinallineamt'].idxmax()]
hgaList.append(ga['xfinallineamt'].max())
infoDictMonthly['Area with Highest Gross Sales'] = hgaList
end_h_g_area=time.time()
print("--- %s seconds for from salesman current month gross to Highest grossing Area  ---" % (end_h_g_area-s_man_gross ))



#Lowest Grossing Area
l_g_a=time.time()

lgaList = [ga['xfinallineamt'].idxmin()]
lgaList.append(ga['xfinallineamt'].min())
infoDictMonthly['Area with Lowest Gross Sales'] = lgaList

#customer current month gross
gc = df_month.groupby(['xcus','xshort','xdiv']).sum()[['xfinallineamt']]

#highest grossing customer
hgcList = list(gc['xfinallineamt'].idxmax())
hgcList.append(gc['xfinallineamt'].max())
infoDictMonthly['Customer with Highest Gross Sales'] = hgcList

#lowest grossing customer
lgcList = list(gc['xfinallineamt'].idxmin())
lgcList.append(gc['xfinallineamt'].min())
infoDictMonthly['Customer with Lowest Gross Sales'] = lgcList
end_lgc=time.time()
print("--- %s seconds for from lowest grossing area to lowest grossing customer   ---" % (end_lgc-l_g_a ))

#item current month gross

icmg=time.time()
gi = df_month.groupby(['xitem','xdesc'])[['xfinallineamt']].sum()

#highest grossing item
hgiList = list(gi['xfinallineamt'].idxmax())
hgiList.append(gi['xfinallineamt'].max())
infoDictMonthly['Item with Highest Gross Sales'] = hgiList

#Lowest Grossing Item
lgiList = list(gi['xfinallineamt'].idxmin())
lgiList.append(gi['xfinallineamt'].min())
infoDictMonthly['Item with Lowest Gross Sales'] = lgiList

#salesman current month unit sold
uss = df_month.groupby(['xsp','xname']).sum()[['xfinalqtydel']]

endicmg=time.time()
print("--- %s seconds for from item current month gross to salesman current month unit sold  ---" % (endicmg-icmg ))


#highest unit selling salesman
huss=time.time()
hussList = list(uss['xfinalqtydel'].idxmax())
hussList.append(uss['xfinalqtydel'].max())
infoDictMonthly['Salesman with Highest Unit Sold'] = hussList

#Lowest unit selling salesman
lussList = list(uss['xfinalqtydel'].idxmin())
lussList.append(uss['xfinalqtydel'].min())
infoDictMonthly['Salesman with Lowest Unit Sold'] = lussList

#Area unit sold
usa = df_month.groupby(['xdiv']).sum()[['xfinalqtydel']]
endhuss=time.time()
print("--- %s seconds for from highest unit selling salesman to area unit sold   ---" % (endhuss-huss))

#Highest unit sold Area
husa=time.time()
husaList = [usa['xfinalqtydel'].idxmax()]
husaList.append(usa['xfinalqtydel'].max())
infoDictMonthly['Area with Highest Unit Sold'] = husaList

#Lowest Unit Selling Area
lusaList = [usa['xfinalqtydel'].idxmin()]
lusaList.append(usa['xfinalqtydel'].min())
infoDictMonthly['Area with Lowest Unit Sold'] = lusaList

#customer unit sold this month
usc = df_month.groupby(['xcus','xshort','xdiv']).sum()[['xfinalqtydel']]

#highest unit sold to customer
huscList = list(usc['xfinalqtydel'].idxmax())
huscList.append(usc['xfinalqtydel'].max())
infoDictMonthly['Customer who bought Highest Units'] = huscList
hustc=time.time()
print("--- %s seconds for from Highest unit sold Area to highest unit sold to customer ---" % (hustc-husa))


#lowest unit sold to customer
lustc=time.time()
luscList = list(usc['xfinalqtydel'].idxmin())
luscList.append(usc['xfinalqtydel'].min())
infoDictMonthly['Customer who bought Lowest Units'] = luscList

#item current month unit sold
usi = df_month.groupby(['xitem','xdesc'])[['xfinalqtydel']].sum()

#highest unit sold item
husiList = list(usi['xfinalqtydel'].idxmax())
husiList.append(usi['xfinalqtydel'].max())
infoDictMonthly['Item which had the Highest Units Sold'] = husiList

#Lowest unit sold Item
lusiList = list(usi['xfinalqtydel'].idxmin())
lusiList.append(usi['xfinalqtydel'].min())
infoDictMonthly['Item which had the Lowest Units Sold'] = lusiList

lusi=time.time()
print("--- %s seconds for from lowest unit sold to customer to lowest unit sold item   ---" % (lusi-lustc ))

#Order number per salesman

onps=time.time()
ocs = df_month.groupby(['xsp','xname'])['xordernum'].nunique().to_frame()

#Highest Order Number of Salesman
hoscList = list(ocs['xordernum'].idxmax())
hoscList.append(ocs['xordernum'].max())
infoDictMonthly['Salesman with the Highest Number of Orders'] = hoscList

#Lowest Order Number of salesman
loscList = list(ocs['xordernum'].idxmin())
loscList.append(ocs['xordernum'].min())
infoDictMonthly['Salesman with the Lowest Number of Orders'] = loscList
lonos=time.time()
print("--- %s seconds for from order number per salesman to lowest order number of salesman  ---" % (lonos-onps ))

#Average order number of Salesman
aonos=time.time()
infoDictMonthly['Average Order Per Salesman'] = np.around(ocs['xordernum'].mean(),decimals=2)

#Order number per customer
occ = df_month.groupby(['xcus','xshort'])['xordernum'].nunique().to_frame()

#Highest Order Number of Customer
hoccList = list(occ['xordernum'].idxmax())
hoccList.append(occ['xordernum'].max())
infoDictMonthly['Customer who gave the Highest number of Orders'] = hoccList

#Lowest Order Number of Customer
loccList = list(occ['xordernum'].idxmin())
loccList.append(occ['xordernum'].min())
infoDictMonthly['Customer who gave the Lowest number of Orders'] = loccList
lonoc=time.time()
print("--- %s seconds for from average order number of salesman to lowest order number of customer  ---" % (lonoc-aonos ))

#Average order number of Customer
aonoc=time.time()
infoDictMonthly['Average Order Per Customer'] = np.around(occ['xordernum'].mean(),decimals=2)

#Order number per Area
oca = df_month.groupby(['xdiv'])['xordernum'].nunique().to_frame()

#Highest Order Number of Area
hocaList = [oca['xordernum'].idxmax()]
hocaList.append(oca['xordernum'].max())
infoDictMonthly['Area with the Highest Number of Orders'] = hocaList

#Lowest Order Number of Area
locaList = [oca['xordernum'].idxmin()]
locaList.append(oca['xordernum'].min())
infoDictMonthly['Area with the Lowest Number of Orders'] = locaList
lonoa=time.time()

print("--- %s seconds for from average order number of customer  ---" % (lonoa-aonoc))
#Average order number of Area

aonoa=time.time()
infoDictMonthly['Average Order Per Area'] = np.around(oca['xordernum'].mean(), decimals=2)

#Order number per Item
oci = df_month.groupby(['xitem','xdesc'])['xordernum'].nunique().to_frame()

#Highest Order Number of Item
hociList = list(oci['xordernum'].idxmax())
hociList.append(oci['xordernum'].max())
infoDictMonthly['Items with the Highest Number of Orders'] = hociList

#Lowest Order Number of Item
lociList = list(oci['xordernum'].idxmin())
lociList.append(oci['xordernum'].min())
infoDictMonthly['Item with the Lowest Number of Orders'] = lociList

#Average order number of Item
infoDictMonthly['Average Order Per Item'] = np.around(oci['xordernum'].mean(), decimals=2)
aonoi=time.time()

print("--- %s seconds for average order number of area to average order number of Item  ---" % (aonoi-aonoa ))
#Customer count in current month per area
ccicmpa=time.time()
cca = df_month.groupby(['xdiv'])['xcus'].nunique().to_frame()

#highest customer count Area
hccaList = [cca['xcus'].idxmax()]
hccaList.append(cca['xcus'].max())
infoDictMonthly['Area with the Highest Number of Customers'] = hccaList

#Lowest Customer Count Area
lccaList = [cca['xcus'].idxmin()]
lccaList.append(cca['xcus'].min())
infoDictMonthly['Area with the Lowest Number of Customers'] = lccaList

#Customer count in current month per Salesman
ccs = df_month.groupby(['xsp','xname'])['xcus'].nunique().to_frame()
ccicmps=time.time()
print("--- %s seconds for from customer count in current month per area to customer count in current month per salesman   ---" % (ccicmps-ccicmpa ))
#highest customer count per Salesman
hcps=time.time()
hccsList = [ccs['xcus'].idxmax()]
hccsList.append(ccs['xcus'].max())
infoDictMonthly['Salesman with the Highest Number of Customers'] = hccsList

#Lowest Customer Count Salesman
lccsList = [ccs['xcus'].idxmin()]
lccsList.append(ccs['xcus'].min())
infoDictMonthly['Salesman with the Lowest Number of Customers'] = lccsList

#Customer count in current month per item
cci = df_month.groupby(['xitem','xdesc'])['xcus'].nunique().to_frame()
cccmpi=time.time()
print("--- %s seconds for from highest customer count per salesman to customer count in current month per item ---" % (cccmpi-ccicmps ))

#highest customer count per Item

hccpi=time.time()
hcciList = [cci['xcus'].idxmax()]
hcciList.append(cci['xcus'].max())
infoDictMonthly['Items that were Distributed the Most'] = hcciList

#Lowest Customer Count Item
lcciList = [cci['xcus'].idxmin()]
lcciList.append(cci['xcus'].min())
infoDictMonthly['Items that were Distributed the Least'] = lcciList

#Total Number of Orders within the Month
infoDictMonthly['Total Number of Orders'] = len(df_month['xordernum'].unique())

#Total Number of Customers Sold to within the month
infoDictMonthly['Total Number of Customers'] = len(df_month['xcus'].unique())
tnocst=time.time()
print("--- %s seconds for from highest customer count per item to total number of customer sold to withing the month  ---" % (tnocst-hccpi))
#Total Number of Unit Sold of All Items
unitsold=time.time()
infoDictMonthly['Total Units Sold this Month'] = df_month['xfinalqtydel'].sum()

#Total Amount sold this month
infoDictMonthly['Total Amount Earned this Month'] = df_month['xfinallineamt'].sum()

#Item sold per area
ipa = df_month.groupby(['xdiv','xitem','xdesc'])[['xfinalqtydel']].sum()
endispa=time.time()
print("--- %s seconds for from total number of unit sold of all items to item sold per area ---" % (endispa-unitsold ))
#make the dictionary into a dataframe for email
dictmail=time.time()

dict_df = pd.DataFrame({key:pd.Series(value) for key, value in infoDictMonthly.items()})
dict_df = pd.melt(dict_df,var_name='All_Info')
dict_df = dict_df.groupby('All_Info')['value'].apply(list).to_frame().reset_index()
dict_df[[0,1,2,3]] = pd.DataFrame(dict_df.value.values.tolist(), index= dict_df.index)
dict_df = dict_df.drop('value',axis=1)
dict_df = dict_df.fillna(value=0,axis=1)

df_rectreturn = create_rectreturn(zid)
df_prmst = create_prmst(zid)
df_caitem = create_caitem(zid)
df_rectreturn = df_rectreturn.merge(df_prmst,on='xemp',how='left')
df_rectreturn = df_rectreturn.merge(df_caitem,on='xitem',how='left')
df_rectreturn = df_rectreturn[df_rectreturn['xper']==thisMonth]

rectsalesman = df_rectreturn.groupby(['xemp','xname'])[['xlineamt']].sum()
rectsalesmanarea = df_rectreturn.groupby(['xemp','xname','xarea'])[['xlineamt']].sum()
rectarea = df_rectreturn.groupby(['xarea'])[['xlineamt']].sum()
rectproductamt = df_rectreturn.groupby(['xitem','xdesc'])[['xlineamt']].sum()
rectproductqty = df_rectreturn.groupby(['xitem','xdesc'])[['xqtyord']].sum()

enddict=time.time()
print("--- %s seconds for from line 507 to 527   ---" % (enddict-dictmail))
######################################################
write=time.time()
#
#writer = pd.ExcelWriter('HmbrSalesInformation.xls')
#
# df_salesman_product.to_excel(writer,'Salesman_Product_Sales')
# df_salesman_product_month.to_excel(writer,'Salesman_Product_Sales_Month')
# df_datewise_product.to_excel(writer,'Datewise_Product_Sales')
# df_areawise_product.to_excel(writer,'Areawise_Product_Sales')
# df_customer_area_product.to_excel(writer,'Customer_perArea_Product_Sales')
#writer.save()
#writer.close()

df_salesman_product.to_csv('Salesman_Product_Sales.csv')
df_salesman_product_month.to_csv('Salesman_Product_Sales_Month.csv')
df_datewise_product.to_csv('Datewise_Product_Sales.csv')
df_areawise_product.to_csv('Areawise_Product_Sales.csv')
df_customer_area_product.to_csv('Customer_perArea_Product_Sales.csv')


writer2 = pd.ExcelWriter('HmbrSalesRatios.xlsx', engine='openpyxl')
gs.to_excel(writer2,'gs')
ga.to_excel(writer2,'ga')
gc.to_excel(writer2,'gc')
gi.to_excel(writer2,'gi')
uss.to_excel(writer2,'uss')
usa.to_excel(writer2,'usa')
usc.to_excel(writer2,'usc')
usi.to_excel(writer2,'usi')
ocs.to_excel(writer2,'ocs')
occ.to_excel(writer2,'occ')
oca.to_excel(writer2,'oca')
oci.to_excel(writer2,'oci')
cca.to_excel(writer2,'cca')
ccs.to_excel(writer2,'ccs')
cci.to_excel(writer2,'cci')
ipa.to_excel(writer2,'ipa')
dict_df.to_excel(writer2,'OverallSummary')

try:
    rectsalesman.to_excel(writer2,'rectsalesman')
    rectsalesmanarea.to_excel(writer2,'rectsalesmanarea')
    rectarea.to_excel(writer2,'rectarea')
    rectproductamt.to_excel(writer2,'rectproductamt')
    rectproductqty.to_excel(writer2,'rectproductqty')
except IndexError as error:
    print(error)

writer2.save()



endwrite=time.time()

print("--- %s seconds for line 532 to 570  writer function  ---" % (endwrite-write ))

stmail=time.time()
me = "XXXXXX@gmail.com"
you = ["XXXXXX"]

msg = MIMEMultipart('alternative')
msg['Subject'] = "Hmbr Sales Information"
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
with open('test.html','w') as f:
    f.write(HEADER)
    f.write(dict_df.to_html(classes='dict_df'))
    f.write(FOOTER)

filename = "test.html"
f = open(filename)
attachment = MIMEText(f.read(),'html')
msg.attach(attachment)

# Salesman_Product_Sales.csv
# Salesman_Product_Sales_Month.csv
# Datewise_Product_Sales.csv
# Areawise_Product_Sales.csv
# Customer_perArea_Product_Sales.csv



part1 = MIMEBase('application', "octet-stream")
part1.set_payload(open("Salesman_Product_Sales.csv", "rb").read())
encoders.encode_base64(part1)
part1.add_header('Content-Disposition', 'attachment; filename="Salesman_Product_Sales.csv"')
msg.attach(part1)

part2 = MIMEBase('application', "octet-stream")
part2.set_payload(open("Salesman_Product_Sales_Month.csv", "rb").read())
encoders.encode_base64(part2)
part2.add_header('Content-Disposition', 'attachment; filename=" Salesman_Product_Sales_Month.csv"')
msg.attach(part2)

part3= MIMEBase('application', "octet-stream")
part3.set_payload(open("Datewise_Product_Sales.csv", "rb").read())
encoders.encode_base64(part3)
part3.add_header('Content-Disposition', 'attachment; filename="Datewise_Product_Sales.csv"')
msg.attach(part3)

part4= MIMEBase('application', "octet-stream")
part4.set_payload(open("Areawise_Product_Sales.csv", "rb").read())
encoders.encode_base64(part4)
part4.add_header('Content-Disposition', 'attachment; filename="Areawise_Product_Sales.csv"')
msg.attach(part4)

part5 = MIMEBase('application', "octet-stream")
part5.set_payload(open("Customer_perArea_Product_Sales.csv", "rb").read())
encoders.encode_base64(part5)
part5.add_header('Content-Disposition', 'attachment; filename="Customer_perArea_Product_Sales.csv"')
msg.attach(part5)

# part6= MIMEBase('application', "octet-stream")
# part6.set_payload(open("HmbrSalesInformation.xlsx", "rb").read())
# encoders.encode_base64(part6)
# part6.add_header('Content-Disposition', 'attachment; filename="HmbrSalesInformation.xlsx"')
# msg.attach(part6)

part7 = MIMEBase('application', "octet-stream")
part7.set_payload(open("HmbrSalesRatios.xlsx", "rb").read())
encoders.encode_base64(part7)
part7.add_header('Content-Disposition', 'attachment; filename="HmbrSalesRatios.xlsx"')
msg.attach(part7)

username = 'XXXXXX'
password = 'XXXXXX'

s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(me,you,msg.as_string())
s.quit()
endmail=time.time()
print("--- %s seconds for mail line 573 to line 623  ---" % (endmail-stmail ))


totaltime=time.time()

print("--- %s seconds elapsed time for whole programme   ---" % (totaltime-main_time ))
