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
from email.header import Header
import openpyxl
from mail import send_mail

# %%

def get_igrn(zid,start_date):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT pogrn.xgrnnum, pogrn.xdate, poodt.xitem
                        FROM poord 
                        JOIN poodt
                        ON poord.xpornum = poodt.xpornum
                        JOIN pogrn
                        ON poord.xpornum = pogrn.xpornum
                        WHERE poord.zid= '%s'
                        AND poodt.zid = '%s'
                        AND pogrn.zid = '%s'
                        AND poord.xpornum LIKE '%s'
                        AND poord.xstatuspor = '%s'
                        AND poord.xdate > '%s'
                        GROUP BY pogrn.xgrnnum, pogrn.xdate, poodt.xitem"""%(zid,zid,zid,'IP--%%','5-Received',start_date),con = engine)
    return df

def get_caitem(zid):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT xitem, xdesc, xgitem, xcitem, xpricecat, xduty, xwh, xstdcost, xstdprice
                        FROM caitem 
                        WHERE zid = '%s'
                        AND xgitem = 'Hardware'
                        OR xgitem = 'Furniture Fittings'
                        OR xgitem = 'Industrial & Household'
                        OR xgitem = 'Sanitary'
                        ORDER BY xgitem ASC"""%(zid),con = engine)
    return df

def get_stock(zid,end_date):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                        FROM imtrn
                        WHERE imtrn.zid = '%s'
                        AND imtrn.xdate <= '%s'
                        GROUP BY imtrn.xitem"""%(zid,end_date),con = engine)
    return df

def get_item_stock(zid,end_date,item):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                        FROM imtrn
                        WHERE imtrn.zid = '%s'
                        AND imtrn.xdate < '%s'
                        AND imtrn.xitem IN %s
                        GROUP BY imtrn.xitem"""%(zid,end_date,item),con = engine)
    return df

def get_item_stock_1(zid,end_date,item):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                        FROM imtrn
                        WHERE imtrn.zid = '%s'
                        AND imtrn.xdate < '%s'
                        AND imtrn.xitem = '%s'
                        GROUP BY imtrn.xitem"""%(zid,end_date,item),con = engine)
    return df

def get_special_price(zid):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT xpricecat, xqty,xdisc
                        FROM opspprc 
                        WHERE zid = '%s'"""%(zid),con = engine)
    return df

def get_sales(zid,start_date,end_date,item):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, imtrn.xyear ,imtrn.xper ,sum(imtrn.xqty*imtrn.xsign) as sales
                        FROM imtrn
                        WHERE imtrn.zid = '%s'
                        AND (imtrn.xdocnum LIKE '%s' OR imtrn.xdocnum LIKE '%s')
                        AND imtrn.xdate >= '%s'
                        AND imtrn.xdate <= '%s'
                        AND imtrn.xitem IN %s
                        GROUP BY imtrn.xitem,imtrn.xyear,imtrn.xper"""%(zid,'DO--%%','SR--%%',start_date,end_date,item),con = engine)
    return df

def get_sales_rate(zid,start_date,item):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT opddt.xitem, opdor.xdate ,SUM(opddt.xlineamt)/SUM(opddt.xqty)
                        FROM opddt 
                        JOIN opdor
                        ON opddt.xdornum = opdor.xdornum
                        WHERE opdor.zid = '%s'
                        AND opddt.zid = '%s'
                        AND opddt.xitem IN %s
                        AND opdor.xdate = '%s'
                        GROUP BY opddt.xitem, opdor.xdate"""%(zid,zid,item,start_date),con = engine)
    return df

def get_sales_1(zid,start_date,end_date,item):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, imtrn.xyear ,imtrn.xper, sum(imtrn.xqty*imtrn.xsign) as sales
                        FROM imtrn
                        WHERE imtrn.zid = '%s'
                        AND (imtrn.xdocnum LIKE '%s' OR imtrn.xdocnum LIKE '%s')
                        AND imtrn.xdate >= '%s'
                        AND imtrn.xdate <= '%s'
                        AND imtrn.xitem = '%s'
                        GROUP BY imtrn.xitem,imtrn.xyear,imtrn.xper"""%(zid,'DO--%%','SR--%%',start_date,end_date,item),con = engine)
    return df

def get_sales_rate_1(zid,start_date,item):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT opddt.xitem, opdor.xdate, AVG(opddt.xdtwotax/opddt.xqty)
                        FROM opddt
                        JOIN opdor
                        ON opddt.xdornum = opdor.xdornum
                        WHERE opdor.zid = '%s'
                        AND opddt.zid = '%s'
                        AND opddt.xitem = '%s'
                        AND opdor.xdate = '%s'
                        GROUP BY opddt.xitem, opdor.xdate"""%(zid,zid,item,start_date),con = engine)
    return df

def get_purchase(zid,start_date):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT poodt.xitem, poord.xcounterno, poodt.xqtyord, poodt.xrate, pogrn.xgrnnum, pogrn.xdate
                        FROM poord 
                        JOIN poodt
                        ON poord.xpornum = poodt.xpornum
                        JOIN pogrn
                        ON poord.xpornum = pogrn.xpornum
                        WHERE poord.zid= '%s'
                        AND poodt.zid = '%s'
                        AND pogrn.zid = '%s'
                        AND poord.xpornum LIKE '%s'
                        AND poord.xstatuspor = '%s'
                        AND poord.xdate > '%s'"""%(zid,zid,zid,'IP--%%','5-Received',start_date),con = engine)
    return df

# %%
#packaging stock
pack_dict = {'HPI000001':'0119',
'HPI000002':'0120',
'HPI000003':'0121',
'HPI000004':'01640',
'HPI000005':'2154',
'HPI000006':'0186',
'HPI000007':'2155',
'HPI000009':'0458',
'HPI000010':'0459',
'HPI000011':'0706',
'HPI000012':'0717',
'HPI000013':'0718',
'HPI000014':'0719',
'HPI000015':'0720',
'HPI000016':'0721',
'HPI000017':'0722',
'HPI000018':'0723',
'HPI000019':'0724',
'HPI000020':'0725',
'HPI000021':'0726',
'HPI000022':'0727',
'HPI000026':'1122',
'HPI000027':'1126',
'HPI000028':'1128',
'HPI000029':'1129',
'HPI000030':'1130',
'HPI000031':'1131',
'HPI000032':'1139',
'HPI000033':'1140',
'HPI000034':'1141',
'HPI000035':'1142',
'HPI000036':'1143',
'HPI000037':'1150',
'HPI000038':'1153',
'HPI000039':'1154',
'HPI000045':'1198',
'HPI000046':'12040',
'HPI000047':'1219',
'HPI000048':'1236',
'HPI000049':'12381',
'HPI000050':'12382',
'HPI000051':'1299',
'HPI000052':'1300',
'HPI000055':'1332',
'HPI000056':'1349',
'HPI000057':'1410',
'HPI000058':'1411',
'HPI000059':'1412',
'HPI000060':'14351',
'HPI000061':'14352',
'HPI000062':'14361',
'HPI000063':'14362',
'HPI000065':'1527',
'HPI000067':'1528',
'HPI000068':'1576',
'HPI000069':'1594',
'HPI000072':'1596',
'HPI000073':'2146',
'HPI000074':'1600',
'HPI000075':'1601',
'HPI000078':'1650',
'HPI000079':'1652',
'HPI000080':'16990',
'HPI000081':'17010',
'HPI000082':'1767',
'HPI000087':'2046',
'HPI000088':'2047',
'HPI000089':'2048',
'HPI000090':'2049',
'HPI000091':'2050',
'HPI000092':'2060',
'HPI000093':'2070',
'HPI000094':'2105',
'HPI000095':'11501',
'HPI000096':'11230',
'HPI000097':'0178',
'HPI000098':'0180',
'HPI000099':'0179',
'HPI000100':'01877',
'HPI000101':'01878',
'HPI000102':'01879',
'HPI000103':'2111',
'HPI000104':'1577',
'HPI000105':'1578',
'HPI000106':'1579',
'HPI000107':'2127',
'HPI000108':'2128',
'HPI000109':'2129',
'HPI000110':'1807',
'HPI000111':'1766',
'HPI000112':'2125',
'HPI000113':'2126',
'HPI000114':'2148',
'HPI000115':'2145'}

# def get_packaging_list:
df_pack = pd.DataFrame(pack_dict.items(), columns=['pack_code', 'xitem'])
df_pack = df_pack[['xitem','pack_code']]

# %%
#get item code description, retail price wholesale price etc
#lets make an initial dataframe without any filters and then dress it
zid_trading = 100001
zid_packaging = 100009

#caitem call for basic
df_caitem = get_caitem(zid_trading)

#wholesale price
df_sp_price = get_special_price(zid_trading).rename(columns={'xpricecat':'xitem'})

# %%
#current stock
end_date = datetime.now() 
end_date = end_date.strftime("%Y-%m-%d")
df_stock_hmbr = get_stock(zid_trading,end_date)
df_stock_pack = get_stock(zid_packaging,end_date)
df_stock_pack = df_stock_pack.rename(columns={'xitem':'pack_code'})

# %%
dff = df_caitem.merge(df_sp_price[['xitem','xdisc']],on=['xitem'],how='left').fillna(0).rename(columns={'xstdprice':'retailP'})
dff['wholesaleP'] = dff['retailP'] - dff['xdisc']
del dff['xdisc']

# %%
dff = df_caitem.merge(df_stock_hmbr[['xitem','stock']],on=['xitem'],how='left').fillna(0).rename(columns={'stock':'hmbr_stock'})
#current packaging stock
dff = dff.merge(df_pack[['xitem','pack_code']],on=['xitem'],how='left').fillna(0)
#match packaging and hmbr products 
dff = dff.merge(df_stock_pack[['pack_code','stock']],on=['pack_code'],how='left').fillna(0).rename(columns={'stock':'pack_stock'})
#Get final stock
dff['Current_Stock'] = dff['hmbr_stock'] + dff['pack_stock']

# %%
#get purchased stock and rate of purchase
#get PO of all in transit shipments
start_date = datetime.now() - timedelta(days = 1400)
start_date = start_date.strftime("%Y-%m-%d")
df_po_trade = get_purchase(zid_trading,start_date)
dff = dff.merge(df_po_trade[['xitem','xqtyord','xrate','xgrnnum','xdate']],on=['xitem'],how='left')
df_po_pack = get_purchase(zid_packaging,start_date).rename(columns={'xitem':'pack_code'})
dff = dff.merge(df_po_pack[['pack_code','xqtyord','xrate','xgrnnum','xdate']],on=['pack_code'],how='left')

dff['xqtyord_x'] = dff['xqtyord_x'].fillna(0)
dff['xqtyord_y'] = dff['xqtyord_y'].fillna(0)
dff['Qty_order'] = dff['xqtyord_x'] + dff['xqtyord_y']
del dff['xqtyord_x']
del dff['xqtyord_y']
dff['xrate_x'] = dff['xrate_x'].fillna(0)
dff['xrate_y'] = dff['xrate_y'].fillna(0)
dff['p_rate'] = dff['xrate_x'] + dff['xrate_y']
del dff['xrate_x']
del dff['xrate_y']
dff['xgrnnum_x'] = dff['xgrnnum_x'].fillna('')
dff['xgrnnum_y'] = dff['xgrnnum_y'].fillna('')


dff['xdate_x'] = dff['xdate_x'].fillna(0)
dff['xdate_y'] = dff['xdate_y'].fillna(0)
condition_1 = dff['xgrnnum_y']!= '' 
condition_2 = dff['xdate_y']!= 0

dff['grunnum'] = np.where(condition_1, dff['xgrnnum_y'], dff['xgrnnum_x'] )
dff['date'] = np.where(condition_2, dff['xdate_y'], dff['xdate_x'] )
del dff['xgrnnum_x']
del dff['xgrnnum_y']
del dff['xdate_x']
del dff['xdate_y']


# %%


# %%
#we need the date from imtrn and we need to relate IGRN with IP number
df_grn_trade = get_igrn(zid_trading,start_date)
df_grn_pack = get_igrn(zid_packaging,start_date)

date_dict = {}
date_dict[zid_trading] = df_grn_trade.groupby('xgrnnum')['xdate'].apply(lambda x: x.to_list()[0].strftime("%Y-%m-%d")).to_dict()
date_dict[zid_packaging] = df_grn_pack.groupby('xgrnnum')['xdate'].apply(lambda x: x.to_list()[0].strftime("%Y-%m-%d")).to_dict()
# print (date_dict  , "Date Dict")
# df =pd.DataFrame(data = trading, index = )
item_dict = {}
item_dict[zid_trading] = df_grn_trade.groupby('xgrnnum')['xitem'].apply(lambda x: x.to_list()).to_dict()
item_dict[zid_packaging] = df_grn_pack.groupby('xgrnnum')['xitem'].apply(lambda x: x.to_list()).to_dict()
# for item,idx in enumerate(grn_dict):
# print (item_dict  , "Item Dict")

# %%
main_dict_trade = {}
for (dk,dv), (ik,iv) in zip(date_dict[zid_trading].items(), item_dict[zid_trading].items()):
    print(dk)
    df_main = dff[dff['grunnum']==dk]
    df_main = df_main.drop(['xgitem','xcitem','xpricecat','xduty','xwh','hmbr_stock','pack_code','pack_stock'],axis=1)
    
    current_date = datetime.strptime(dv, '%Y-%m-%d')
    end_date_object = current_date + timedelta(days=183)
    end_date = end_date_object.strftime('%Y-%m-%d')

    df_sale = get_sales(zid_trading,dv,end_date,tuple(iv))
    df_sale = df_sale.pivot(index='xitem',columns=['xyear','xper'],values='sales').sort_index(axis=1)
    df_sale.columns = df_sale.columns.map(lambda x: '_'.join(map(str, x)))
    df_sale = df_sale.reset_index()
    df_main = pd.merge(df_main,df_sale,on=['xitem'],how='left').fillna(0).rename(columns={'date':'xdate'})
    
    df_sale_rate = get_sales_rate(zid_trading,dv,tuple(iv))
    df_main = pd.merge(df_main,df_sale_rate,on=['xitem','xdate'],how='left').fillna(0)
    
    main_dict_trade[dk] = df_main
#     print(dv, 'dv')
#     print(ik, 'ik')
#     print(iv, 'iv')

# %%
main_dict_pack = {}
for (dk,dv), (ik,iv) in zip(date_dict[zid_packaging].items(), item_dict[zid_packaging].items()):
    print(dk)
    df_main = dff[dff['grunnum']==dk]
    df_main = df_main.drop(['xgitem','xcitem','xpricecat','xduty','xwh','hmbr_stock','pack_code','pack_stock'],axis=1)
    
    iv = df_main['xitem'].to_list()
    
    current_date = datetime.strptime(dv, '%Y-%m-%d')
    end_date_object = current_date + timedelta(days=183)
    end_date = end_date_object.strftime('%Y-%m-%d')
    if len(iv)>1:
        print('greater')
        df_sale = get_sales(zid_trading,dv,end_date,tuple(iv))
    else:
        print('not_greater')
        df_sale = get_sales_1(zid_trading,dv,end_date,iv[0])
    df_sale = df_sale.pivot(index='xitem',columns=['xyear','xper'],values='sales').sort_index(axis=1)
    df_sale.columns = df_sale.columns.map(lambda x: '_'.join(map(str, x)))
    df_sale = df_sale.reset_index()
    df_main = pd.merge(df_main,df_sale,on=['xitem'],how='left').fillna(0).rename(columns={'date':'xdate'})
    
    if len(iv)>1:
        df_sale_rate = get_sales_rate(zid_trading,dv,tuple(iv))
    else:
        df_sale_rate = get_sales_rate_1(zid_trading,dv,iv[0])
    df_main = pd.merge(df_main,df_sale_rate,on=['xitem','xdate'],how='left').fillna(0)
    
    main_dict_pack[dk] = df_main

# %%
main_dict_trade_df = pd.concat(main_dict_trade.values(), axis=0).fillna(0)

# %%
main_dict_pack_df = pd.concat(main_dict_pack.values(),axis=0).fillna(0)

# %%
#bashar just need to send the two excel files for main_dict_trade_df and main_dict_pack_df
main_dict_trade_df.to_excel('main_dict_trade_df.xlsx' , engine='openpyxl')
main_dict_pack_df.to_excel('main_dict_pack_df.xlsx' , engine='openpyxl')

# send mail



# %%

attachments = ['main_dict_trade_df.xlsx' , 'main_dict_pack_df.xlsx']
subject = 'Purchase Cohort Analysis'
bodytext = f'Dear Sir, \n\n Please find the attachment regrading {subject}.'
recipients = ['XXXXXX']

send_mail(subject, bodytext, attachments, recipients)
# df_sales_test.columns = df_sales_test.columns.map(lambda x: '_'.join(map(str, x)))


