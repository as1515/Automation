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

def get_caitem(zid):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT xitem, xdesc, xgitem, xcitem, xpricecat, xduty, xwh, xstdcost, xstdprice
                        FROM caitem 
                        WHERE zid = '%s'
                        AND xgitem = 'Hardware'
                        OR xgitem = 'Furniture Fittings'
                        OR xgitem = 'Indutrial & Household'
                        OR xgitem = 'Sanitary'
                        ORDER BY xgitem ASC"""%(zid),con = engine)
    return df

def get_stock(zid):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                        FROM imtrn
                        WHERE imtrn.zid = '%s'
                        GROUP BY imtrn.xitem"""%(zid),con = engine)
    return df

def get_special_price(zid):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT xpricecat, xqty,xdisc
                        FROM opspprc 
                        WHERE zid = '%s'"""%(zid),con = engine)
    return df


def get_last_purchase_rate(zid, start_date):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT * FROM (
                        SELECT poodt.xitem, poord.xdate, poodt.xqtyord, poodt.xrate,
                        ROW_NUMBER() OVER(PARTITION BY xitem ORDER BY xdate DESC) AS rn
                        FROM poord 
                        JOIN poodt
                        ON poord.xpornum = poodt.xpornum
                        WHERE poord.zid = '%s'
                        AND poodt.zid = '%s'
                        AND poord.xpornum LIKE '%s' 
                        AND poord.xstatuspor = '%s'
                        AND poord.xdate > '%s'
                        ) t
                        WHERE t.rn = 1"""%(zid,zid,'IP-%%','5-Received',start_date),con = engine)
    return df

def get_sales(zid,start_date,end_date):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, sum(imtrn.xqty*imtrn.xsign) as sales
                        FROM imtrn
                        WHERE imtrn.zid = '%s'
                        AND imtrn.xdocnum LIKE '%s'
                        AND imtrn.xdate >= '%s'
                        AND imtrn.xdate <= '%s'
                        GROUP BY imtrn.xitem"""%(zid,'DO--%%',start_date,end_date),con = engine)
    return df

def get_purchase(zid,start_date):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT poodt.xitem, poord.xcounterno,poord.xdate, poodt.xqtyord, poodt.xrate
                        FROM poord 
                        JOIN poodt
                        ON poord.xpornum = poodt.xpornum
                        WHERE poord.zid='%s'
                        AND poodt.zid = '%s'
                        AND poord.xpornum LIKE '%s'
                        AND poord.xstatuspor = '%s'
                        AND poord.xdate > '%s'"""%(zid,zid,'IP--%%','1-Open',start_date),con = engine)
    return df

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

#lets make an initial dataframe without any filters and then dress it
zid_trading = 100001
zid_packaging = 100009

#caitem call for basic
df_caitem = get_caitem(zid_trading)

#rmb rate how do we get this (use an excel sheet that is updated every time before this analysis runs)

#wholesale price
df_sp_price = get_special_price(zid_trading).rename(columns={'xpricecat':'xitem'})


#current stock
df_stock_hmbr = get_stock(zid_trading)
df_stock_pack = get_stock(zid_packaging)
df_stock_pack = df_stock_pack.rename(columns={'xitem':'pack_code'})

dff = df_caitem.merge(df_sp_price[['xitem','xdisc']],on=['xitem'],how='left').fillna(0).rename(columns={'xstdprice':'retailP'})
dff['wholesaleP'] = dff['retailP'] - dff['xdisc']
del dff['xdisc']

dff = dff.merge(df_stock_hmbr[['xitem','stock']],on=['xitem'],how='left').fillna(0).rename(columns={'stock':'hmbr_stock'})

#current packaging stock
dff = dff.merge(df_pack[['xitem','pack_code']],on=['xitem'],how='left').fillna(0)
#match packaging and hmbr products 
dff = dff.merge(df_stock_pack[['pack_code','stock']],on=['pack_code'],how='left').fillna(0).rename(columns={'stock':'pack_stock'})
#Get final stock
dff['Current_Stock'] = dff['hmbr_stock'] + dff['pack_stock']

#get  last purchase rate
year_date = datetime.now() - timedelta(days = 1825)
year_date = year_date.strftime("%Y-%m-%d")
df_pur_last = get_last_purchase_rate(zid_trading,year_date)
dff = dff.merge(df_pur_last[['xitem','xrate']],on=['xitem'],how='left').fillna(0)

#get last 6 months average sales
sixmonth_date = datetime.now() - timedelta(days = 183)
start_date = sixmonth_date.strftime("%Y-%m-%d")
end_date = datetime.now().date().strftime("%Y-%m-%d")
df_six_sales = get_sales(zid_trading, start_date, end_date)
dff = dff.merge(df_six_sales[['xitem','sales']],on=['xitem'],how='left').fillna(0)
dff['sales'] = (dff['sales']*-1)/6
dff = dff.rename(columns={'sales':'6_avg_sales'})


#get last year same time 3 months sales
threemonth_date = datetime.now() - timedelta(days = 305)
start_date = threemonth_date.strftime("%Y-%m-%d")
end_date = datetime.now() - timedelta(days = 215)
end_date = end_date.date().strftime("%Y-%m-%d")
df_three_sales = get_sales(zid_trading, start_date, end_date)
dff = dff.merge(df_three_sales[['xitem','sales']],on=['xitem'],how='left').fillna(0)
dff['sales'] = (dff['sales']*-1)/3
dff = dff.rename(columns={'sales':'3_ly_sales'})


#get PO of all in transit shipments
start_date = datetime.now() - timedelta(days = 120)
start_date = start_date.strftime("%Y-%m-%d")
df_po = get_purchase(zid_trading,start_date)
#can put an order by date here so that all the columns become serialized

df_po[['xcounterno','doa']] = df_po['xcounterno'].str.split(',',expand=True)
print (df_po, "df_po printing IN LINE 246")
df_po = df_po.sort_values(by='doa', ascending=True)
print (df_po, "df_PO printing IN LINE 247")
po_date_dict = df_po.set_index('xcounterno').to_dict()['doa']
print (po_date_dict, "date_dict printing IN line 249")
max_date = max(po_date_dict.values())
max_date = datetime.strptime(max_date, '%Y-%m-%d')
max_days = (max_date + timedelta(31) - datetime.now()).days

po_list = df_po['xcounterno'].unique().tolist()
po_dict = {}
for po in po_list:
    po_dict[po] = df_po[df_po['xcounterno'] == po]
    
po_day_dict = {}
for item,idx in enumerate(po_date_dict):
    po_date_dict[idx]= datetime.strptime(po_date_dict[idx], '%Y-%m-%d')
    po_day_dict[idx] = po_date_dict[idx] - datetime.now()
    po_day_dict[idx] =  po_day_dict[idx].days                                                      
                                                                                          
po_day_dict = dict(sorted(po_day_dict.items(), key=lambda item: item[1]))

po_diff_dict = {}
for item,idx in enumerate(po_day_dict):
    if item != 0:
        po_diff_dict[idx] =  po_day_dict[idx] - po_day_dict[list(po_day_dict.keys())[item-1]]

for item,idx in enumerate(po_dict):
    dff = dff.merge(po_dict[idx][['xitem','xqtyord']],on='xitem',how='left').fillna(0).rename(columns={'xqtyord':idx + po_date_dict[idx].strftime("%Y-%m-%d")})
    if item == 0:
        dff[idx + 'after stock'] =  dff['Current_Stock'] - (dff['6_avg_sales']*(po_day_dict[idx]/30.5))
        dff[idx + 'after stock'] = dff[idx + 'after stock'].mask(dff[idx + 'after stock'].lt(0),0)
        dff[idx + 'after stock'] = dff[idx + 'after stock'] + dff[idx + po_date_dict[idx].strftime("%Y-%m-%d")]
    else:
        last_column_name = dff.columns[-2]
        dff[idx + 'after stock'] =  dff[last_column_name] - (dff['6_avg_sales']*(po_diff_dict[idx]/30.5))
        dff[idx + 'after stock'] = dff[idx + 'after stock'].mask(dff[idx + 'after stock'].lt(0),0)
        dff[idx + 'after stock'] = dff[idx + 'after stock'] + dff[idx + po_date_dict[idx].strftime("%Y-%m-%d")]
        
last_column_name = dff.columns[-1]
dff['New Shipment Stock'] =  dff[last_column_name] - dff['6_avg_sales']
dff['New Shipment Stock']=dff['New Shipment Stock'].mask(dff['New Shipment Stock'].lt(0),0)
dff['months'] = (dff['Current_Stock']/dff['6_avg_sales']).fillna(0)
dff = dff.sort_values(by='6_avg_sales',ascending=False)

dff = dff.drop(columns=['xcitem','xpricecat','xduty','xwh','hmbr_stock','pack_code','pack_stock']).rename(columns={'xitem':'Item_code','xdesc':'Item_name','xgitem':'Item_Group','xstdcost':'cost_price','xrate':'last_price'}).round(2)

#just attach the dff in an email and send it out to assistant commercials, store hmbr, me

writer = pd.ExcelWriter('purchase_requisition.xlsx')
dff.to_excel(writer,'Purchase')
writer.save()

me = "XXXXXX@gmail.com"
you = ["XXXXXX"]

msg = MIMEMultipart('alternative')
msg['Subject'] = "Requisition of Purchase"
msg['From'] = me
msg['To'] = ", ".join(you)


part1 = MIMEBase('application', "octet-stream")
part1.set_payload(open("purchase_requisition.xlsx", "rb").read())
encoders.encode_base64(part1)
part1.add_header('Content-Disposition', 'attachment; filename="purchase_requisition.xlsx"')
msg.attach(part1)

username = 'XXXXXX@gmail.com'
password = 'XXXXXX'

s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(me,you,msg.as_string())
s.quit()
