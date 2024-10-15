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

def get_igrn(zid,start_date):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
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
                        AND pogrn.xdate > '%s'
                        GROUP BY pogrn.xgrnnum, pogrn.xdate, poodt.xitem"""%(zid,zid,zid,'IP--%%','5-Received',start_date),con = engine)
    return df

def get_caitem(zid):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT xitem, xdesc, xgitem, xcitem, xpricecat, xduty, xwh, xstdcost, xstdprice
                        FROM caitem 
                        WHERE zid = '%s'
                        AND xgitem = 'Hardware'
                        OR xgitem = 'Furniture Fittings'
                        OR xgitem = 'Indutrial & Household'
                        OR xgitem = 'Sanitary'
                        ORDER BY xgitem ASC"""%(zid),con = engine)
    return df

def get_stock(zid,end_date):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                        FROM imtrn
                        WHERE imtrn.zid = '%s'
                        AND imtrn.xdate <= '%s'
                        GROUP BY imtrn.xitem"""%(zid,end_date),con = engine)
    return df

def get_item_stock(zid,end_date,item):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                        FROM imtrn
                        WHERE imtrn.zid = '%s'
                        AND imtrn.xdate < '%s'
                        AND imtrn.xitem IN %s
                        GROUP BY imtrn.xitem"""%(zid,end_date,item),con = engine)
    return df

def get_item_stock_1(zid,end_date,item):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                        FROM imtrn
                        WHERE imtrn.zid = '%s'
                        AND imtrn.xdate < '%s'
                        AND imtrn.xitem = '%s'
                        GROUP BY imtrn.xitem"""%(zid,end_date,item),con = engine)
    return df

def get_special_price(zid):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT xpricecat, xqty,xdisc
                        FROM opspprc 
                        WHERE zid = '%s'"""%(zid),con = engine)
    return df

def get_sales(zid,start_date,end_date,item):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, imtrn.xdate, imtrn.xcus, cacus.xstate ,SUM(imtrn.xval) as xval, sum(imtrn.xqty*imtrn.xsign) as sales
                        FROM imtrn
                        JOIN cacus
                        ON imtrn.xcus = cacus.xcus
                        WHERE imtrn.zid = '%s'
                        AND cacus.zid = '%s'
                        AND imtrn.xdocnum LIKE '%s'
                        AND imtrn.xdate >= '%s'
                        AND imtrn.xdate <= '%s'
                        AND imtrn.xitem IN %s
                        GROUP BY imtrn.xitem, imtrn.xdate, imtrn.xcus, cacus.xstate"""%(zid,zid,'DO--%%',start_date,end_date,item),con = engine)
    return df

def get_sales_1(zid,start_date,end_date,item):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, imtrn.xdate, imtrn.xcus, cacus.xstate ,SUM(imtrn.xval) as xval, sum(imtrn.xqty*imtrn.xsign) as sales
                        FROM imtrn
                        JOIN cacus
                        ON imtrn.xcus = cacus.xcus
                        WHERE imtrn.zid = '%s'
                        AND cacus.zid = '%s'
                        AND imtrn.xdocnum LIKE '%s'
                        AND imtrn.xdate >= '%s'
                        AND imtrn.xdate <= '%s'
                        AND imtrn.xitem = '%s'
                        GROUP BY imtrn.xitem, imtrn.xdate, imtrn.xcus, cacus.xstate"""%(zid,zid,'DO--%%',start_date,end_date,item),con = engine)
    return df

def get_return(zid,start_date,end_date,item):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT imtrn.xitem, imtrn.xdate, imtrn.xcus, cacus.xstate , SUM(imtrn.xval) as xval, sum(imtrn.xqty*imtrn.xsign) as rtn
                        FROM imtrn
                        JOIN cacus
                        ON imtrn.xcus = cacus.xcus
                        WHERE imtrn.zid = '%s'
                        AND cacus.zid = '%s'
                        AND (imtrn.xdoctype = '%s' OR imtrn.xdoctype = '%s')
                        AND imtrn.xdate >= '%s'
                        AND imtrn.xdate <= '%s'
                        AND imtrn.xitem IN %s
                        GROUP BY imtrn.xitem, imtrn.xdate, imtrn.xcus, cacus.xstate"""%(zid,zid,'SR--','DSR-',start_date,end_date,item),con = engine)
    return df

def get_area_sales(zid,start_date,end_date,item):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT opddt.xitem, opdor.xdate, opdor.xdiv, SUM(opddt.xdtwotax) as total_amount, SUM(opddt.xqty) as total_qty
                        FROM opddt
                        JOIN opdor
                        ON opdor.xdornum = opddt.xdornum
                        WHERE opdor.zid = '%s'
                        AND opddt.zid = '%s'
                        AND opdor.xdornum LIKE '%s'
                        AND opdor.xdate >= '%s'
                        AND opdor.xdate <= '%s'
                        AND opddt.xitem IN %s
                        GROUP BY opddt.xitem, opdor.xdate, opdor.xdiv"""%(zid,zid,'DO--%%',start_date,end_date,item),con = engine)
    return df

def get_area_sales_1(zid,start_date,end_date,item):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT opddt.xitem, opdor.xdate, opdor.xdiv, SUM(opddt.xdtwotax) as total_amount, SUM(opddt.xqty) as total_qty
                        FROM opddt
                        JOIN opdor
                        ON opdor.xdornum = opddt.xdornum
                        WHERE opdor.zid = '%s'
                        AND opddt.zid = '%s'
                        AND opdor.xdornum LIKE '%s'
                        AND opdor.xdate >= '%s'
                        AND opdor.xdate <= '%s'
                        AND opddt.xitem = '%s'
                        GROUP BY opddt.xitem, opdor.xdate, opdor.xdiv"""%(zid,zid,'DO--%%',start_date,end_date,item),con = engine)
    return df



### take this counter number and tell siam to add the MD text to all of MD order. starts with MD
def get_purchase(zid,start_date):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
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
                        AND pogrn.xdate > '%s'"""%(zid,zid,zid,'IP--%%','5-Received',start_date),con = engine)
    return df

def get_gl_details_bs_project(zid,date):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
    year = date.split('-')[0]
    df = pd.read_sql("""select glmst.zid, glmst.xacc, glmst.xdesc, gldetail.xsub, SUM(gldetail.xprime)
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
                        AND gldetail.xvoucher NOT LIKE '%s'
                        AND glheader.xdate <= '%s'
                        AND glheader.xyear < '%s'
                        AND glmst.xacc IN ('10010007','10020015','10020001','10010003','10010006')
                        GROUP BY glmst.zid, glmst.xacc, glmst.xdesc, gldetail.xsub"""%(zid,zid,zid,'GULSHAN TRADING','OB-%%',date,year),con = engine)
    return df

def get_vat_amount(zid,ship_name):
    engine = create_engine('postgresql://XXXXX:XXXXX@localhost:5432/da')
    df = pd.read_sql("""SELECT xprime 
                        FROM gldetail 
                        WHERE zid = '%s' 
                        AND xacc = '%s' 
                        AND xlong = '%s'"""%(zid,'01050007',ship_name),con=engine)
    return df
    
    
    #packaging stock
pack_dict = {
'HPI000001':'0119',
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
'HPI000115':'2145'
}

# def get_packaging_list:
df_pack = pd.DataFrame(pack_dict.items(), columns=['pack_code', 'xitem'])
df_pack = df_pack[['xitem','pack_code']]

area_dict ={
'Lakshmipur':'District',
'Saver':'Dhaka',
'Basundhara':'General', 
'Central-6(Nawab pur-1)':'Wholesale', 
'Central-5(Nawab pur-2)':'Wholesale', 
'Habiganj':'District', 
'Shymolly':'Dhaka',
'Noyabazar':'Dhaka',
'Chittagong':'District', 
'Ibrahimpur':'Dhaka',
'Naogaon':'District', 
'Kazipara':'Dhaka',
'Sirajganj':'District',
'Lalbag':'Dhaka', 
'Mawna':'General', 
'Tangail':'District', 
'Sunamganj':'District', 
'Narayanganj':'General', 
'Tongi':'Dhaka',
'Kalir Bazar':'Dhaka', 
'Natore':'District', 
'Sylhet':'Dhaka', 
'Bagerhat':'District', 
'Nobi Nagar':'Dhaka', 
'Ctg.Road':'Dhaka', 
'Gopalgang':'District', 
'Coxs Bazar':'Dhaka', 
'Ghorasal':'General', 
'Askona':'Dhaka', 
'Central-8(Kawranbazar-1)':'Wholesale', 
'Narsingdi':'General', 
'Sariatpur':'District', 
'Jessore':'District', 
'Thakurgaon':'District', 
'Jhalakati':'District', 
'Munshiganj':'General', 
'Khulna':'District', 
'Fakir Market':'Dhaka',
'Comilla':'Distributor',
'Mohammadpur':'Dhaka', 
'Basabo':'Dhaka', 
'Central-2(Imamgonj)':'Wholesale', 
'Pirojpur':'District', 
'Asulia':'Dhaka', 
'Sreepur':'District', 
'Mirpur-1,2':'Dhaka', 
'Kurigram':'District', 
'Jaypurhat':'District', 
'Mirpur-11,12':'Dhaka', 
'Keraniganj':'General', 
'Marura':'District', 
'Jaipurhat':'District', 
'Zigatala':'Dhaka', 
'New Market':'Dhaka', 
'Panchagarh':'District', 
'Patuakhali':'District', 
'Uttar Badda':'Dhaka', 
'Sayedpur':'District', 
'Jhenaidah':'District', 
'Barguna':'District', 
'Abdulla Pur':'Dhaka', 
'Malibag':'Dhaka', 
'Safipur':'Dhaka', 
'Barisal':'District', 
'Rampura':'Dhaka', 
'Kaylanpur':'Dhaka', 
'Chuadanga':'District', 
'Manikgonj':'General', 
'Goshbug':'Dhaka', 
'Noakhali':'District', 
'Moulvibazer':'District', 
'Narail':'District', 
'Jattrabari':'Dhaka', 
'Bogra':'District', 
'Bhola':'District', 
'Gaibandha':'District', 
'Central-3(Alubazar-2)':'Wholesale',
'Lamonirhat':'District', 
'Coxs Bazar':'District', 
'Dinajpur':'District', 
'Central-1(Imamgonj)':'Wholesale', 
'Kalachadpur':'Dhaka', 
'Kushtia':'District', 
'Rangamati':'District', 
'Central-4(Alubazar-1)':'Wholesale', 
'Shirajgonj':'District', 
'Ctg. Road':'Dhaka', 
'Voyrab':'General', 
'Satkhira':'District', 
'Kishoreganj':'General', 
'Jamalpur':'District', 
'Netrokona':'District', 
'Magura':'District', 
'Shibbari':'Dhaka', 
'Vatara':'Dhaka', 
'Board Bazer':'Dhaka', 
'firmget':'Dhaka', 
'Feni':'District', 
'Damra':'Dhaka', 
'Faridpur':'District', 
'Dohar':'General', 
'Pagla':'Dhaka', 
'Rangpur':'District', 
'Kaliakoir':'General', 
'Mirzapur':'General', 
'Rajbari':'District', 
'Rajshahi':'District', 
'Nilphamari':'District',
'Meherpur':'District', 
'Uttora':'Dhaka', 
'Motijil':'Dhaka', 
'Cherag Ali':'Dhaka', 
'Mohakhali':'Dhaka', 
'Brahmanbaria':'General', 
'Chapainawabganj':'District', 
'Sherpur':'District',
'Khilket':'Dhaka', 
'Gulshan-1':'Dhaka', 
'Pabna':'District', 
'Banani':'Dhaka', 
'Central-7(Kawranbazar-2)':'Wholesale', 
'Mymensingh':'District', 
'Madaripur':'Dhaka'
}

df_area = pd.DataFrame(area_dict.items(), columns=['xdiv', 'market'])
df_area = df_area[['xdiv','market']]

#get item code description, retail price wholesale price etc
#lets make an initial dataframe without any filters and then dress it
zid_trading = 100001
zid_packaging = 100009

#caitem call for basic
df_caitem = get_caitem(zid_trading)


#wholesale price
df_sp_price = get_special_price(zid_trading).rename(columns={'xpricecat':'xitem'})

#current stock
## this is the date we needd to change
end_date = datetime.now()
end_date = end_date.strftime("%Y-%m-%d")
df_stock_hmbr = get_stock(zid_trading,end_date)
df_stock_pack = get_stock(zid_packaging,end_date)
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

#get purchased stock and rate of purchase
#get PO of all in transit shipments
start_date = datetime.now() - timedelta(days = 130)
start_date = start_date.strftime("%Y-%m-%d")
df_po_trade = get_purchase(zid_trading,start_date)
dff = dff.merge(df_po_trade[['xitem','xcounterno','xqtyord','xrate','xgrnnum','xdate']],on=['xitem'],how='left')
df_po_pack = get_purchase(zid_packaging,start_date).rename(columns={'xitem':'pack_code'})
dff = dff.merge(df_po_pack[['pack_code','xcounterno','xqtyord','xrate','xgrnnum','xdate']],on=['pack_code'],how='left')


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
dff['grunnum'] = dff['xgrnnum_x'] + dff['xgrnnum_y']
del dff['xgrnnum_x']
del dff['xgrnnum_y']
dff['xdate_x'] = dff['xdate_x'].fillna(0)
dff['xdate_y'] = dff['xdate_y'].fillna(0)
dff['date'] = dff['xdate_x'].where(dff['xdate_x'] != 0, dff['xdate_y'])
del dff['xdate_x']
del dff['xdate_y']
dff['xcounterno_x'] = dff['xcounterno_x'].fillna('')
dff['xcounterno_y'] = dff['xcounterno_y'].fillna('')
dff['xcounterno'] = dff['xcounterno_x'] + dff['xcounterno_y']
del dff['xcounterno_x']
del dff['xcounterno_y']
dff=dff.rename(columns={'xcounterno': 'counter_split'})
dff[['xcounterno', 'counterdate']] = dff.counter_split.str.split(",", expand=True)

dff = dff[dff['xcounterno'].str.contains("MD")]


date_dict = dff.groupby('xcounterno')['date'].apply(lambda x: x.to_list()[0].strftime("%Y-%m-%d")).to_dict()
item_dict = dff.groupby('xcounterno')['xitem'].apply(lambda x: x.to_list()).to_dict()


main_df_dict = {}
main_area_dict = {}
main_summary_dict = {}
main_summary_df = {}

for (dk,dv), (ik,iv) in zip(date_dict.items(), item_dict.items()):
    df_main = dff[dff['xcounterno']==dk]
    vat_amount = get_vat_amount(zid_trading,dk)['xprime'][0]

    item_list = df_main['xitem'].to_list()
    if len(item_list) == 1:
        df_stock = get_item_stock_1(zid_trading,dv,iv[0])
        df_sales = get_sales_1(zid_trading,dv,end_date,iv[0])
        df_sales_item = df_sales.groupby('xitem')['sales'].sum().to_frame().reset_index()
        df_return = get_return(zid_trading,dv,end_date,iv[0])
        df_return_item = df_return.groupby('xitem')['rtn'].sum().to_frame().reset_index()
    else:
        df_stock = get_item_stock(zid_trading,dv,tuple(iv))
        df_sales = get_sales(zid_trading,dv,end_date,tuple(iv))
        df_sales_item = df_sales.groupby('xitem')['sales'].sum().to_frame().reset_index()
        df_return = get_return(zid_trading,dv,end_date,tuple(iv))
        df_return_item = df_return.groupby('xitem')['rtn'].sum().to_frame().reset_index()
        
        
    df_main = df_main.merge(df_stock[['xitem','stock']],on=['xitem'],how='left').merge(df_sales_item[['xitem','sales']],on=['xitem'],how='left').merge(df_return_item[['xitem','rtn']],on=['xitem'],how='left').fillna(0).rename(columns={'stock':'pre_stock','xitem':'code','xdesc':'name','xgitem':'group','xstdcost':'avg_cost'})
    df_main = df_main.drop(columns=['xcitem','xpricecat','xduty','xwh'])
    df_main = df_main[['xcounterno','code','pack_code','name', 'group','grunnum','date','avg_cost','p_rate','retailP','wholesaleP','pre_stock','Qty_order','sales','rtn','Current_Stock','hmbr_stock','pack_stock']]
    df_main = df_main.rename(columns={'sales':'pre_sales'})
    df_main['sales'] = df_main['pre_sales']+df_main['rtn']
    df_main['sales'] = np.where(((df_main['sales']*-1))>df_main['Qty_order'],(df_main['Qty_order']*-1),df_main['sales'])
    df_main['total_p_rev'] = df_main['Qty_order'] *df_main['wholesaleP']*-1
    df_main['total_p_cost'] = df_main['Qty_order'] *df_main['p_rate']
    df_main = df_main.rename(columns={'total_p_cost':'total_p_cost_exVat'})
    df_main['vat_amount'] = (df_main['total_p_cost_exVat']/df_main['total_p_cost_exVat'].sum())*vat_amount
    df_main['total_p_cost'] = (df_main['total_p_cost_exVat']+df_main['vat_amount']).round(2)
    ##
    df_main['p_rate_vat'] = df_main['total_p_cost']/df_main['Qty_order']
    ##
    df_main['total_rev'] = df_main['sales']*df_main['wholesaleP']*-1
    df_main['total_cost'] = df_main['sales']*df_main['p_rate_vat']
    df_main['total_gp'] = (df_main['Qty_order']*df_main['wholesaleP']) - (df_main['Qty_order']*df_main['p_rate_vat']) 
    df_main['gp'] = df_main['total_rev'] + df_main['total_cost']
    df_main['perc'] = (df_main['gp']/df_main['total_gp'])*100
    df_main = df_main.round(2)

    df_sales = df_sales.rename(columns={'xitem':'code','sales':'pre_sales'})
    df_return = df_return.rename(columns={'xitem':'code'})
    df_sales_area = df_sales.merge(df_main[['code','wholesaleP','p_rate_vat']],on=['code'],how='left').merge(df_return[['code','xcus','rtn']],on=['code','xcus'],how='left').fillna(0)
    df_sales_area['sales'] = df_sales_area['pre_sales']+df_sales_area['rtn']
    df_sales_area['total_sales'] = df_sales_area['sales']*df_sales_area['wholesaleP']*-1
    df_sales_area['total_cost'] = df_sales_area['sales']*df_sales_area['p_rate_vat']
    df_sales_area['gp'] = df_sales_area['total_sales']+df_sales_area['total_cost']
    df_sales_area['md_gp'] = df_sales_area['gp'] * 0.1
    df_sales_area['month'] = df_sales_area['xdate'].astype(str).str.split("-",expand=True)[1]
    df_sales_area['year'] = df_sales_area['xdate'].astype(str).str.split("-",expand=True)[0] 
    #     df_sales_area = df_sales_area.merge(df_area[['xdiv','market']],on=['xdiv'],how='left').fillna('Unknown')
    df_sales_area = df_sales_area.groupby(['xstate','month','year'])['total_sales','total_cost','gp','md_gp'].sum().reset_index().round(2).sort_values(['year','month'])
    df_sales_area.loc[len(df_sales_area.index),:]=df_sales_area.sum(axis=0,numeric_only = True)
    # print('hello is anybody there in sales')
    main_area_dict[dk] = df_sales_area
    main_df_dict[dk] = df_main

    # #     main_summary_dict['Date (Shipment Received)']=dv
    main_summary_dict['Number of Days Passed'] = (datetime.strptime(dv, '%Y-%m-%d').date() - datetime.today().date()).days*-1
    main_summary_dict['Total Prossible Revenue']=df_main['total_p_rev'].sum()*-1
    main_summary_dict['Total Revenue To Date']=df_main['total_rev'].sum()
    main_summary_dict['Total Possible Gross Profit']=df_main['total_gp'].sum().round(2)
    main_summary_dict['Total Gross Profit To Date']=df_main['gp'].sum().round(2)
    main_summary_dict['Total Possible Cost']=df_main['total_p_cost'].sum()
    main_summary_dict['Total Cost to Date']=df_main['total_cost'].sum()*-1
    main_summary_dict['Total possible payment to Md Sir'] = (main_summary_dict['Total Possible Cost'] + (main_summary_dict['Total Possible Gross Profit']*0.1)).round(2)
    main_summary_dict['Total payment to Md Sir To Date'] = (main_summary_dict['Total Cost to Date'] + (main_summary_dict['Total Gross Profit To Date']*0.1)).round(2)
                                                            
    df = pd.DataFrame(list(main_summary_dict.items()),columns = ['Topic','Value']).round(2)
    df['Value'] = df.apply(lambda x: "BDT "+"{:,}".format(x['Value']), axis=1)

    main_summary_df[dk] = df

    
bank_details =  get_gl_details_bs_project(zid_trading,end_date).round(1).rename(columns={'sum':'Balance'})
# print('hello is anybody there')
# bank_details['Balance'] = bank_details.apply(lambda x: "BDT "+"{:,}".format(x['Balance']), axis=1)

main_bank_dict = {}

dhaka_bank = bank_details[bank_details['xacc']=='10010003'].reset_index()
dhaka_bank.at[len(dhaka_bank.index),'Balance'] = 50000000
dhaka_bank.at[len(dhaka_bank.index)-1,'xdesc'] = 'Limit'
dhaka_bank.loc[len(dhaka_bank.index),:]=dhaka_bank.sum(axis=0,numeric_only = True)
dhaka_bank.at[len(dhaka_bank.index)-1,'xdesc'] = 'Balance'
dhaka_bank = dhaka_bank.fillna('-')
dhaka_bank['Balance'] = dhaka_bank.apply(lambda x: "BDT "+"{:,}".format(x['Balance']), axis=1)

main_bank_dict['Dhaka Bank Balance'] = dhaka_bank

ucb_bank = bank_details[bank_details['xacc']=='10010006'].reset_index()
ucb_bank.at[len(ucb_bank.index),'Balance'] = 25000000
ucb_bank.at[len(ucb_bank.index)-1,'xdesc'] = 'Limit'
ucb_bank.loc[len(ucb_bank.index),:]=ucb_bank.sum(axis=0,numeric_only = True).round(2)
ucb_bank.at[len(ucb_bank.index)-1,'xdesc'] = 'Balance'
ucb_bank = ucb_bank.fillna('-')
ucb_bank['Balance'] = ucb_bank.apply(lambda x: "BDT "+"{:,}".format(x['Balance']), axis=1)

main_bank_dict['UCB Bank Balance'] = ucb_bank

md_od_bank = bank_details[bank_details['xacc']=='10010007'].reset_index()
md_od_bank.at[len(md_od_bank.index),'Balance'] = 40000000
md_od_bank.at[len(md_od_bank.index)-1,'xdesc'] = 'Limit'
md_od_bank.loc[len(md_od_bank.index),:]=md_od_bank.sum(axis=0,numeric_only = True).round(2)
md_od_bank.at[len(md_od_bank.index)-1,'xdesc'] = 'Balance'
md_od_bank = md_od_bank.fillna('-')
md_od_bank['Balance'] = md_od_bank.apply(lambda x: "BDT "+"{:,}".format(x['Balance']), axis=1)

main_bank_dict['MD sir Overdraft Balance'] = md_od_bank

loan_md_bank = bank_details[bank_details['xacc']=='10020001'].reset_index()
loan_md_bank.loc[len(loan_md_bank.index),:]=loan_md_bank.sum(axis=0,numeric_only = True).round(2)
loan_md_bank.at[len(loan_md_bank.index)-1,'xdesc'] = 'Loans Received From MD sir'
loan_md_bank = loan_md_bank.fillna('-')
loan_md_bank['Balance'] = loan_md_bank.apply(lambda x: "BDT "+"{:,}".format(x['Balance']), axis=1)

main_bank_dict['Loan received from MD Sir (Ex Mfg)'] = loan_md_bank

loan_mfg_bank = bank_details[bank_details['xacc']=='10020015'].reset_index()
loan_mfg_bank.loc[len(loan_mfg_bank.index),:]=loan_mfg_bank.sum(axis=0,numeric_only = True).round(2)
loan_mfg_bank.at[len(loan_mfg_bank.index)-1,'xdesc'] = 'Loans Received From MD sir for Manufacturing'
loan_mfg_bank = loan_mfg_bank.fillna('-')
loan_mfg_bank['Balance'] = loan_mfg_bank.apply(lambda x: "BDT "+"{:,}".format(x['Balance']), axis=1)

main_bank_dict['Loan Received from MD Sir (Only Mfg) Balance'] = loan_mfg_bank

### reminder change the database 
print (type(main_summary_df))
print (type(main_bank_dict))
print (main_summary_df)

### main_summary_df (all the df inside) will be on the email body html
main_summary_list = (list (main_summary_df.keys()))

if main_summary_list:
    with open('main_summary_df.html','w' , encoding='utf-8') as f:
        for i in main_summary_list:
            f.write (f"<h1> {i}</h1>")
            f.write (main_summary_df[i].to_html() )
else:
    with open('main_summary_df.html','w' , encoding='utf-8') as f:
        f.write (f"<h1> Main Summary List is empty </h1>")



### main_bank_dict (always just one df) will be on the email body html after all the df's in main summary df
bank_list = (list (main_bank_dict.keys()))

with open('main_summary_df.html','a' , encoding='utf-8') as f:
    if bank_list:
        for i in bank_list:
            try:
                f.write (f"<h1> {i}</h1>")
                f.write (main_bank_dict[i].to_html() )
            except:
                 f.write (f"<h1> bank dict list is empty </h1>")

main_area_dict_list = (list (main_area_dict.keys()))

with open('main_summary_df.html','a' , encoding='utf-8') as f:
    if main_area_dict_list:
        for i in main_area_dict_list:
            try:
                f.write (f"<h1> {i + 'Area Sales'}</h1>")
                f.write (main_area_dict[i].to_html() )
            except:
                 f.write (f"<h1> Area Sales is empty </h1>")


### main_area_dict all the values in this dictionary will be in sheets named by the keys (the values are dataframes)

with pd.ExcelWriter('main_area.xlsx', engine='openpyxl' ) as writer:
    # if main_area_dict_list:
        for i in main_area_dict_list:
            try:
                main_area_dict[i].to_excel(writer, sheet_name=f"{i[:7]}--{i[10::]}--")
            except ValueError as e:
                print (e)



### main_df_dict all the values in this dictionary will be in sheets named by the keys (the values are dataframes)
main_df_dict_list = (list (main_df_dict.keys()))
with pd.ExcelWriter('main_df.xlsx') as writer:
    if main_df_dict_list:
        for i in main_df_dict_list:
            try:
                main_df_dict[i].to_excel(writer, sheet_name=f"{i[:7]}--{i[10::]}--")
            except ValueError as e:
                print (e)


### first send it to me as a test and then I will tell you who to send it to.

me = "XXXXXX@gmail.com"
you = ["XXXXXX@gmail.com", "XXXXXX@gmail.com","XXXXXX@gmail.com", "XXXXXX@gmail.com"]

msg = MIMEMultipart('alternative')
msg['Subject'] = f"Special Shipment information-of-{','.join(main_area_dict_list)}"
msg['From'] = me
msg['To'] = ", ".join(you)

filename = "main_summary_df.html"
f = open(filename)
attachment = MIMEText(f.read(),'html')
msg.attach(attachment)


part1 = MIMEBase('application', "octet-stream")
part1.set_payload(open("main_area.xlsx", "rb").read())
encoders.encode_base64(part1)
part1.add_header('Content-Disposition', 'attachment; filename="main_area.xlsx"')
msg.attach(part1)

part2 = MIMEBase('application', "octet-stream")
part2.set_payload(open("main_df.xlsx", "rb").read())
encoders.encode_base64(part2)
part2.add_header('Content-Disposition', 'attachment; filename="main_df.xlsx"')
msg.attach(part2)


username = 'XXXXXX'
password = 'XXXXXX'

s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(me,you,msg.as_string())
s.quit()
