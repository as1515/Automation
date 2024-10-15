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
from email.mime.image import MIMEImage
from email import encoders
from datetime import datetime
# for visualization
import matplotlib.pyplot as plt

""" create date function """ 
#here if we give only first parameter then it should return those timedelta date, if we given second parameter then it return specific date
#and if no argument pass then it return 2 days before 
def date_delta(sales_date_timedelta : int = 2, first_date:str = None  )-> str:
    now_date = datetime.now()
    date_ = now_date - timedelta(days=sales_date_timedelta)
    strf_date = date_.strftime(f"%Y-%m-{first_date if first_date else '%d'}")
    return strf_date

# Date Variable. Default timedelta 2 days
from_date= date_delta(2)
from_first_day_of_month = date_delta(8, "02")# first parameter is day 2nd parameter is first day of month
to_date= date_delta(2)
# Buisiness ID
hmbr_id = 100001
zepto_id = 100005
# Return date
frm_return_date = date_delta(1)
to_return_date = date_delta(1)
from_first_day_of_month

# Sales Function
engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')
def get_sales(zid, frm_date, to_date):
    df = pd.read_sql("""SELECT   opord.xordernum, opord.xdate, opord.xsp , prmst.xname, caitem.xitem, caitem.xgitem, opodt.xdtwotax
                FROM opord
                JOIN opodt
                ON opord.xordernum = opodt.xordernum
                JOIN caitem
                ON opodt.xitem = caitem.xitem
                JOIN prmst
                ON opord.xsp = prmst.xemp
                AND opodt.zid = '%s'
                AND opord.zid = '%s'
                AND caitem.zid = '%s'
                AND prmst.zid = '%s'
                AND opord.xdate between '%s' And '%s'
                group by opord.xsp , prmst.xname, caitem.xitem, caitem.xgitem , opodt.xdtwotax , opord.xordernum, opord.xdate
                order by opord.xsp asc"""%(zid,zid,zid,zid,frm_date, to_date),con=engine)
    return df

# return Function from opcrn
def get_return(zid,frm_date, to_date):
    df = pd.read_sql("""
                        select opcrn.xemp,  sum(opcdt.xlineamt)  from opcrn
                        JOIN opcdt
                        ON opcrn.xcrnnum = opcdt.xcrnnum
                        AND opcrn.zid = %s
                        AND opcdt.zid = %s
                        AND opcrn.xdate between '%s' and '%s'
                        GROUP BY opcrn.xemp"""%(zid,zid,frm_date, to_date),con=engine)
    return df

# return Function from imtemptrn
def get_return_reca(zid,frm_date, to_date):
    df = pd.read_sql("""
                        select imtemptrn.xemp,  sum(imtemptdt.xlineamt)  from imtemptrn
                        JOIN imtemptdt
                        ON imtemptrn.ximtmptrn = imtemptdt.ximtmptrn
                        AND imtemptrn.zid = %s
                        AND imtemptdt.zid = %s
                        AND imtemptrn.xdate between '%s' and '%s'
                        AND imtemptrn.ximtmptrn like '%s'
                        GROUP BY imtemptrn.xemp, imtemptrn.ximtmptrn"""%(zid,zid,frm_date, to_date, '%%RECA%%'),con=engine)
    return df

# Get all salesman
def get_employee(zid):
    df = pd.read_sql("""SELECT xemp FROM prmst WHERE zid = '%s'"""%(zid),con=engine)
    return df

# HMBR SALE
def net_sale(zid,frm_date, to_date):
    df_one_day_sale             = get_sales(zid, frm_date, to_date) # default timedelta 2
    df_1day_sale_total          = df_one_day_sale.groupby(['xsp' , 'xname']).sum().reset_index() # total sale salesman wise
    df_1day_sale_home_product   = df_one_day_sale.groupby(['xsp', 'xgitem']).sum().reset_index() # for home product sale salesman wise
    df_1day_sale_home_product   = df_1day_sale_home_product[ df_1day_sale_home_product['xgitem'] == 'Industrial & Household']\
                                        .drop('xgitem', axis=1)\
                                        .reset_index(drop=True)\
                                        .rename(columns = {'xlineamt':'home_product_sale'}) # find which salesman sale total homeproduct

    df_1day_sale_zepto          = df_one_day_sale.query('(xgitem == "Industrial & Household" and xitem.str.contains("Z00"))'                                              ).groupby(['xsp', 'xitem']).sum().reset_index()\
                                        .drop('xitem', axis=1)\
                                        .rename(columns={'xlineamt':'zepto_sale'}) # find which salesman sales zepto item in zepto

    alls_one_day_sale_summary   = pd.merge(
                                    pd.merge(df_1day_sale_total, df_1day_sale_home_product, on = 'xsp', how= 'left')\
                                    ,df_1day_sale_zepto, on='xsp', how ='left') # total summary ,[salesman id , name ]

    return alls_one_day_sale_summary


one_day_sale_hmbr = net_sale(hmbr_id , from_date, to_date )\
                                .rename(columns={
                                        'xdtwotax_x':'gross_sale_hmbr',
                                        'xdtwotax_y':'home_product_sale_hmbr',
                                        })\
                                                .drop(columns=['xdtwotax'] , errors='ignore')

one_month_sale_hmbr = net_sale(hmbr_id , from_first_day_of_month, to_date )\
                                .rename(columns={
                                        'xdtwotax_x':'gross_sale_hmbr',
                                        'xdtwotax_y':'home_product_sale_hmbr',
                                        })\
                                                .drop(columns=['xdtwotax'],errors='ignore')
# # get one day or cumulative return # from date  to todate
# Return part > First we take opcrn then we take imtemptrn return and then outer or full join two dataframe
# To get all return value
# This is One day Total return of HMBRR
def return_item(zid , frm_date, to_date):
        #call back
        df_reca = get_return_reca(zid, frm_date , to_date).rename(columns = {'sum' : 'reca_sum'}).groupby('xemp').sum().reset_index()
        df_sr = get_return(zid, frm_date , to_date).rename(columns = {'sum' : 'sr_sum'}).groupby('xemp').sum().reset_index()
        df_salesman = get_employee(zid)
        df_return_asper_salesman_hmbr = pd.merge(get_employee(zid), df_sr, on='xemp' , how='left')
        df_return_ = pd.merge(df_return_asper_salesman_hmbr, df_reca, on='xemp' , how='left').fillna(0)
        df_return_['sum_of_return'] = df_return_ ['sr_sum'] + df_return_ ['reca_sum']
        df_return_ = df_return_[df_return_['sum_of_return']>0].reset_index()
        return df_return_
return_total_one_day_hmbr = return_item(hmbr_id , frm_return_date, to_return_date)
return_total_one_month_hmbr = return_item(hmbr_id , from_first_day_of_month, to_return_date)


net_one_day_sale_hmbr = pd.merge(one_day_sale_hmbr, return_total_one_day_hmbr, left_on='xsp', right_on = 'xemp', how= 'left')\
                        .drop(columns = 'xemp' , errors='ignore')\
                        .rename(columns = {'xdtwotax':'zepto_product_in_hmbr'})

net_one_month_sale_hmbr = pd.merge(one_month_sale_hmbr, return_total_one_month_hmbr, left_on='xsp', right_on = 'xemp', how= 'left')\
                        .drop(columns = 'xemp' )\
                        .rename(columns = {'xdtwotax':'zepto_product_in_hmbr'})
# END OF HMBR SALES AND RETURN PART
net_one_day_sale_hmbr = net_one_day_sale_hmbr.fillna(0)

#Sales in Zepto
def net_sale_zepto(zid,frm_date, to_date):
    df_one_day_sale             = get_sales(zid, frm_date, to_date) # default timedelta 2
    df_1day_sale_total          = df_one_day_sale.groupby(['xsp' , 'xname']).sum().reset_index() # total sale salesman wise
    df_1day_sale_total   = df_one_day_sale.groupby(['xsp', 'xgitem','xname']).sum().reset_index() # for home product sale salesman wise
    return df_1day_sale_total

one_day_sale_zepto = net_sale_zepto(zepto_id , from_date, to_date )\
                    .rename(columns={'xdtwotax':'gross_sale_zepto'})

# Zepto Return if no return found in dataframe then catch the error and return new 0 based dataframe
try:
    return_total_one_day_zepto = return_item(zepto_id , frm_return_date, to_return_date)\
                                            .groupby(['xemp']).sum().reset_index()\
                                                .drop(columns=['index'])
except KeyError as e:
    print ("no value found in return_total_one_day_zepto")
    data = {'xemp': 'SA',
        'sr_sum': 0,
        'reca_sum' : 0,
        'sum_of_return' : 0
        }
    return_total_one_day_zepto = pd.DataFrame(data , index = [1])


# Zepto Return one month if no return found in dataframe then catch the error and return new 0 based dataframe
try:
    return_total_one_month_zepto = return_item(zepto_id , from_first_day_of_month, to_return_date)\
                                            .groupby(['xemp']).sum().reset_index()\
                                                .drop(columns=['index'])
except KeyError as e:
    print ("no value found in return_total_one_month_zepto")
    data = {'xemp': 'SA',
            'sr_sum': 0,
            'reca_sum' : 0,
            'sum_of_return' : 0
            }
    return_total_one_month_zepto = pd.DataFrame(data , index = [1])
    
# Zepto Sales one day
one_day_sale_zepto = net_sale_zepto(zepto_id , from_date, to_date )\
                    .rename(columns={'xdtwotax':'gross_sale_zepto'})

# Zepto Sales one month
one_month_sale_zepto = net_sale_zepto(zepto_id , from_first_day_of_month, to_date )\
                    .rename(columns={'xdtwotax':'gross_sale_zepto'})


#merge sales with return zepto
net_one_day_sale_zepto = pd.merge(one_day_sale_zepto, return_total_one_day_zepto, left_on='xsp', right_on = 'xemp', how= 'left')\
                        .drop(columns = ['xgitem',  'xemp' , 'index'] , errors='ignore')\
                        .rename(columns = {'xsp': 'zepto_sid', 'sr_sum':'zepto_sr_sum' , 'reca_sum' : 'zepto_reca_sum' , 'sum_of_return' : 'sum_of_return_zepto'})\
                        .fillna(0)\
                        

net_one_month_sale_zepto = pd.merge(one_month_sale_zepto, return_total_one_month_zepto, left_on='xsp', right_on = 'xemp', how= 'left')\
                        .drop(columns = ['xgitem',  'xemp' , 'index'] , errors='ignore')\
                        .rename(columns = {'xsp': 'zepto_sid', 'sr_sum':'zepto_sr_sum' , 'reca_sum' : 'zepto_reca_sum' , 'sum_of_return' : 'sum_of_return_zepto'})\
                        .fillna(0)
net_one_month_sale_zepto

net_one_month_sale_zepto.to_csv('zepto.csv')

net_one_day_sale_zepto

#  NOW we will merge hmbr with zepto salesman with name column to see zepto sales in hmbr
NET_SALES_HMBR_WITH_ZEPTO_ONE_DAY = pd.merge(net_one_day_sale_hmbr, net_one_day_sale_zepto , on = 'xname' , how='left')\
                                        .rename(columns={
                                        'total_return' : 'total_return_hmbr',
                                        'total_sale_y' : 'sales_in_zepto',
                                        'xsp_y' : 's_id_in_zepto',
                                        'xsp_x' : 's_id_in_hmbr',
                                        
                                    })\
                                        .fillna(0)

# AND For Monthly Sale cumulative
NET_SALES_HMBR_WITH_ZEPTO_ONE_MONTH = pd.merge(net_one_month_sale_hmbr, net_one_month_sale_zepto , on = 'xname' , how='left').fillna(0)

#excel export
writer = pd.ExcelWriter('zepto_n_homeproduct.xlsx' , engine= 'openpyxl')
NET_SALES_HMBR_WITH_ZEPTO_ONE_DAY.to_excel(writer,'daily')
NET_SALES_HMBR_WITH_ZEPTO_ONE_MONTH.to_excel(writer,'monthly')
writer.save()


# visualization
df_1 =  NET_SALES_HMBR_WITH_ZEPTO_ONE_DAY
df_m =  NET_SALES_HMBR_WITH_ZEPTO_ONE_MONTH

def salesGraph(df , width, height, name ,frm):
    main_x = df['xname']
    x = df['total_sale_without_return']
    y = df['home_product_sale']
    z = df['sales_in_zepto']
    return_ = df['return_amount']
    # plt.figure(figsize=(width, height))
    fig = plt.figure()
    fig.set_size_inches(width, height)
    plt.bar(main_x, x , label = 'Sale without return' , color = "#54B435" )
    plt.bar(main_x, y, label = 'Home product sale' , color = "#0F3460")
    plt.bar(main_x, z, label = 'Zepto Product Sale' , color = "#E0144C")
    plt.bar(main_x, return_, label = 'Total Return' , color = "#3C4048")
    plt.xlabel("Salesman")
    plt.ylabel("Amount")
    plt.title(f'Salesgraph from {frm}' , weight='bold')
    plt.legend()
    plt.xticks(rotation=90)
    plt.savefig(f'{name}.png' ,dpi = 120 ,bbox_inches='tight')
    plt.show()

#MAil Part
strFrom = 'XXXXXX@gmail.com'
strTo = ['XXXXXX@gmail.com' ,'XXXXXX@gmail.com']
# strTo = ['ithmbrbd@gmail.com' ]
msgRoot = MIMEMultipart('related')
msgRoot['Subject'] = 'Salesman wise cumulative sale HMBR and Zepto'
msgRoot['From'] = strFrom
msgRoot['To'] = ", ".join(strTo)

# msgRoot['Cc'] =cc
msgRoot.preamble = 'Multi-part message in MIME format.'

msgAlternative = MIMEMultipart('alternative')
msgRoot.attach(msgAlternative)

#Attach Image
try:
    msgText = MIMEText('Alternative plain text message.')
    msgText = MIMEText('<b>last day hmbr and zepto sales</b><br><img src="cid:image1"><br><b>Last One Month Sale</b><br>\<img src="cid:image2"><br>', 'html')
    msgAlternative.attach(msgText)
    fp = open('one_day.png', 'rb') #Read image 
    msgImage = MIMEImage(fp.read())
    fp.close()
    # Define the image's ID as referenced above
    msgImage.add_header('Content-ID', '<image1>')
    msgRoot.attach(msgImage)
    fp1 = open('one_month.png', 'rb') #Read image 
    msgImage1 = MIMEImage(fp1.read())
    fp1.close()
    # Define the image's ID as referenced above
    msgImage1.add_header('Content-ID', '<image2>')
    msgRoot.attach(msgImage1)
except:
    pass

#Attach excel file
part3 = MIMEBase('application', "octet-stream")
part3.set_payload(open("zepto_n_homeproduct.xlsx", "rb").read())
encoders.encode_base64(part3)
part3.add_header('Content-Disposition', 'attachment; filename="zepto_n_homeproduct.xlsx"')
msgRoot.attach(part3)
username = 'XXXXXX'
password = 'XXXXXX'

s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(strFrom,strTo,msgRoot.as_string())
s.quit()