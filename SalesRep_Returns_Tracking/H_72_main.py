
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import date,datetime,timedelta
import psycopg2
from mail import send_mail

engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')

start_date = input(str("input from date "))
end_date = input(str("input last date "))

# ==================== get the sales from last years ===============
def get_sales(zid, start_date, end_date):
    df = pd.read_sql(f"""
                select opord.xsp, prmst.xname ,sum (opodt.xlineamt) as total_sales  from opord
                left join opodt on opord.xordernum= opodt.xordernum
                JOIN prmst ON opord.xsp = prmst.xemp
                where opord.zid = {zid}
                and opodt.zid = {zid}
                and prmst.zid = {zid}
                and opord.xdate between '{start_date}' and '{end_date}'
                group by opord.xsp, prmst.xname order by opord.xsp
    """ , con = engine)
    return df

# ==================== get the return from last years =============
def get_return(zid, start_date, end_date):
    df = pd.read_sql(f"""select 
                            opcrn.xemp as xsp, sum (opcdt.xlineamt) as total_return  from opcrn
                            inner join opcdt
                            on opcrn.xcrnnum= opcdt.xcrnnum
                            where opcrn.zid={zid}
                            and opcdt.zid = {zid}
                            and opcrn.xdate between '{start_date}' and '{end_date}'
                            group by opcrn.xemp  """, con = engine)
    return df

# ==================== call sales function ========================
df_get_sales = get_sales(100001, start_date, end_date )

# ==================== call return function =======================
df_return = get_return(100001,start_date,end_date )

# ==================== merge sales with return ====================
df_net_sales = pd.merge(df_get_sales, df_return, on='xsp', how= 'left').fillna(0)

# ==================== create net sales column ====================
df_net_sales['net_sales'] = df_net_sales['total_sales'] - df_net_sales['total_return']

# ==================== create a new row for total amount and append with sales df ==========
total_row = {
    'xsp' : "Net Sales From",
    'xname' : f"{start_date} to {end_date}" ,
    'total_sales' : df_net_sales['total_sales'].sum(),
    'total_return' : df_net_sales['total_return'].sum(),
    'net_sales' : df_net_sales['net_sales'].sum()
}
df_net_sales = df_net_sales.append(total_row , ignore_index=True)

# ==================== export to excel ====================
df_net_sales.to_excel("H_72salesman_net_sales.xlsx", index=False)

# ==================== send mail to ====================
send_mail("H_72_SalesMan Wise Net Sales", "Please Find The Attached File",
attachment=['H_72salesman_net_sales.xlsx'], recipient=['XXXXXX'], )