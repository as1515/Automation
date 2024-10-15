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
from dateutil.relativedelta import relativedelta
from datetime import datetime
from email.utils import COMMASPACE


# %%
""" create date function """
def date_delta(sales_date_timedelta : int = 2 )-> str:
    now_date = datetime.now()
    date_ = now_date - timedelta(days=sales_date_timedelta)
    strf_date = date_.strftime('%Y-%m-%d')
    return strf_date

# %%
# Sales Function
def get_sales(zid, date):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')

    df = pd.read_sql("""select  opdor.xdate ,opdor.xdiv, opdor.xcus,  sum (opdor.xdtwotax) as total  from opdor
                        where opdor.zid= {}
                        and opdor.xdate>= '{}'
                        group by opdor.xdate , opdor.xcus , opdor.xdiv , opdor.xcus
                        order by xdate asc
""".format(zid,date),con=engine)
    return df

def get_cacus(zid):
    engine = create_engine('postgresql://XXXXXX:XXXXXX@localhost:5432/da')

    df = pd.read_sql("""select cacus.xcus ,cacus.xshort,
                        cacus.xmobile ,cacus.xadd1, cacus.xadd2 from cacus
                        where zid = {}
""".format(zid),con=engine)
    return df


# %%
df = get_sales(100001, '2023-01-01')
df_cus = get_cacus(100001)

df

# %%
df_cus

# %%
df['xdate'] = pd.to_datetime (df['xdate'])

# %%
df['year'] = df['xdate'].dt.year
df['month'] = df['xdate'].dt.month

# %%
df = df.iloc[:,1::]

df

# %%
df = df.groupby(['xdiv','xcus','month', 'year']).sum(numeric_only=False).sort_values(by='month').reset_index()
df

# %%
grand_total = df.groupby('xcus')['total'].transform('sum')
# Add the grand total as a new column
df['grand_total'] = grand_total


df

# %%
pivot = pd.pivot_table(df, values='total', index=['xcus', 'year','xdiv'], columns=['month'], aggfunc='sum')
# Reset the index to make xcus and year regular columns
pivot = pivot.reset_index()
# Add a column for the grand total
pivot['grand_total'] = pivot.iloc[:, 2:].sum(axis=1)
pivot

# %%
df_main = pd.merge(pivot, df_cus , on = 'xcus' , how= 'left')

for i in df_main.columns:
    if i == 1:
        df_main = df_main.rename(columns={i: 'January'})
    if i == 2:
        df_main = df_main.rename(columns={i: 'February'})
    if i == 3:
        df_main = df_main.rename(columns={i: 'March'})
    if i == 4:
        df_main = df_main.rename(columns={i: 'April'})
    if i == 5:
        df_main = df_main.rename(columns={i: 'May'})
    if i == 6:
        df_main = df_main.rename(columns={i: 'June'})
    if i == 7:
        df_main = df_main.rename(columns={i: 'July'})
    if i == 8:
        df_main = df_main.rename(columns={i: 'August'})
    if i == 9:
        df_main = df_main.rename(columns={i: 'September'})
    if i == 10:
        df_main = df_main.rename(columns={i: 'October'})
    if i == 11:
        df_main = df_main.rename(columns={i: 'November'})
    if i == 12:
        df_main = df_main.rename(columns={i: 'December'})
        

# %%
df_main

# %%
monthly_cols = [col for col in df_main.columns if col not in ['xcus', 'year',  'xshort' , 'xmobile' , 'xdiv' , 'xadd2']]
# move the non-monthly columns to the front
new_order = ['xcus', 'year', 'xshort', 'xmobile' , 'xdiv' , 'xadd2'] + monthly_cols

new_order

# %%
df_main = df_main[new_order]

# %%
df_main.to_excel('customer_wise_monthly_sales_hmbr.xlsx' , engine= 'openpyxl')

# %%



sender = 'XXXXXX@gmail.com'
password = 'XXXXXX'

recipients = ['XXXXXX@gmail.com', 'XXXXXX@gmail.com']

# email message details
subject = 'Monthly sales report Customer wise HMBR'
body = 'Please find attached the monthly sales report by customer'
filename = 'customer_wise_monthly_sales_hmbr.xlsx'

# create email message object
msg = MIMEMultipart()
msg['From'] = sender
msg['To'] = COMMASPACE.join(recipients)
msg['Subject'] = subject

# attach file to email message
with open(filename, 'rb') as f:
    part = MIMEBase('application', "octet-stream")
    part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(part)

# add message body to email
msg.attach(MIMEText(body, 'plain'))

# send email message
smtp = smtplib.SMTP('smtp.gmail.com', 587)
smtp.ehlo()
smtp.starttls()
smtp.login(sender, password)
smtp.sendmail(sender, recipients, msg.as_string())
smtp.quit()





