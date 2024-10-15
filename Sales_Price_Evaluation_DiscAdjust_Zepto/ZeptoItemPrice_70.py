# %%
# System Import
import sys
from mail import send_mail
from datetime import date,datetime,timedelta
import os
# thirdparty import
import pandas as pd
import numpy as np
import psycopg2
# explicitely relative module import
from dcon import ZEPTO_ID, HMBR_LOCAL_SERVER , IT_MAIL, ADMIN_MAIL, DIRECTOR_MAIL, IBRAHIM_MAIL

sys.path.append('E:/')  # Add the path to the '_CONFIG' folder

ENGINE = HMBR_LOCAL_SERVER
print (ENGINE)

# %%
ZID = ZEPTO_ID

def time_delta (days):
    today_date = datetime.today()
    delta_day = today_date - timedelta(days=days)
    delta_day = delta_day.strftime("%Y-%m-%d")
    return delta_day


# %%
def avg_rate (ZID, time_delta):
    query = """SELECT
        opodt.xitem, 
        caitem.xdesc,
        SUM(opodt.xqtyord) as qty,
        sum(opodt.xlineamt) as TotalSales,
    ( sum(opodt.xlineamt) / SUM(opodt.xqtyord) ) as avgprice,
    caitem.xstdprice as caitemprice
    FROM
        opord
        LEFT JOIN opodt ON opord.xordernum = opodt.xordernum
        LEFT JOIN caitem ON opodt.xitem = caitem.xitem
    WHERE
        opord.zid = {}
        AND opodt.zid = {}
        AND caitem.zid = {}
        AND  opord.xdate >= '{}'
    AND opord.xstatusord = '5-Delivered'


    GROUP BY
        opodt.xitem, caitem.xdesc, caitem.xstdprice
    ORDER BY
        opodt.xitem
        
    """.format(ZID, ZID, ZID, time_delta )
    df = pd.read_sql(query, con = ENGINE)
    return df

# %%
df_avg_1 = avg_rate (ZID, time_delta(30))
df_avg_2 = avg_rate (ZID, time_delta(15))
df_avg_3 = avg_rate (ZID, time_delta(7))

# %%
df_avg_1 = avg_rate (ZID, time_delta(30))
df_avg_1.columns.values[4] = 'last_30_days_avg'

df_avg_2 = df_avg_2.loc[: , ['xitem','avgprice']].rename(columns={'avgprice': 'last_15_days_avg'})
df_avg_3 = df_avg_3.loc[: , ['xitem','avgprice']].rename(columns={'avgprice': 'last_7_days_avg'})

df_avg_main = pd.merge(df_avg_1, df_avg_2, on='xitem', how='left')

# Merge the result with df_avg_3 on the 'xitem' column
df_avg_main = pd.merge(df_avg_main, df_avg_3, on='xitem', how='left')
caitem_col = df_avg_main.pop('caitemprice')
df_avg_main

# %%
df_avg_main.insert(7 , 'present_price' , caitem_col)
df_avg_main

# %%
with pd.ExcelWriter("ZeptoItemPrice_70.xlsx" ) as writer:
    df_avg_main.to_excel(writer, sheet_name='ItemPrice')

# %%
#  call the function

subject = f"H 70 ZEPTO MONTHLY PRODUCT AVERAGE PRICE LAST 30 DAYS FROM {time_delta(30)} "
body_text = "Please find the attachment.\n"
excel_files = ['ZeptoItemPrice_70.xlsx', ] #optional if any
mail_to = [IT_MAIL, ADMIN_MAIL , DIRECTOR_MAIL, IBRAHIM_MAIL]
html_df_list = [(df_avg_main, f'ITEM AVERAGE PRICE FROM {time_delta(30)} ')] #optional if any

send_mail(subject, body_text, excel_files, mail_to,html_df_list )

# %%


# %%



