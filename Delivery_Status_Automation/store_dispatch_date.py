# %%

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from datetime import datetime
from dcon import get_data
from mail import send_mail

zid = 100001
# district = ('Sylhet Retail', 'Rahima enterprise', 'District', 'Chittagong retail', 'Imamgonj') 

salesman = ('SA--000068',
'SA--000224',
'SA--000038',
'SA--000144',
'SA--000021',
'SA--000193',
'SA--000114',
'SA--000011',
'SA--000192',
'SA--000098',
'SA--000227',
'SA--000242')

now_date = datetime.now()

prev_date = now_date - timedelta(days = 1)
prev_date_name = prev_date.strftime("%A")
# check if previous date is friday/ if friday then count the date of thursday
if prev_date_name == 'Friday':
    prev_date = now_date - timedelta (days = 2)

else:
    prev_date = now_date - timedelta (days = 1)


prev_date = prev_date.strftime("%Y-%m-%d")

prev_date
# check if prev date is fridyay



#%%
to_store_query = f"""
            SELECT opdor.zid, opdor.xdornum, opdor.xdate AS dodate, opdor.xcus, cacus.xcity, cacus.xstate, opdor.xsp, opdor.xtotamt,
                opdor.xdatestor AS dispatchdate
                FROM opdor
                LEFT JOIN cacus ON opdor.xcus = cacus.xcus AND opdor.zid = cacus.zid
                WHERE opdor.zid = {zid}
                AND opdor.xdate = '{prev_date}'
                AND ((opdor.xstatusdor = '1-Open') OR (opdor.xstatusdor = '2-Confirmed'))
                AND opdor.xsp IN {salesman} order by xdornum;

        """
print (to_store_query)
df_send_to_store = get_data (to_store_query , fetchOneOrAll = 'all' , df = True, conn = "local")


excel_file_name = f"H_71_store_dispatch{prev_date}.xlsx"

df_send_to_store.to_excel(excel_file_name, engine = 'openpyxl')


send_mail (f"H_71_Need Store Dispatch date to fillup for {prev_date}", "Please fill the dispatch date", attachment= [excel_file_name], recipient= ['XXXXXX'])

