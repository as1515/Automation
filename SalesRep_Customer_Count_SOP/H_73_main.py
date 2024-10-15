# %%
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import date,datetime,timedelta
import psycopg2
from dcon import get_data

# %%
now = datetime.now()
yesterday = now - timedelta(1)
yesterday = yesterday.strftime("%Y-%m-%d")
yesterday
zid = 100001

# %%
query = f"""select opdor.xsp, prmst.xname, sum (opdor.xtotamt), count (opdor.xcus)
            from opdor
            join prmst on opdor.xsp = prmst.xemp 
            where opdor.xdate = '{yesterday}' 
            and opdor.zid = {zid} 
            and prmst.zid = {zid} 
            group by opdor.xsp, prmst.xname 
            order by opdor.xsp"""

# %%
df = get_data(query, df = True, conn = "local")
df

# %%
excel_file = "H_73_salesman_wise_sales_customer_count.xlsx"

df.to_excel(excel_file, index= False, engine='openpyxl')

# %%
from mail import send_mail

send_mail("H_73_SalesMan Wise Net Sales with customer count", "Please Find The Attached File",
attachment=[excel_file], recipient=['XXXXXX'], )

# %%



