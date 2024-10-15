# %%
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from datetime import datetime
from dcon import get_data
from mail import send_mail
import time
import os
import glob

zid = 100001
# district = ('Sylhet Retail', 'Rahima enterprise', 'District', 'Chittagong retail', 'Imamgonj') 
salesman_info_dict = {
    'SA--000068': ('XXXXXX@gmail.com', 'Md. Salimullah (Salim)'),
    'SA--000224': ('XXXXXX@gmail.com', 'Md. Ariful Islam-2'),
    'SA--000038': ('XXXXXX@gmail.com', 'Md. Belal Hossain'),
    'SA--000144': ('XXXXXX@gmail.com', 'Syed Mobarok Hossain'),
    'SA--000021': ('XXXXXX@gmail.com', 'Md. Maruful Islam'),
    'SA--000193': ('XXXXXX@gmail.com', 'Limon Mridha'),
    'SA--000114': ('XXXXXX@gmail.com', 'Md. Pavel Mia'),
    'SA--000011': ('XXXXXX@gmail.com', 'Jamal Hossain Titu'),
    'SA--000192': ('XXXXXX@gmail.com', 'Sojib Hossen'),
    'SA--000098': ('XXXXXX@gmail.com', 'Md. Belayet Hossen'),
    'SA--000227': ('XXXXXX@gmail.com', 'Md. Sumon Hossain Mithu'),
    'SA--000242': ('XXXXXX@gmail.com', 'Md. Forhadul Islam')
}
# should be taken from the prmst - email relation through the ERP instead of hard codes
salesman = tuple(salesman_info_dict.keys())

# Get Today Date
date = datetime.today()
today_date = date.strftime("%Y-%m-%d")
# today_date = '2023-12-05'
print (today_date)




#%%
to_salesman_query = f"""
            SELECT opdor.xdornum as do_number,
             opdor.xdate AS dodate, opdor.xcus, concat (opdor.xcus, '--', cacus.xshort) as customer, cacus.xadd1 as address, cacus.xtaxnum as mobile_number, cacus.xcity, opdor.xsp, opdor.xtotamt as total_amt,
                opdor.xdatestor AS dispatchdate
                FROM opdor
                LEFT JOIN cacus ON opdor.xcus = cacus.xcus AND opdor.zid = cacus.zid
                WHERE opdor.zid = {zid}
                AND opdor.xdatestor = '{today_date}'
                AND opdor.xflagdor = 'Goods Delivered'
                AND opdor.xsp IN {salesman} order by xdornum;

        """

df_send_to_salesman = get_data (to_salesman_query , fetchOneOrAll = 'all' , df = True, conn = "local")
df_send_to_salesman['customer_receive_date'] = ""
df_send_to_salesman



# %%
# Find out Customer Balance 
# At first we will get customers ID from above df_send_salesman dataframe and the filter out their previous days until balance

# get customer ID
get_customer_id = tuple(df_send_to_salesman['xcus'].to_list())


# now get their untill previous days balance
today_balance_col = f"till_{today_date}_balance".replace("-","_")

balance_query = f"""
    SELECT gldetail.xsub AS xcus, SUM(gldetail.xprime) AS {today_balance_col}
    FROM glheader
    JOIN gldetail ON glheader.xvoucher = gldetail.xvoucher
    JOIN cacus ON gldetail.xsub = cacus.xcus
    WHERE glheader.zid = {zid}
        AND gldetail.zid = {zid}
        AND cacus.zid = {zid}
        AND gldetail.xproj = 'GULSHAN TRADING'
        AND gldetail.xvoucher NOT LIKE '%%OB%%'
        AND glheader.xdate <= '{today_date}'
        AND gldetail.xsub IN {get_customer_id}
    GROUP BY gldetail.xsub
    HAVING SUM(gldetail.xprime) > 500
"""


# Execute the query using psycopg2 not pd.read sql
df_prev_balance_customer = get_data (balance_query , fetchOneOrAll = 'all' , df = True, conn = "local")

# now merge with df_send_to_salesman
df_send_to_salesman_with_balance = pd.merge(df_send_to_salesman, df_prev_balance_customer, on = 'xcus', how='left')
df_send_to_salesman_with_balance

# for send to salesman
df_send_to_salesman = df_send_to_salesman.drop(columns=['xcus'])
df_send_to_salesman

# %%
for salesman_id, (email, salesman_name) in salesman_info_dict.items():
    print(salesman_id)

    # Filter the DataFrame for the specific salesman_id
    salesman_df = df_send_to_salesman_with_balance[df_send_to_salesman_with_balance['xsp'] == salesman_id]

    # Check if the DataFrame is not empty
    if not salesman_df.empty:
        # Create a unique Excel file for each salesman
        excel_file_path = f"salesman_{salesman_id}.xlsx"
        salesman_df.to_excel(excel_file_path, index=False)

        # Create the HTML body with the filtered DataFrame
        html_body = [(salesman_df, f" On the Way To Delivery for {salesman_name} (DO Confirm Date {today_date})")]

        # Send email with the Excel file as the attachment
        send_mail(
            subject=f"H_71.1[Delivered from store] On the way to delivery goods to customer for the day of {today_date}",
            bodyText=f"""
            জনাব {salesman_name} <br>
            অনুগ্রহ করে আজকের {today_date} এ সংযুক্ত ডেলিভারি পণ্যের এক্সেল ফাইলটি open করুন <br> এবং যখন আপনি  আইটেমগুলো  পাবেন
             তখন customer_receive_date কলামটি পূরণ করে <br> উক্ত Excel ফাইলটি  sohelsorkar356648@gmail.com এই মেইল এ পাঁঠিয়ে দিবেন।
             """,
            attachment=[excel_file_path],
            recipient=[email, 'ithmbrbd@gmail.com'],
            html_body=html_body
        )
    else:
        print(f"No data for salesman: {salesman_id}")
# delete all excelfile from current directory
def delete_xlsx_files():
    try:
        for file in glob.glob("*.xlsx"):
            os.remove(file)
        print("All .xlsx files deleted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Call the function to delete .xlsx files in the current working directory
delete_xlsx_files()


# %%
## %%
# For Riyad vai
excel_file_path = f"H71.1_delivered{today_date}.xlsx"
df_send_to_salesman = df_send_to_salesman.drop('xsp' , axis=1)
df_send_to_salesman.to_excel(excel_file_path, index=False)
html_body = [(df_send_to_salesman, f" On the Way To Delivery")]

send_mail(
    subject="H71.1 On the way to delivery",
    bodyText=f"Todays {today_date} mail sent to all salesman and find the excel sheet of district delivered goods",
    attachment=[excel_file_path],
    recipient=['XXXXXX'],
    html_body=html_body
)


# %%
df_send_to_salesman_with_balance = df_send_to_salesman_with_balance.drop(columns=['customer_receive_date'])
df_send_to_salesman_with_balance

# %%
# For Director Sir with customer balance

excel_file_path = f"H71.1_delivered_with_balance.xlsx"
df_send_to_salesman_with_balance = df_send_to_salesman_with_balance.drop('xsp' , axis=1)
df_send_to_salesman_with_balance.to_excel(excel_file_path, index=False)
html_body = [(df_send_to_salesman_with_balance, f" On the Way To Delivery")]

send_mail(
    subject="H71.1 On the way to delivery",
    bodyText=f"Todays {today_date} mail sent to all salesman and find the excel sheet of district delivered goods",
    attachment=[excel_file_path],
    recipient=['XXXXXX'],
    html_body=html_body
)


# %%



