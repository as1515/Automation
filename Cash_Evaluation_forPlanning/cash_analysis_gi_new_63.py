from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import openpyxl
from mail import send_mail
from query_function import (
    sales_discount,
    sales_return_opcrn,
    sales_return_imtemptrn,
)

pd.options.display.float_format = '{:.2f}'.format

start_date = input("input start date----  ")
end_date = input("input end date----  ")
zid_hmbr = 100001
zid_GI = 100000

# SALES-DISCOUNT PART
df_sales_discount_trading = sales_discount(zid_hmbr, start_date, end_date).rename(columns={'sales_amt': 'trading_sales', 'disc_amt': 'trading_disc'})
df_sales_discount_home_product = sales_discount(zid_GI, start_date, end_date).rename(columns={'sales_amt': 'home_sales', 'disc_amt': 'home_disc'})

df_sale_disc_all = pd.merge(df_sales_discount_trading, df_sales_discount_home_product, on='month')

# RETURN PART HMBR TRADING
df_return_trading_opcrn = sales_return_opcrn(zid_hmbr, start_date, end_date)
df_return_trading_imtemptrn = sales_return_imtemptrn(zid_hmbr, start_date, end_date)

df_total_return_hmbr = pd.merge(df_return_trading_opcrn[['month']], df_return_trading_imtemptrn[['month']], on='month')
df_total_return_hmbr['total_return_trading'] = df_return_trading_opcrn['total'] + df_return_trading_imtemptrn['total']

# RETURN PART ALL ITEMS HOME PRODUCT
df_return_GI_opcrn = sales_return_opcrn(zid_GI, start_date, end_date, False)
df_return_GI_imtemptrn = sales_return_imtemptrn(zid_GI, start_date, end_date, False)

df_total_return_GI = pd.merge(df_return_GI_opcrn[['month']], df_return_GI_imtemptrn[['month']], on='month')
df_total_return_GI['total_return_home'] = df_return_GI_opcrn['total'] + df_return_GI_imtemptrn['total']

df_sale_disc_return_hmbr = pd.merge(df_sale_disc_all, df_total_return_hmbr, on='month', how='left')
df_sale_disc_return_all = pd.merge(df_sale_disc_return_hmbr, df_total_return_GI, on='month', how='left')

df_sale_disc_return_all['net_home_product_sale'] = df_sale_disc_return_all['home_sales'] - (df_sale_disc_return_all['total_return_home'] + df_sale_disc_return_all['home_disc'])
df_sale_disc_return_all['net_trading_product_sale'] = df_sale_disc_return_all['trading_sales'] - (df_sale_disc_return_all['total_return_trading'] + df_sale_disc_return_all['trading_disc'])

# Create a Pandas Excel writer
writer = pd.ExcelWriter('cash_analysis_GI_63.xlsx', engine='openpyxl')
df_sale_disc_return_all.to_excel(writer, sheet_name='cash_analysis_GI', index=False)
writer.save()

# Add specific column for HTML
df_sale_for_email_body = df_sale_disc_return_all[['month', 'net_trading_product_sale', 'trading_disc', 'net_home_product_sale', 'home_disc']]

# Email part
date_string = end_date
dt = datetime.strptime(date_string, '%Y-%m-%d')
month_name = dt.strftime('%B-%Y')

subject = f"H_63_SALES & PURCHASE CASH ANALYSIS PLANNING {month_name}"
body_text = "Please find the attachment.\n"
excel_files = ['cash_analysis_GI_63.xlsx']
mail_to = ['XXXXXX', ]
html_df_list = [(df_sale_for_email_body, 'SALES PART')]

send_mail(subject, body_text, excel_files, mail_to, html_df_list)

if send_mail:
    print(f"Mail sent to {mail_to} successfully")