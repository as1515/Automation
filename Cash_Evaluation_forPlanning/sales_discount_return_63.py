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
from email.header import Header
import openpyxl
from mail import send_mail
from query_function import (
    sales_discount,
    sales_return_opcrn,
    sales_return_imtemptrn,
    get_sales_rate, get_stock_home, get_caitem, get_special_price, price, purchase_qty, get_stock_all
 )

pd.options.display.float_format = '{:.2f}'.format


# %%
start_date = input("input start date----  ")
end_date = input("input end date----  ")
zid = 100001

# %%
#--------------SALES-DISCOUNT PART------------------#
#  Trading item sales and discount
df_sales_discount_trading = sales_discount(zid , start_date, end_date)
#  Home Product item sales and discount
df_sales_discount_home_product = sales_discount(zid, start_date, end_date, True)
df_sales_discount_home_product = df_sales_discount_home_product.rename(columns={'sales_amt' : 'home_sales', 'disc_amt' : 'home_disc'})
# merging trading and homeproduct according to month
df_sale_disc_all = pd.merge (df_sales_discount_trading, df_sales_discount_home_product, on= 'month')
#--------------END OF SALES-DISCOUNT PART------------------#

# %%
#--------------RETURN PART ALL ITEMS------------------#
#--Trading + Home product both return opcrn--#
df_return_trading_home_opcrn = sales_return_opcrn (zid, start_date, end_date)
#  Trading + Home product both return imtemptrn
df_return_trading_home_imtemptrn = sales_return_imtemptrn (zid, start_date, end_date)

df_total_return = pd.merge(df_return_trading_home_opcrn[['month']], df_return_trading_home_imtemptrn[['month']], on='month')
# Calculate the sum of the "total" columns from both DataFrames and assign it to the "total" column in df_total_return
df_total_return['total_return'] = df_return_trading_home_opcrn['total'] + df_return_trading_home_imtemptrn['total']

# %%
#--------------RETURN PART HOME PRODUCT------------------#
#--Home product return opcrn--#
df_return_home_opcrn = sales_return_opcrn (zid, start_date, end_date, True)

#--Home product return imtemptrn--#
df_return_home_imtemptrn = sales_return_imtemptrn (zid, start_date, end_date, True)
df_total_return_home = pd.merge(df_return_home_opcrn[['month']], df_return_home_imtemptrn[['month']], on='month')
df_total_return_home['total_return_home'] = df_return_home_opcrn['total'] + df_return_home_imtemptrn['total']

# now merge all return with home product 
df_total_return_all = pd.merge (df_total_return, df_total_return_home, on= 'month')

# %%
#--------------MERGE RETURN DF TO SALES DISCOUNT DF USING MONTH COLUMN TO GET SALES, DISCOUNT, RETURN------------------#
df_sale_disc_return_all = pd.merge(df_sale_disc_all, df_total_return_all, on= 'month' )

# %%
#--------------CALCULATION------------------#
# net_home_product_sale = home_sales - (total_return_home + home_disc)

# trading_product_sale = sales_amt - home_sales

# trading_disc =  disc_amt - home_disc
# trading_return =  total_return - total_return_home

# net_trading_sale = trading_prod_sale - (trading_disc + trading_return)


# %%
df_sale_disc_return_all['net_home_product_sale'] = df_sale_disc_return_all['home_sales']-\
                                                (df_sale_disc_return_all['total_return_home'] + df_sale_disc_return_all['home_disc']) 

# %%
df_sale_disc_return_all['trading_product_sales'] = df_sale_disc_return_all['sales_amt']- df_sale_disc_return_all['home_sales'] 

# %%
# trading_disc =  disc_amt - home_disc
df_sale_disc_return_all['trading_disc'] = df_sale_disc_return_all['disc_amt']- df_sale_disc_return_all['home_disc']

# %%
# trading_return =  total_return - total_return_home
df_sale_disc_return_all['trading_return'] = df_sale_disc_return_all['total_return']- df_sale_disc_return_all['total_return_home']

# %%
# net_trading_sale = trading_product_sale - (trading_disc + trading_return)
df_sale_disc_return_all['net_trading_sale'] = df_sale_disc_return_all['trading_product_sales']-\
                                                (df_sale_disc_return_all['trading_disc'] + df_sale_disc_return_all['trading_return']) 


# %%
df_sale_disc_return_all['net_all_product_sales'] = df_sale_disc_return_all['net_home_product_sale'] + df_sale_disc_return_all['net_trading_sale']

# %%
df_sale_disc_return_all.to_excel('net_sales_analysis_63.xlsx' , engine='openpyxl')

# %%
#---------------------------------------------------#
#             PURCHASE PART                         #
#---------------------------------------------------#

df_rh = get_sales_rate(zid,start_date,end_date)

# %%
df_sh = get_stock_home(zid,end_date)


# %%
df_sp = get_special_price(zid)
df_caitem = get_caitem(zid)
df_caitem = pd.merge(df_caitem,df_rh,on=['xitem'],how='left')
df_caitem = pd.merge(df_caitem,df_sh,on=['xitem'],how='left')
df_caitem = pd.merge(df_caitem,df_sp,on=['xpricecat'],how='left')


df_p = purchase_qty(zid,start_date)
df_r = price(zid,start_date).rename(columns={'date_part':'xper'})
df_f = pd.merge(df_r,df_p,on=['xitem','xper'],how='left')

df_month = get_stock_all(zid,end_date)
df_month['xper'] =  int (end_date.split('-')[1])
df_month = pd.merge(df_month,df_f,on=['xitem','xper'],how='left')

# %%
# Assuming you have a dataframe named df_f with two columns named 'avg'

# # Get the column names as a list
# column_names = list(df_f.columns)

# # Find the indices of the columns with the name 'avg'
# avg_indices = [i for i, col in enumerate(column_names) if col == 'avg']

# # Rename the columns with the desired names
# column_names[avg_indices[0]] = 'avg_sales_rate'


# Update the column names in the dataframe
# df_f.columns = column_names

# %%
# multiply avg_sale_rate * purchase qty 
df_f['home_product_purchase'] = df_f.apply(lambda x : x['avg_sales_rate'] * x['purchase_qty'] ,  axis=1)


# %%
grouped_df = df_f.groupby(['xyear','xper'])['home_product_purchase'].sum().reset_index()

# %%
grouped_df[['xper','xyear']] = grouped_df[['xper','xyear']].astype(int)


# %%
grouped_df['year_month'] = grouped_df['xyear'].astype(str) + '-' + grouped_df['xper'].astype(str).str.zfill(2)
grouped_df.drop(['xyear', 'xper'], axis=1, inplace=True)
cols = grouped_df.columns.tolist()
cols = ['year_month'] + cols[:-1]
grouped_df = grouped_df[cols]

# %%
# Create a Pandas Excel writer
writer = pd.ExcelWriter('purchase_stock_63.xlsx', engine='openpyxl')

# Write df_f to a sheet named 'df_f'
df_f.to_excel(writer, sheet_name='df_f', index=False) 

# Write df_month to a sheet named 'df_month'
df_month.to_excel(writer, sheet_name='df_month', index=False)
grouped_df.to_excel(writer, sheet_name='group_by_purchase')
# Save the Excel file
writer.save()

#---------------------------------------------------#
#             END OF PURCHASE PART                  #
#---------------------------------------------------#

# %%
#---------------------------------------------------#
#     ADD SPECIFIC  COLUMN FOR HTML                 #
#---------------------------------------------------#

#sales part
df_sale_for_email_body = df_sale_disc_return_all [ ['month','net_trading_sale' , 'trading_disc' , 'net_home_product_sale', 'home_disc']]


#---------------------------------------------------#
#     EMAIL PART                                    #
#---------------------------------------------------#


# %%
subject = "H_63_SALES & PURCHASE CASH ANALYSIS PLANNING"
body_text = "Please find the attachment.\n"
excel_files = ['net_sales_analysis_63.xlsx', 'purchase_stock_63.xlsx'] #optional if any
mail_to = ['XXXXXX']
html_df_list = [(df_sale_for_email_body, 'SALES PART '), (grouped_df, 'PURCHASE PART')] #optional if any

send_mail(subject, body_text, excel_files, mail_to,html_df_list )

# %

if send_mail:
    print (f"mail send to {mail_to} successfully")