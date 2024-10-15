# %%
# %%

import requests
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import psycopg2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import openpyxl
import xlrd
from dateutil.relativedelta import relativedelta
from _config import (
    HMBR_ID,
    HMBR_LOCAL_SERVER_ENGINE,
    IT_MAIL,
    DIRECTOR_MAIL,
    MOTIUR_SIR_MAIL,
    SHAHALAM_MAIL,
)
from mail import send_mail
import sys

sys.path.append(r"E:\_Config")  # Assuming 'holiday.py' is in this directory
from holiday import holiday
import time


PROJ_TRADING = "GULSHAN TRADING"

# check if it is holiday, if so then exit the program
# check if today is friday or holiday. if holiday or friday then exit the program
def is_today_holiday(holiday_list):
    today_date = datetime.now().date().strftime("%Y-%m-%d")
    return today_date in holiday_list


def is_today_friday():
    return datetime.now().weekday() == 4  # Friday is 4


if is_today_holiday(holiday()):
    print("Today is a holiday. Exiting program.")
    sys.exit()
else:
    print("Ok")


# %%

ENGINE = HMBR_LOCAL_SERVER_ENGINE

"""
        we need 

        last two month's before due of customer 
        suppose if current month is June then last 2 
        months ago will be April and date will be 30 April

        last months total sales amount of customer
        last months total collection from customer
        last months total return amount of customer

        current date till now sales
        current date till now return
        current date till now collection

        current date balance.

        At first we need to find out last two months ago's date, last months date and current month's date
        and put in the seperate variable
"""
check_today_is_friday = datetime.today().strftime("%A")
print(check_today_is_friday)

if check_today_is_friday == "Friday":
    sys.exit()

#  Actual time delta will be one days before. As per direction of Bijoy Sir we reduce the period.
two_days_ago = datetime.now() - timedelta(1)
check_friday = two_days_ago.strftime("%A")

print (PROJ_TRADING)

# %%

if check_friday == "Friday":
    two_days_ago = datetime.now() - timedelta(2)

two_days_ago = two_days_ago.strftime("%Y-%m-%d")

print(two_days_ago)

# %%

### get current date
current_month_date = datetime.now().strftime("%Y-%m-%d")
print(current_month_date, end="/")

# %%


def get_balance(zid, proj, till_date):

    df = pd.read_sql(
        f"""SELECT gldetail.xsub as xcus, cacus.xshort , cacus.xstate, cacus.xtaxnum , SUM(gldetail.xprime) as balance
                        FROM cacus
                        JOIN gldetail
                        ON gldetail.xsub = cacus.xcus
                        JOIN glheader
                        ON gldetail.xvoucher = glheader.xvoucher
                        WHERE glheader.zid = {zid}
                        AND gldetail.zid = {zid}
                        AND cacus.zid = {zid}
                        AND gldetail.xproj = '{PROJ_TRADING}'
                        AND gldetail.xvoucher not like '%%OB%%'
                        AND glheader.xdate <= '{till_date}'
                        GROUP BY gldetail.xsub, cacus.xshort, cacus.xtaxnum, cacus.xstate """,

        con=ENGINE,
    )
    return df


#
pd.set_option("display.float_format", "{:.2f}".format)

# %%

# now current month e.g june month sale from day 01 to till date
df_current_balance = get_balance(HMBR_ID, PROJ_TRADING, current_month_date)
# add to previous dataframe and merge
df_current_balance.head(20)

# %%

# %%
def get_payment(zid):
    df = pd.read_sql(
        f"""SELECT glheader.xdate as last_pay_date, gldetail.xsub as xcus, gldetail.xamount as last_rec_amt from glheader
                inner join gldetail
                on glheader.xvoucher = gldetail.xvoucher
                where
                glheader.zid = {zid}
                and gldetail.zid = {zid}
                and gldetail.xsub like '%%CUS%%'
                
                and (
                glheader.xvoucher like '%%RCT-%%' or
                glheader.xvoucher  LIKE 'JV--%%' OR 
                glheader.xvoucher LIKE 'RCT-%%' OR
                glheader.xvoucher LIKE 'CRCT%%' OR
                glheader.xvoucher LIKE 'STJV%%' OR
                glheader.xvoucher LIKE 'BRCT%%'
                )
                --group by gldetail.xsub, glheader.xdate, gldetail.xamount
                order by gldetail.xsub, glheader.xdate """,
        con=ENGINE,
    )
    return df


df_payment = get_payment(100001)
df_payment.head(5)


# %%

# %%

# %%
df_payment["last_pay_date"] = pd.to_datetime(df_payment["last_pay_date"])

# Group by 'xcus' and get the max value of 'last_pay_date'
last_pay_dates = (
    df_payment.groupby("xcus")
    .agg({"last_pay_date": "max", "last_rec_amt": "last"})
    .reset_index()
)
last_pay_dates

# %%
df_balance_last_payment = pd.merge(
    df_current_balance, last_pay_dates, on="xcus", how="left"
)
# %%
# get last day payment

df_last_day_payment = df_balance_last_payment[
    df_balance_last_payment["last_pay_date"] == two_days_ago
].reset_index(drop=True)
df_last_day_payment

# %%



# %%
# District Customer Balance every 10 days


# with mobile number
df_district_customer_w_mobile = df_balance_last_payment[
    (df_balance_last_payment["xtaxnum"] != "")
    & (df_balance_last_payment["balance"] > 500)
    & (df_balance_last_payment["xstate"] == "District")
].reset_index(drop=True)

# without mobile number
df_district_customer_wo_mobile = df_balance_last_payment[
    (df_balance_last_payment["balance"] > 500)
    & (df_balance_last_payment["xstate"] == "District")
].reset_index(drop=True)
df_district_customer_w_mobile

# %%

df_district_customer_w_mobile.to_excel(
    engine="openpyxl", index=False, excel_writer="district_customer_balance.xlsx"
)



df_last_day_payment.to_excel(engine='openpyxl', index=False, excel_writer="customer_balance.xlsx")



# %%
subject = "59 Send Customer Balance SMS"
body_text = "Please find the attachment.\n"
excel_files = ["customer_balance.xlsx", "district_customer_balance.xlsx"]  # optional if any
mail_to = ["XXXXXX"]


send_mail(subject, body_text, excel_files, mail_to)


# %%



