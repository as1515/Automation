# %%
import requests
import pandas as pd
import time
import numpy as np
from mail import send_mail

# balance	last_pay_date	last_rec_amt

url = "https://XXXXXX/sendsms"

file_path = 'district_customer_balance.xlsx'


df = pd.read_excel(file_path, )
df['xtaxnum'] = df['xtaxnum'].astype(str)
df['last_pay_date'] = df['last_pay_date'].astype(str)
df['xtaxnum'] = df['xtaxnum'].replace('\.0$', '', regex=True)


# for statictics sms to director
total_collection = df['last_rec_amt'].sum()
total_collection = f"{total_collection:,.2f}"
total_customer = df['xcus'].count()
payment_date = df['last_pay_date'].tolist()[0]
total_collection

# %% [markdown]
# 

# %%
# filter out which customer has no mobile number and which have negative balance
df = df.query(" xtaxnum != 'nan'  and balance >=0 ")
df

# %%
sms_dict = df.to_dict('records')

status = []
for sms_body in sms_dict:
   trunked_id = sms_body['xcus'].split('-')[1].lstrip('0')
   trunked_name = sms_body['xshort'].split(' ')[0].strip()
   # print (trunked_name , trunked_id)
   if (sms_body['last_pay_date'] != '') and (sms_body['balance'] != ''):
      msg = f"""Dear Customer, ID-{trunked_id},\nLast Deposit Date : {sms_body['last_pay_date']}\nDeposit : {sms_body['last_rec_amt']} Tk\nCurrent Due {sms_body['balance']} Tk"""
   else:
      msg = f"""Dear {sms_body['xcus']}, ID-{trunked_id}, Your current Due is {sms_body['balance']} Tk"""

   payload = {'api_key': 'XXXXXX',
    'msg': msg,
    'to': sms_body['xtaxnum'],
    'sender_id' : 'HMBR'
    }
   print (payload)

   response = requests.request("POST", url, data=payload)
   print (response.status_code)
   print (response.text)
   status.append(response.text)
   x = response.text
   print ("x is", x[0])
   print (f"""message length is {len(payload['msg'])} """)
   print ("Sleep time 7 second")
   time.sleep(7)
# print (sms_body)
# print (sms_body)
print ("all message successfully sent")

# %%
# summary
total_message_sent = len(status)
# total_message_sent = 70


msg = f"""Customer payment message successfully sent today. Collection Date: {payment_date}. Total paying customer {total_customer}. Total collection {total_collection} Tk"""

# %%

subject = "Customer's collection and due Message report"
body_text = msg
mail_to = ['XXXXXX']
excel = []
send_mail(subject, body_text, excel, mail_to )

