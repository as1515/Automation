import sys
sys.path.append('E:/')  # Add the path to the '_CONFIG' folder
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
import xlrd
from dateutil.relativedelta import relativedelta

from _CONFIG.main_config import (
    HMBR_ID, CHEMICAL_ID, PLASTIC_ID, ZEPTO_ID, PAINTROLLER_ID, SCRUBBER_ID, PACKAGING_ID , KARIGOR_ID, THREADTAPE_ID, GROCERY_ID,
    PROJ_TRADING, PROJ_KARIGOR, PROJ_CHEMICAL, PROJ_PLASTIC, PROJ_ZEPTO, PROJ_GROCERY, PROJ_PAINT_ROLLER, PROJ_SCRUBBER, PROJ_THREAD_TAPE, PROJ_PACKAGING)

from _CONFIG.mail import send_mail
from rcvpay_query import get_acc_payable, get_acc_receivable

this_datetime = datetime.now()
number_day = this_datetime.day

month_list_6 = [(this_datetime - relativedelta(months=i)).strftime('%Y/%m') for i in range(6)]
month_list_24 = [(this_datetime - relativedelta(months=i)).strftime('%Y/%m') for i in range(24)]

end_year = int(month_list_24[0].split('/')[0])
end_month = int(month_list_24[0].split('/')[1])

last_year =  int(month_list_24[1].split('/')[0])
last_month = int(month_list_24[1].split('/')[1])

df_acc_h = get_acc_receivable(HMBR_ID,PROJ_TRADING,end_year,end_month).rename(columns={'xsub':'xcus'})
df_acc_h_l = get_acc_receivable(HMBR_ID,PROJ_TRADING,last_year,last_month).rename(columns={'xsub':'xcus'})
df_acc_h = df_acc_h.merge(df_acc_h_l[['xcus','ar']],on=['xcus'],how='left').rename(columns={'xcus':'Code',
                                                                                            'xshort':'Name',
                                                                                            'xadd2':'Address',
                                                                                            'xcity':'City',
                                                                                            'xstate':'Market',
                                                                                            'ar_x':month_list_24[0],
                                                                                            'ar_y':month_list_24[1]
                                                                                           })
df_hmbr_summary = df_acc_h.groupby(['City'])[month_list_24[0],month_list_24[1]].sum().reset_index().round(2)

df_acc_z = get_acc_receivable(ZEPTO_ID,PROJ_ZEPTO,end_year,end_month).rename(columns={'xsub':'xcus'})
df_acc_z_l = get_acc_receivable(ZEPTO_ID,PROJ_ZEPTO,last_year,last_month).rename(columns={'xsub':'xcus'})
df_acc_z = df_acc_z.merge(df_acc_z_l[['xcus','ar']],on=['xcus'],how='left').rename(columns={'xcus':'Code',
                                                                                            'xshort':'Name',
                                                                                            'xadd2':'Address',
                                                                                            'xcity':'City',
                                                                                            'xstate':'Market',
                                                                                            'ar_x':month_list_24[0],
                                                                                            'ar_y':month_list_24[1]
                                                                                           })
df_zepto_summary = df_acc_z.groupby(['City'])[month_list_24[0],month_list_24[1]].sum().reset_index().round(2)

df_acc_k = get_acc_receivable(KARIGOR_ID,PROJ_KARIGOR,end_year,end_month).rename(columns={'xsub':'xcus'})
df_acc_k_l = get_acc_receivable(KARIGOR_ID,PROJ_KARIGOR,last_year,last_month).rename(columns={'xsub':'xcus'})
df_acc_k = df_acc_k.merge(df_acc_k_l[['xcus','ar']],on=['xcus'],how='left').rename(columns={'xcus':'Code',
                                                                                            'xshort':'Name',
                                                                                            'xadd2':'Address',
                                                                                            'xcity':'City',
                                                                                            'xstate':'Market',
                                                                                            'ar_x':month_list_24[0],
                                                                                            'ar_y':month_list_24[1]
                                                                                           })
df_karigor_summary = df_acc_k.groupby(['City'])[month_list_24[0],month_list_24[1]].sum().reset_index().round(2)


#separate emails for accounts receivable and accounts payable 
#make accounts receivable excel file (hmbr,karigor,zepto)
#this email will go to me, motiur, shahalam

#df_hmbr_summary (HTML)
#df_zepto_summary (HTML)
#df_karigor_summary (HTML)
#make one xlsx file and put these in as sheets
#df_acc_h
#df_acc_z
#df_acc_k


writer = pd.ExcelWriter('accountsReceivable.xlsx', engine= 'openpyxl')

df_acc_h.to_excel(writer, sheet_name="hmbrReceivable", index= False)
df_acc_z.to_excel(writer, sheet_name="zeptoReceivable", index= False)
df_acc_k.to_excel(writer, sheet_name="karigorReceivable", index= False)

writer.save()
# call the email function

subject = "Accounts Receivable Details"
body_text = "Please find the attachment.\n"
excel_files = ['accountsReceivable.xlsx'] #optional if any
mail_to = ["XXXXXX"]
mail_to = [ "XXXXXX" ]
html_df_list = [ ( df_hmbr_summary, 'HMBR Accts Summary' ), ( df_zepto_summary, 'Zepto Accts Summary' ), (df_karigor_summary, 'df_zepto_summary') ] #optional if any

send_mail(subject, body_text, excel_files, mail_to,html_df_list )




#Accounts payable email will go to me, admin, motiur

df_kp = get_acc_payable ( KARIGOR_ID, PROJ_KARIGOR, end_year, end_month )
df_tp = get_acc_payable ( HMBR_ID, PROJ_TRADING, end_year, end_month )
df_cp = get_acc_payable ( CHEMICAL_ID, PROJ_CHEMICAL, end_year, end_month )
df_ttp = get_acc_payable ( THREADTAPE_ID, PROJ_THREAD_TAPE, end_year, end_month )
df_zp = get_acc_payable ( ZEPTO_ID, PROJ_ZEPTO, end_year, end_month )
df_gp = get_acc_payable ( GROCERY_ID, PROJ_GROCERY, end_year, end_month )
df_prp = get_acc_payable ( PAINTROLLER_ID, PROJ_PAINT_ROLLER, end_year, end_month )
df_sp = get_acc_payable ( SCRUBBER_ID, PROJ_SCRUBBER, end_year, end_month )
df_pkp = get_acc_payable ( PACKAGING_ID, PROJ_PACKAGING, end_year, end_month )
df_pls = get_acc_payable ( PLASTIC_ID, PROJ_PLASTIC, end_year, end_month )

#df_kp
#df_tp
#df_cp
#df_ttp
#df_zp
#df_gp
#df_prp
#df_sp
#df_pkp

#all html account payable separte email to me and 


writer2 = pd.ExcelWriter('accountsPayable.xlsx')

df_kp.to_excel(writer2,"KarigorPayable")
df_tp.to_excel(writer2,"HmbrPayable")
df_cp.to_excel(writer2,"ChemicalPayable")
df_ttp.to_excel(writer2,"ThreadTapePayable")
df_zp.to_excel(writer2,"ZeptoPayable")
df_gp.to_excel(writer2,"GroceryPayable")
df_prp.to_excel(writer2,"PaintRollerPayable")
df_sp.to_excel(writer2,"SteelScrubberPayable")
df_pkp.to_excel(writer2,"PackagingPayable")
df_pls.to_excel(writer2,"PlasticPayable")

writer2.save()
writer2.close()

#add html file for all payable dataframe

HEADER = '''
<html>
    <head>

    </head>
    <body>
'''

FOOTER = '''
    </body>
</html>
'''
with open("index2.html",'w') as f:
    f.write(HEADER)
    f.write("<h2 style='color:red;'>Karigor Payable</h2>")
    f.write(df_kp.to_html(classes='df_kp'))
    f.write("<h2 style='color:red;'>HMBR Payable</h2>")
    f.write(df_tp.to_html(classes='df_tp'))
    f.write("<h2 style='color:red;'>Chemical Payable</h2>")
    f.write(df_cp.to_html(classes='df_cp'))
    f.write("<h2 style='color:red;'>Thread Tape Payable</h2>")
    f.write(df_ttp.to_html(classes='df_ttp'))
    f.write("<h2 style='color:red;'>Zepto Payable</h2>")
    f.write(df_zp.to_html(classes='df_zp'))
    f.write("<h2 style='color:red;'>Grocery Payable</h2>")
    f.write(df_gp.to_html(classes='df_gp'))
    f.write("<h2 style='color:red;'>Paint Roller Payable</h2>")
    f.write(df_prp.to_html(classes='df_prp'))
    f.write("<h2 style='color:red;'>Steel Scrubber Payable</h2>")
    f.write(df_sp.to_html(classes='df_sp'))
    f.write("<h2 style='color:red;'>Packaging Payable</h2>")
    f.write(df_pkp.to_html(classes='df_pkp'))
    f.write("<h2 style='color:red;'>Plastic Payable</h2>")
    f.write(df_pls.to_html(classes='df_pls'))

    f.write(FOOTER)


me = "XXXXXX@gmail.com"
you = ["XXXXXX"]


msg = MIMEMultipart('alternative')
msg['Subject'] = "Accounts Payable Details"
msg['From'] = me
msg['To'] = ", ".join(you)

filename = "index2.html"
f = open(filename)
attachment = MIMEText(f.read(),'html')
msg.attach(attachment)




part1 = MIMEBase('application', "octet-stream")
part1.set_payload(open("accountsPayable.xlsx", "rb").read())
encoders.encode_base64(part1)
part1.add_header('Content-Disposition', 'attachment; filename="accountsPayable.xlsx"')
msg.attach(part1)




username = 'XXXXXX'
password = 'XXXXXX'

s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(me,you,msg.as_string())
s.quit()

