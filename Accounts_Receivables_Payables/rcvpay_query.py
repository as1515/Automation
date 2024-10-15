# Standard library modules
from datetime import datetime

# Third-party modules
import pandas as pd

# Local or project-specific modules
from _CONFIG.main_config import HMBR_LOCAL_SERVER


def get_acc_receivable(zid, proj, year, month):
    year_month = str(year) + str(month)
    query = f"""SELECT gldetail.xsub,cacus.xorg,cacus.xadd2,cacus.xcity,cacus.xstate,SUM(gldetail.xprime) as AR
                        FROM glheader
                        JOIN gldetail
                        ON glheader.xvoucher = gldetail.xvoucher
                        JOIN cacus
                        ON gldetail.xsub = cacus.xcus
                        WHERE glheader.zid = {zid}
                        AND gldetail.zid = {zid}
                        AND cacus.zid = {zid}
                        AND gldetail.xproj = '{proj}'
                        AND gldetail.xvoucher NOT LIKE 'OB-%%'
                        AND CONCAT(glheader.xyear,glheader.xper) <= '{year_month}'
                        GROUP BY gldetail.xsub,cacus.xorg,cacus.xadd2,cacus.xcity,cacus.xstate """
                        
    df = pd.read_sql(query,con= HMBR_LOCAL_SERVER)
    return df


def get_acc_payable(zid, proj, year, month):
    year_month = str(year) + str(month)
    query = f"""SELECT gldetail.xsub,casup.xorg,SUM(gldetail.xprime) as AP
                        FROM glheader
                        JOIN gldetail
                        ON glheader.xvoucher = gldetail.xvoucher
                        JOIN casup
                        ON gldetail.xsub = casup.xsup
                        WHERE glheader.zid = {zid}
                        AND gldetail.zid = {zid}
                        AND casup.zid = {zid}
                        AND gldetail.xproj = '{proj}'
                        AND gldetail.xvoucher NOT LIKE 'OB-%%'
                        AND CONCAT(glheader.xyear,glheader.xper) <= '{year_month}'
                        GROUP BY gldetail.xsub,casup.xorg"""

    df = pd.read_sql(query,con= HMBR_LOCAL_SERVER)
    return df
