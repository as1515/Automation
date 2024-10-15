import pandas as pd
from config import HMBR_LOCAL_SERVER


SERVER = HMBR_LOCAL_SERVER

#------------------------- SALES-DISCOUNT-RETURN-QUERY-PART--------------------------#
home_product_list = ('0787','2079','1780','0790','1571','1779',
                     '11723','16550','1280','1483','1484','1481',
                     '1482','1691','17872','0191','1654','2078',
                     '0788','17870','01951','01953','2077','2073',
                     '0792','17873','17874','11721','2074','0192',
                     '01955','0190','01954','07860','17800','2072',
                     '1655','16540','0793','0786','17220','01952',
                     '1479','1278','01956','1281','1005','1569',
                     '1279','1479','1567','1478')
# get all sales and discount for home_product and trading item
def sales_discount(zid, start_date, end_date, home_product=None):
    query = f"""select TO_CHAR(opdor.xdate, 'YYYY-MM') as month, 
                        sum(opddt.xdtwotax) as sales_amt, sum(opddt.xdtdisc) as disc_amt
                        from opdor
                        inner join opddt 
                        on opdor.xdornum = opddt.xdornum
                        join caitem
                        on opddt.xitem=caitem.xitem
                        where opdor.zid={zid}
                        and opddt.zid = {zid}
                        and caitem.zid={zid}
                        and opdor.xdate between '{start_date}' and '{end_date}'
                        and opdor.xstatusdor = '3-Invoiced'"""

    if home_product:
        query += f" and caitem.xitem in {home_product_list}"

    if zid == 100000:
        query += f"and caitem.xwh = 'Finished Goods Store'"
    query += " group by month order by month"

    df = pd.read_sql(query, con=SERVER)
    return df

# get return amount of home_product and trading item from opcrn (SR--) table
def sales_return_opcrn(zid, start_date, end_date, home_product=None):
    query = f"""select TO_CHAR(opcrn.xdate, 'YYYY-MM') as month,  sum (opcdt.xlineamt) as total  from opcrn
                join opcdt
                on opcrn.xcrnnum= opcdt.xcrnnum
                join caitem
                on opcdt.xitem=caitem.xitem
                where opcrn.zid={zid}
                and opcdt.zid = {zid}
                and caitem.zid ={zid}
                and opcrn.xdate between '{start_date}' and '{end_date}'
            """
    
    if zid == 100000:
        query += f"and caitem.xwh = 'Finished Goods Store'"

    if home_product:
        query += f" and caitem.xitem in {home_product_list}"
    query += " group by month order by month"

    df = pd.read_sql(query, con=SERVER)
    return df

# get return amount of home_product and trading item from imtemptrn (RECA, RECT, DSR, etc voucher) table
def sales_return_imtemptrn(zid, start_date, end_date, home_product=None):

    query = f"""select TO_CHAR(imtemptrn.xdate, 'YYYY-MM') as month,  SUM (imtemptdt.xlineamt) as total  FROM imtemptrn
                    JOIN imtemptdt
                    ON imtemptrn.ximtmptrn= imtemptdt.ximtmptrn
                    JOIN caitem
                    ON imtemptdt.xitem=caitem.xitem
                    WHERE imtemptrn.zid={zid}
                    AND imtemptdt.zid = {zid}
                    AND caitem.zid ={zid}
                    AND (imtemptrn.ximtmptrn LIKE '%%RECA%%' OR
                        imtemptrn.ximtmptrn LIKE '%%SRE-%%' OR
                        imtemptrn.ximtmptrn LIKE '%%RECT-%%' OR
                        imtemptrn.ximtmptrn LIKE '%%DSR-%%')
                    AND imtemptrn.xdate BETWEEN '{start_date}' AND '{end_date}'
            """
    if zid == 100000:
        query += f"and caitem.xwh = 'Finished Goods Store'"
        
    if home_product:
        query += f" and caitem.xitem in {home_product_list}"
    query += " GROUP BY month ORDER BY month"

    df = pd.read_sql(query, con=SERVER)
    return df

#---------------------- CASH-ANALYSIS-QUERY-PART-----------------------------#
def get_sales_rate(zid,start_date,end_date):
    query = f"""SELECT opddt.xitem, AVG(opddt.xdtwotax/opddt.xqty), SUM(opddt.xqty) as total_qty
                        FROM opddt
                        JOIN opdor
                        ON opddt.xdornum = opdor.xdornum
                        WHERE opdor.zid = {zid}
                        AND opddt.zid = {zid}
                        AND opdor.xdate >= '{start_date}'
                        AND opdor.xdate <= '{end_date}'
                        GROUP BY opddt.xitem"""
    df = pd.read_sql ( query, con = SERVER )
    return df

def get_stock_all(zid,end_date):
    query = f"""SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as Stock
                        FROM imtrn
                        WHERE imtrn.zid = {zid}
                        AND imtrn.xdate <= '{end_date}'
                        GROUP BY imtrn.xitem"""

    df = pd.read_sql ( query, con = SERVER )
    return df


def get_caitem(zid):
    query = f"""SELECT xitem, xdesc, xgitem, xcitem, xpricecat, xduty, xwh, xstdcost, xstdprice
                        FROM caitem
                        WHERE zid = {zid}
                        AND xgitem = 'Hardware'
                        OR xgitem = 'Furniture Fittings'
                        OR xgitem = 'Industrial & Household'
                        OR xgitem = 'Sanitary'
                        ORDER BY xgitem ASC"""

    df = pd.read_sql ( query, con = SERVER )
    return df


def get_special_price(zid):
    query = f"""SELECT xpricecat, xqty,xdisc
                        FROM opspprc
                        WHERE zid = {zid}"""

    df = pd.read_sql(query,con = SERVER)
    return df


def price(zid,start_date):
    query = f"""SELECT opddt.xitem, EXTRACT(MONTH FROM opdor.xdate), SUM(opddt.xdtwotax)/SUM(opddt.xqty) as avg_sales_rate, AVG(opddt.xdiscf) as avg_discount, SUM(opddt.xqty) as sale_qty
                        FROM opddt
                        JOIN opdor
                        ON opddt.xdornum = opdor.xdornum
                        WHERE opdor.zid = {zid}
                        AND opddt.zid = {zid}
                        AND opddt.xitem IN {home_product_list}
                        AND opdor.xdate >= '{start_date}'
                        GROUP BY opddt.xitem, EXTRACT(MONTH FROM opdor.xdate)"""
    
    df = pd.read_sql ( query, con = SERVER )
    return df

def purchase_qty(zid,start_date):
    query = f"""SELECT imtrn.xitem, imtrn.xyear, imtrn.xper ,SUM(imtrn.xqty) as purchase_qty
                        FROM imtrn
                        WHERE imtrn.zid = {zid}
                        AND imtrn.xdocnum LIKE 'GRN-%%'
                        AND imtrn.xitem IN {home_product_list}
                        AND imtrn.xdate >= '{start_date}'
                        GROUP BY imtrn.xitem, imtrn.xyear, imtrn.xper"""
    df = pd.read_sql ( query, con = SERVER )
    return df

def get_stock_home(zid,end_date):
    query = f"""SELECT xitem, SUM(xqty*xsign) as Stock
                        FROM imtrn
                        WHERE zid = {zid}
                        AND xdate <= '{end_date}'
                        AND xitem IN {home_product_list}
                        GROUP BY xitem"""

    df = pd.read_sql(query,con = SERVER)
    return df