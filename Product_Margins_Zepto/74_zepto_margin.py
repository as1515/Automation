# %%
import pandas as pd
from datetime import datetime, timedelta
from dcon import HMBR_LOCAL_SERVER
from mail import send_mail

# Get date before 30 days
now = datetime.now()
thirty_days_before = (now - timedelta(30)).strftime("%Y-%m-%d")

# get sales without discount.
def get_sales(zid, thirty_days_before):
    df = pd.read_sql(f"""SELECT
    opodt.xitem, caitem.xdesc,    
    SUM(opodt.xqtyord) as total_qty_sold,    
    SUM(opodt.xlineamt) as net_sales_amt,
    (SUM(opodt.xlineamt) / SUM(opodt.xqtyord)) as avg_sale_price

FROM
    opord
    INNER JOIN opodt ON opord.xordernum = opodt.xordernum
    INNER JOIN caitem ON opodt.xitem = caitem.xitem
WHERE
    opord.zid = {zid}
    AND opodt.zid = {zid}
    AND caitem.zid = {zid}
    AND  opord.xdate >= '{thirty_days_before}'
    AND opodt.xitem like '%%FZ%%'
 
GROUP BY
    opodt.xitem, caitem.xdesc
HAVING
    SUM(opodt.xlineamt) > 0
ORDER BY
    opodt.xitem""", con=HMBR_LOCAL_SERVER)
    return df


# %%
# get mo quantity and mo cost
def get_mo_details(zid, thirty_days_before):
    df = pd.read_sql(f"""SELECT  moord.xitem,  
                        SUM((moodt.xqty*moodt.xrate)/moord.xqtyprd) as mo_cost, moord.xqtyprd as mo_qty
                        FROM moord
                        JOIN moodt ON moord.xmoord = moodt.xmoord
                        WHERE moord.zid = '{zid}'
                        AND moodt.zid = '{zid}'
                        AND moord.xdatemo >= '{thirty_days_before}'
                        GROUP BY moord.xitem, moord.xmoord, moord.xqtyprd
                        ORDER BY moord.xitem ASC""", con=HMBR_LOCAL_SERVER)
    df['mo_cost'] = df['mo_cost'].round(2)
    return df


# %%
df_net_sales = get_sales(100005, thirty_days_before)
df_net_sales.head(5)

# %%

df_MO = get_mo_details(100005, thirty_days_before)
df_mo_cost = df_MO.groupby(['xitem']).agg({'mo_cost': 'mean', 'mo_qty': 'sum'}).reset_index()
df_mo_cost.head(5)

# %%
df_margin = pd.merge(df_net_sales, df_mo_cost, on='xitem', how='left').fillna(0)
# first finished dataframe which exclude the trading items and which items has no mo from last 30 days
df_margin_with_mo_cost = df_margin[df_margin['mo_cost'] != 0]


# %%

df_margin_with_mo_cost

# %%
df_margin_without_mo_cost = df_margin[df_margin['mo_cost'] == 0]
df_margin_without_mo_cost = df_margin_without_mo_cost.drop(columns=['mo_cost', 'mo_cost' , 'mo_qty'])
df_margin_without_mo_cost

# %%
get_last_mo_cost_of_items = tuple (df_margin_without_mo_cost['xitem'].to_list())
get_last_mo_cost_of_items

# %%
# Now get the Items which has no production on last 30 days
def get_mo_details_for_last_mo_cost(zid, items: tuple):
    df = pd.read_sql(f"""SELECT moord.xdatemo, moord.xitem,  
                        SUM((moodt.xqty*moodt.xrate)/moord.xqtyprd) as mo_cost, moord.xqtyprd as mo_qty
                        FROM moord
                        JOIN moodt ON moord.xmoord = moodt.xmoord
                        WHERE moord.zid = '{zid}'
                        AND moodt.zid = '{zid}'
                        AND moord.xitem in {items}
                        GROUP BY moord.xitem, moord.xmoord, moord.xqtyprd, moord.xdatemo

                        ORDER BY moord.xitem ASC, moord.xdatemo DESC""", con=HMBR_LOCAL_SERVER)
                        
    df['mo_cost'] = df['mo_cost'].round(2)
    return df

# %%
df_last_mo = get_mo_details_for_last_mo_cost (100005, get_last_mo_cost_of_items )


# %%
df_latest_mo = df_last_mo.drop_duplicates(subset='xitem', keep='first').reset_index(drop=True).drop(columns = ['xdatemo'])
df_latest_mo

# %%
df_margin_cost_items_last_mo = pd.merge(df_margin_without_mo_cost, df_latest_mo, on = 'xitem', how= 'left')


# filter out the trading item
# second finished dataframe which exclude the trading items
df_margin_cost_items_exclude_trading_items = df_margin_cost_items_last_mo.dropna()
df_margin_cost_items_exclude_trading_items

# %%

df_trading_item = df_margin_cost_items_last_mo[pd.isna(df_margin_cost_items_last_mo['mo_cost'])].drop(columns = ['mo_cost', 'mo_qty']).reset_index(drop = True)
df_trading_item

# %%
trading_items_from_GI = ("FZ000023", "FZ000024", "FZ000179")


# %%

# Now get the trading item's cost from Purchase


def get_mo_cost_of_trading_items_from_purchase_cost(zid, items: tuple):
    df = pd.read_sql(
        f"""SELECT
    xitem,
    SUM(xval) / SUM(xqty) AS mo_cost,
    SUM(xqty) AS mo_qty
FROM
    imtrn
WHERE
    xitem IN {items}
    AND xdate >= '{thirty_days_before}'
    AND zid = {zid}
    AND xdocnum LIKE '%%GRN%%'
GROUP BY
    xitem;
""",
        con=HMBR_LOCAL_SERVER,
    )

    df["mo_cost"] = df["mo_cost"].round(2)
    return df


# %%
df_trading_item_cost = get_mo_cost_of_trading_items_from_purchase_cost(100005,trading_items_from_GI )
df_trading_item_cost

# %%

# third finished dataframe which get the trading items cost
df_trading_item_cost = pd.merge(df_trading_item, df_trading_item_cost, on='xitem', how='left')
df_trading_item_cost

# %%
# now concat three dataframe

df_all_margin_cost = pd.concat([df_margin_with_mo_cost, df_margin_cost_items_exclude_trading_items , df_trading_item_cost] )
df_all_margin_cost.sort_values(by=['net_sales_amt'], ascending=False).reset_index(drop=True)


def calculate_margins(df):
    df['total_cogs'] = df['total_qty_sold'] * df['mo_cost']
    df['gross_margin'] = df['net_sales_amt'] - df['total_cogs']
    return df


def add_total_row(df):
    sums = df.select_dtypes(include='number').sum().to_frame().T
    sums['xitem'] = 'Total'
    sums['xdesc'] = 'Total'
    df = pd.concat([df, sums], ignore_index=True)
    return df


# Apply the calculate_margins function
df_margin_with_mo_cost = calculate_margins(df_margin_with_mo_cost)
df_margin_cost_items_exclude_trading_items = calculate_margins(df_margin_cost_items_exclude_trading_items)
df_trading_item_cost = calculate_margins(df_trading_item_cost)
df_all_margin_cost = calculate_margins(df_all_margin_cost)

# Add total rows
df_margin_with_mo_cost = add_total_row(df_margin_with_mo_cost)
df_margin_cost_items_exclude_trading_items = add_total_row(df_margin_cost_items_exclude_trading_items)
df_trading_item_cost = add_total_row(df_trading_item_cost)
df_all_margin_cost = add_total_row(df_all_margin_cost)

# Round the DataFrames
df_margin_with_mo_cost = df_margin_with_mo_cost.round(2).reset_index(drop=True)
df_margin_cost_items_exclude_trading_items = df_margin_cost_items_exclude_trading_items.round(2).reset_index(drop=True)
df_trading_item_cost = df_trading_item_cost.round(2).reset_index(drop=True)
df_all_margin_cost = df_all_margin_cost.round(2).reset_index(drop=True)

df_all_margin_cost['avg_costing_%'] = df_all_margin_cost['mo_cost'] * 100 / df_all_margin_cost['avg_sale_price']
df_all_margin_cost['avg_costing_%'] = df_all_margin_cost['avg_costing_%'].round(2)
df_all_margin_cost = df_all_margin_cost.sort_values(by= 'avg_costing_%', ascending=False)


# %%
df_all_margin_cost.to_excel("zepto_margin_cost.xlsx", engine='openpyxl', index=False)


# %%
# df_margin_with_mo_cost, df_margin_cost_items_exclude_trading_items , df_trading_item_cost


# Email Part
subject = "H_74. Zepto Margin Cost"
body_text = "Please find the attachment.\n"
excel_files = ["zepto_margin_cost.xlsx"]  # optional if any
mail_to = ["XXXXXX"]
html_df_list = [
    (df_margin_with_mo_cost, "Items margin cost which has production last 30 days"),
    (df_margin_cost_items_exclude_trading_items, "Items margin cost which has no production last 30 days"),
    (df_trading_item_cost, "Items margin cost of trading item"),

]  # optional if any
# Send mail
send_mail(subject, body_text, excel_files, mail_to, html_df_list)

