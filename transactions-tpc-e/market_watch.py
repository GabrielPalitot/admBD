import psycopg2
from psycopg2 import sql


def execute_market_watch(conn, cust_id, industry_name, acct_id, start_date):

    with conn.cursor() as cur:
        stock_list = []
        
        
        if cust_id != 0:
            cur.execute("""
                SELECT WI.wi_s_symb
                FROM WATCH_ITEM WI
                JOIN WATCH_LIST WL ON WI.wi_wl_id = WL.wl_id
                WHERE WL.wl_c_id = %s
                """, (cust_id,))
            stock_list = [row[0] for row in cur.fetchall()]

        elif industry_name != "":
            cur.execute("""
                SELECT S.s_symb
                FROM INDUSTRY I
                JOIN COMPANY C ON I.in_id = C.co_in_id
                JOIN SECURITY S ON C.co_id = S.s_co_id
                WHERE I.in_name = %s
                """, (industry_name,))
            stock_list = [row[0] for row in cur.fetchall()]
        
        elif acct_id != 0:
            cur.execute("SELECT hs_s_symb FROM HOLDING_SUMMARY WHERE hs_ca_id = %s", (acct_id,))
            stock_list = [row[0] for row in cur.fetchall()]

        if not stock_list:
            return True

        old_mkt_cap = 0.0
        new_mkt_cap = 0.0

        for symbol in stock_list:
            cur.execute("SELECT lt_price FROM LAST_TRADE WHERE lt_s_symb = %s", (symbol,))
            new_price_res = cur.fetchone()
            
            cur.execute("SELECT s_num_out FROM SECURITY WHERE s_symb = %s", (symbol,))
            s_num_out_res = cur.fetchone()
            
            cur.execute(
                "SELECT dm_close FROM DAILY_MARKET WHERE dm_s_symb = %s AND dm_date = %s",
                (symbol, start_date)
            )
            old_price_res = cur.fetchone()
            
            if new_price_res and s_num_out_res and old_price_res:
                new_price = new_price_res[0]
                s_num_out = s_num_out_res[0]
                old_price = old_price_res[0]
                
                old_mkt_cap += s_num_out * old_price
                new_mkt_cap += s_num_out * new_price

        if old_mkt_cap != 0:
            pct_change = 100 * (new_mkt_cap / old_mkt_cap - 1)
        else:
            pct_change = 0.0
        


    return True