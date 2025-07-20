# Em transactions.py
import psycopg2
from psycopg2 import sql
import random


def execute_customer_position(conn, cust_id, tax_id, get_history):

    with conn.cursor() as cur:

        
        if cust_id == 0:
            cur.execute("SELECT c_id FROM CUSTOMER WHERE c_tax_id = %s", (tax_id,))
            cust_id_result = cur.fetchone()
            if not cust_id_result:

                return False 
            cust_id = cust_id_result[0]
            
        cur.execute("""
            SELECT c_st_id, c_l_name, c_f_name, c_m_name, c_gndr, c_tier, c_dob,
                   c_ad_id, c_ctry_1, c_area_1, c_local_1, c_ext_1, c_ctry_2,
                   c_area_2, c_local_2, c_ext_2, c_ctry_3, c_area_3, c_local_3,
                   c_ext_3, c_email_1, c_email_2
            FROM CUSTOMER
            WHERE c_id = %s
            """, (cust_id,))
        customer_details = cur.fetchone() 


        query_frame1_assets = sql.SQL("""
            SELECT
                ca_id,
                ca_bal,
                COALESCE(SUM(hs_qty * lt_price), 0.0) AS assets_total
            FROM
                CUSTOMER_ACCOUNT
            LEFT JOIN
                HOLDING_SUMMARY ON hs_ca_id = ca_id
            LEFT JOIN
                LAST_TRADE ON lt_s_symb = hs_s_symb
            WHERE
                ca_c_id = %s
            GROUP BY
                ca_id, ca_bal
            ORDER BY
                assets_total ASC
            LIMIT 10
        """)
        cur.execute(query_frame1_assets, (cust_id,))
        accounts = cur.fetchall()

        
        if get_history and accounts:

            
            selected_account_id = random.choice(accounts)[0]
            
            query_frame2_history = sql.SQL("""
                SELECT
                    T1.t_id, T1.t_s_symb, T1.t_qty, ST.st_name, TH.th_dts
                FROM
                    TRADE_HISTORY AS TH
                JOIN
                    STATUS_TYPE AS ST ON ST.st_id = TH.th_st_id
                JOIN
                    TRADE AS T1 ON TH.th_t_id = T1.t_id
                JOIN
                    (SELECT t_id FROM TRADE WHERE t_ca_id = %s ORDER BY t_dts DESC LIMIT 10) AS T_RECENT
                ON T1.t_id = T_RECENT.t_id
                ORDER BY
                    TH.th_dts DESC
                LIMIT 30
            """)
            cur.execute(query_frame2_history, (selected_account_id,))
            history = cur.fetchall() 
    return True