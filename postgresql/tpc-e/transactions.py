import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import random
import decimal


def execute_broker_volume(conn, broker_list, sector_name):
    with conn.cursor() as cur:
        broker_list_tuple = tuple(broker_list)
        
        query = sql.SQL("""
            SELECT
                B.b_name,
                SUM(TR.tr_qty * TR.tr_bid_price) AS volume
            FROM
                BROKER B
            JOIN
                TRADE_REQUEST TR ON B.b_id = TR.tr_b_id
            JOIN
                SECURITY S ON TR.tr_s_symb = S.s_symb
            JOIN
                COMPANY C ON S.s_co_id = C.co_id
            JOIN
                INDUSTRY I ON C.co_in_id = I.in_id
            JOIN
                SECTOR SC ON I.in_sc_id = SC.sc_id
            WHERE
                B.b_name IN %s AND
                SC.sc_name = %s
            GROUP BY
                B.b_name
            ORDER BY
                volume DESC
        """)
        
        cur.execute(query, (broker_list_tuple, sector_name))
        results = cur.fetchall()
    return results


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

def execute_market_feed(conn, ticker_tape, status_submitted, type_stop_loss, type_limit_sell, type_limit_buy):

    with conn.cursor() as cur:
        now_dts = datetime.now()
        for update in ticker_tape:
            symbol = update["symbol"]
            price_quote = update["price_quote"]
            trade_qty = update["trade_qty"]
            
            cur.execute("""
                UPDATE LAST_TRADE
                SET lt_price = %s,
                    lt_vol = lt_vol + %s,
                    lt_dts = %s
                WHERE lt_s_symb = %s
                """, (price_quote, trade_qty, now_dts, symbol))

            query_find_triggers = sql.SQL("""
                SELECT tr_t_id, tr_bid_price, tr_tt_id, tr_qty
                FROM TRADE_REQUEST
                WHERE tr_s_symb = %s AND (
                    (tr_tt_id = %s AND tr_bid_price >= %s) OR
                    (tr_tt_id = %s AND tr_bid_price <= %s) OR
                    (tr_tt_id = %s AND tr_bid_price >= %s)
                )
            """)
            cur.execute(query_find_triggers, (
                symbol,
                type_stop_loss, price_quote,
                type_limit_sell, price_quote,
                type_limit_buy, price_quote
            ))
            
            triggered_trades = cur.fetchall()
            
            if triggered_trades:
                for req_trade_id, req_price_quote, req_trade_type, req_trade_qty in triggered_trades:
                    cur.execute(
                        "UPDATE TRADE SET t_dts = %s, t_st_id = %s WHERE t_id = %s",
                        (now_dts, status_submitted, req_trade_id)
                    )
                    
                    cur.execute(
                        "INSERT INTO TRADE_HISTORY (th_t_id, th_dts, th_st_id) VALUES (%s, %s, %s)",
                        (req_trade_id, now_dts, status_submitted)
                    )

                triggered_ids = [t[0] for t in triggered_trades]
                cur.execute(
                    "DELETE FROM TRADE_REQUEST WHERE tr_t_id IN %s",
                    (tuple(triggered_ids),)
                )

            conn.commit()


            if triggered_trades:
                print(f"  [Market-Feed] {len(triggered_trades)} ordens para o símbolo {symbol} foram desencadeadas e enviadas para o mercado.")

    return True




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



def execute_security_detail(conn, symbol, access_lob_flag, max_rows_to_return, start_date):

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                S.s_name, S.s_co_id, C.co_name, C.co_sp_rate, C.co_ceo, C.co_desc,
                C.co_open_date, C.co_st_id, CA.ad_line1, CA.ad_line2, ZCA.zc_town,
                ZCA.zc_div, CA.ad_zc_code, CA.ad_ctry, S.s_num_out, S.s_start_date,
                S.s_exch_date, S.s_pe, S.s_52wk_high, S.s_52wk_high_date,
                S.s_52wk_low, S.s_52wk_low_date, S.s_dividend, S.s_yield,
                ZEA.zc_div, EA.ad_ctry, EA.ad_line1, EA.ad_line2, ZEA.zc_town,
                EA.ad_zc_code, E.ex_close, E.ex_desc, E.ex_name, E.ex_num_symb, E.ex_open
            FROM
                SECURITY S, COMPANY C, ADDRESS CA, ADDRESS EA,
                ZIP_CODE ZCA, ZIP_CODE ZEA, EXCHANGE E
            WHERE
                S.s_symb = %s AND C.co_id = S.s_co_id AND CA.ad_id = C.co_ad_id
                AND EA.ad_id = E.ex_ad_id AND E.ex_id = S.s_ex_id
                AND CA.ad_zc_code = ZCA.zc_code AND EA.ad_zc_code = ZEA.zc_code
            """, (symbol,))
        
        main_details = cur.fetchone()
        if not main_details:
            return False 
        
        co_id = main_details[1] 

        cur.execute("""
            SELECT C.co_name, I.in_name
            FROM COMPANY_COMPETITOR CC, COMPANY C, INDUSTRY I
            WHERE CC.cp_co_id = %s AND C.co_id = CC.cp_comp_co_id AND I.in_id = CC.cp_in_id
            LIMIT 3
            """, (co_id,))
        cur.fetchall() 

        cur.execute("""
            SELECT fi_year, fi_qtr, fi_qtr_start_date, fi_revenue, fi_net_earn,
                   fi_basic_eps, fi_dilut_eps, fi_margin, fi_inventory, fi_assets,
                   fi_liability, fi_out_basic, fi_out_dilut
            FROM FINANCIAL
            WHERE fi_co_id = %s
            ORDER BY fi_year ASC, fi_qtr ASC
            LIMIT 20
            """, (co_id,))
        cur.fetchall() 

        cur.execute("""
            SELECT dm_date, dm_close, dm_high, dm_low, dm_vol
            FROM DAILY_MARKET
            WHERE dm_s_symb = %s AND dm_date >= %s
            ORDER BY dm_date ASC
            LIMIT %s
            """, (symbol, start_date, max_rows_to_return))
        cur.fetchall()

        cur.execute("SELECT lt_price, lt_open_price, lt_vol FROM LAST_TRADE WHERE lt_s_symb = %s", (symbol,))
        cur.fetchone()

        if access_lob_flag:
            cur.execute("""
                SELECT NI.ni_item, NI.ni_dts, NI.ni_source, NI.ni_author
                FROM NEWS_XREF NX, NEWS_ITEM NI
                WHERE NI.ni_id = NX.nx_ni_id AND NX.nx_co_id = %s
                LIMIT 2
                """, (co_id,))
        else:
            cur.execute("""
                SELECT NI.ni_dts, NI.ni_source, NI.ni_author, NI.ni_headline, NI.ni_summary
                FROM NEWS_XREF NX, NEWS_ITEM NI
                WHERE NI.ni_id = NX.nx_ni_id AND NX.nx_co_id = %s
                LIMIT 2
                """, (co_id,))
        cur.fetchall() 

    return True



def _get_trade_details(cur, trade_id_list):
    for trade_id in trade_id_list:
        cur.execute("SELECT se_amt FROM SETTLEMENT WHERE se_t_id = %s", (trade_id,))
        settlement_info = cur.fetchone()

        cur.execute("SELECT is_cash FROM TRADE WHERE t_id = %s", (trade_id,))
        is_cash = cur.fetchone()
        if is_cash and is_cash[0]:
            cur.execute("SELECT ct_amt FROM CASH_TRANSACTION WHERE ct_t_id = %s", (trade_id,))
            cur.fetchone()
        
        cur.execute("SELECT th_dts FROM TRADE_HISTORY WHERE th_t_id = %s ORDER BY th_dts LIMIT 3", (trade_id,))
        cur.fetchall()

def execute_trade_lookup(conn, frame_to_execute, **kwargs):

    with conn.cursor() as cur:
        if frame_to_execute == 1:
            trade_id_list = kwargs.get("trade_id_list", [])
            for trade_id in trade_id_list:
                cur.execute("""
                    SELECT T.t_bid_price, T.t_exec_name, T.t_is_cash, TT.tt_is_mrkt, T.t_trade_price
                    FROM TRADE T, TRADE_TYPE TT
                    WHERE T.t_id = %s AND T.t_tt_id = TT.tt_id
                    """, (trade_id,))
                cur.fetchone()
            _get_trade_details(cur, trade_id_list)

        elif frame_to_execute == 2:
            acct_id = kwargs.get("acct_id")
            start_dts = kwargs.get("start_trade_dts")
            end_dts = kwargs.get("end_trade_dts")
            max_trades = kwargs.get("max_trades")
            
            cur.execute("""
                SELECT t_id FROM TRADE
                WHERE t_ca_id = %s AND t_dts >= %s AND t_dts <= %s
                ORDER BY t_dts ASC LIMIT %s
                """, (acct_id, start_dts, end_dts, max_trades))
            trade_id_list = [row[0] for row in cur.fetchall()]
            _get_trade_details(cur, trade_id_list)

        elif frame_to_execute == 3:
            symbol = kwargs.get("symbol")
            start_dts = kwargs.get("start_trade_dts")
            end_dts = kwargs.get("end_trade_dts")
            max_trades = kwargs.get("max_trades")

            cur.execute("""
                SELECT t_id FROM TRADE
                WHERE t_s_symb = %s AND t_dts >= %s AND t_dts <= %s
                ORDER BY t_dts ASC LIMIT %s
                """, (symbol, start_dts, end_dts, max_trades))
            trade_id_list = [row[0] for row in cur.fetchall()]
            _get_trade_details(cur, trade_id_list)
            
        elif frame_to_execute == 4:
            acct_id = kwargs.get("acct_id")
            start_dts = kwargs.get("start_trade_dts")

            cur.execute("""
                SELECT t_id FROM TRADE
                WHERE t_ca_id = %s AND t_dts >= %s
                ORDER BY t_dts ASC LIMIT 1
                """, (acct_id, start_dts))
            
            trade_id_res = cur.fetchone()
            if trade_id_res:
                trade_id = trade_id_res[0]
                cur.execute("""
                    SELECT hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty
                    FROM HOLDING_HISTORY
                    WHERE hh_h_t_id IN (
                        SELECT hh_h_t_id
                        FROM HOLDING_HISTORY
                        WHERE hh_t_id = %s
                    )
                    LIMIT 20
                    """, (trade_id,))
                cur.fetchall()
    return True


class RollbackException(Exception):
    pass


def execute_trade_order(conn, acct_id, exec_f_name, exec_l_name, exec_tax_id, 
                        symbol, co_name, issue, trade_type_id, st_pending_id, 
                        st_submitted_id, trade_qty, is_lifo, type_is_margin, roll_it_back):

    with conn.cursor() as cur:

        cur.execute("""
            SELECT ca_b_id, ca_c_id, ca_tax_st
            FROM CUSTOMER_ACCOUNT WHERE ca_id = %s
            """, (acct_id,))
        broker_id, cust_id, tax_status = cur.fetchone()

        cur.execute("""
            SELECT c_f_name, c_l_name, c_tier, c_tax_id
            FROM CUSTOMER WHERE c_id = %s
            """, (cust_id,))
        cust_f_name, cust_l_name, cust_tier, tax_id = cur.fetchone()


        if exec_l_name != cust_l_name or exec_f_name != cust_f_name or exec_tax_id != tax_id:
            cur.execute("""
                SELECT ap_acl FROM ACCOUNT_PERMISSION
                WHERE ap_ca_id = %s AND ap_f_name = %s AND ap_l_name = %s AND ap_tax_id = %s
                """, (acct_id, exec_f_name, exec_l_name, exec_tax_id))
            permission = cur.fetchone()
            if not permission:

                raise ValueError("Executor não tem permissão para esta conta.")
        

        if symbol == "":
            cur.execute("SELECT co_id FROM COMPANY WHERE co_name = %s", (co_name,))
            co_id = cur.fetchone()[0]
            cur.execute("SELECT s_ex_id, s_symb FROM SECURITY WHERE s_co_id = %s AND s_issue = %s", (co_id, issue))
            exch_id, symbol = cur.fetchone()
        else:
            cur.execute("SELECT s_co_id, s_ex_id FROM SECURITY WHERE s_symb = %s", (symbol,))
            co_id, exch_id = cur.fetchone()

        cur.execute("SELECT lt_price FROM LAST_TRADE WHERE lt_s_symb = %s", (symbol,))
        market_price = cur.fetchone()[0]
        
        cur.execute("SELECT tt_is_mrkt, tt_is_sell FROM TRADE_TYPE WHERE tt_id = %s", (trade_type_id,))
        type_is_market, type_is_sell = cur.fetchone()

        requested_price = market_price if type_is_market else random.uniform(float(market_price) * 0.95, float(market_price) * 1.05)
        


        cur.execute("SELECT cr_rate FROM COMMISSION_RATE WHERE cr_c_tier = %s AND cr_tt_id = %s AND cr_ex_id = %s AND cr_from_qty <= %s AND cr_to_qty >= %s",
                    (cust_tier, trade_type_id, exch_id, trade_qty, trade_qty))
        comm_rate_res = cur.fetchone()
        comm_rate = comm_rate_res[0] if comm_rate_res else 0.0

        cur.execute("SELECT ch_chrg FROM CHARGE WHERE ch_c_tier = %s AND ch_tt_id = %s", (cust_tier, trade_type_id))
        charge_amount = cur.fetchone()[0]
        
        status_id = st_submitted_id if type_is_market else st_pending_id

        now_dts = datetime.now()
        comm_amount = (float(comm_rate) / 100) * trade_qty * float(requested_price)
        is_cash = not type_is_margin
        exec_name = f"{exec_f_name} {exec_l_name}"

        cur.execute("""
            INSERT INTO TRADE (t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name, t_chrg, t_comm, t_tax, t_lifo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING t_id
            """, (now_dts, status_id, trade_type_id, is_cash, symbol, trade_qty, requested_price, acct_id, exec_name, charge_amount, comm_amount, 0, is_lifo))
        trade_id = cur.fetchone()[0]

        if not type_is_market:
            cur.execute("""
                INSERT INTO TRADE_REQUEST (tr_t_id, tr_tt_id, tr_s_symb, tr_qty, tr_bid_price, tr_b_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (trade_id, trade_type_id, symbol, trade_qty, requested_price, broker_id))

        cur.execute("INSERT INTO TRADE_HISTORY (th_t_id, th_dts, th_st_id) VALUES (%s, %s, %s)",
                    (trade_id, now_dts, status_id))

        if roll_it_back:
            raise RollbackException("Rollback intencional da transação.")

        
    return True



def execute_trade_result(conn, trade_id, trade_price):

    with conn.cursor() as cur:

        cur.execute("""
            SELECT t_ca_id, t_tt_id, t_s_symb, t_qty, t_chrg, t_lifo, t_is_cash
            FROM TRADE WHERE t_id = %s
            """, (trade_id,))
        trade_info = cur.fetchone()
        if not trade_info: return False 
        acct_id, type_id, symbol, trade_qty, charge, is_lifo, trade_is_cash = trade_info

        cur.execute("SELECT tt_name, tt_is_sell FROM TRADE_TYPE WHERE tt_id = %s", (type_id,))
        type_name, type_is_sell = cur.fetchone()

        cur.execute("SELECT hs_qty FROM HOLDING_SUMMARY WHERE hs_ca_id = %s AND hs_s_symb = %s", (acct_id, symbol))
        hs_qty_res = cur.fetchone()
        hs_qty = hs_qty_res[0] if hs_qty_res else 0

        now_dts = datetime.now()
        buy_value = decimal.Decimal('0.0')
        sell_value = decimal.Decimal('0.0')

        cur.execute("SELECT ca_b_id, ca_c_id, ca_tax_st FROM CUSTOMER_ACCOUNT WHERE ca_id = %s", (acct_id,))
        broker_id, cust_id, tax_status = cur.fetchone()


        if type_is_sell:
            if hs_qty > 0: 
                order = "DESC" if is_lifo else "ASC"
                cur.execute(f"SELECT h_t_id, h_qty, h_price FROM HOLDING WHERE h_ca_id = %s AND h_s_symb = %s ORDER BY h_dts {order}", (acct_id, symbol))

            cur.execute("UPDATE HOLDING_SUMMARY SET hs_qty = hs_qty - %s WHERE hs_ca_id = %s AND hs_s_symb = %s", (trade_qty, acct_id, symbol))
        else: 
            if hs_qty < 0: 
                order = "DESC" if is_lifo else "ASC"
                cur.execute(f"SELECT h_t_id, h_qty, h_price FROM HOLDING WHERE h_ca_id = %s AND h_s_symb = %s ORDER BY h_dts {order}", (acct_id, symbol))

            cur.execute("UPDATE HOLDING_SUMMARY SET hs_qty = hs_qty + %s WHERE hs_ca_id = %s AND hs_s_symb = %s", (trade_qty, acct_id, symbol))
            cur.execute("INSERT INTO HOLDING (h_t_id, h_ca_id, h_s_symb, h_dts, h_price, h_qty) VALUES (%s, %s, %s, %s, %s, %s)",
                        (trade_id, acct_id, symbol, now_dts, trade_price, trade_qty))
        

        tax_amount = decimal.Decimal('0.0')
        if sell_value > buy_value and tax_status in (1, 2):
            cur.execute("SELECT sum(tx_rate) FROM TAXRATE WHERE tx_id IN (SELECT cx_tx_id FROM CUSTOMER_TAXRATE WHERE cx_c_id = %s)", (cust_id,))
            tax_rates = cur.fetchone()[0]
            if tax_rates:
                tax_amount = (sell_value - buy_value) * tax_rates
                cur.execute("UPDATE TRADE SET t_tax = %s WHERE t_id = %s", (tax_amount, trade_id))


        cur.execute("SELECT s_ex_id, s_name FROM SECURITY WHERE s_symb = %s", (symbol,))
        s_ex_id, s_name = cur.fetchone()
        cur.execute("SELECT c_tier FROM CUSTOMER WHERE c_id = %s", (cust_id,))
        c_tier = cur.fetchone()[0]
        cur.execute("SELECT cr_rate FROM COMMISSION_RATE WHERE cr_c_tier = %s AND cr_tt_id = %s AND cr_ex_id = %s AND cr_from_qty <= %s AND cr_to_qty >= %s",
                    (c_tier, type_id, s_ex_id, trade_qty, trade_qty))
        comm_rate_res = cur.fetchone()
        comm_rate = comm_rate_res[0] if comm_rate_res else decimal.Decimal('0.0')


        comm_amount = (comm_rate / 100) * trade_qty * trade_price
        st_completed_id = 'CMPT'
        
        cur.execute("UPDATE TRADE SET t_comm = %s, t_dts = %s, t_st_id = %s, t_trade_price = %s WHERE t_id = %s",
                    (comm_amount, now_dts, st_completed_id, trade_price, trade_id))
        
        cur.execute("INSERT INTO TRADE_HISTORY (th_t_id, th_dts, th_st_id) VALUES (%s, %s, %s)",
                    (trade_id, now_dts, st_completed_id))
        
        cur.execute("UPDATE BROKER SET b_comm_total = b_comm_total + %s, b_num_trades = b_num_trades + 1 WHERE b_id = %s",
                    (comm_amount, broker_id))


        due_date = now_dts.date() + timedelta(days=2)
        se_amount = (decimal.Decimal(trade_qty) * trade_price) - charge - comm_amount if type_is_sell else -((decimal.Decimal(trade_qty) * trade_price) + charge + comm_amount)
        if tax_status == 1:
            se_amount -= tax_amount
        
        cash_type = "Cash Account" if trade_is_cash else "Margin"

        cur.execute("INSERT INTO SETTLEMENT (se_t_id, se_cash_type, se_cash_due_date, se_amt) VALUES (%s, %s, %s, %s)",
                    (trade_id, cash_type, due_date, se_amount))

        if trade_is_cash:
            cur.execute("UPDATE CUSTOMER_ACCOUNT SET ca_bal = ca_bal + %s WHERE ca_id = %s", (se_amount, acct_id))
            ct_name = f"{type_name} {trade_qty} shares of {s_name}"
            cur.execute("INSERT INTO CASH_TRANSACTION (ct_dts, ct_t_id, ct_amt, ct_name) VALUES (%s, %s, %s, %s)",
                        (now_dts, trade_id, se_amount, ct_name))

    return True



def execute_trade_status(conn, acct_id):

    with conn.cursor() as cur:

        cur.execute("""
            SELECT
                T.t_id, T.t_dts, ST.st_name, TT.tt_name, T.t_s_symb, T.t_qty,
                T.t_exec_name, T.t_chrg, S.s_name, E.ex_name
            FROM
                TRADE AS T,
                STATUS_TYPE AS ST,
                TRADE_TYPE AS TT,
                SECURITY AS S,
                EXCHANGE AS E
            WHERE
                T.t_ca_id = %s AND
                ST.st_id = T.t_st_id AND
                TT.tt_id = T.t_tt_id AND
                S.s_symb = T.t_s_symb AND
                E.ex_id = S.s_ex_id
            ORDER BY
                T.t_dts DESC
            LIMIT 50
            """, (acct_id,))
        
        trade_history = cur.fetchall()

        cur.execute("""
            SELECT C.c_l_name, C.c_f_name, B.b_name
            FROM
                CUSTOMER_ACCOUNT AS CA,
                CUSTOMER AS C,
                BROKER AS B
            WHERE
                CA.ca_id = %s AND
                C.c_id = CA.ca_c_id AND
                B.b_id = CA.ca_b_id
            """, (acct_id,))
            
        cur.fetchone()

    return True



def execute_trade_update(conn, frame_to_execute, **kwargs):

    with conn.cursor() as cur:
        max_updates = kwargs.get("max_updates", 20)
        
        if frame_to_execute == 1:

            trade_id_list = kwargs.get("trade_id_list", [])
            num_updated = 0
            for trade_id in trade_id_list:
                if num_updated < max_updates:
                    cur.execute("SELECT t_exec_name FROM TRADE WHERE t_id = %s", (trade_id,))
                    exec_name_res = cur.fetchone()
                    if exec_name_res:
                        exec_name = exec_name_res[0]
                        new_exec_name = exec_name.replace(" X ", " ") if " X " in exec_name else exec_name.replace(" ", " X ")
                        
                        cur.execute("UPDATE TRADE SET t_exec_name = %s WHERE t_id = %s", (new_exec_name, trade_id))
                        num_updated += cur.rowcount
            _get_trade_details(cur, trade_id_list)

        elif frame_to_execute == 2:
            acct_id = kwargs.get("acct_id")
            start_dts = kwargs.get("start_trade_dts")
            end_dts = kwargs.get("end_trade_dts")
            max_trades = kwargs.get("max_trades")
            
            cur.execute("""
                SELECT t_id, t_is_cash FROM TRADE
                WHERE t_ca_id = %s AND t_dts >= %s AND t_dts <= %s
                ORDER BY t_dts ASC LIMIT %s
                """, (acct_id, start_dts, end_dts, max_trades))
            trades_to_update = cur.fetchall()
            
            num_updated = 0
            for trade_id, is_cash in trades_to_update:
                if num_updated < max_updates:
                    cur.execute("SELECT se_cash_type FROM SETTLEMENT WHERE se_t_id = %s", (trade_id,))
                    cash_type_res = cur.fetchone()
                    if cash_type_res:
                        cash_type = cash_type_res[0]
                        if is_cash:
                            new_cash_type = "Cash" if cash_type == "Cash Account" else "Cash Account"
                        else:
                            new_cash_type = "Margin" if cash_type == "Margin Account" else "Margin Account"
                        
                        cur.execute("UPDATE SETTLEMENT SET se_cash_type = %s WHERE se_t_id = %s", (new_cash_type, trade_id))
                        num_updated += cur.rowcount
            
            trade_id_list = [t[0] for t in trades_to_update]
            _get_trade_details(cur, trade_id_list)

        elif frame_to_execute == 3:
            symbol = kwargs.get("symbol")
            start_dts = kwargs.get("start_trade_dts")
            end_dts = kwargs.get("end_trade_dts")
            max_trades = kwargs.get("max_trades")
            
            cur.execute("""
                SELECT t.t_id, t.t_is_cash, t.t_qty, tt.tt_name, s.s_name
                FROM TRADE t, TRADE_TYPE tt, SECURITY s
                WHERE t.t_s_symb = %s AND t.t_dts >= %s AND t.t_dts <= %s
                AND t.t_tt_id = tt.tt_id AND t.t_s_symb = s.s_symb
                ORDER BY t.t_dts ASC LIMIT %s
                """, (symbol, start_dts, end_dts, max_trades))
            trades_to_update = cur.fetchall()

            num_updated = 0
            for trade_id, is_cash, qty, type_name, s_name in trades_to_update:
                if is_cash and num_updated < max_updates:
                    cur.execute("SELECT ct_name FROM CASH_TRANSACTION WHERE ct_t_id = %s", (trade_id,))
                    ct_name_res = cur.fetchone()
                    if ct_name_res:
                        ct_name = ct_name_res[0]
                        new_ct_name = f"{type_name} {qty} Shares of {s_name}" if " shares of " in ct_name else f"{type_name} {qty} shares of {s_name}"
                        
                        cur.execute("UPDATE CASH_TRANSACTION SET ct_name = %s WHERE ct_t_id = %s", (new_ct_name, trade_id))
                        num_updated += cur.rowcount

            trade_id_list = [t[0] for t in trades_to_update]
            _get_trade_details(cur, trade_id_list)
            
    return True