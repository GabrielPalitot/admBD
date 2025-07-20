# Em transactions.py
import psycopg2
from psycopg2 import sql
from datetime import datetime


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