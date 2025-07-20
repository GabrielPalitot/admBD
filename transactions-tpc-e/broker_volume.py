import psycopg2
from psycopg2 import sql

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