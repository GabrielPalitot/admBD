import psycopg2
from psycopg2 import sql
from datetime import datetime
DB_SETTINGS = {
    "dbname": "tpce",
    "user": "postgres",
    "password": "sua_senha_aqui",
    "host": "localhost",
    "port": "5461"
}

def trade_cleanup(start_trade_id=0, st_canceled_id='CNCL', st_submitted_id='SBMT'):
    conn = None
    updated_pending = 0
    updated_submitted = 0
    try:
        conn = psycopg2.connect(**DB_SETTINGS)
        cur = conn.cursor()
        
        cur.execute("SELECT tr_t_id FROM TRADE_REQUEST ORDER BY tr_t_id")
        pending_trades = [row[0] for row in cur.fetchall()]
        
        if not pending_trades:
        else:
            
            for trade_id in pending_trades:
                now_dts = datetime.now()

                cur.execute(
                    sql.SQL("INSERT INTO TRADE_HISTORY (th_t_id, th_dts, th_st_id) VALUES (%s, %s, %s)"),
                    (trade_id, now_dts, st_submitted_id)
                )

                cur.execute(
                    sql.SQL("UPDATE TRADE SET t_st_id = %s, t_dts = %s WHERE t_id = %s"),
                    (st_canceled_id, now_dts, trade_id)
                )

                cur.execute(
                    sql.SQL("INSERT INTO TRADE_HISTORY (th_t_id, th_dts, th_st_id) VALUES (%s, %s, %s)"),
                    (trade_id, now_dts, st_canceled_id)
                )
                updated_pending += 1

            cur.execute("DELETE FROM TRADE_REQUEST")

        
        cur.execute(
            sql.SQL("SELECT t_id FROM TRADE WHERE t_id >= %s AND t_st_id = %s"),
            (start_trade_id, st_submitted_id)
        )
        submitted_trades = [row[0] for row in cur.fetchall()]

        if not submitted_trades:
            print("Nenhuma ordem submetida correspondente encontrada.")
        else:

            for trade_id in submitted_trades:
                now_dts = datetime.now()
                cur.execute(
                    sql.SQL("UPDATE TRADE SET t_st_id = %s, t_dts = %s WHERE t_id = %s"),
                    (st_canceled_id, now_dts, trade_id)
                )
                cur.execute(
                    sql.SQL("INSERT INTO TRADE_HISTORY (th_t_id, th_dts, th_st_id) VALUES (%s, %s, %s)"),
                    (trade_id, now_dts, st_canceled_id)
                )
                updated_submitted += 1
        conn.commit()

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()