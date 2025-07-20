# Em transactions.py
import psycopg2
from psycopg2 import sql

# ... (outras funções) ...

def _get_trade_details(cur, trade_id_list):
    """Função auxiliar para buscar detalhes de uma lista de negociações."""
    for trade_id in trade_id_list:
        # Get settlement information
        cur.execute("SELECT se_amt FROM SETTLEMENT WHERE se_t_id = %s", (trade_id,))
        settlement_info = cur.fetchone()

        # Get cash/history info (aqui simplificado, apenas executando a query)
        cur.execute("SELECT is_cash FROM TRADE WHERE t_id = %s", (trade_id,))
        is_cash = cur.fetchone()
        if is_cash and is_cash[0]:
            cur.execute("SELECT ct_amt FROM CASH_TRANSACTION WHERE ct_t_id = %s", (trade_id,))
            cur.fetchone()
        
        cur.execute("SELECT th_dts FROM TRADE_HISTORY WHERE th_t_id = %s ORDER BY th_dts LIMIT 3", (trade_id,))
        cur.fetchall()

def execute_trade_lookup(conn, frame_to_execute, **kwargs):
    """
    Executa a transação Trade-Lookup (Cláusula 3.3.6).
    Executa um dos quatro frames com base no input `frame_to_execute`.
    """
    with conn.cursor() as cur:
        if frame_to_execute == 1:
            # --- FRAME 1: Pesquisa por uma lista de trade_ids ---
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
            # --- FRAME 2: Pesquisa por conta e intervalo de datas ---
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
            # --- FRAME 3: Pesquisa por símbolo e intervalo de datas ---
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
            # --- FRAME 4: Pesquisa pelo histórico de posses (holding history) ---
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