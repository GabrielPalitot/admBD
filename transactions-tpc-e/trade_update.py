# Em transactions.py
import psycopg2
from psycopg2 import sql

# ... (outras funções, incluindo a _get_trade_details do Trade-Lookup) ...

def execute_trade_update(conn, frame_to_execute, **kwargs):
    """
    Executa a transação Trade-Update (Cláusula 3.3.10).
    Executa um dos três frames para atualizar dados de negociações.
    """
    with conn.cursor() as cur:
        max_updates = kwargs.get("max_updates", 20)
        
        if frame_to_execute == 1:
            # --- FRAME 1: Atualiza T_EXEC_NAME por lista de trade_ids ---
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
            # --- FRAME 2: Atualiza SE_CASH_TYPE por conta e data ---
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
            # --- FRAME 3: Atualiza CT_NAME por símbolo e data ---
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