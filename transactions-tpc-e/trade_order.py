# Em transactions.py
import psycopg2
from psycopg2 import sql
from datetime import datetime

class RollbackException(Exception):
    """Exceção personalizada para sinalizar um rollback intencional."""
    pass

# ... (outras funções de transação) ...

def execute_trade_order(conn, acct_id, exec_f_name, exec_l_name, exec_tax_id, 
                        symbol, co_name, issue, trade_type_id, st_pending_id, 
                        st_submitted_id, trade_qty, is_lifo, type_is_margin, roll_it_back):

    with conn.cursor() as cur:
        # ======================================================================
        # --- FRAME 1: Obter dados da conta, cliente e corretor ---
        # ======================================================================
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
                # Se não houver permissão, a transação deve falhar.
                # Lançar uma exceção fará o worker dar rollback.
                raise ValueError("Executor não tem permissão para esta conta.")
        
        # ======================================================================
        # --- FRAME 3: Calcular impacto financeiro (lógica principal) ---
        # ======================================================================
        
        # Obter detalhes da ação
        if symbol == "":
            cur.execute("SELECT co_id FROM COMPANY WHERE co_name = %s", (co_name,))
            co_id = cur.fetchone()[0]
            cur.execute("SELECT s_ex_id, s_symb FROM SECURITY WHERE s_co_id = %s AND s_issue = %s", (co_id, issue))
            exch_id, symbol = cur.fetchone()
        else:
            cur.execute("SELECT s_co_id, s_ex_id FROM SECURITY WHERE s_symb = %s", (symbol,))
            co_id, exch_id = cur.fetchone()

        # Obter preço de mercado
        cur.execute("SELECT lt_price FROM LAST_TRADE WHERE lt_s_symb = %s", (symbol,))
        market_price = cur.fetchone()[0]
        
        # Obter características da negociação (comprar/vender, mercado/limite)
        cur.execute("SELECT tt_is_mrkt, tt_is_sell FROM TRADE_TYPE WHERE tt_id = %s", (trade_type_id,))
        type_is_market, type_is_sell = cur.fetchone()

        requested_price = market_price if type_is_market else random.uniform(float(market_price) * 0.95, float(market_price) * 1.05)
        
        # ... Lógica complexa de cálculo de buy_value, sell_value, impostos, etc. ...
        # (Esta parte é uma simplificação para manter o código legível,
        # mas a estrutura das queries abaixo está correta)

        cur.execute("SELECT cr_rate FROM COMMISSION_RATE WHERE cr_c_tier = %s AND cr_tt_id = %s AND cr_ex_id = %s AND cr_from_qty <= %s AND cr_to_qty >= %s",
                    (cust_tier, trade_type_id, exch_id, trade_qty, trade_qty))
        comm_rate_res = cur.fetchone()
        comm_rate = comm_rate_res[0] if comm_rate_res else 0.0

        cur.execute("SELECT ch_chrg FROM CHARGE WHERE ch_c_tier = %s AND ch_tt_id = %s", (cust_tier, trade_type_id))
        charge_amount = cur.fetchone()[0]
        
        status_id = st_submitted_id if type_is_market else st_pending_id

        # ======================================================================
        # --- FRAME 4: Inserir os registos da negociação ---
        # ======================================================================
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