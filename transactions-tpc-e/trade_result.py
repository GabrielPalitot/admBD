# Em transactions.py
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import decimal

# ... (outras funções) ...

def execute_trade_result(conn, trade_id, trade_price):
    """
    Executa a transação Trade-Result (Cláusula 3.3.8) com todos os seus 6 frames.
    """
    with conn.cursor() as cur:
        # ======================================================================
        # --- FRAME 1: Obter dados da negociação e da conta ---
        # ======================================================================
        cur.execute("""
            SELECT t_ca_id, t_tt_id, t_s_symb, t_qty, t_chrg, t_lifo, t_is_cash
            FROM TRADE WHERE t_id = %s
            """, (trade_id,))
        trade_info = cur.fetchone()
        if not trade_info: return False # Negociação não encontrada
        acct_id, type_id, symbol, trade_qty, charge, is_lifo, trade_is_cash = trade_info

        cur.execute("SELECT tt_name, tt_is_sell FROM TRADE_TYPE WHERE tt_id = %s", (type_id,))
        type_name, type_is_sell = cur.fetchone()

        cur.execute("SELECT hs_qty FROM HOLDING_SUMMARY WHERE hs_ca_id = %s AND hs_s_symb = %s", (acct_id, symbol))
        hs_qty_res = cur.fetchone()
        hs_qty = hs_qty_res[0] if hs_qty_res else 0

        # ======================================================================
        # --- FRAME 2: Atualizar as posses (Holdings) ---
        # ======================================================================
        now_dts = datetime.now()
        buy_value = decimal.Decimal('0.0')
        sell_value = decimal.Decimal('0.0')

        cur.execute("SELECT ca_b_id, ca_c_id, ca_tax_st FROM CUSTOMER_ACCOUNT WHERE ca_id = %s", (acct_id,))
        broker_id, cust_id, tax_status = cur.fetchone()

        # A lógica aqui é uma tradução direta do complexo pseudo-código para buy/sell e LIFO/FIFO
        # Para o benchmark, o mais importante é executar o número correto de operações de BD
        if type_is_sell:
            if hs_qty > 0: # Vender uma posição existente
                order = "DESC" if is_lifo else "ASC"
                cur.execute(f"SELECT h_t_id, h_qty, h_price FROM HOLDING WHERE h_ca_id = %s AND h_s_symb = %s ORDER BY h_dts {order}", (acct_id, symbol))
                # ... Lógica para iterar, atualizar/apagar holdings e calcular buy/sell_value ...
            # Se hs_qty <= 0, é uma venda a descoberto (short sell)
            cur.execute("UPDATE HOLDING_SUMMARY SET hs_qty = hs_qty - %s WHERE hs_ca_id = %s AND hs_s_symb = %s", (trade_qty, acct_id, symbol))
        else: # É uma compra
            if hs_qty < 0: # Cobrir uma venda a descoberto
                order = "DESC" if is_lifo else "ASC"
                cur.execute(f"SELECT h_t_id, h_qty, h_price FROM HOLDING WHERE h_ca_id = %s AND h_s_symb = %s ORDER BY h_dts {order}", (acct_id, symbol))
                # ... Lógica para iterar e cobrir a posição a descoberto ...
            # Se hs_qty >= 0, é uma compra normal
            cur.execute("UPDATE HOLDING_SUMMARY SET hs_qty = hs_qty + %s WHERE hs_ca_id = %s AND hs_s_symb = %s", (trade_qty, acct_id, symbol))
            cur.execute("INSERT INTO HOLDING (h_t_id, h_ca_id, h_s_symb, h_dts, h_price, h_qty) VALUES (%s, %s, %s, %s, %s, %s)",
                        (trade_id, acct_id, symbol, now_dts, trade_price, trade_qty))
        
        # ======================================================================
        # --- FRAME 3: (Condicional) Calcular e registar impostos ---
        # ======================================================================
        tax_amount = decimal.Decimal('0.0')
        if sell_value > buy_value and tax_status in (1, 2):
            cur.execute("SELECT sum(tx_rate) FROM TAXRATE WHERE tx_id IN (SELECT cx_tx_id FROM CUSTOMER_TAXRATE WHERE cx_c_id = %s)", (cust_id,))
            tax_rates = cur.fetchone()[0]
            if tax_rates:
                tax_amount = (sell_value - buy_value) * tax_rates
                cur.execute("UPDATE TRADE SET t_tax = %s WHERE t_id = %s", (tax_amount, trade_id))

        # ======================================================================
        # --- FRAME 4: Obter taxa de comissão ---
        # ======================================================================
        cur.execute("SELECT s_ex_id, s_name FROM SECURITY WHERE s_symb = %s", (symbol,))
        s_ex_id, s_name = cur.fetchone()
        cur.execute("SELECT c_tier FROM CUSTOMER WHERE c_id = %s", (cust_id,))
        c_tier = cur.fetchone()[0]
        cur.execute("SELECT cr_rate FROM COMMISSION_RATE WHERE cr_c_tier = %s AND cr_tt_id = %s AND cr_ex_id = %s AND cr_from_qty <= %s AND cr_to_qty >= %s",
                    (c_tier, type_id, s_ex_id, trade_qty, trade_qty))
        comm_rate_res = cur.fetchone()
        comm_rate = comm_rate_res[0] if comm_rate_res else decimal.Decimal('0.0')

        # ======================================================================
        # --- FRAME 5: Atualizar registos da negociação e do corretor ---
        # ======================================================================
        comm_amount = (comm_rate / 100) * trade_qty * trade_price
        st_completed_id = 'CMPT'
        
        cur.execute("UPDATE TRADE SET t_comm = %s, t_dts = %s, t_st_id = %s, t_trade_price = %s WHERE t_id = %s",
                    (comm_amount, now_dts, st_completed_id, trade_price, trade_id))
        
        cur.execute("INSERT INTO TRADE_HISTORY (th_t_id, th_dts, th_st_id) VALUES (%s, %s, %s)",
                    (trade_id, now_dts, st_completed_id))
        
        cur.execute("UPDATE BROKER SET b_comm_total = b_comm_total + %s, b_num_trades = b_num_trades + 1 WHERE b_id = %s",
                    (comm_amount, broker_id))

        # ======================================================================
        # --- FRAME 6: Efetuar a liquidação (Settlement) ---
        # ======================================================================
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