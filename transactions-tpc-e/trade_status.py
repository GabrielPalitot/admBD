# Em transactions.py
import psycopg2
from psycopg2 import sql

# ... (outras funções de transação) ...

def execute_trade_status(conn, acct_id):
    """
    Executa a transação Trade-Status (Cláusula 3.3.9).
    Obtém um resumo das 50 negociações mais recentes para uma conta de cliente.
    """
    with conn.cursor() as cur:
        # --- Query 1: Obter as 50 negociações mais recentes ---
        # Esta query junta várias tabelas para obter os nomes em vez de apenas os IDs.
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
        
        # Apenas consumimos os resultados para gerar a carga na base de dados
        trade_history = cur.fetchall()

        # --- Query 2: Obter detalhes do cliente e do corretor ---
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
            
        # Consumir o resultado
        cur.fetchone()

    return True