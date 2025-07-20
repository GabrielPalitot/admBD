# Em transactions.py
import psycopg2
from psycopg2 import sql


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