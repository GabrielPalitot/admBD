import random
import decimal
from datetime import date, timedelta

def generate_customer_position_inputs(cur):

    use_tax_id = random.choice([True, False])
    
    if use_tax_id:
        cur.execute("SELECT c_tax_id FROM CUSTOMER ORDER BY RANDOM() LIMIT 1")
        tax_id = cur.fetchone()[0]
        cust_id = 0 
    else:
        cur.execute("SELECT c_id FROM CUSTOMER ORDER BY RANDOM() LIMIT 1")
        cust_id = cur.fetchone()[0]
        tax_id = "" 

    get_history = random.choice([True, False])

    return {
        "cust_id": cust_id,
        "tax_id": tax_id,
        "get_history": get_history
    }




def generate_market_feed_inputs(cur):

    cur.execute("SELECT st_id FROM STATUS_TYPE WHERE st_id = 'SBMT'")
    status_submitted = cur.fetchone()[0]
    
    cur.execute("SELECT tt_id FROM TRADE_TYPE WHERE tt_id IN ('TSL', 'TLS', 'TLB')")
    trade_types = {row[0]: row[0] for row in cur.fetchall()}
    type_stop_loss = trade_types.get('TSL')
    type_limit_sell = trade_types.get('TLS')
    type_limit_buy = trade_types.get('TLB')

    max_feed_len = 20
    feed = []
    
    cur.execute("SELECT lt_s_symb, lt_price FROM LAST_TRADE ORDER BY RANDOM() LIMIT %s", (max_feed_len,))
    securities = cur.fetchall()

    for symbol, current_price in securities:
        price_change = decimal.Decimal(random.uniform(-0.05, 0.05))
        new_price = round(current_price * (1 + price_change), 2)
        
        trade_qty = random.randint(100, 1000)
        
        feed.append({
            "symbol": symbol,
            "price_quote": new_price,
            "trade_qty": trade_qty
        })

    return {
        "ticker_tape": feed,
        "status_submitted": status_submitted,
        "type_stop_loss": type_stop_loss,
        "type_limit_sell": type_limit_sell,
        "type_limit_buy": type_limit_buy
    }



def generate_market_watch_inputs(cur):

    scenario = random.choice(['cust_id', 'industry_name', 'acct_id'])
    
    inputs = {
        "cust_id": 0,
        "industry_name": "",
        "acct_id": 0
    }
    
    if scenario == 'cust_id':
        cur.execute("SELECT wl_c_id FROM WATCH_LIST ORDER BY RANDOM() LIMIT 1")
        inputs['cust_id'] = cur.fetchone()[0]
    
    elif scenario == 'industry_name':
        cur.execute("SELECT in_name FROM INDUSTRY ORDER BY RANDOM() LIMIT 1")
        inputs['industry_name'] = cur.fetchone()[0]

    
    elif scenario == 'acct_id':
        cur.execute("SELECT hs_ca_id FROM HOLDING_SUMMARY ORDER BY RANDOM() LIMIT 1")
        inputs['acct_id'] = cur.fetchone()[0]


    start_day = date.today() - timedelta(days=random.randint(1, 365))
    inputs['start_date'] = start_day.strftime('%Y-%m-%d')
    
    return inputs




def generate_security_detail_inputs(cur):

    cur.execute("SELECT s_symb FROM SECURITY ORDER BY RANDOM() LIMIT 1")
    symbol = cur.fetchone()[0]


    access_lob_flag = (random.randint(1, 100) == 1)

    max_rows_to_return = random.randint(5, 20)


    end_range = date(2005, 1, 1) - timedelta(days=max_rows_to_return)
    start_range = date(2000, 1, 3)
    
    random_days = random.randint(0, (end_range - start_range).days)
    start_date = start_range + timedelta(days=random_days)

    return {
        "symbol": symbol,
        "access_lob_flag": access_lob_flag,
        "max_rows_to_return": max_rows_to_return,
        "start_date": start_date.strftime('%Y-%m-%d')
    }




def generate_trade_lookup_inputs(cur):

    frame_to_execute = random.randint(1, 4)
    inputs = {"frame_to_execute": frame_to_execute}

    max_trades = 20
    
    end_date = date(2005, 1, 1) 
    start_date_range = end_date - timedelta(days=100)
    random_days = random.randint(0, (end_date - start_date_range).days)
    start_trade_dts = start_date_range + timedelta(days=random_days)
    end_trade_dts = start_trade_dts + timedelta(days=random.randint(1, 5))

    if frame_to_execute == 1:
        cur.execute("SELECT t_id FROM TRADE ORDER BY RANDOM() LIMIT %s", (max_trades,))
        inputs["trade_id_list"] = [row[0] for row in cur.fetchall()]
        inputs["max_trades"] = max_trades

    elif frame_to_execute == 2:
        cur.execute("SELECT ca_id FROM CUSTOMER_ACCOUNT ORDER BY RANDOM() LIMIT 1")
        inputs["acct_id"] = cur.fetchone()[0]
        inputs["start_trade_dts"] = start_trade_dts.strftime('%Y-%m-%d %H:%M:%S')
        inputs["end_trade_dts"] = end_trade_dts.strftime('%Y-%m-%d %H:%M:%S')
        inputs["max_trades"] = max_trades

    elif frame_to_execute == 3:
        cur.execute("SELECT s_symb FROM SECURITY ORDER BY RANDOM() LIMIT 1")
        inputs["symbol"] = cur.fetchone()[0]
        inputs["start_trade_dts"] = start_trade_dts.strftime('%Y-%m-%d %H:%M:%S')
        inputs["end_trade_dts"] = end_trade_dts.strftime('%Y-%m-%d %H:%M:%S')
        inputs["max_trades"] = max_trades

    elif frame_to_execute == 4:
        cur.execute("SELECT ca_id FROM CUSTOMER_ACCOUNT ORDER BY RANDOM() LIMIT 1")
        inputs["acct_id"] = cur.fetchone()[0]
        inputs["start_trade_dts"] = start_trade_dts.strftime('%Y-%m-%d %H:%M:%S')

    return inputs



def generate_trade_order_inputs(cur):

    cur.execute("SELECT ca_id FROM CUSTOMER_ACCOUNT ORDER BY RANDOM() LIMIT 1")
    acct_id = cur.fetchone()[0]

    cur.execute("SELECT st_id FROM STATUS_TYPE WHERE st_id IN ('PNDG', 'SBMT')")
    statuses = {row[0]: row[0] for row in cur.fetchall()}
    st_pending_id = statuses.get('PNDG')
    st_submitted_id = statuses.get('SBMT')

    cur.execute("SELECT tt_id FROM TRADE_TYPE ORDER BY RANDOM() LIMIT 1")
    trade_type_id = cur.fetchone()[0]

    cur.execute("""
        SELECT C.c_f_name, C.c_l_name, C.c_tax_id
        FROM CUSTOMER C JOIN CUSTOMER_ACCOUNT CA ON C.c_id = CA.ca_c_id
        WHERE CA.ca_id = %s
        """, (acct_id,))
    f_name, l_name, tax_id = cur.fetchone()

    if random.randint(1, 100) <= 10:
        cur.execute("""
            SELECT ap_f_name, ap_l_name, ap_tax_id FROM ACCOUNT_PERMISSION
            WHERE ap_ca_id = %s ORDER BY RANDOM() LIMIT 1
            """, (acct_id,))
        executor_details = cur.fetchone()
        if executor_details:
             f_name, l_name, tax_id = executor_details
    
    use_co_name = random.randint(1, 100) <= 40
    symbol, co_name, issue = "", "", ""
    if use_co_name:
        cur.execute("""
            SELECT C.co_name, S.s_issue FROM SECURITY S JOIN COMPANY C ON S.s_co_id = C.co_id
            ORDER BY RANDOM() LIMIT 1
        """)
        co_name, issue = cur.fetchone()
    else:
        cur.execute("SELECT s_symb FROM SECURITY ORDER BY RANDOM() LIMIT 1")
        symbol = cur.fetchone()[0]

    return {
        "acct_id": acct_id,
        "exec_f_name": f_name,
        "exec_l_name": l_name,
        "exec_tax_id": tax_id,
        "symbol": symbol,
        "co_name": co_name,
        "issue": issue,
        "trade_type_id": trade_type_id,
        "st_pending_id": st_pending_id,
        "st_submitted_id": st_submitted_id,
        "trade_qty": random.choice([100, 200, 400, 800]),
        "is_lifo": random.choice([True, False]),
        "type_is_margin": (random.randint(1, 100) <= 8),
        "roll_it_back": (random.randint(1, 101) == 1) # ~1% de chance
    }




def generate_trade_result_inputs(cur):

    cur.execute("SELECT t_id, t_bid_price FROM TRADE WHERE t_st_id = 'SBMT' ORDER BY RANDOM() LIMIT 1")
    trade_result = cur.fetchone()

    if not trade_result:

        return {"trade_id": 0, "trade_price": 0.0}

    trade_id, bid_price = trade_result

    price_fluctuation = decimal.Decimal(random.uniform(-0.02, 0.02))
    trade_price = round(bid_price * (1 + price_fluctuation), 2)

    return {
        "trade_id": trade_id,
        "trade_price": trade_price
    }



def generate_trade_status_inputs(cur):

    cur.execute("SELECT ca_id FROM CUSTOMER_ACCOUNT ORDER BY RANDOM() LIMIT 1")
    acct_id = cur.fetchone()[0]
    
    return {
        "acct_id": acct_id
    }





def generate_trade_update_inputs(cur):

    frame_to_execute = random.randint(1, 3)
    inputs = {"frame_to_execute": frame_to_execute}
    
    max_trades = 20
    max_updates = 20 

    end_date = date(2005, 1, 1)
    start_date_range = end_date - timedelta(days=100)
    random_days = random.randint(0, (end_date - start_date_range).days)
    start_trade_dts = start_date_range + timedelta(days=random_days)
    end_trade_dts = start_trade_dts + timedelta(days=random.randint(1, 5))

    if frame_to_execute == 1:
        cur.execute("SELECT t_id FROM TRADE ORDER BY RANDOM() LIMIT %s", (max_trades,))
        inputs["trade_id_list"] = [row[0] for row in cur.fetchall()]
        inputs["max_trades"] = max_trades
        inputs["max_updates"] = max_updates

    elif frame_to_execute == 2:
        cur.execute("SELECT ca_id FROM CUSTOMER_ACCOUNT ORDER BY RANDOM() LIMIT 1")
        inputs["acct_id"] = cur.fetchone()[0]
        inputs["start_trade_dts"] = start_trade_dts.strftime('%Y-%m-%d %H:%M:%S')
        inputs["end_trade_dts"] = end_trade_dts.strftime('%Y-%m-%d %H:%M:%S')
        inputs["max_trades"] = max_trades
        inputs["max_updates"] = max_updates

    elif frame_to_execute == 3:
        cur.execute("SELECT s_symb FROM SECURITY ORDER BY RANDOM() LIMIT 1")
        inputs["symbol"] = cur.fetchone()[0]
        inputs["start_trade_dts"] = start_trade_dts.strftime('%Y-%m-%d %H:%M:%S')
        inputs["end_trade_dts"] = end_trade_dts.strftime('%Y-%m-%d %H:%M:%S')
        inputs["max_trades"] = max_trades
        inputs["max_updates"] = max_updates

    return inputs



def generate_broker_volume_inputs(cur):
    
    num_brokers_to_select = random.randint(20, 40)

    cur.execute("SELECT b_name FROM BROKER ORDER BY RANDOM() LIMIT %s", (num_brokers_to_select,))
    
    broker_list = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT sc_name FROM SECTOR ORDER BY RANDOM() LIMIT 1")
    sector_name = cur.fetchone()[0]
    
    return {
        "broker_list": broker_list,
        "sector_name": sector_name
    }