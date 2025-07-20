import psycopg2
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

from transactions import (
    execute_broker_volume,
    execute_customer_position,
    execute_market_feed,
    execute_market_watch,
    execute_security_detail,
    execute_trade_lookup,
    execute_trade_order,
    execute_trade_result,
    execute_trade_status,
    execute_trade_update,
)

from input_generator import (
    generate_broker_volume_inputs,
    generate_customer_position_inputs,
    generate_market_feed_inputs,
    generate_market_watch_inputs,
    generate_security_detail_inputs,
    generate_trade_lookup_inputs,
    generate_trade_order_inputs,
    generate_trade_result_inputs,
    generate_trade_status_inputs,
    generate_trade_update_inputs,
)



DB_SETTINGS = {
    "dbname": "tpc-e",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5461"
}

ISOLATION_LEVEL = "SERIALIZABLE"
NUM_WORKERS = 4
TEST_DURATION_SECS = 120 



class RollbackException(Exception):
    pass

def worker_task(transaction_function, transaction_inputs):

    conn = None
    status = "success"
    error_detail = None
    start_time = time.time()
    
    try:
        conn = psycopg2.connect(**DB_SETTINGS)
        conn.set_session(isolation_level=ISOLATION_LEVEL)
        
        transaction_function(conn, **transaction_inputs)
        conn.commit()

    except RollbackException:
        status = "rollback_ok"
        if conn:
            conn.rollback()
    except psycopg2.Error as e:
        status = "abort"
        error_detail = str(e).strip()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            
    end_time = time.time()
    return {
        "transaction": transaction_function.__name__.replace('execute_', ''),
        "status": status,
        "duration_ms": (end_time - start_time) * 1000,
        "error": error_detail
    }



if __name__ == "__main__":

    TRANSACTION_MIX = {
        execute_broker_volume: {"gen": generate_broker_volume_inputs, "weight": 4.9},
        execute_customer_position: {"gen": generate_customer_position_inputs, "weight": 13.0},
        execute_market_feed: {"gen": generate_market_feed_inputs, "weight": 1.0},
        execute_market_watch: {"gen": generate_market_watch_inputs, "weight": 18.0},
        execute_security_detail: {"gen": generate_security_detail_inputs, "weight": 14.0},
        execute_trade_lookup: {"gen": generate_trade_lookup_inputs, "weight": 8.0},
        execute_trade_order: {"gen": generate_trade_order_inputs, "weight": 10.1},
        execute_trade_result: {"gen": generate_trade_result_inputs, "weight": 10.0},
        execute_trade_status: {"gen": generate_trade_status_inputs, "weight": 19.0},
        execute_trade_update: {"gen": generate_trade_update_inputs, "weight": 2.0},
    }
    
    transaction_pool = []
    for func, props in TRANSACTION_MIX.items():
        transaction_pool.extend([func] * int(props['weight'] * 10))

    results = []
    
    print(f"🚀 Iniciando benchmark com {NUM_WORKERS} workers por {TEST_DURATION_SECS} segundos...")
    print(f"Nível de Isolamento: {ISOLATION_LEVEL}")
    print("-" * 50)
    
    driver_conn = psycopg2.connect(**DB_SETTINGS)
    driver_cur = driver_conn.cursor()

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        start_test_time = time.time()
        futures = []
        
        for _ in range(NUM_WORKERS):
            selected_function = random.choice(transaction_pool)
            input_generator = TRANSACTION_MIX[selected_function]["gen"]
            inputs = input_generator(driver_cur)
            futures.append(executor.submit(worker_task, selected_function, inputs))

        while time.time() - start_test_time < TEST_DURATION_SECS:
            try:
                for future in as_completed(futures, timeout=1.0):
                    futures.remove(future)
                    result = future.result()
                    results.append(result)
                    print(f"  {result['transaction']:<20} | {result['status']:<12} | {result['duration_ms']:.2f} ms")
                    
                    selected_function = random.choice(transaction_pool)
                    input_generator = TRANSACTION_MIX[selected_function]["gen"]
                    inputs = input_generator(driver_cur)
                    futures.append(executor.submit(worker_task, selected_function, inputs))
            except Exception:
                continue
    
    driver_conn.close()
    
    print("\n" + "="*50)
    print("📊 RESULTADOS FINAIS DO BENCHMARK")
    print("="*50)
    
    total_transacoes = len(results)
    sucessos = sum(1 for r in results if r['status'] in ['success', 'rollback_ok'])
    aborts = total_transacoes - sucessos
    
    vazao_tps = sucessos / TEST_DURATION_SECS
    taxa_abort = (aborts / total_transacoes) * 100 if total_transacoes > 0 else 0
    
    print(f"Tempo Total do Teste:       {TEST_DURATION_SECS} segundos")
    print(f"Total de Transações Tentadas: {total_transacoes}")
    print(f"  - Sucesso (Commit+Rollback OK): {sucessos}")
    print(f"  - Aborts (Erros):               {aborts}")
    print("-" * 25)
    print(f"VAZÃO (Throughput):           {vazao_tps:.2f} transações/segundo")
    print(f"TAXA DE ABORTS:               {taxa_abort:.2f}%")
    
    print("\n**Lembre-se de recolher a métrica de DEADLOCKS diretamente da base de dados!**")

    print("\n--- Detalhes por Transação ---")
    by_type = defaultdict(lambda: {'count': 0, 'aborts': 0, 'total_time': 0})
    for r in results:
        stats = by_type[r['transaction']]
        stats['count'] += 1
        stats['total_time'] += r['duration_ms']
        if r['status'] == 'abort':
            stats['aborts'] += 1
            
    for name, stats in sorted(by_type.items()):
        avg_time = stats['total_time'] / stats['count'] if stats['count'] > 0 else 0
        print(f"- {name:<20} | Execuções: {stats['count']:<5} | Aborts: {stats['aborts']:<4} | Tempo Médio: {avg_time:.2f} ms")