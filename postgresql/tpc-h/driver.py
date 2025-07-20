import psycopg2
import time
import math
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from input_generator import INPUT_GENERATORS
from queries import QUERIES

# --- Configurações do Benchmark ---
DB_SETTINGS = {
    "dbname": "tpc-h", 
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5461" 
}
SCALE_FACTOR = 1
NUM_WORKERS = 4  
TEST_DURATION_SECS = 120 
ISOLATION_LEVEL = "READ COMMITTED"

BENCHMARK_MODE = "SIMPLIFIED"

# =============================================================================
# WORKERS E LÓGICA DE EXECUÇÃO
# =============================================================================

def execute_single_query(worker_id, query_num):
    """Worker para o MODO SIMPLIFICADO: executa uma única query aleatória."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_SETTINGS)
        conn.set_session(isolation_level=ISOLATION_LEVEL, readonly=True)
        cur = conn.cursor()

        query_sql = QUERIES.get(query_num)
        input_gen_func = INPUT_GENERATORS.get(query_num)

        if not query_sql or not input_gen_func:
            return None

        inputs = input_gen_func(sf=SCALE_FACTOR) if query_num == 11 else input_gen_func()
        
        # --- INÍCIO DA CORREÇÃO ---
        # Tratamento especial para placeholders que esperam uma lista
        if query_num == 16:
            inputs['SIZES'] = ', '.join(map(str, inputs['SIZES']))
        elif query_num == 22:
            inputs['CODES'] = ', '.join(f"'{c}'" for c in inputs['CODES'])
        # --- FIM DA CORREÇÃO ---
            
        formatted_sql = query_sql.format(**inputs)

        start_time = time.time()
        cur.execute(formatted_sql)
        cur.fetchall()
        duration = time.time() - start_time
        
        return {"query": f"Q{query_num}", "duration": duration, "worker_id": worker_id}

    except psycopg2.Error as e:
        print(f"!!! ERRO no Worker {worker_id} ao executar Q{query_num}: {e}")
        return None
    finally:
        if conn:
            conn.close()

def run_query_stream(stream_id):
    """Worker para o MODO OFICIAL: executa uma stream completa (22 queries)."""
    conn = None
    timings = {}
    print(f"Stream {stream_id}: Iniciando sequência completa...")
    try:
        conn = psycopg2.connect(**DB_SETTINGS)
        conn.set_session(isolation_level=ISOLATION_LEVEL, readonly=True)
        
        for i in range(1, 23):
            cur = conn.cursor()
            query_sql = QUERIES.get(i)
            input_gen_func = INPUT_GENERATORS.get(i)
            
            inputs = input_gen_func(sf=SCALE_FACTOR) if i == 11 else input_gen_func()

            # --- INÍCIO DA CORREÇÃO ---
            # Tratamento especial para placeholders que esperam uma lista
            if i == 16:
                inputs['SIZES'] = ', '.join(map(str, inputs['SIZES']))
            elif i == 22:
                inputs['CODES'] = ', '.join(f"'{c}'" for c in inputs['CODES'])
            # --- FIM DA CORREÇÃO ---
            
            formatted_sql = query_sql.format(**inputs)
            
            start_time = time.time()
            cur.execute(formatted_sql)
            cur.fetchall()
            duration = time.time() - start_time
            
            timings[f'Q{i}'] = duration
            print(f"Stream {stream_id}: Q{i} concluída em {duration:.2f}s")
            cur.close()

    except psycopg2.Error as e:
        print(f"!!! ERRO na Stream {stream_id}: {e}")
        return None
    finally:
        if conn:
            conn.close()
            
    print(f"Stream {stream_id}: Sequência completa finalizada.")
    return timings

# =============================================================================
# FUNÇÕES DE CÁLCULO E EXECUÇÃO DOS BENCHMARKS (sem alterações)
# =============================================================================

def run_official_benchmark():
    """Executa o benchmark TPC-H seguindo as fases Power e Throughput."""
    print("\n--- Executando Modo: FIEL À ESPECIFICAÇÃO TPC-H ---")
    
    # --- 1. POWER TEST ---
    print("\n--- Fase 1: Power Test (1 Stream) ---")
    power_start_time = time.time()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run_query_stream, 0)
        power_timings = future.result()
    power_end_time = time.time()
    
    power_metric = 0
    if power_timings:
        product_of_times = 1
        for t in power_timings.values(): product_of_times *= t
        geometric_mean = product_of_times ** (1 / len(power_timings))
        power_metric = (3600 * SCALE_FACTOR) / geometric_mean
        print(f"\nPower Test concluído em {power_end_time - power_start_time:.2f} segundos.")
    else:
        print("\nPower Test falhou.")

    # --- 2. THROUGHPUT TEST ---
    print(f"\n--- Fase 2: Throughput Test ({NUM_WORKERS} Streams) ---")
    throughput_start_time = time.time()
    all_stream_timings = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(run_query_stream, i + 1) for i in range(NUM_WORKERS)]
        for future in as_completed(futures):
            result = future.result()
            if result: all_stream_timings.append(result)
    throughput_end_time = time.time()
    
    throughput_metric = 0
    total_throughput_duration = throughput_end_time - throughput_start_time
    if len(all_stream_timings) == NUM_WORKERS:
        throughput_metric = (NUM_WORKERS * 22 * 3600) / total_throughput_duration
        print(f"\nThroughput Test concluído em {total_throughput_duration:.2f} segundos.")
    else:
        print(f"\nThroughput Test falhou.")

    # --- 3. RESULTADO FINAL (MODO OFICIAL) ---
    print("\n" + "="*50)
    print("📊 RESULTADOS FINAIS (MODO OFICIAL)")
    print("="*50)
    
    if power_metric > 0 and throughput_metric > 0:
        composite_metric = math.sqrt(power_metric * throughput_metric)
        print(f"MÉTRICA 1 - Tempo de Resposta (via Power Metric): {power_metric:.2f} QphH@{SCALE_FACTOR}GB")
        print(f"MÉTRICA 2 - Vazão (via Throughput Metric):      {throughput_metric:.2f} QphH@{SCALE_FACTOR}GB")
        print("-" * 25)
        print(f"Métrica Composta Final: {composite_metric:.2f} QphH@{SCALE_FACTOR}GB")
    else:
        print("Não foi possível calcular a métrica final.")

def run_simplified_benchmark():
    """Executa o benchmark TPC-H seguindo o modelo simplificado do trabalho."""
    print("\n--- Executando Modo: SIMPLIFICADO (SUGERIDO NO SLIDE) ---")
    
    all_results = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        start_test_time = time.time()
        futures = {executor.submit(execute_single_query, i + 1, random.randint(1, 22)): (i + 1) for i in range(NUM_WORKERS)}

        while time.time() - start_test_time < TEST_DURATION_SECS:
            try:
                for future in as_completed(futures, timeout=1.0):
                    worker_id = futures.pop(future)
                    result = future.result()
                    if result:
                        all_results.append(result)
                        print(f"  {result['query']:<5} | Worker {result['worker_id']} | {result['duration']:.2f}s")
                    
                    new_query = random.randint(1, 22)
                    new_future = executor.submit(execute_single_query, worker_id, new_query)
                    futures[new_future] = worker_id
            except Exception:
                continue
    
    print("\n" + "="*50)
    print("📊 RESULTADOS FINAIS (MODO SIMPLIFICADO)")
    print("="*50)
    
    total_queries = len(all_results)
    throughput = total_queries / TEST_DURATION_SECS
    print(f"MÉTRICA 1 - VAZÃO")
    print(f"  - Tempo Total do Teste:      {TEST_DURATION_SECS} segundos")
    print(f"  - Total de Queries Executadas: {total_queries}")
    print(f"  - Vazão (Throughput):        {throughput:.2f} queries/segundo")

    print("\nMÉTRICA 2 - TEMPO DE RESPOSTA (médio por query)")
    by_query = defaultdict(list)
    for res in all_results:
        by_query[res['query']].append(res['duration'])
    
    print(f"  {'Query':<7} | {'Execuções':<11} | {'Tempo Médio (ms)':<15}")
    print(f"  {'-'*7} | {'-'*11} | {'-'*15}")
    for query_name, durations in sorted(by_query.items()):
        avg_time_ms = (sum(durations) / len(durations)) * 1000
        print(f"  {query_name:<7} | {len(durations):<11} | {avg_time_ms:<15.2f}")

if __name__ == "__main__":
    if BENCHMARK_MODE == "OFFICIAL":
        run_official_benchmark()
    elif BENCHMARK_MODE == "SIMPLIFIED":
        run_simplified_benchmark()
    else:
        print("Modo de benchmark inválido. Escolha 'OFFICIAL' ou 'SIMPLIFIED'.")