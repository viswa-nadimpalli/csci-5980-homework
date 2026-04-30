import threading
import queue
import requests
import time

from uhashring import HashRing

# Configure the number of threads and operations
NUM_THREADS = 5
OPS_PER_THREAD = 10000
PRINT_INTERVAL = 3  # Interval for printing intermediate results

ALL_NODES = {
    "node1": {"host": "http://127.0.0.1:8081"},
    "node2": {"host": "http://127.0.0.1:8082"},
    "node3": {"host": "http://127.0.0.1:8083"},
}

node_count = 3
NODES = dict(list(ALL_NODES.items())[:node_count])

ring = HashRing(nodes=NODES)

def get_node_url(key):
    node = ring.get_node(key)
    return NODES[node]["host"]

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, 'session'):
        thread_local.session = requests.Session()
    return thread_local.session

# Queues for managing operations and latencies
operations_queue = queue.Queue()
latencies_queue = queue.Queue()
error_count = 0
error_lock = threading.Lock()
phase_start_time = [0.0]  # shared so the monitor can reset its window per phase

# Client operation function
def kv_store_operation(op_type, key, value=None):
    session = get_session()
    try:
        base_url = get_node_url(key)
        if op_type == 'set':
            response = session.post(f"{base_url}/key_{key}", json={'value': value})
        elif op_type == 'get':
            response = session.get(f"{base_url}/key_{key}")
        elif op_type == 'delete':
            response = session.delete(f"{base_url}/key_{key}")
        else:
            raise ValueError("Invalid operation type")
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Error during {op_type} operation for key '{key}': {e}")
        with error_lock:
            global error_count
            error_count += 1
        return False

def worker_thread(barrier):
    session = get_session()
    for node_info in NODES.values():
        try:
            session.post(f"{node_info['host']}/key_warmup", json={"value": "warmup"})
        except Exception:
            pass

    barrier.wait()  # wait until all threads are warmed up, then start together

    while not operations_queue.empty():
        op, key, value = operations_queue.get()
        start_time = time.time()
        if kv_store_operation(op, key, value):
            latency = time.time() - start_time
            latencies_queue.put(latency)

# Monitoring thread function
def monitor_performance():
    last_print = time.time()
    while True:
        time.sleep(PRINT_INTERVAL)
        if phase_start_time[0] > last_print:
            last_print = phase_start_time[0]
        current_time = time.time()
        elapsed_time = current_time - last_print
        latencies = []
        while not latencies_queue.empty():
            latencies.append(latencies_queue.get())

        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            throughput = len(latencies) / elapsed_time
            print(f"[Last {PRINT_INTERVAL} seconds] Throughput: {throughput:.2f} ops/sec, "
                  f"Avg Latency: {avg_latency * 1000:.3f} ms/op")
        last_print = time.time()

# Start the monitoring thread
monitoring_thread = threading.Thread(target=monitor_performance, daemon=True)
monitoring_thread.start()

for op_type in ['set', 'get', 'delete']:
    # Populate the operation queue with requests
    for i in range(NUM_THREADS * OPS_PER_THREAD):
        key = f"key_{i}"
        value = f"value_{i}"
        operations_queue.put((op_type, key, value))

    barrier = threading.Barrier(NUM_THREADS + 1)
    threads = [threading.Thread(target=worker_thread, args=(barrier,)) for _ in range(NUM_THREADS)]

    for thread in threads:
        thread.start()

    error_count = 0
    barrier.wait()
    start_time = time.time()
    phase_start_time[0] = start_time

    for thread in threads:
        thread.join()

    # Calculate final results
    total_time = time.time() - start_time
    total_ops = NUM_THREADS * OPS_PER_THREAD
    total_latencies = list(latencies_queue.queue)
    while not latencies_queue.empty():
        latencies_queue.get()
    average_latency = sum(total_latencies) / len(total_latencies) if total_latencies else float('nan')
    throughput = total_ops / total_time
    error_rate = (error_count / total_ops) * 100

    print(f"\n'{op_type}' Final Results:")
    print(f"Total operations: {total_ops}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} operations per second")
    print(f"Average Latency: {average_latency * 1000:.3f} ms per operation")
    print(f"Error Rate: {error_rate:.2f}%")
