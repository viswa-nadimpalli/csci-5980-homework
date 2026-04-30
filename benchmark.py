import threading
import queue
import requests
import time
import random

from uhashring import HashRing

# The base URL of the Flask server
# BASE_URL = 'http://127.0.0.1:8080'

# Configure the number of threads and operations
NUM_THREADS = 50
OPS_PER_THREAD = 1000
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

# Synchronize the starting of threads
start_event = threading.Event()

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
        return False

# Worker thread function
def worker_thread():
    while not start_event.is_set():
        # Wait until all threads are ready to start
        pass

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
        current_time = time.time()
        elapsed_time = current_time - last_print
        latencies = []
        while not latencies_queue.empty():
            latencies.append(latencies_queue.get())

        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            throughput = len(latencies) / elapsed_time
            print(f"[Last {PRINT_INTERVAL} seconds] Throughput: {throughput:.2f} ops/sec, "
                  f"Avg Latency: {avg_latency:.5f} sec/ops")
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

    # Create and start worker threads
    threads = [threading.Thread(target=worker_thread) for _ in range(NUM_THREADS)]

    # Starting benchmark
    start_time = time.time()
    start_event.clear()

    for thread in threads:
        thread.start()

    start_event.set()  # Signal threads to start

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

    print(f"\n'{op_type}' Final Results:")
    print(f"Total operations: {total_ops}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} operations per second")
    print(f"Average Latency: {average_latency:.5f} seconds per operation")