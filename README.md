# CSCI 5980 KV Store

## Starting the Cluster

**Prerequisites:** Docker and Docker Compose installed.

```bash
docker compose up --build
```

This builds and starts all 3 nodes:
- Node 1: `http://localhost:8081`
- Node 2: `http://localhost:8082`
- Node 3: `http://localhost:8083`

To stop the cluster:

```bash
docker compose down
```

## Running the Benchmark

With the cluster running, in a separate terminal:

```bash
pip install -r requirements.txt
python benchmark.py
```

The benchmark runs three phases (set, get, delete) with `NUM_THREADS` concurrent workers and `OPS_PER_THREAD` operations per thread. It reports:
- **Throughput** — operations per second
- **Average Latency** — average time per operation in seconds
- **Error Rate** — percent of failed requests

### Configuration

Edit the constants at the top of `benchmark.py`:

| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_THREADS` | 5 | Number of concurrent worker threads |
| `OPS_PER_THREAD` | 10000 | Operations per thread per phase |
| `PRINT_INTERVAL` | 3 | Seconds between live throughput updates |
