# CSCI 5980 KV Store

## Directory Structure

```
csci-5980-homework/
├── main.py              # FastAPI KV store node (set, get, delete endpoints)
├── benchmark.py         # Benchmarking script with consistent hashing and metrics
├── Dockerfile           # Container image for the KV store nodes
├── docker-compose.yml   # Defines the 3-node cluster
├── pyproject.toml       # Python project metadata and dependencies
├── uv.lock              # Locked dependency versions
└── README.md            
```

## Running the KV stores

Install dependencies:

```bash
uv sync
```

Then start each node in a separate terminal:

```bash
uvicorn main:app --host 0.0.0.0 --port 8081
```
```bash
uvicorn main:app --host 0.0.0.0 --port 8082
```
```bash
uvicorn main:app --host 0.0.0.0 --port 8083
```

The 3 nodes will be available at:
- Node 1: `http://localhost:8081`
- Node 2: `http://localhost:8082`
- Node 3: `http://localhost:8083`

## Running the Benchmark

With the nodes running, run the benchmark outside the container in a separate terminal:

```bash
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
