from fastapi import FastAPI, HTTPException, Request
from uhashring import HashRing
import httpx
import os

app = FastAPI()

ALL_NODES = {
    "node1": {"hostname": "node1", "port": 8081},
    "node2": {"hostname": "node2", "port": 8082},
    "node3": {"hostname": "node3", "port": 8083},
}

node_count = int(os.environ.get("NODE_COUNT", 3))
NODES = dict(list(ALL_NODES.items())[:node_count])

ring = HashRing(nodes=NODES)

def get_node_url(key):
    node = ring.get_node(key)
    host = NODES[node]["hostname"]
    port = NODES[node]["port"]
    return f"http://{host}:{port}"

@app.post("/key_{key}")
async def put(key: str, request: Request):
    url = f"{get_node_url(key)}/key_{key}"
    body = await request.json()
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=body)
    return response.json()

@app.get("/key_{key}")
async def get(key: str):
    url = f"{get_node_url(key)}/key_{key}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail=response.json()["detail"])
    return response.json()

@app.delete("/key_{key}")
async def delete(key: str):
    url = f"{get_node_url(key)}/key_{key}"
    async with httpx.AsyncClient() as client:
        response = await client.delete(url)
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail=response.json()["detail"])
    return response.json()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)