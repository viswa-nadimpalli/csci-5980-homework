from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

kv_store = {}

class ValueRequest(BaseModel):
    value: str

@app.post("/key_{key}")
async def put_key_value(key: str, body: ValueRequest):
    kv_store[key] = body.value
    return {"message": f"Key '{key}' set successfully."}

@app.get("/key_{key}")
async def get_key_value(key: str):
    if key not in kv_store:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found.")
    return {"key": key, "value": kv_store[key]}

@app.delete("/key_{key}")
async def delete_key_value(key: str):
    if key not in kv_store:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found.")
    del kv_store[key]
    return {"message": f"Key '{key}' deleted successfully."}
