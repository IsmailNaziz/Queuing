from fastapi import FastAPI, HTTPException
from api.models import Chunk

app = FastAPI()

@app.post("/chunks/")
async def receive_chunk(chunk: Chunk):
    # Push to the queue (placeholder)
    # queue_manager.push(chunk.dict())
    return {"message": "Chunk received successfully"}