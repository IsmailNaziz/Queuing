from fastapi import FastAPI
from api.models import Chunk
from queue_components.manager import QueueManager
from config.settings import CHUNKS_QUEUE_NAME

app = FastAPI()
manager = QueueManager(queue_name=CHUNKS_QUEUE_NAME)

@app.post("/chunks/")
async def receive_chunk(chunk: Chunk):
    manager.push(chunk.dict())
    return {"message": "Chunk received successfully"}
