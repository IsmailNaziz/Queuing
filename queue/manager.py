import redis
import json
from config.settings import REDIS_HOST, REDIS_PORT, REDIS_DB

class QueueManager:
    def __init__(self, queue_name: str, tracking_prefix: str = "chunk_tracking:"):
        self.redis = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        self.queue_name = queue_name
        self.tracking_prefix = tracking_prefix

    def push(self, chunk: dict):
        # Add chunk to the queue
        self.redis.rpush(self.queue_name, json.dumps(chunk))

        # Track chunks by ID
        tracking_key = f"{self.tracking_prefix}{chunk['id']}"
        self.redis.hincrby(tracking_key, "received_chunks", 1)
        if chunk.get("isLastChunk"):
            self.redis.hset(tracking_key, "total_chunks", chunk["order"] + 1)

    def pop(self):
        message = self.redis.lpop(self.queue_name)
        return json.loads(message) if message else None

    def is_complete(self, id: str) -> bool:
        tracking_key = f"{self.tracking_prefix}{id}"
        total_chunks = int(self.redis.hget(tracking_key, "total_chunks") or 0)
        received_chunks = int(self.redis.hget(tracking_key, "received_chunks") or 0)
        return total_chunks > 0 and total_chunks == received_chunks
