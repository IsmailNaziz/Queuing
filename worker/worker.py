import redis
import time
import json
from queue.manager import QueueManager
from queue.tasks import process_chunk
from config.settings import REDIS_HOST, REDIS_PORT, REDIS_DB, WORKER_SLEEP_TIME

class Worker:
    def __init__(self, queue_name: str, worker_id: str):
        self.manager = QueueManager(queue_name=queue_name)
        self.redis = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        self.worker_id = worker_id
        self.running = True
        self.lock_prefix = "chunk_lock:"

    def run(self):
        while self.running:
            # Fetch a chunk with blocking
            chunk = self._fetch_chunk()
            if chunk:
                result = process_chunk(chunk)
                self._store_result(result)
            else:
                time.sleep(WORKER_SLEEP_TIME)  # Configurable sleep

    def _fetch_chunk(self):
        while self.running:
            chunk = self.manager.pop()
            if chunk and self._lock_chunk(chunk):
                return chunk
            elif not chunk:
                return None

    def _lock_chunk(self, chunk: dict) -> bool:
        """Lock the chunk to prevent double-processing."""
        chunk_id = f"{chunk['id']}_{chunk['order']}"
        lock_key = f"{self.lock_prefix}{chunk_id}"
        return self.redis.setnx(lock_key, self.worker_id)

    def _store_result(self, result: dict):
        """Store the processed result in Redis."""
        result_key = f"processed_result:{result['id']}:{result['order']}"
        self.redis.set(result_key, json.dumps(result))

    def stop(self):
        self.running = False
