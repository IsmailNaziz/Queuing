import redis
import json
from queue.manager import QueueManager  # Import the manager for `is_complete`
from queue.tasks import process_chunk  # Import the processing function


class Collector:
    def __init__(self, tracking_prefix: str = "chunk_tracking:", results_prefix: str = "results:", strategy="order_then_process"):
        self.redis = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.tracking_prefix = tracking_prefix
        self.results_prefix = results_prefix
        self.strategy = strategy
        self.manager = QueueManager(queue_name="dummy_queue")  # Replace with actual queue name

    def collect_chunks(self, id: str) -> dict:
        # Check if all chunks are complete
        if not self.manager.is_complete(id):
            return {"error": f"Chunks not complete for ID: {id}"}

        # Retrieve tracking info
        tracking_key = f"{self.tracking_prefix}{id}"
        total_chunks = int(self.redis.hget(tracking_key, "total_chunks") or 0)

        if total_chunks == 0:
            return {"error": f"No chunks available for ID: {id}"}

        # Combine chunks based on strategy
        if self.strategy == "order_then_process":
            chunks = [self._get_chunk(tracking_key, i) for i in range(total_chunks)]
            result = {"id": id, "chunks": chunks}
        elif self.strategy == "process_then_order":
            chunks = [self._process_chunk(self._get_chunk(tracking_key, i)) for i in range(total_chunks)]
            result = {"id": id, "processed_chunks": chunks}
        else:
            return {"error": f"Invalid strategy: {self.strategy}"}

        # Store final result
        result_key = f"{self.results_prefix}{id}"
        self.redis.set(result_key, json.dumps(result))
        return result

    def _get_chunk(self, tracking_key: str, order: int) -> dict:
        chunk_key = f"{tracking_key}:chunk:{order}"
        chunk = self.redis.get(chunk_key)
        return json.loads(chunk) if chunk else None

    def _process_chunk(self, chunk: dict) -> dict:
        if chunk:
            return process_chunk(chunk)  # Process the chunk using the tasks module logic
        return {"error": "Invalid chunk"}
    def get_result(self, id: str) -> dict:
        # Retrieve the consolidated result
        result_key = f"{self.results_prefix}{id}"
        result = self.redis.get(result_key)
        return json.loads(result) if result else {"error": f"Result not found for ID: {id}"}
