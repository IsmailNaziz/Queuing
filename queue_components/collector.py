import redis
import json
from config.settings import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    TRACKING_PREFIX,
    FINAL_RESULTS_PREFIX,
)

class Collector:
    def __init__(self, tracking_prefix: str = TRACKING_PREFIX, final_prefix: str = FINAL_RESULTS_PREFIX):
        self.redis = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        self.tracking_prefix = tracking_prefix
        self.final_prefix = final_prefix

    def await_chunks(self, id: str, total_chunks: int) -> bool:
        """
        Wait until all chunks for a job are received.
        This method blocks until all chunks are received or a timeout occurs (if implemented).
        """
        tracking_key = f"{self.tracking_prefix}{id}"
        while True:
            received_chunks = int(self.redis.hget(tracking_key, "received_chunks") or 0)
            if received_chunks == total_chunks:
                self.signal_processing(id)
                return True

    def signal_processing(self, id: str):
        """
        Signal workers that all chunks for a job are ready for processing.
        Sets a Redis flag to indicate readiness.
        """
        tracking_key = f"{self.tracking_prefix}{id}"
        self.redis.hset(tracking_key, "ready_for_processing", 1)

    def is_ready_for_processing(self, id: str) -> bool:
        """
        Check if all chunks for a job are ready for processing.
        Workers can use this method to decide whether to start processing a job.
        """
        tracking_key = f"{self.tracking_prefix}{id}"
        return bool(self.redis.hget(tracking_key, "ready_for_processing"))

    def consolidate_results(self, id: str, total_chunks: int) -> dict:
        """
        Consolidate processed results for a job once all chunks are processed.
        Fetch processed chunks, combine them, and store the final result.
        """
        chunks = []
        for order in range(total_chunks):
            result_key = f"{self.tracking_prefix}{id}:chunk:{order}"
            chunk = self.redis.get(result_key)
            if chunk:
                chunks.append(chunk.decode())  # Assuming chunks are stored as JSON strings
            else:
                return {"error": f"Missing processed chunk for ID: {id}, order: {order}"}

        # Store the consolidated result
        final_result = {"id": id, "chunks": chunks}
        final_result_key = f"{self.final_prefix}{id}"
        self.redis.set(final_result_key, json.dumps(final_result))
        return {"message": f"Final result stored for ID: {id}", "result_key": final_result_key}

    def get_final_result(self, id: str) -> dict:
        """
        Retrieve the final consolidated result for a job.
        """
        final_result_key = f"{self.final_prefix}{id}"
        result = self.redis.get(final_result_key)
        return json.loads(result) if result else {"error": f"Final result not found for ID: {id}"}
