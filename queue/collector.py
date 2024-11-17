import redis
import json
from config.settings import REDIS_HOST, REDIS_PORT, REDIS_DB, RESULTS_PREFIX, FINAL_RESULTS_PREFIX

class Collector:
    def __init__(self, results_prefix: str = RESULTS_PREFIX, final_prefix: str = FINAL_RESULTS_PREFIX):
        self.redis = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        self.results_prefix = results_prefix
        self.final_prefix = final_prefix

    def collect_and_store(self, id: str, total_chunks: int) -> dict:
        # Fetch all processed chunks
        chunks = []
        for order in range(total_chunks):
            result_key = f"{self.results_prefix}{id}:{order}"
            chunk = self.redis.get(result_key)
            if chunk:
                chunks.append(json.loads(chunk))
            else:
                chunks.append(None)  # Missing chunks get a placeholder

        # Verify all chunks are complete
        if None in chunks:
            return {"error": f"Missing chunks for ID: {id}"}

        # Combine and store the final result
        final_result = {"id": id, "chunks": chunks}
        final_result_key = f"{self.final_prefix}{id}"
        self.redis.set(final_result_key, json.dumps(final_result))

        return {"message": f"Final result stored for ID: {id}", "result_key": final_result_key}

    def get_final_result(self, id: str) -> dict:
        # Retrieve the final combined result
        final_result_key = f"{self.final_prefix}{id}"
        result = self.redis.get(final_result_key)
        return json.loads(result) if result else {"error": f"Final result not found for ID: {id}"}
