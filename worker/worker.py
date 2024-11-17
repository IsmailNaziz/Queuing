import redis
import json
import time
from queue_components.manager import QueueManager
from queue_components.tasks import process_chunk
from config.settings import TRACKING_PREFIX, RESULTS_PREFIX

class Worker:
    def __init__(self, queue_name: str, worker_id: str):
        self.manager = QueueManager(queue_name=queue_name)
        self.redis = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.worker_id = worker_id
        self.running = True
        self.lock_prefix = "chunk_lock:"

    def run(self):
        while self.running:
            # Check jobs ready for processing
            jobs = self._get_ready_jobs()
            for job_id in jobs:
                self._process_job(job_id)
            time.sleep(1)  # Sleep to prevent busy looping

    def _get_ready_jobs(self) -> list:
        """Fetch jobs ready for processing."""
        keys = self.redis.scan_iter(f"{TRACKING_PREFIX}*")
        ready_jobs = []
        for key in keys:
            job_id = key.decode().replace(TRACKING_PREFIX, "")
            if self.redis.hget(key, "ready_for_processing"):
                ready_jobs.append(job_id)
        return ready_jobs

    def _process_job(self, job_id: str):
        """Process all chunks of a given job."""
        tracking_key = f"{TRACKING_PREFIX}{job_id}"
        total_chunks = int(self.redis.hget(tracking_key, "total_chunks") or 0)

        # Process chunks in order
        for order in range(total_chunks):
            chunk_key = f"{TRACKING_PREFIX}{job_id}:chunk:{order}"
            chunk = self.redis.get(chunk_key)
            if chunk:
                processed_chunk = process_chunk(json.loads(chunk))
                result_key = f"{RESULTS_PREFIX}{job_id}:{order}"
                self.redis.set(result_key, json.dumps(processed_chunk))

        # Mark job as completed
        self.redis.hset(tracking_key, "completed", 1)

    def stop(self):
        self.running = False
