from api.server import app
from queue_components.manager import QueueManager
from queue_components.collector import Collector
from worker.worker import Worker
from config.settings import CHUNKS_QUEUE_NAME, WORKER_COUNT
import threading
import time
import uvicorn

def start_worker(worker_id: str):
    """Start a single worker."""
    worker = Worker(queue_name=CHUNKS_QUEUE_NAME, worker_id=worker_id)
    worker.run()

def start_api_server():
    """Start the API server."""
    uvicorn.run(app, host="0.0.0.0", port=8000)

def orchestrate():
    """Orchestrate the queue, workers, and collector."""
    # Configuration
    collection_strategy = "order_then_process"  # Choose strategy: "order_then_process" or "process_then_order"
    collector = Collector(strategy=collection_strategy)
    manager = QueueManager(queue_name=CHUNKS_QUEUE_NAME)
    workers = []

    # Start API server in a thread
    api_thread = threading.Thread(target=start_api_server, daemon=True)
    api_thread.start()

    # Start workers
    for i in range(WORKER_COUNT):
        worker_thread = threading.Thread(target=start_worker, args=(f"worker_{i}",))
        workers.append(worker_thread)
        worker_thread.start()

    # Simulate collector waiting for chunks
    job_id = "job_123"
    total_chunks = 5

    print(f"Waiting for chunks for job: {job_id}")
    if collector.await_chunks(job_id, total_chunks):
        print(f"All chunks for job {job_id} received. Workers will process them.")

    # Wait for workers to complete processing
    time.sleep(3)  # Adjust based on expected workload

    # Retrieve final result
    result_key = f"final_result:{job_id}"
    final_result = collector.redis.get(result_key)
    if final_result:
        print(f"Final result for job {job_id}: {final_result.decode()}")

    # Stop worker threads gracefully
    for worker_thread in workers:
        worker_thread.join()

if __name__ == "__main__":
    orchestrate()
