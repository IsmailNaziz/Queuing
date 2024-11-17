from config.settings import WORKER_COUNT, CHUNKS_QUEUE_NAME
from api.server import app
from queue.collector import Collector
from worker import Worker
import threading
import time


def start_worker(queue_name, worker_id):
    """Start a worker."""
    worker = Worker(queue_name=queue_name, worker_id=worker_id)
    worker.run()

def start_api_server():
    """Start the API server."""
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

def orchestrate():
    """Orchestrate the system components."""
    # Configuration
    queue_name = "chunks_queue"
    total_chunks = 5  # For simulation
    collector = Collector()
    manager = QueueManager(queue_name=queue_name)

    # Start workers
    workers = []
    for i in range(2):  # Two workers for demonstration
        worker_thread = threading.Thread(target=start_worker, args=(queue_name, f"worker_{i}"))
        workers.append(worker_thread)
        worker_thread.start()

    # Simulate incoming chunks
    for i in range(total_chunks):
        chunk = {"id": "job_123", "chunk": f"chunk_{i}", "order": i, "isLastChunk": i == total_chunks - 1}
        manager.push(chunk)
        time.sleep(0.1)  # Simulate staggered chunk arrival

    # Simulate collection
    time.sleep(3)  # Wait for workers to process
    result = collector.collect_and_store("job_123", total_chunks)
    print(result)

    # Retrieve final result
    final_result = collector.get_final_result("job_123")
    print(final_result)

    # Clean up workers
    for worker_thread in workers:
        worker_thread.join()

if __name__ == "__main__":
    # Start the orchestrator
    orchestrate()
