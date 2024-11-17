import time
import threading
import json
from api.server import app
from fastapi.testclient import TestClient
from queue_components.manager import QueueManager
from queue_components.collector import Collector
from worker.worker import Worker
from config.settings import CHUNKS_QUEUE_NAME, WORKER_COUNT

client = TestClient(app)

def start_worker(worker_id: str):
    """Start a single worker."""
    worker = Worker(queue_name=CHUNKS_QUEUE_NAME, worker_id=worker_id)
    worker.run()

def test_end_to_end():
    """End-to-end test for processing chunks."""
    # Initialize components
    collector = Collector()
    manager = QueueManager(queue_name=CHUNKS_QUEUE_NAME)
    workers = []

    # Start workers in threads
    for i in range(WORKER_COUNT):
        worker_thread = threading.Thread(target=start_worker, args=(f"worker_{i}",), daemon=True)
        workers.append(worker_thread)
        worker_thread.start()

    # Define job details
    job_id = "test_job"
    total_chunks = 3
    chunks = [
        {"id": job_id, "chunk": "data_1", "order": 0, "isLastChunk": False},
        {"id": job_id, "chunk": "data_2", "order": 1, "isLastChunk": False},
        {"id": job_id, "chunk": "data_3", "order": 2, "isLastChunk": True},
    ]

    # Send chunks via API
    for chunk in chunks:
        response = client.post("/chunks/", json=chunk)
        assert response.status_code == 200
        assert response.json() == {"message": "Chunk received successfully"}

    # Wait for all chunks to be processed
    assert collector.await_chunks(job_id, total_chunks)

    # Wait for workers to process the job
    time.sleep(2)

    # Retrieve and validate the final result
    final_result = collector.get_final_result(job_id)
    assert "error" not in final_result
    assert final_result["id"] == job_id
    assert len(final_result["chunks"]) == total_chunks
    assert final_result["chunks"] == ["data_1", "data_2", "data_3"]

    # Stop worker threads
    for worker_thread in workers:
        worker_thread.join(timeout=1)
