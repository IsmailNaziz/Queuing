# Redis Configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# API Server Configuration
API_HOST = "0.0.0.0"
API_PORT = 8000

# Queue Configuration
CHUNKS_QUEUE_NAME = "chunks_queue"
RESULTS_PREFIX = "processed_result:"
FINAL_RESULTS_PREFIX = "final_result:"
TRACKING_PREFIX = "chunk_tracking:"

# Worker Configuration
WORKER_COUNT = 2  # Number of worker threads
WORKER_SLEEP_TIME = 1  # Time (in seconds) to wait if no chunks available

# Logging Configuration
LOG_FILE = "logs/app.log"
LOG_LEVEL = "INFO"
