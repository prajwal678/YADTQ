import os
import socket

# Check if we're running inside Docker or locally
def is_running_in_docker():
    try:
        with open('/proc/self/cgroup', 'r') as f:
            return any('docker' in line for line in f)
    except:
        return False

# Kafka configuration
# If running locally, use localhost instead of the Docker service name
if is_running_in_docker():
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
else:
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# Kafka topics
KAFKA_TOPICS = {
    "TASK": "TASK_TOPIC",
    "TASK_WORKER": "TASK_TOPIC_WORKER",
    "RESULT": "RESULT_TOPIC",
    "RESULT_CLIENT": "RESULT_TOPIC_CLIENT",
    "HEARTBEAT": "HEARTBEAT_TOPIC",
    "LOG": "LOG_TOPIC",
    "STATUS_CLIENT": "STATUS_TOPIC_CLIENT",
    "STATUS_SERVER": "STATUS_TOPIC_SERVER",
}

# For backward compatibility
TASK_TOPIC = KAFKA_TOPICS["TASK"]
TASK_TOPIC_WORKER = KAFKA_TOPICS["TASK_WORKER"]
RESULT_TOPIC = KAFKA_TOPICS["RESULT"]
RESULT_TOPIC_CLIENT = KAFKA_TOPICS["RESULT_CLIENT"]
HEARTBEAT_TOPIC = KAFKA_TOPICS["HEARTBEAT"]
LOG_TOPIC = KAFKA_TOPICS["LOG"]
STATUS_TOPIC_CLIENT = KAFKA_TOPICS["STATUS_CLIENT"]
STATUS_TOPIC_SERVER = KAFKA_TOPICS["STATUS_SERVER"]

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_CONFIG = {
    "HOST": REDIS_HOST,
    "PORT": REDIS_PORT,
    "DB": int(os.getenv("REDIS_DB", 0)),
    "DECODE_RESPONSES": True,
}

# Worker configuration
WORKER_CONFIG = {
    "TOTAL_WORKERS": int(os.getenv("TOTAL_WORKERS", 4)),
    "HEARTBEAT_INTERVAL": int(os.getenv("WORKER_HEARTBEAT_INTERVAL", 5)),
    "TIMEOUT": int(os.getenv("WORKER_TIMEOUT", 30)),
}
NO_OF_WORKERS = WORKER_CONFIG["TOTAL_WORKERS"]

# Task configuration
TASK_CONFIG = {
    "MAX_RETRIES": int(os.getenv("TASK_MAX_RETRIES", 3)),
    "RETRY_DELAY": int(os.getenv("TASK_RETRY_DELAY", 5)),
    "MESSAGE_SIZE_LIMIT": 1024 * 1024,  # 1MB
}

# Logging configuration
LOG_CONFIG = {
    "FILE": os.getenv("LOG_FILE", "logs/yadtq.log"),
    "BUFFER_WRITE_INTERVAL": int(os.getenv("LOG_BUFFER_INTERVAL", 5)),
}

# FTP configuration (if needed)
FTP_CONFIG = {
    "HOST": os.getenv("FTP_HOST", "localhost"),
    "USER": os.getenv("FTP_USER", "ftpuser"),
    "PASS": os.getenv("FTP_PASS", "123"),
}
