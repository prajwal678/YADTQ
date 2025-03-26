import redis
from datetime import datetime as dt
import time
import uuid
import signal
import sys
from kafka import KafkaConsumer, KafkaProducer
from config import (
    REDIS_HOST,
    REDIS_PORT,
    KAFKA_BROKER,
    KAFKA_TOPICS,
)
from threading import Thread
import base64, json
import zlib
from cryptography.fernet import Fernet

# redis task format:
# task id, client id, worker id, task type, task status, timestamp,  error

# need to see how it works, please ensure since we are outght to communicate using json shit, some of the thigns might need tweakign to acept n reject, i have made the consumer and producer serialzie and deserialze json data, so work accordingly wihtotu changing that as it makes passing data across the dis. sys. easy

encryption_key = Fernet.generate_key()
cipher = Fernet(encryption_key)


class YADTQWorker:
    def __init__(self, workerID=None):
        self.workerID = workerID or f"w{uuid.uuid4()}"
        self.redis = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.consumer = KafkaConsumer(
            KAFKA_TOPICS["TASK_WORKER"],
            bootstrap_servers=KAFKA_BROKER,
            group_id=f"worker_group_{self.workerID}",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        self.current_task = None
        self.client_id = None
        self.running = True
        self.heartbeat_thread = None
        
        # setup signal handlers
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        print(f"worker {self.workerID} shutting down gracefully...")
        self.running = False
        if self.current_task:
            print(f"abandoning current task {self.current_task}")
            # report that task is abandoned due to shutdown
            self.redis.hset(
                f"task:{self.current_task}",
                mapping={
                    "status": "abandoned",
                    "error": "worker shutdown",
                    "timestamp": str(dt.now()),
                },
            )
        if self.producer:
            self.producer.close()
        if self.consumer:
            self.consumer.close()
        sys.exit(0)

    def send_heartbeat(self):
        while self.running:
            working_status = "employed" if self.current_task else None
            heartbeatData = {
                "worker_id": self.workerID,
                "timestamp": str(dt.now()),
                "current_task": self.current_task,
                "client_id": self.client_id,
                "working_status": working_status,
            }
            try:
                self.producer.send(KAFKA_TOPICS["HEARTBEAT"], value=heartbeatData)
            except Exception as e:
                print(f"error sending heartbeat: {e}")
            time.sleep(5)

    def file_op(self, task, file_data):
        try:
            match task:
                case "compression":
                    processed_data = zlib.compress(file_data.encode("utf-8"))
                    return base64.b64encode(processed_data).decode("utf-8")
                case "decompression":
                    file_str = base64.b64decode(file_data)
                    processed_data = zlib.decompress(file_str)
                    return processed_data.decode("utf-8")
                case "encryption":
                    encrypted_data = cipher.encrypt(file_data.encode("utf-8"))
                    return encrypted_data.decode("utf-8")
                case _:
                    raise ValueError("invalid task type")
        except Exception as e:
            print(f"error in file_op ({task}): {e}")
            raise

    def run_task(self, task_data):
        task_id = task_data.get("task_id")
        task_type = task_data.get("task_type")
        self.client_id = task_data.get("client_id")
        args = task_data.get("args", {})
        
        if not args:
            raise ValueError("missing task arguments")
            
        file_content_base64 = args.get("file_content")
        if not file_content_base64:
            raise ValueError("missing file content in task arguments")

        try:
            file_content = base64.b64decode(file_content_base64)
            file_content_str = file_content.decode("utf-8")
        except Exception as e:
            raise ValueError(f"error decoding file content: {e}")

        # update task status in redis
        self.redis.hset(
            f"task:{task_id}",
            mapping={
                "client_id": self.client_id,
                "worker_id": self.workerID,
                "type": task_type,
                "status": "processing",
                "timestamp": str(dt.now()),
                "error": "",
            },
        )

        try:
            print(f"processing task {task_id}: {task_type}")
            self.current_task = task_id
            processed_data = self.file_op(task_type, file_content_str)

            # simulate work for testing purposes
            time.sleep(10)
            
            print(f"finished task {task_id}: {task_type}")
            
            result_data = {
                "task_id": task_id,
                "client_id": self.client_id,
                "worker_id": self.workerID,
                "task_status": "success",
                "result": processed_data,
            }

            self.producer.send(KAFKA_TOPICS["RESULT"], value=result_data)
            self.current_task = None
            self.client_id = None
            return True
            
        except Exception as e:
            err = str(e)
            print(f"error processing task {task_id}: {err}")
            result_data = {
                "task_id": task_id,
                "task_status": "failed",
                "client_id": self.client_id,
                "worker_id": self.workerID,
                "error": err,
            }
            self.producer.send(KAFKA_TOPICS["RESULT"], value=result_data)

            # update task status in redis
            self.redis.hset(
                f"task:{task_id}",
                mapping={
                    "client_id": self.client_id,
                    "worker_id": self.workerID,
                    "type": task_type,
                    "status": "failed",
                    "timestamp": str(dt.now()),
                    "error": err,
                },
            )

            self.current_task = None
            self.client_id = None
            return False

    def start(self):
        print(f"worker {self.workerID} started")
        self.heartbeat_thread = Thread(target=self.send_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                    
                task_data = message.value
                if task_data["wID"] == self.workerID:
                    try:
                        self.run_task(task_data)
                    except Exception as e:
                        print(f"error running task: {e}")
                        
        except KeyboardInterrupt:
            print("worker interrupted, shutting down")
            self.running = False
            
        except Exception as e:
            print(f"unexpected error in worker: {e}")
            self.running = False
            
        finally:
            if self.producer:
                self.producer.close()
            if self.consumer:
                self.consumer.close()
            print(f"worker {self.workerID} stopped")

if __name__ == "__main__":
    worker = YADTQWorker()
    worker.start()
