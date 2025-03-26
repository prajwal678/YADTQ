import subprocess
import time
import os
import signal
import sys
import json
from kafka import KafkaConsumer, KafkaProducer
from config import KAFKA_BROKER, KAFKA_TOPICS, WORKER_CONFIG

class WorkerManager:
    def __init__(self):
        self.workers = {}
        self.min_workers = WORKER_CONFIG.get("TOTAL_WORKERS", 4)
        self.max_workers = int(os.environ.get("MAX_WORKERS", 8))
        self.worker_timeout = WORKER_CONFIG.get("TIMEOUT", 30)
        self.consumer = KafkaConsumer(
            KAFKA_TOPICS["HEARTBEAT"],
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            group_id="worker_manager_group"
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
        self.running = True
        self.worker_heartbeats = {}

    def start_worker(self):
        process = subprocess.Popen(["python", "worker.py"])
        worker_id = f"w{process.pid}"
        self.workers[worker_id] = {
            "process": process,
            "pid": process.pid,
            "start_time": time.time(),
            "status": "starting"
        }
        print(f"started worker {worker_id} with pid {process.pid}")
        return worker_id

    def stop_worker(self, worker_id):
        if worker_id in self.workers:
            worker = self.workers[worker_id]
            try:
                os.kill(worker["pid"], signal.SIGTERM)
                print(f"stopped worker {worker_id}")
            except ProcessLookupError:
                print(f"worker {worker_id} already dead")
            except Exception as e:
                print(f"error stopping worker {worker_id}: {e}")
            finally:
                if worker_id in self.workers:
                    del self.workers[worker_id]

    def ensure_min_workers(self):
        while len(self.workers) < self.min_workers:
            self.start_worker()
            time.sleep(1)  # prevent starting too many workers at once

    def monitor_workers(self):
        while self.running:
            # check heartbeats
            for worker_id, last_heartbeat in list(self.worker_heartbeats.items()):
                if time.time() - last_heartbeat > self.worker_timeout:
                    print(f"worker {worker_id} heartbeat timeout, restarting")
                    if worker_id in self.workers:
                        self.stop_worker(worker_id)
                    del self.worker_heartbeats[worker_id]

            # check process status
            for worker_id, worker in list(self.workers.items()):
                if worker["process"].poll() is not None:
                    print(f"worker {worker_id} died, removing from active workers")
                    del self.workers[worker_id]
                    if worker_id in self.worker_heartbeats:
                        del self.worker_heartbeats[worker_id]

            # ensure minimum workers
            self.ensure_min_workers()

            # record new workers in heartbeat tracker
            for worker_id in self.workers:
                if worker_id not in self.worker_heartbeats:
                    self.worker_heartbeats[worker_id] = time.time()

            time.sleep(5)

    def process_heartbeats(self):
        for message in self.consumer:
            if not self.running:
                break
                
            try:
                heartbeat = message.value
                worker_id = heartbeat.get("worker_id")
                if worker_id:
                    self.worker_heartbeats[worker_id] = time.time()
            except Exception as e:
                print(f"error processing heartbeat: {e}")

    def start(self):
        print("starting worker manager")
        # start initial workers
        self.ensure_min_workers()
        
        try:
            # start monitoring thread
            import threading
            monitor_thread = threading.Thread(target=self.monitor_workers)
            monitor_thread.daemon = True
            monitor_thread.start()
            
            # process heartbeats in main thread
            self.process_heartbeats()
        except KeyboardInterrupt:
            print("shutting down worker manager")
            self.running = False
            for worker_id in list(self.workers.keys()):
                self.stop_worker(worker_id)

if __name__ == "__main__":
    manager = WorkerManager()
    manager.start() 