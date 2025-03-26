import json
import time
from kafka import KafkaConsumer
from datetime import datetime as dt
from config import KAFKA_BROKER, KAFKA_TOPICS
import os


class KafkaLogger:
    def __init__(self, log_file="logs.txt"):
        self.consumer_log = KafkaConsumer(
            KAFKA_TOPICS["LOG"],
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        self.log_file = log_file
        self.buffer = []
        self.buffer_write_interval = 5
        self.running = True
        # ensure logs directory exists
        os.makedirs(os.path.dirname(log_file) or '.', exist_ok=True)
        with open(log_file, "w") as _:
            pass

    def write_logs_to_file(self):
        if not self.buffer:
            return

        try:
            with open(self.log_file, "a") as log_file:
                for log_entry in self.buffer:
                    log_file.write(f"{json.dumps(log_entry, indent=4)}\n")

            self.buffer = []

        except Exception as e:
            print(f"error writing to log file: {e}")

    def start_logging(self):
        last_write_time = time.time()
        print("logging started...")

        try:
            for message in self.consumer_log:
                if not self.running:
                    break
                    
                try:
                    log_message = message.value
                    print("received log message: ", log_message)
                    self.buffer.append(log_message)

                    if time.time() - last_write_time >= self.buffer_write_interval:
                        self.write_logs_to_file()
                        last_write_time = time.time()
                except Exception as e:
                    print(f"error processing log message: {e}")

        except KeyboardInterrupt:
            print("stopping logger...")
            self.write_logs_to_file()
            self.running = False

        except Exception as e:
            print(f"error in kafka logger: {e}")
            self.running = False

        finally:
            self.consumer_log.close()
            print("log consumer closed.")


if __name__ == "__main__":
    kafka_logger = KafkaLogger("logs/yadtq.log")
    kafka_logger.start_logging()
