import json
import time
from kafka import KafkaConsumer
from datetime import datetime as dt
from config import KAFKA_BROKER, LOG_TOPIC


class KafkaLogger:
    def __init__(self, log_file="logs.txt"):
        self.consumer_log = KafkaConsumer(
            LOG_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        )
        self.log_file = log_file
        self.buffer = []
        self.buffer_write_interval = 5  
        self.running = True
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
            print(f"Error writing to log file: {e}")    
            

    def start_logging(self):

        last_write_time = time.time()
        print("Logging...")
        
        try:
            for message in self.consumer_log:
                log_message = message.value
                print("Received log message: ", log_message)
                self.buffer.append(log_message)

                if time.time() - last_write_time >= self.buffer_write_interval:
                    self.write_logs_to_file()
                    last_write_time = time.time()

        except KeyboardInterrupt:
            print("stopping logger...")
            self.write_logs_to_file()
            self.running = False

        except Exception as e:
            print(f"error in Kafka Logger: {e}")
            self.running = False

        finally:
            self.consumer_log.close()
            print("log consumer closed.")




if __name__ == "__main__":
    kafka_logger = KafkaLogger()
    kafka_logger.start_logging()
