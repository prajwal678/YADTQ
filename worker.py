import redis
from datetime import datetime as dt
import time
import uuid
from kafka import KafkaConsumer, KafkaProducer
from config import REDIS_HOST, REDIS_PORT, KAFKA_BROKER, TASK_TOPIC_WORKER, RESULT_TOPIC, HEARTBEAT_TOPIC
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
    def __init__(self, workerID):
        self.workerID = workerID
        self.redis = redis.Redis(host = REDIS_HOST, port = REDIS_PORT, decode_responses = True)
        self.consumer = KafkaConsumer(
                                      TASK_TOPIC_WORKER,
                                      bootstrap_servers = KAFKA_BROKER,
                                      group_id = f"worker_group_{self.workerID}",
                                      value_deserializer = lambda x: json.loads(x.decode('utf-8'))
        )
        self.producer = KafkaProducer(
                                      bootstrap_servers = KAFKA_BROKER,
                                      value_serializer = lambda x: json.dumps(x).encode('utf-8')
        )
        self.current_task = None
        self.client_id = None

    # ok shit had to do it one under another, aghhhh
    def send_heartbeat(self):
        while True:
            working_status = "employed" if self.current_task != None else None
            heartbeatData = {
                            "worker_id" : self.workerID,
                            "timestamp" : str(dt.now()),
                            "current_task" : self.current_task,
                            "client_id" : self.client_id,
                            "working_status" : working_status
            } # lil redundant to send current_task as well as working status but makes it clean and easy so let it be 
            try:
                self.producer.send(HEARTBEAT_TOPIC, value = heartbeatData)
                #print(f"Heartbeat sent: {heartbeatData}")
            except Exception as e:
                print(f"error: ", e)
            time.sleep(5) 

    def file_op(self, task, file_data):
        match task:
            case "compression":
                processed_data = zlib.compress(file_data.encode('utf-8'))
                return base64.b64encode(processed_data).decode('utf-8')
            case "decompression":
                file_str = base64.b64decode(file_data)
                processed_data = zlib.decompress(file_str)
                return processed_data.decode('utf-8')
            case "encryption":
                encrypted_data = cipher.encrypt(file_data.encode('utf-8'))
                return encrypted_data.decode('utf-8') 
            case _:
                raise ValueError("Invalid task type")


    def run_task(self, task_data):

        task_id = task_data.get("task_id")
        task_type = task_data.get("task_type")
        self.client_id = task_data.get("client_id")
        args = task_data.get("args")
        file_content = base64.b64decode(args["file_content"]) 
        file_content_str = file_content.decode("utf-8")

        # print(file_content_str, type(file_content_str))
        # print("---------------------------------------")

        # task id, client id, worker id, task type, task status, timestamp,  error
        self.redis.hset(f"task:{task_id}", mapping={"client_id": self.client_id,"worker_id": self.workerID, "type": task_type,"status": "processing", "timestamp": str(dt.now()), "error": ""})
        
        try:
            print(f"doing task {task_id}: {task_data['task_type']} with {file_content_str}")
            self.current_task = task_id
            processed_data = self.file_op(task_type, file_content_str)
            
            time.sleep(20)
            print(f"Finished task {task_id}: {task_data['task_type']} with {task_data['args']}")
            # self.client_id = None
            # little confusion, have to rename status as task_status or wtv status prply, and other things n all
            result_data = {
                          "task_id" : task_id,
                          "client_id" : self.client_id,
                          "worker_id" : self.workerID,
                          "task_status" : "success",
                          "result" : processed_data
            }

            # task id, client id, worker id, task type, task status, timestamp,  error
            #self.redis.hset(f"task:{task_id}", mapping={"client_id": self.client_id,"worker_id": self.workerID, "type": task_type,"status": "success", "timestamp": str(dt.now()), "error": ""})
        
            # print("result: ", processed_data)
            # print("type: ", type(processed_data))
            # print("---------------------------------------")
            #ordering matter
            self.producer.send(RESULT_TOPIC, value = result_data)
            # self.redis.hset(
            #                 f"task : {task_id}",
            #                 mapping = {"status" : "success", "type" : task_type, "timestamp" : str(dt.now()), "error" : "", "result" : processed_data}
            # )
            self.current_task = None
            self.client_id = None
        except Exception as e:
            err = str(e)
            print(f"Error: {err}")
            result_data = {
                           "task_id" : task_id,
                           "status" : "failed",
                           "client_id" : self.client_id,
                           "worker_id" : self.workerID,
                           "error" : err
            }
            self.producer.send(RESULT_TOPIC, value = result_data)
            
            # task id, client id, worker id, task type, task status, timestamp,  error
            self.redis.hset(f"task:{task_id}", mapping={"client_id": self.client_id,"worker_id": self.workerID, "type": task_type,"status": "failed", "timestamp": str(dt.now()), "error": err})
            
            self.current_task = None
            self.client_id = None
        

            # self.redis.hset(
            #                 f"task:{task_id}",
            #                 mapping={"status": "failed", "type": task_type, "timestamp": str(dt.now()), "error": err}
            # )


    def start(self):
        print(f"worker {self.workerID} start")
        heartThread = Thread(target = self.send_heartbeat)
        heartThread.daemon = True 
        heartThread.start()
        try:
            for message in self.consumer:
                task_data = message.value
                print(task_data)
                if(task_data["wID"] == self.workerID):
                    self.run_task(task_data)
        except KeyboardInterrupt:
            self.producer.close()
            self.consumer.close()


wID = "w" + str(uuid.uuid4()) 
worker = YADTQWorker(workerID = wID)
worker.start()
