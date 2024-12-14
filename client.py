import uuid
import json
import base64
from datetime import datetime as dt
from kafka import KafkaProducer, KafkaConsumer
from config import KAFKA_BROKER, TASK_TOPIC, RESULT_TOPIC_CLIENT, STATUS_TOPIC_SERVER, STATUS_TOPIC_CLIENT
from threading import Thread
import time

class YADTQClient:

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        self.consumer_result = KafkaConsumer(
            RESULT_TOPIC_CLIENT,
            bootstrap_servers=KAFKA_BROKER,
            # group_id="client_group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            # enable_auto_commit=True,
            # auto_offset_reset="earliest",
        )
        self.consumer_status = KafkaConsumer(
            STATUS_TOPIC_SERVER,
            bootstrap_servers=KAFKA_BROKER,
            # group_id="status_group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            # enable_auto_commit=True,
            # auto_offset_reset="earliest",
        )
        self.client_ID = str(uuid.uuid4())
        self.tasks = {}
        # self.MESSAGE_LIMIT = 1024 * 1024 
        # self.estimated_metadata_size = 1024
        # self.max_chunk_size = self.calculate_max_chunk_size()

    # def calculate_max_chunk_size(self):
    #     overhead_factor = 4 / 3
    #     return int((self.MESSAGE_LIMIT - self.estimated_metadata_size) / overhead_factor)

    def send_task(self, task_type, file_path=None, dst_file_path=None):
        task_id = str(uuid.uuid4())
        task_data = {
            "client_id": self.client_ID,
            "task_id": task_id,
            "task": task_type,
            "timestamp": str(dt.now()),
            "args": {"file_content":""}
        }
        
        self.tasks[task_id] = {}
        self.tasks[task_id]["dst_file_path"] = dst_file_path

        try:
            with open(file_path, "rb") as file:
                file_content = file.read()
                # print(file_content)
                
            task_data["args"]["file_content"] = base64.b64encode(file_content).decode("utf-8")
            self.producer.send(TASK_TOPIC, value=task_data)
            
        except FileNotFoundError:
            print("Invalid File Path")
            return None
        
        except ValueError as e:
            print(e)
            return None

        return task_id
    
    def get_results(self):
        for message in self.consumer_result:
            result_data = message.value
            # print(result_data)
            if result_data["task_id"] in self.tasks.keys():
                task_id = result_data["task_id"]
                dst_file_path = self.tasks[task_id]["dst_file_path"]
                status = result_data["task_status"]
                self.tasks.pop(task_id)
                if status == "success":
                    file_content = result_data["result"]
                    if dst_file_path:
                        try:
                            with open(dst_file_path, "w") as file:
                                file.write(file_content)
                            print(f"\nTask {task_id} \nResult file saved as {dst_file_path}")
                        except Exception as e:
                            print(f"\nTask {task_id} \nError writing to file: {e}")

                elif status == "failed":
                    print(f"\nTask {task_id} \nTask failed: {result_data['error']}")
                
    def request_task_status(self, task_id):
        status_request = {
            "client_id": self.client_ID,
            "task_id": task_id,
            "timestamp": str(dt.now()),
        }
        
        self.producer.send(STATUS_TOPIC_CLIENT, value=status_request)
        print(f"Requested status for Task {task_id}.")
        print()

    def fetch_task_status(self):
        # print("+++++++++=")
        for message_stat in self.consumer_status:
            # print(message_stat)
            response_stat = message_stat.value
            if response_stat.get("client_id") == self.client_ID:
                task_id = response_stat.get("task_id")
                status = response_stat.get("status")
                print("--------------------------------------")
                print(f"Task {task_id} Status: {status}")
                return status    

    def choose_task(self):
        print(
              """\tMenu
              1. Compression
              2. Decompression
              3. Encryption
              4. Check Task Status
              5. Exit
              """
        )
        try :
            ch = int(input("Enter your choice : "))
        except:
            ch = 7

        if ch in [1, 2, 3]:
            file_path = input("Enter path of file to process: ")
            dst_file_path = input("Enter destination path to save result: ")

            print()
            task_type = ["compression", "decompression", "encryption"][ch - 1]
            task_id = self.send_task(task_type, file_path=file_path, dst_file_path=dst_file_path)

            if task_id is None:
                return 1

            print(f"\n~Task {task_id} sent. Waiting for result...")

        elif ch == 4:
            print()
            if (len(self.tasks)) > 0 :
                for x,task_id in enumerate(self.tasks.keys(), start=1):
                    print(f"{x}. {task_id}")
                
                try:
                    task = int(input("Enter Choice : "))
                
                    for taskid ,x in enumerate(self.tasks.keys(), start=1):
                        if x == task:
                            task_id = taskid
                            
                    self.request_task_status(task_id)
                    time.sleep(1)
                    # self.fetch_task_status(task_id)
                    
                except:
                    print("Invalid Input")
            else:
                print("All tasks completed")

        elif ch == 5:
            return 0
        
        else:
            print("Invalid choice.")

        print("\n\t----------------------------------------------------------------\n\n")
        return 1
    
    def start(self):
        ResultThread = Thread(target = self.get_results)
        StatusThread = Thread(target = self.fetch_task_status)
        
        ResultThread.daemon = True 
        StatusThread .daemon = True 
        
        ResultThread.start()
        StatusThread .start()
    
        while client.choose_task():
            pass

# Run the client
if __name__ == "__main__":
    client = YADTQClient()
    client.start()
