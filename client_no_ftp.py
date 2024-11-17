import uuid
import json
import base64
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaTimeoutError
from config import KAFKA_BROKER, TASK_TOPIC, RESULT_TOPIC


class YADTQClient:
    TIMEOUT = 30  # Timeout in seconds to wait for a result before resending
    MAX_RETRIES = 3  # Maximum number of retries for resending tasks

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        self.consumer = KafkaConsumer(
            RESULT_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id="client_group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        self.ID = str(uuid.uuid4())

    def read_file(self, file_path):
        """Reads the content of a file and encodes it in Base64."""
        with open(file_path, "rb") as file:
            return base64.b64encode(file.read()).decode("utf-8")

    def write_file(self, content, dst_file_path):
        """Decodes Base64 content and writes it to a file."""
        with open(dst_file_path, "wb") as file:
            file.write(base64.b64decode(content.encode("utf-8")))

    def send_task(self, task_type, file_path=None):
        task_id = str(uuid.uuid4())
        task_data = {"client_id": self.ID, "task_id": task_id, "task": task_type}

        if file_path:
            file_content = self.read_file(file_path)
            task_data["args"] = {"file_content": file_content}

        self.producer.send(TASK_TOPIC, value=task_data)
        return task_id, task_data

    def get_result(self, task_id, dst_file_path=None, task_data=None):
        retries = 0

        while retries < self.MAX_RETRIES:
            start_time = time.time()

            while time.time() - start_time < self.TIMEOUT:
                message = self.consumer.poll(timeout_ms=1000)
                if message:
                    for _, records in message.items():
                        for record in records:
                            result_data = record.value
                            if result_data["task_id"] == task_id:
                                status = result_data["status"]
                                if status == "success":
                                    if "file_content" in result_data:
                                        file_content = result_data["file_content"]
                                        self.write_file(file_content, dst_file_path)
                                        return f"Result file saved as {dst_file_path}"
                                    else:
                                        return result_data["result"]
                                elif status == "failed":
                                    return f"Task failed: {result_data['error']}"

            # Timeout elapsed, resend the task
            retries += 1
            print(f"Timeout reached. Resending task... (Attempt {retries}/{self.MAX_RETRIES})")
            self.producer.send(
                TASK_TOPIC, value=task_data
            )

        return "Task failed: Max retries exceeded."

    def choose_task(self):
        print(
            """\tMenu
              1. Encode
              2. Decode
              3. Compression
              4. Decompression
              """
        )
        ch = int(input("Enter your choice : "))

        if ch in [1, 2, 3, 4]:
            file_path = input("Enter path of file to process: ")
            dst_file_path = input("Enter destination path to save result: ")
            
            task_type = ["encode", "decode", "compression", "decompression"][ch - 1]
            task_id, task_data = self.send_task(task_type, file_path=file_path)
            
            print(f"Task {task_id} sent. Waiting for result...")
            res = self.get_result(task_id, dst_file_path=dst_file_path, task_data=task_data)
            print(f"Result: {res}")
        else:
            print("Invalid choice.")


# Run the client
if __name__ == "__main__":
    client = YADTQClient()
    client.choose_task()
