import uuid
import json
import base64
import time
from datetime import datetime as dt
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaTimeoutError
from ftplib import FTP
from config import (
    KAFKA_BROKER,
    TASK_TOPIC,
    RESULT_TOPIC,
    FTP_HOST,
    FTP_USER,
    FTP_PASS,
)

class YADTQClient:
    TIMEOUT = 30
    MAX_RETRIES = 3

    def __init__(self):
        self.client_ID = str(uuid.uuid4())
        #above is shit im(prajwal) adding so we can revet back if needed

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

    def upload_to_ftp(self, file_path, task_id):
        """Uploads a file to the FTP server and returns the uploaded file name."""
        file_name = task_id + "." + file_path.split(".")[-1]
        with FTP(FTP_HOST) as ftp:
            ftp.login(FTP_USER, FTP_PASS)
            with open(file_path, "rb") as file:
                ftp.storbinary(f"STOR {file_name}", file)
        return file_name

    def download_from_ftp(self, ftp_file_name, dst_file_path):
        """Downloads a file from the FTP server."""
        with FTP(FTP_HOST) as ftp:
            ftp.login(FTP_USER, FTP_PASS)
            with open(dst_file_path, "wb") as file:
                ftp.retrbinary(f"RETR {ftp_file_name}", file.write)
        return dst_file_path

    def send_task(self, task_type, file_path=None):
        task_id = str(uuid.uuid4())
        task_data = {"task_id" : task_id, "task" : task_type, "client_id" : self.client_ID, "timestamp" : str(dt.now())}

        if file_path:
            ftp_file_name = self.upload_to_ftp(file_path, task_id)
            task_data["args"] = {"ftp_file_name": ftp_file_name}

        self.producer.send(TASK_TOPIC, value = task_data)
        return task_id

    def get_result(self, task_id, dst_file_path=None):
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
                                    if "ftp_file_name" in result_data:
                                        ftp_file_name = result_data["ftp_file_name"]
                                        self.download_from_ftp(ftp_file_name, dst_file_path)
                                        return f"Result file saved as {dst_file_path}"
                                    else:
                                        return result_data["result"]
                                elif status == "failed":
                                    return f"Task failed: {result_data['error']}"

            # Timeout elapsed, resend the task
            retries += 1
            print(f"Timeout reached. Resending task... (Attempt {retries}/{self.MAX_RETRIES})")
            self.producer.send(TASK_TOPIC, value={"task_id": task_id, "task": "resend"})

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
            task_id = self.send_task(task_type, file_path=file_path)
            print(f"Task {task_id} sent. Waiting for result...")
            res = self.get_result(task_id, dst_file_path=dst_file_path)
            print(f"Result: {res}")
        else:
            print("Invalid choice.")


# Run the client
if __name__ == "__main__":
    client = YADTQClient()
    client.choose_task()
