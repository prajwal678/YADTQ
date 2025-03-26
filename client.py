import uuid
import json
import base64
from datetime import datetime as dt
from kafka import KafkaProducer, KafkaConsumer
from config import KAFKA_BROKER, KAFKA_TOPICS, TASK_CONFIG


class YADTQClient:

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        self.consumer = KafkaConsumer(
            KAFKA_TOPICS["RESULT_CLIENT"],
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            # group_id="client_group",
            # enable_auto_commit=True,
            # auto_offset_reset="earliest",
        )
        self.ID = str(uuid.uuid4())
        self.MESSAGE_LIMIT = 1024 * 1024
        self.estimated_metadata_size = 1024
        self.max_chunk_size = self.calculate_max_chunk_size()

    def calculate_max_chunk_size(self):
        overhead_factor = 4 / 3
        return int(
            (self.MESSAGE_LIMIT - self.estimated_metadata_size) / overhead_factor
        )

    def write_file(self, content, dst_file_path):
        with open(dst_file_path, "w") as file:
            file.write(content)

    def send_task(self, task_type, file_path=None):
        task_id = str(uuid.uuid4())
        base_task_data = {
            "client_id": self.ID,
            "task_id": task_id,
            "task": task_type,
            "timestamp": str(dt.now()),
            "total_parts": 1,
            "args": {"part": 1, "file_content": ""},
        }
        total_parts = 1

        try:
            with open(file_path, "rb") as file:
                file_content = file.read()
                print(file_content)
            file_size = len(file_content)

            if file_size > self.max_chunk_size:
                chunks = [
                    file_content[i : i + self.max_chunk_size]
                    for i in range(0, file_size, self.max_chunk_size)
                ]
                total_parts = len(chunks)
                base_task_data["total_parts"] = total_parts

                for part_num, chunk in enumerate(chunks, start=1):
                    task_chunk = base_task_data.copy()
                    task_chunk["args"]["part"] = part_num
                    task_chunk["args"]["file_content"] = base64.b64encode(chunk).decode(
                        "utf-8"
                    )

                    # serialized_chunk = json.dumps(task_chunk).encode("utf-8")
                    # if len(serialized_chunk) > self.MESSAGE_LIMIT:
                    #     raise ValueError(f"Message size exceeds 1 MB for part {part_num}. Reduce metadata size or chunk size.")

                    # print(task_chunk)
                    self.producer.send(KAFKA_TOPICS["TASK"], value=task_chunk)
            else:
                base_task_data["args"]["file_content"] = base64.b64encode(
                    file_content
                ).decode("utf-8")
                print(base_task_data)
                self.producer.send(KAFKA_TOPICS["TASK"], value=base_task_data)

            print(f"Task {task_id} sent with {total_parts} part(s).")

        except FileNotFoundError:
            print("Invalid File Path")
            return None

        except ValueError as e:
            print(e)
            return None

        return task_id

    def get_result(self, task_id, dst_file_path=None, task_type=None):
        for message in self.consumer:
            result_data = message.value
            print(result_data)
            if result_data["task_id"] == task_id:
                status = result_data["task_status"]
                if status == "success":
                    file_content = result_data["result"]
                    if dst_file_path:
                        try:
                            # Add proper padding to the Base64 string
                            file_content_padded = file_content + "" * (
                                -len(file_content) % 4
                            )

                            if task_type in ["encryption", "compression"]:
                                self.write_file(file_content, dst_file_path)
                            elif task_type in ["decryption", "decompression"]:
                                with open(dst_file_path, "w") as file:
                                    file.write(file_content_padded)
                            else:
                                self.write_file(file_content, dst_file_path)

                            return f"Result file saved as {dst_file_path}"

                        except Exception as e:
                            return f"Error writing to file: {e}"

                elif status == "failed":
                    return f"Task failed: {result_data['error']}"

    def choose_task(self):
        print(
            """\tMenu
              1. Compression
              2. Decompression
              3. Encryption
              4. Exit
              """
        )
        ch = int(input("Enter your choice : "))

        if ch in [1, 2, 3]:
            file_path = input("Enter path of file to process: ")
            dst_file_path = input("Enter destination path to save result: ")

            print()
            task_type = ["compression", "decompression", "encryption"][ch - 1]
            task_id = self.send_task(task_type, file_path=file_path)

            if task_id is None:
                return 1

            print(f"\n~Task {task_id} sent. Waiting for result...")
            res = self.get_result(task_id, dst_file_path, task_type)
            # print(f"Result: {res}")

        elif ch == 5:
            return 0
        else:
            print("Invalid choice.")

        print(
            "\n\t----------------------------------------------------------------\n\n"
        )
        return 1


# Run the client
if __name__ == "__main__":
    client = YADTQClient()
    while client.choose_task():
        pass
