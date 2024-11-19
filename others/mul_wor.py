import time
from worker import YADTQWorker

def run_worker():
    worker = YADTQWorker()
    for message in worker.consumer:
        task_data = eval(message.value.decode("utf-8"))
        worker.run_task(task_data)


for _ in range(3):
    run_worker()
