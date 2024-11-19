import time
from client import YADTQClient

def make_send():
    client = YADTQClient()
    task_type = input('compress/decompress/encode:')
    inp = input("a,b: ").split(",")
    inp = [int(i) for i in inp]
    task_id = client.send_task(task_type, inp)
    print(f"task id {task_id} added")

    while True:
        res = client.get_result(task_id)
        if res != "task hpnin":
            print(f"task {task_id} res: {res}")
            break
        time.sleep(2)


for _ in range(3):
    make_send()
