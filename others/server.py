import redis
from threading import Thread, Lock
from kafka import KafkaProducer, KafkaConsumer
import time, json
from datetime import datetime as dt, timedelta
from config import REDIS_HOST, REDIS_PORT, KAFKA_BROKER, TASK_TOPIC, RESULT_TOPIC, HEARTBEAT_TOPIC


class YADTQServer:
    def __init__(self):
        self.redis = redis.Redis(host = REDIS_HOST, port = REDIS_PORT, decode_responses = True)
        # real smart way to encode off when its loaded only
        self.producer = KafkaProducer(bootstrap_servers = KAFKA_BROKER, value_serializer = lambda x: json.dumps(x).encode('utf-8'))
        self.consumer_for_task = KafkaConsumer(TASK_TOPIC, bootstrap_servers = KAFKA_BROKER, group_id = "server_group", value_deserializer = lambda x: json.loads(x.decode('utf-8')))
        self.consumer_for_heartbeat = KafkaConsumer(HEARTBEAT_TOPIC, bootstrap_servers = KAFKA_BROKER, group_id = 'heartbeat_server_group', value_deserializer = lambda x: json.loads(x.decode('utf-8')))
        self.active_workers = {}
        self.timeout = 12 # because I DID NOT CONSIDER THAT SERVER CAN ALSO MESS UP
        self.retry_max = 3
        self.retry_delay = 7

        #yea thats right ur boi wasted almost an hour cuz HE DID NOT LOCK THREADS OMG THIS IS NOT C++, AUGHHH
        self.running = True
        self.lock = Lock()


    def process_task(self, task_data):
        task_id = task_data["task_id"]
        try:
            print(f"doing task {task_id}: {task_data['task']} with {task_data['args']}")
            retry_count = task_data.get('retry_count', 0)
            self.redis.hset(f"task : {task_id}", mapping={"status" : "processing", "start_time" : str(dt.now()), "type" : task_data['task'], "retry_count" : retry_count})
            self.producer.send(TASK_TOPIC, value = task_data)
        except Exception as e:
            print(f"task processing error for {task_id}:{e}")
            self.redis.hset(f"task : {task_id}", mapping={"status" : "failed", "error" : str(e)})


    def monitor_heartbeats(self):
        while self.running:
            for msg in self.consumer_for_heartbeat:
                if self.running == False:
                    break
                try:
                    heartbeat = msg.value
                    w_id = heartbeat['worker_id']
                    current_task = heartbeat.get('current_task')
                    
                    self.active_workers[w_id] = {'last_heartbeat' : time.time(), 'status' : 'active', 'current_task' : current_task}
                    now_time = time.time()
                    timeout_workers = [w_id for w_id, w_data in self.active_workers.items() if now_time - w_data['last_heartbeat'] > self.worker_timeout]

                    for w_id in timeout_workers:
                        print(f"worker {w_id} timed out")
                        w_data = self.active_workers.pop(w_id)
                        
                        if 'current_task' in w_data and w_data['current_task']:
                            task_id = w_data['current_task']
                            print(f"workwr {w_id} failed processing task {task_id}")
                            self.retry_task(task_id)
                except Exception as e:
                    print(f"error heartbeat monitoring: {e}")


    def monitor_tasks(self):
        while self.running:
            for msg in self.consumer_for_task:
                if self.running == False:
                    break
                try:
                    task_data = msg.value
                    task_id = task_data["task_id"]

                    if not self.redis.exists(task_id):
                        print(f"task {task_id} not in redis, retrying")
                        self.redis.hset(f"task : {task_id}", mapping={"status" : "queued", "created_at" : str(dt.now()), "retry_count" : 0})

                    task_status = self.redis.hget(f"task : {task_id}", "status")
                    if task_status == "queued":
                        self.process_task(task_data)
                except Exception as e:
                    print(f"error reading msg: {e}")
                
                time.sleep(2)


    def monitor_zombie_tasks(self): # shit thats tehre but not executing
        while self.running:
            try:
                tasks_in_progress = []
                for key in self.redis.scan_iter("task : *"):
                    task_data = self.redis.hgetall(key)
                    if task_data.get('status') == 'processing':
                        task_id = key.split(':')[1]
                        tasks_in_progress.append((task_id, task_data))

                now_time = dt.now()
                for task_id, task_data in tasks_in_progress:
                    start_time = dt.fromisoformat(task_data.get('start_time', ''))
                    runtime = now_time - start_time
                    
                    if runtime > timedelta(seconds = self.worker_timeout):
                        print(f"task {task_id} stuck for {runtime}")
                        
                        w_id = task_data.get('worker_id')
                        if not w_id or w_id not in self.active_workers:
                            print(f"worker for {task_id} not active retrying")
                            self.retry_task(task_id)
                        else:
                            w_data = self.active_workers[w_id]
                            last_heartbeat = now_time - timedelta(seconds = now_time.timestamp() - w_data['last_heartbeat'])
                            
                            if last_heartbeat > timedelta(seconds = self.worker_timeout):
                                print(f"worker {w_id} dead lmao retrying {task_id}")
                                self.retry_task(task_id)
            except Exception as e:
                print(f"error in staggered tasks: {e}")
            
            time.sleep(self.retry_delay)


    def retry_task(self, task_id):
        try:
            task_data = self.redis.hgetall(f"task : {task_id}") # my monkey brain cant think of a better way to id it without changing all the work i hv already done
            retry_count = int(task_data.get('retry_count', 0))
            if retry_count >= self.max_retries:
                print(f"task {task_id} max retries done")
                self.redis.hset(f"task : {task_id}", mapping={"status" : "failed", "error" : "Exceeded maximum retry attempts"})
                return False

            self.redis.hset(f"task : {task_id}", mapping={"status" : "queued", "retry_count" : retry_count + 1, "last_retry" : str(dt.now())})
            task_message = {"task_id" : task_id, "task" : task_data['type'], "args" : task_data.get('args', ''), "retry_count" : retry_count + 1}
            
            print(f"retrying task {task_id} [attempt {retry_count + 1}]")
            self.producer.send(TASK_TOPIC, value = task_message)
            return True
        except Exception as e:
            print(f"error in retrying itself, task {task_id}: {e}")
            return False
        

    def start(self):
        print("started yadtq server")
        monitor_thread = Thread(target = self.monitor_tasks)
        heartbeat_thread = Thread(target = self.monitor_heartbeats)
        stuck_thread = Thread(target = self.monitor_zombie_tasks)

        monitor_thread.daemon = True
        heartbeat_thread.daemon = True
        stuck_thread.daemon = True
        monitor_thread.start()
        heartbeat_thread.start()
        stuck_thread.start()

        try:
            while True:
                print('workers ->')
                for w_id, w_data in self.active_workers.items():
                    print(f"worker {w_id}: "f"Last heartbeat {time.time() - w_data['last_heartbeat']:.1f}s ago, "f"Current task: {w_data.get('current_task', 'None')}")
                time.sleep(5)
        except KeyboardInterrupt:
            self.running = False
            self.producer.close()
            self.consumer_for_task.close()
            self.consumer_for_heartbeat.close()


server = YADTQServer()
server.start()
