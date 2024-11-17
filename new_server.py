import redis
from threading import Thread, Lock
from kafka import KafkaProducer, KafkaConsumer
import time, json
from datetime import datetime as dt, timedelta
from config import REDIS_HOST, REDIS_PORT, KAFKA_BROKER, LOG_TOPIC, TASK_TOPIC, TASK_TOPIC_WORKER, RESULT_TOPIC, HEARTBEAT_TOPIC, NO_OF_WORKERS

# change args across all to file_content
#loads or dumps check maadi
class YADTQServer:
    def __init__(self):
        self.redis = redis.Redis(host = REDIS_HOST, port = REDIS_PORT, decode_responses = True)
        # real smart way to encode off when its loaded only
        # ok shit had to do it one under another, aghhhh
        self.producer = KafkaProducer(
                                      bootstrap_servers = KAFKA_BROKER,
                                      value_serializer = lambda x: json.dumps(x).encode('utf-8')
        )
        self.consumer_for_task = KafkaConsumer(
                                               TASK_TOPIC,
                                               bootstrap_servers = KAFKA_BROKER,
                                               group_id = "server_group",
                                               value_deserializer = lambda x: json.loads(x.decode('utf-8'))
        )
        self.consumer_for_result = KafkaConsumer(
                                               RESULT_TOPIC,
                                               bootstrap_servers = KAFKA_BROKER,
                                               group_id = "server_group",
                                               value_deserializer = lambda x: json.loads(x.decode('utf-8'))
        )
        self.consumer_for_heartbeat = KafkaConsumer(
                                                    HEARTBEAT_TOPIC,
                                                    bootstrap_servers = KAFKA_BROKER,
                                                    group_id = 'heartbeat_server_group',
                                                    value_deserializer = lambda x: json.loads(x.decode('utf-8'))
        )

        self.active_workers = {}
        self.available_workers = set()
        self.worker_states = {}
        self.running = True
        self.lock = Lock()

        # some variables might be useless need to recheck i defined them before writing code
        self.heartbeat_interval_time = 15
        self.verify_timeout = 10 #lil random
        self.worker_timeout = 12 # because I DID NOT CONSIDER THAT SERVER CAN ALSO MESS UP

        self.retry_max = 3
        self.retry_delay = 7
        # can use this as a direct varibale rn its useless
        # self.free_workers = NO_OF_WORKERS

        #yea thats right ur boi wasted almost an hour cuz HE DID NOT LOCK THREADS OMG THIS IS NOT C++, AUGHHH

    def verify_worker(self, worker_id):
        try:
            verification_msg = {
                                'type' : 'ping',
                                'timestamp' : str(dt.now())
            }
            self.producer.send(
                               f"worker {worker_id} verification msg", value = verification_msg
            )

            start_time = time.time()
            while (time.time() - start_time) < self.verify_timeout:
                for msg in self.consumer_for_heartbeat:
                    if (msg.value.get('worker_id') == worker_id) and (msg.value.get('type') == 'ping'):
                        print(f"worker {worker_id} ping saksus")
                        return True

                time.sleep(0.5)          
            print(f"worker {worker_id} ping FAIL")
            return False
        except Exception as e:
            print(f"verification for {worker_id} fail")
            return False


    def update_worker_state(self, worker_id, heartbeat_data):
        with self.lock:
            now_time = dt.now()
            working_status = heartbeat_data.get('working_status')
            current_task = heartbeat_data.get('current_task')

            if worker_id not in self.worker_states:
                self.worker_states[worker_id] = {
                                                 "last_seen" : str(now_time),
                                                 "status" : "active",
                                                 'current_task' : None,
                                                 'ver_tries' : 0
                }
            else:
                self.worker_states[worker_id].update({
                                                      'last_seen' : str(now_time),
                                                      'status' : "active",
                })

            if (working_status) and (current_task):
                self.active_workers[worker_id] = {
                                                  'last_seen': time.time(),
                                                  'status': 'active',
                                                  'current_task': current_task
                }
                self.available_workers.discard(worker_id)
            else:
                if worker_id in self.active_workers:
                    self.active_workers.pop(worker_id)
                self.available_workers.add(worker_id)


    def check_worker_health(self):
        while self.running:
            try:
                with self.lock:
                    now_time = dt.now()
                    dead_worker = [] # pop these lil shiz

                    for worker_id, worker_state in self.worker_states.items():
                        time_since_last_seen = now_time - worker_state['last_seen']
                        
                        if time_since_last_seen > self.worker_timeout:
                            if worker_state['ver_tries'] < 3:
                                print(f"worker {worker_id} missed heartbeat Verify maadiii")
                                
                                if self.verify_worker(worker_id):
                                    worker_state['last_seen'] = now_time
                                    worker_state['ver_tries'] = 0
                                    continue
                                
                                worker_state['ver_tries'] += 1
                            else:
                                dead_worker.append(worker_id)

                    for worker_id in dead_worker:
                        worker_state = self.worker_states.pop(worker_id)
                        self.available_workers.discard(worker_id)
                        
                        # adding task top of queue when all chances are given n still not performed (KLR)
                        if worker_id in self.active_workers:
                            w_data = self.active_workers.pop(worker_id)
                            if w_data.get('current_task'):
                                self.retry_task(w_data['current_task'])

            except Exception as e:
                print(f"err in checking health: {e}")
            
            time.sleep(self.retry_delay)


    def monitor_tasks(self):
        while self.running:
            for msg in self.consumer_for_task:
                if self.running == False:
                    break
                try:
                    task_data = msg.value
                    task_id = task_data.get("task_id")

                    if not self.redis.exists(task_id):
                        #print(f"task {task_id} not in redis, retrying")
                        #self.redis.hset(f"task : {task_id}", mapping={"status" : "queued", "created_at" : str(dt.now()), "retry_count" : 0})
                        # task id, client id, worker id, task type, task status, timestamp,  error
                        self.redis.hset(
                                        f"task:{task_id}",
                                        mapping = {
                                                   "client_id" : task_data.get('client_id'),
                                                   "worker_id" : "not assigned",
                                                   "type" : task_data.get('task'),
                                                   "status" : "queued",
                                                   "retry_count" : 0,
                                                   "created_at" : str(dt.now()),
                                                   "timestamp" : str(dt.now()),
                                                   "error" : None
                                                }
                        )
                        # log queued tasks
                        # log client details
                        

                    while (len(self.available_workers) == 0):
                        print("_________________Busy_______________")
                        time.sleep(6) # why 6 me reading htis midnight cant make sesne
                    
                    self.process_task(task_data)

                    
                    # task_status = self.redis.hget(f"task : {task_id}", "status")
                    # if task_status == "queued":
                    #     self.process_task(task_data)

                except Exception as e:
                    # log error
                    print(f"error reading msg: {e}")


    def monitor_reuslts(self):
        while self.running:
            for msg in self.consumer_for_result:
                if self.running == False:
                    break
                try:
                    res_data = msg.value
                    task_id = res_data.get("task_id")
                    print("result:", res_data)
                    self.redis.hset(
                                    f"task : {task_id}",
                                    mapping = {
                                               "status" : "success",
                                               "finished_at" : str(dt.now()),
                                               "timestamp": str(dt.now()),
                                               "error": None
                                            }
                    )
                    # log success tasks
                    # needto remove worker from active workers here?

                except Exception as e:
                    # log error
                    print(f"error reading res: {e}")
                

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
                            # log dead worker
                        # else:
                        #     w_data = self.active_workers[w_id]
                        #     last_heartbeat = now_time - timedelta(seconds = now_time.timestamp() - w_data['last_heartbeat'])
                            
                        #     if last_heartbeat > timedelta(seconds = self.worker_timeout):
                        #         print(f"worker {w_id} dead lmao retrying {task_id}")
                        #         # log zombie worker
                        #         self.retry_task(task_id)
            except Exception as e:
                print(f"error in zombie tasks: {e}")
                # log error
            
            time.sleep(self.retry_delay)


    def monitor_heartbeats(self):
        while self.running:
            for msg in self.consumer_for_heartbeat:
                if self.running == False:
                    break
                try:
                    
                    heartbeat = msg.value
                    w_id = heartbeat.get('worker_id')
                    w_status = heartbeat.get('worker_status')
                    current_task = heartbeat.get('current_task')
                    
                    self.update_worker_state(w_id, heartbeat)
                    
                    print(f"Active workers: {self.active_workers}")
                    print(f"Available workers: {self.available_workers}")
                    print(f"Worker states: {self.worker_states}")

                    task_data = self.redis.hgetall(f"task : {task_id}")
                    # log busy and idle workers                  
                    # log processing tasks (based on employed or idle)

                    ''' this was uhhh well uk wt to explain but keeping it here'''
                    # if (current_task):
                    #     # need to remove from this is it becomes idle/dead
                    #     self.active_workers[w_id] = {
                    #                                 'last_heartbeat' : time.time(),
                    #                                 'status' : 'active',
                    #                                 'current_task' : current_task
                    #     }


                    # now_time = time.time()
                    # timeout_workers = [w_id for w_id, w_data in self.active_workers.items() if ((now_time - w_data['last_heartbeat']) > self.worker_timeout)]

                    # for w_id in timeout_workers:
                    #     print(f"worker {w_id} timed out")
                    #     #log timed out worker

                    #     w_data = self.active_workers.pop(w_id)
                    #     self.available_workers.discard(w_id)  
                        
                    #     if w_data.get('current_task'):
                    #         task_id = w_data['current_task']
                    #         print(f"worker {w_id} failed processing task {task_id}")
                    #         self.retry_task(task_id)

                except Exception as e:
                    # log error
                    print(f"error heartbeat monitoring: {e}")


    def retry_task(self, task_id):
        try:
            task_key = f"task : {task_id}"
            task_data = self.redis.hgetall(f"task : {task_id}") # my monkey brain cant think of a better way to id it without changing all the work i hv already done
            if task_data == None:
                print(f"task {task_id} not in redis, cant retry")
                return False
            
            retry_count = int(task_data.get('retry_count', 0))
            client_id = task_data.get('client_id', '') # errorzzz
            
            if retry_count >= self.retry_max:
                print(f"task {task_id} max retries done")
                # log max attempt task
                self.redis.hset(
                                task_key,
                                mapping = {
                                           "status" : "failed",
                                           "timestamp": str(dt.now()),
                                           "error" : "Exceeded maximum retry attempts"
                                        }
                )
                msg_for_failures = {
                                    "task_id" : task_id,
                                    "status" : "failed",
                                    "error" : "Exceeded maximum retry attempts",
                                    "client_id" : client_id
                }
                self.producer.send(RESULT_TOPIC, value = msg_for_failures)
                return False

            #log retrying, queued task

            self.redis.hset(
                            f"task : {task_id}",
                            mapping = {
                                       "status" : "queued",
                                       "retry_count" : (retry_count + 1),
                                       "timestamp": str(dt.now()),
                                       "error" : None
                                    }
            )

            ''' frgot why we added this please reviewww cu we aint usign it for shit '''
            # task_message = {
            #                 "task_id" : task_id,
            #                 "task": task_data.get('type', 'unknown'),
            #                 "args" : task_data.get('args', ''),
            #                 "retry_count" : (retry_count + 1),
            #                 "client_id" : client_id
            # }
            
            print(f"retrying task {task_id} [attempt {retry_count + 1}]")
            # add this to the beginning of the queue with a new worker
            # self.producer.send(TASK_TOPIC, value = task_message)
            return True
        except Exception as e:
            #log error
            print(f"error in retrying itself, task {task_id}: {e}")
            return False


    def process_task(self, task_data):
        task_id = task_data["task_id"]
        try:
            if self.available_workers == None:
                print("no free workers")
                return
            
            wID = self.available_workers.pop()
            print(f"doing task {task_id}: {task_data['task']} with {task_data['args']}")
            # retry_count = task_data.get('retry_count', 0)
            data = {
                    "wID" : wID,
                    "task_id" : task_id,
                    "task_type" : task_data.get('task'),
                    "client_id" : task_data.get('client_id'),
                    "args" : task_data.get('args'),
                    "timestamp" : str(dt.now())
            }
            # self.redis.hset(
            #                 f"task : {task_id}",
            #                 mapping = {"status" : "processing", "start_time" : str(dt.now()), "type" : task_data['task'], "retry_count" : retry_count}
            # )
            self.producer.send(TASK_TOPIC_WORKER, value = data)
            print("giving to: ", wID)
            # log processing tasks
        except Exception as e:
            print(f"task processing error for {task_id}:{e}")

            # task id, client id, worker id, task type, task status, timestamp,  error
            self.redis.hset(
                            f"task:{task_id}",
                            mapping = {
                                        "client_id" : task_data.get('client_id'),
                                        "worker_id": None,
                                        "type": task_data.get('task'),
                                        "status": "failed",
                                        "timestamp": str(dt.now()),
                                        "error": e
                                    }
            )
            # log error

            # self.redis.hset(
            #                 f"task : {task_id}",
            #                 mapping = {"status" : "failed", "error" : str(e)}
            # )


    def start(self):
        print("started yadtq server")
        monitor_thread = Thread(target = self.monitor_tasks)
        monitor_reuslts_thread = Thread(target = self.monitor_reuslts)
        heartbeat_thread = Thread(target = self.monitor_heartbeats)
        zombie_thread = Thread(target = self.monitor_zombie_tasks)
        worker_health_thread = Thread(target = self.check_worker_health)

        monitor_thread.daemon = True
        monitor_reuslts_thread.daemon = True
        heartbeat_thread.daemon = True
        zombie_thread.daemon = True
        worker_health_thread.daemon = True
        monitor_thread.start()
        monitor_reuslts_thread.start()
        heartbeat_thread.start()
        zombie_thread.start()
        worker_health_thread.start()

        try:
            while True:
                print('workers ->')
                with self.lock:
                    now_time = dt.now()
                    for w_id, w_data in self.active_workers.items():
                        worker_status = "active" if w_data['status'] == 'active' else "inactive"
                        task_status = f"doing {w_data['current_task']}" if w_data['current_task'] else "free"
                        print(f"Worker {w_id} : {worker_status} | {task_status} | Last seen: {now_time - w_data['last_seen']:.1f}s ago")
                time.sleep(5)
        except KeyboardInterrupt:
            self.running = False
            self.redis.flushall()
            self.producer.close()
            self.consumer_for_task.close()
            self.consumer_for_heartbeat.close()
            self.consumer_for_result.close()


server = YADTQServer()
server.start()
