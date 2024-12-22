import redis
from threading import Thread, Lock
from kafka import KafkaProducer, KafkaConsumer
import time, json
from datetime import datetime as dt, timedelta
from config import REDIS_HOST, REDIS_PORT, KAFKA_BROKER, STATUS_TOPIC_CLIENT, STATUS_TOPIC_SERVER, LOG_TOPIC, TASK_TOPIC, TASK_TOPIC_WORKER, RESULT_TOPIC_CLIENT, RESULT_TOPIC, HEARTBEAT_TOPIC, NO_OF_WORKERS

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
        self.consumer_for_task_status = KafkaConsumer(
                                                    STATUS_TOPIC_CLIENT,
                                                    bootstrap_servers = KAFKA_BROKER,
                                                    group_id = 'status_group',
                                                    value_deserializer = lambda x: json.loads(x.decode('utf-8'))
        )

        self.active_workers = {}
        self.available_workers = set()
        self.worker_states = {}
        self.running = True
        self.lock = Lock()

        # some variables might be useless need to recheck i defined them before writing code
        self.heartbeat_interval_time = 3
        self.verify_timeout = 3
        self.worker_timeout = 7

        self.retry_max = 3
        self.retry_delay = 5
        # can use this as a direct varibale rn its useless
        # self.free_workers = NO_OF_WORKERS

        #yea thats right ur boi wasted almost an hour cuz HE DID NOT LOCK THREADS OMG THIS IS NOT C++, AUGHHH

    def status_fetch(self):
        while self.running:
            # print("+++++++++++++")
            for message in self.consumer_for_task_status:
                if self.running == False:
                    break
                try:
                    req_data = message.value
                    # print("------------\n",req_data)
                    task_id = req_data.get('task_id') 
                    # client_id = req_data.get('client_id')
                    for key in self.redis.scan_iter("task : *"):
                        # print("~~~~~")
                        # print(key.split(':')[1])
                        # print(task_id)
                        
                        if key.split(':')[1].strip() == task_id:
                            # print("+++++++")
                            task_data = self.redis.hgetall(key)
                            status_task = {
                                "timestamp" : str(dt.now()),
                                "client_id" : task_data.get('client_id'),
                                "task_id" : task_id,
                                "status" : task_data.get('status','NA')
                            }
                            # print("---------", status_task)
                            self.producer.send(STATUS_TOPIC_SERVER,value=status_task)
                            print("---------", status_task)
                        else:
                            pass
                except Exception as e:
                    # log error
                    error_log_status_fetch = {
                        "timestamp" : str(dt.now()),
                        "title" : "error_Information",
                        "function" : "Status_Fetch",
                        "error" : str(e)
                    }
                    self.producer.send(LOG_TOPIC,value=error_log_status_fetch)
     

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
                        print(f"\nworker {worker_id} ping saksus")
                        return True

                time.sleep(0.5)          
            print(f"\nworker {worker_id} ping FAIL")
            return False
        except Exception as e:
            print(f"\nverification for {worker_id} fail")
            return False


    def update_worker_state(self, worker_id, heartbeat_data):
        with self.lock:
            now_time = dt.now()
            working_status = heartbeat_data.get('working_status')
            current_task = heartbeat_data.get('current_task')
            # log busy and idle workers
            worker_stat_data = {
                "timestamp" : str(dt.now()),
                "title" : "Worker_Information",
                "worker_id" : worker_id,
                "client_id" : heartbeat_data.get('client_id'),
                "worker_status" : working_status,
                "current_task" : current_task
            }                  
            self.producer.send(LOG_TOPIC,value=worker_stat_data)


            if worker_id not in self.worker_states:
                self.worker_states[worker_id] = {
                                                 "last_seen" : now_time,
                                                 "status" : "active",
                                                 'current_task' : None,
                                                 'ver_tries' : 0,
                                                 'client_id' : None
                }
            else:
                self.worker_states[worker_id].update({
                                                      'last_seen' : now_time,
                                                      'status' : "active",
                                                      'client_id' : heartbeat_data.get('client_id')
                })

            if (working_status) and (current_task):

                # log processing tasks (based on employed or idle)
                proc_task_data = {
                    "timestamp" : str(dt.now()),
                    "title" : "Task_Information",
                    "task_id" : current_task,
                    "client_id" : heartbeat_data.get('client_id'),
                    "worker_id" : worker_id,
                    "task_type" : "?",
                    "task_status" : "Processing",
                    "retry_count" : "?",
                    "error" : ""
                }
                self.producer.send(LOG_TOPIC,value=proc_task_data)

                self.active_workers[worker_id] = {
                                                  'last_seen': dt.now(),
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
                        time_since_last_seen = (now_time - worker_state['last_seen']).total_seconds()
                        
                        if time_since_last_seen > self.worker_timeout:
                            if worker_state['ver_tries'] < 3:
                                print(f"\nworker {worker_id} missed heartbeat Verify maadiii")
                                
                                if self.verify_worker(worker_id):
                                    worker_state['last_seen'] = now_time
                                    worker_state['ver_tries'] = 0
                                    continue
                                
                                worker_state['ver_tries'] += 1
                            else:
                                dead_worker.append(worker_id)
                                # dead worker log
                                worker_dead_data = {
                                    "timestamp" : str(dt.now()),
                                    "title" : "Worker_Information",
                                    "worker_id" : worker_id,
                                    "client_id" : worker_state['client_id'],
                                    "worker_status" : "Dead",
                                    "current_task" : None
                                }   
                                self.producer.send(LOG_TOPIC,value=worker_dead_data)

                    for worker_id in dead_worker:
                        worker_state = self.worker_states.pop(worker_id)
                        self.available_workers.discard(worker_id)
                        
                        # adding task top of queue when all chances are given n still not performed (KLR)
                        if worker_id in self.active_workers:
                            w_data = self.active_workers.pop(worker_id)
                            if w_data.get('current_task'):
                                self.retry_task(w_data['current_task'])

            except Exception as e:
                print(f"\nerr in checking health: {e}")
                error_log_health_check = {
                    "timestamp" : str(dt.now()),
                    "title" : "error_Information",
                    "function" : "Check_Worker_Health",
                    "error" : str(e)
                }
                self.producer.send(LOG_TOPIC,value=error_log_health_check)
            
            time.sleep(self.retry_delay)


    def monitor_tasks(self):
        while self.running:
            for msg in self.consumer_for_task:
                if self.running == False:
                    break
                try:
                    task_data = msg.value
                    print(task_data)
                    task_id = task_data.get("task_id")
                    if not self.redis.exists(task_id):
                        # print("\nclient_id", task_data.get('client_id'))
                        # print("\ntask", task_data.get('task'))
                        #print(f"\ntask {task_id} not in redis, retrying")
                        #self.redis.hset(f"task : {task_id}", mapping={"status" : "queued", "created_at" : dt.now(), "retry_count" : 0})
                        # task id, client id, worker id, task type, task status, timestamp,  error
                        self.redis.hset(
                                        f"task : {task_id}",
                                        mapping = {
                                                   "client_id" : task_data.get('client_id'),
                                                   "worker_id" : "not assigned",
                                                   "type" : task_data.get('task'),
                                                   "status" : "queued",
                                                   "retry_count" : 0,
                                                   "created_at" : str(dt.now()),
                                                   "timestamp" : str(dt.now()),
                                                   "error" : ""
                                                }
                        )
                        # print("\n~~~~")
                        # log queued tasks
                        queue_task_data = {
                            "timestamp" : str(dt.now()),
                            "title" : "Task_Information",
                            "task_id" : task_id,
                            "client_id" : task_data.get('client_id'),
                            "worker_id" : "Not_Assigned_Yet",
                            "task_type" : task_data.get('task'),
                            "task_status" : "Queued",
                            "retry_count" : 0,
                            "error" : ""
                        }
                        self.producer.send(LOG_TOPIC,value=queue_task_data)
                        # print("\n------")
                        # log client details
                        client_data = {
                            "timestamp" : str(dt.now()),
                            "title" : "Client_Information",
                            "client_id" : task_data.get('client_id'),
                            "task_id" : task_id,
                            "task_type" : task_data.get('task'),
                        }
                        self.producer.send(LOG_TOPIC,value=client_data)

                    while (len(self.available_workers) == 0):
                        print("\n_________________Busy_______________")
                        time.sleep(2) # why 6 me reading htis midnight cant make sesne
                    
                    self.process_task(task_data)

                    
                    # task_status = self.redis.hget(f"task : {task_id}", "status")
                    # if task_status == "queued":
                    #     self.process_task(task_data)

                except Exception as e:
                    # log error
                    error_log_montior_task = {
                        "timestamp" : str(dt.now()),
                        "title" : "error_Information",
                        "function" : "Monitor_Tasks",
                        "error" : str(e)
                    }
                    self.producer.send(LOG_TOPIC,value=error_log_montior_task)
                    print(f"\nerror reading msg: {e}")


    def monitor_reuslts(self):
        while self.running:
            for msg in self.consumer_for_result:
                if self.running == False:
                    break
                try:
                    res_data = msg.value
                    task_id = res_data.get("task_id")
                    print("\nresult:", res_data)
                    result_data_client = {
                          "task_id" : task_id,
                          "client_id" : res_data.get('client_id'),
                          "task_status" : res_data.get('task_status'),
                          "result" : res_data.get('result')
                    }
                    self.producer.send(RESULT_TOPIC_CLIENT,value=result_data_client)

                    self.redis.hset(
                                    f"task : {task_id}",
                                    mapping = {
                                               "status" : "success",
                                               "finished_at" : str(dt.now()),
                                               "timestamp": str(dt.now()),
                                               "error": ""
                                            }
                    )
                    # log success tasks
                    success_task_data = {
                        "timestamp" : str(dt.now()),
                        "title" : "Task_Information",
                        "task_id" : task_id,
                        "client_id" : res_data.get('client_id'),
                        "worker_id" : res_data.get('worker_id'),
                        "task_type" : "?",
                        "task_status" : "Success",
                        "retry_count" : "?",
                        "error" : ""
                    }
                    self.producer.send(LOG_TOPIC,value=success_task_data)

                except Exception as e:
                    # log error
                    error_log_montior_result = {
                        "timestamp" : str(dt.now()),
                        "title" : "error_Information",
                        "function" : "Monitor_Results",
                        "error" : str(e)
                    }
                    self.producer.send(LOG_TOPIC,value=error_log_montior_result)
                    print(f"\nerror reading res: {e}")
                

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
                    try:
                        start_time = dt.fromisoformat(task_data.get('start_time', ''))
                    except Exception as e:
                        print(f"\nerror in zombie tasks: {e}")
                        # log error
                        error_log_montior_zombie = {
                            "timestamp" : str(dt.now()),
                            "title" : "error_Information",
                            "function" : "Monitor_Zombie_Tasks",
                            "error" : str(e)
                        }
                        self.producer.send(LOG_TOPIC,value=error_log_montior_zombie)
                    runtime = now_time - start_time
                    
                    if runtime > timedelta(seconds = self.worker_timeout):
                        print(f"\ntask {task_id} stuck for {runtime}")
                        w_id = task_data.get('worker_id')

                        if not w_id or w_id not in self.active_workers:
                            print(f"\nworker for {task_id} not active retrying")
                            self.retry_task(task_id)
                            # log dead worker
                        # else:
                            # log zombie worker 

                        #     w_data = self.active_workers[w_id]
                        #     last_heartbeat = now_time - timedelta(seconds = now_time.timestamp() - w_data['last_heartbeat'])
                            
                        #     if last_heartbeat > timedelta(seconds = self.worker_timeout):
                        #         print(f"\nworker {w_id} dead lmao retrying {task_id}")
                        #         # log zombie worker
                        #         self.retry_task(task_id)
            except Exception as e:
                print(f"\nerror in zombie tasks: {e}")
                # log error
                error_log_montior_zombie = {
                    "timestamp" : str(dt.now()),
                    "title" : "error_Information",
                    "function" : "Monitor_Zombie_Tasks",
                    "error" : str(e)
                }
                self.producer.send(LOG_TOPIC,value=error_log_montior_zombie)        

            time.sleep(self.retry_delay)


    def monitor_heartbeats(self):
        while self.running:
            for msg in self.consumer_for_heartbeat:
                if self.running == False:
                    break
                try:
                    
                    heartbeat = msg.value
                    task_id = heartbeat.get('task_id')
                    w_id = heartbeat.get('worker_id')
                    w_status = heartbeat.get('worker_status')
                    current_task = heartbeat.get('current_task')
                    
                    self.update_worker_state(w_id, heartbeat)
                    
                    print(f"\nActive workers: {self.active_workers}")
                    print(f"\nAvailable workers: {self.available_workers}")
                    print(f"\nWorker states: {self.worker_states}")

                    task_data = self.redis.hgetall(f"task : {task_id}")



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
                    #     print(f"\nworker {w_id} timed out")
                    #     #log timed out worker

                    #     w_data = self.active_workers.pop(w_id)
                    #     self.available_workers.discard(w_id)  
                        
                    #     if w_data.get('current_task'):
                    #         task_id = w_data['current_task']
                    #         print(f"\nworker {w_id} failed processing task {task_id}")
                    #         self.retry_task(task_id)

                except Exception as e:
                    # log error
                    error_log_heartbeat = {
                        "timestamp" : str(dt.now()),
                        "title" : "error_Information",
                        "function" : "Monitor_Heartbeat",
                        "error" : str(e)
                    }
                    self.producer.send(LOG_TOPIC,value=error_log_heartbeat)                     
                    print(f"\nerror heartbeat monitoring: {e}")


    def retry_task(self, task_id):
        try:
            task_key = f"task : {task_id}"
            task_data = self.redis.hgetall(f"task : {task_id}") # my monkey brain cant think of a better way to id it without changing all the work i hv already done
            if task_data == None:
                print(f"\ntask {task_id} not in redis, cant retry")
                return False
            
            retry_count = int(task_data.get('retry_count', 0))
            client_id = task_data.get('client_id', '') # errorzzz
            
            if retry_count >= self.retry_max:   
                print(f"\ntask {task_id} max retries done")
                # log max attempt task
                max_retry_task_data = {
                    "timestamp" : str(dt.now()),
                    "title" : "Task_Information",
                    "task_id" : task_id,
                    "client_id" : task_data.get('client_id'),
                    "worker_id" : task_data.get('worker_id'),
                    "task_type" : task_data.get('type'),
                    "task_status" : "Failed",
                    "retry_count" : "3",
                    "error" : "Maximum attempts at retry done"
                }
                self.producer.send(LOG_TOPIC,value=max_retry_task_data)

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
                self.producer.send(RESULT_TOPIC_CLIENT, value = msg_for_failures)
                return False

            #log retrying, queued task

            self.redis.hset(
                            task_key,
                            mapping = {
                                       "status" : "queued",
                                       "retry_count" : (retry_count + 1),
                                       "timestamp": str(dt.now()),
                                       "error" : "",
                                       "worker_id" : ""
                                    }
            )

            retry_task_message = {
                                  "task_id" : task_id,
                                  "task": task_data.get('type', 'unknown'),
                                  "args" : task_data.get('args', ''),
                                  "retry_count" : (retry_count + 1),
                                  "client_id" : client_id
            }
            
            retry_log_data = {
                              "timestamp" : dt.now(),
                              "title" : "Task_Information",
                              "task_id" : task_id,
                              "client_id" : client_id,
                              "worker_id" : "Not_Assigned_Yet",
                              "task_type" : task_data.get('type'),
                              "task_status" : "Retry_Queued",
                              "retry_count" : retry_count + 1,
                              "error" : None
            }
            self.producer.send(LOG_TOPIC, value = retry_log_data)

            # task back into queue
            print(f"\nretrying task {task_id} [attempt {retry_count + 1}]")
            self.producer.send(TASK_TOPIC, value=retry_task_message)
            return True
        except Exception as e:
            error_log = {
                         "timestamp" : str(dt.now()),
                         "title" : "error_Information",
                         "function" : "Retry_Task",
                         "error" : str(e)
            }
            self.producer.send(LOG_TOPIC, value=error_log)
            print(f"\nerror in retrying itself, task {task_id}: {e}")
            return False


    def process_task(self, task_data):
        task_id = task_data["task_id"]
        try:
            if self.available_workers == None:
                print("\nno free workers")
                return
            
            wID = self.available_workers.pop()
            self.redis.hset(
                            f"task : {task_id}",
                            mapping = {
                                        "client_id" : task_data.get('client_id'),
                                        "worker_id" : wID,
                                        "type" : task_data.get('task'),
                                        "status" : "processing",
                                        "timestamp" : str(dt.now()),
                                        "error" : ""
                                    }
            )
            print(f"\ndoing task {task_id}: {task_data['task']} with {task_data['args']}")
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
            #                 mapping = {"status" : "processing", "start_time" : dt.now(), "type" : task_data['task'], "retry_count" : retry_count}
            # )
            self.producer.send(TASK_TOPIC_WORKER, value = data)
            print("\ngiving to: ", wID)

        except Exception as e:
            print(f"\ntask processing error for {task_id}:{e}")

            # task id, client id, worker id, task type, task status, timestamp,  error
            self.redis.hset(
                            f"task : {task_id}",
                            mapping = {
                                        "client_id" : task_data.get('client_id'),
                                        "worker_id": "",
                                        "type": task_data.get('task'),
                                        "status": "failed",
                                        "timestamp": str(dt.now()),
                                        "error": str(e)
                                    }
            )
            # log error
            error_log_proc_task = {
                "timestamp" : str(dt.now()),
                "title" : "error_Information",
                "function" : "Process_Task",
                "error" : str(e)
            }
            self.producer.send(LOG_TOPIC,value=error_log_proc_task) 
            # self.redis.hset(
            #                 f"task : {task_id}",
            #                 mapping = {"status" : "failed", "error" : str(e)}
            # )


    def start(self):
        print("\nstarted yadtq server")
        monitor_thread = Thread(target = self.monitor_tasks)
        monitor_reuslts_thread = Thread(target = self.monitor_reuslts)
        heartbeat_thread = Thread(target = self.monitor_heartbeats)
        zombie_thread = Thread(target = self.monitor_zombie_tasks)
        worker_health_thread = Thread(target = self.check_worker_health)
        status_fetch_thread = Thread(target = self.status_fetch)

        monitor_thread.daemon = True
        monitor_reuslts_thread.daemon = True
        heartbeat_thread.daemon = True
        zombie_thread.daemon = True
        worker_health_thread.daemon = True
        status_fetch_thread.daemon = True
        monitor_thread.start()
        monitor_reuslts_thread.start()
        heartbeat_thread.start()
        zombie_thread.start()
        worker_health_thread.start()
        status_fetch_thread.start()

        try:
            while True:
                print('workers ->')
                with self.lock:
                    now_time = dt.now()
                    for w_id, w_data in self.active_workers.items():
                        worker_status = "active" if w_data['status'] == 'active' else "inactive"
                        task_status = f"doing {w_data['current_task']}" if w_data['current_task'] else "free"
                        time_since_last_seen = (now_time - w_data['last_seen']).total_seconds()
                        print(f"\nWorker {w_id} : {worker_status} | {task_status} | Last seen: {time_since_last_seen:.1f}s ago")
                time.sleep(5)
        except KeyboardInterrupt:
            self.running = False
            self.redis.flushall()
            self.producer.close()
            self.consumer_for_task.close()
            self.consumer_for_heartbeat.close()
            self.consumer_for_task_status.close()
            self.consumer_for_result.close()


server = YADTQServer()
server.start()
