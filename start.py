#!/usr/bin/env python3
import subprocess
import argparse
import time
import os
import atexit
import signal
import sys

processes = []

def cleanup():
    print("Shutting down all components...")
    for p in processes:
        if p.poll() is None:  # if process is still running
            p.terminate()
    
    # Wait a bit for processes to terminate gracefully
    time.sleep(1)
    
    # Force kill any process that didn't terminate
    for p in processes:
        if p.poll() is None:
            try:
                p.kill()
            except:
                pass

def signal_handler(sig, frame):
    print('Interrupt received, shutting down...')
    cleanup()
    sys.exit(0)

def start_component(component, log_file=None):
    print(f"Starting {component}...")
    if log_file:
        with open(log_file, 'w') as f:
            p = subprocess.Popen(['python', f'{component}.py'], stdout=f, stderr=subprocess.STDOUT)
    else:
        p = subprocess.Popen(['python', f'{component}.py'])
    processes.append(p)
    return p

def main():
    parser = argparse.ArgumentParser(description='YADTQ System Starter')
    parser.add_argument('--workers', type=int, default=4, help='Number of workers to start')
    parser.add_argument('--no-logs', action='store_true', help='Don\'t redirect output to log files')
    args = parser.parse_args()
    
    # Register cleanup handlers
    atexit.register(cleanup)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create logs directory if needed
    if not args.no_logs:
        os.makedirs('logs', exist_ok=True)
    
    # Start server
    server_log = None if args.no_logs else 'logs/server.log'
    server = start_component('server', server_log)
    
    # Start logger
    logger_log = None if args.no_logs else 'logs/logger.log'
    logger = start_component('logger', logger_log)
    
    # Give server and logger time to initialize
    time.sleep(3)
    
    # Start worker manager
    manager_log = None if args.no_logs else 'logs/worker_manager.log'
    manager = start_component('worker_manager', manager_log)
    
    # Start individual workers if not using worker manager
    worker_processes = []
    if args.workers > 0:
        print(f"Starting {args.workers} workers...")
        for i in range(args.workers):
            worker_log = None if args.no_logs else f'logs/worker_{i}.log'
            worker = start_component('worker', worker_log)
            worker_processes.append(worker)
    
    print("All components started")
    print("Press Ctrl+C to shut down the system")
    
    try:
        # Keep main process running
        while True:
            # Check if server is still running
            if server.poll() is not None:
                print("Server stopped unexpectedly, shutting down...")
                break
                
            # Check if logger is still running
            if logger.poll() is not None:
                print("Logger stopped unexpectedly, shutting down...")
                break
                
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        cleanup()

if __name__ == "__main__":
    main() 