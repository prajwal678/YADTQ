# YADTQ (Yet Another Distributed Task Queue)

YADTQ is a distributed task queue system built with Python, Kafka, and Redis. It provides a scalable architecture for processing tasks across multiple workers with features like task retry, health monitoring, automatic worker recovery, and fault tolerance.

## System Architecture

- **Server**: Central coordinator that manages task distribution and worker health
- **Workers**: Process tasks and report status via heartbeats
- **Worker Manager**: Monitors and manages worker processes, ensuring minimum worker count
- **Logger**: Maintains system logs and task history
- **Clients**: Submit tasks and receive results
- **Storage**: Redis for task state management
- **Message Queue**: Kafka for communication between components

## Features

- **Fault Tolerance**: Automatic task retries and worker recovery
- **Scalability**: Easily scale workers up or down
- **Monitoring**: Real-time monitoring of worker health
- **Task Management**: Queue, process, and track tasks throughout their lifecycle
- **Graceful Shutdown**: Properly handle shutdowns and resource cleanup
- **Multiple Task Types**: Supports compression, decompression, and encryption tasks

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Available ports: 9092 (Kafka), 6379 (Redis)

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/prajwal678/YADTQ
cd yadtq
```

2. Start the system using Docker:
```bash
docker-compose up -d
```

3. Or start using the provided script:
```bash
python start.py
```

4. Run a client:
```bash
python client.py
```

## Running with Docker

The recommended way to run YADTQ is with Docker, which takes care of all dependencies and service coordination.

1. Start all services:
```bash
docker-compose up -d
```

2. Scale workers as needed:
```bash
docker-compose up -d --scale worker=6
```

3. View logs:
```bash
docker-compose logs -f
```

4. Shut down:
```bash
docker-compose down
```

## Running Locally

For development or testing, you can run components directly:

1. Start all components with the starter script:
```bash
python start.py --workers 4
```

This will start:
- 1 server
- 1 logger
- 1 worker manager
- 4 workers (default, configurable)

## Configuration

All configuration is managed through environment variables with defaults in `config.py`. Key configurations:

- `KAFKA_BROKER`: Kafka broker address (default: "kafka:9092")
- `REDIS_HOST`: Redis host (default: "redis")
- `WORKER_TIMEOUT`: Worker health check timeout (default: 30s)
- `TASK_MAX_RETRIES`: Maximum task retry attempts (default: 3)

## Task Types

Currently supported task types:
- Compression
- Decompression
- Encryption

## Fault Tolerance

YADTQ has several mechanisms for fault tolerance:

1. **Task Retries**: Failed tasks are automatically retried up to a configurable limit
2. **Worker Recovery**: Dead workers are detected and new workers started
3. **Heartbeat Monitoring**: Regular health checks ensure the system knows worker status
4. **Graceful Shutdown**: Components handle shutdown signals properly

## Worker Manager

The Worker Manager is a new component that:
- Ensures a minimum number of workers are always running
- Detects and restarts failed workers
- Scales workers up or down based on demand
- Monitors worker health via heartbeats

## Monitoring

- Worker health is monitored via heartbeats
- Task status can be tracked through the logger
- System logs are available in the `logs` directory
- Worker Manager provides real-time worker status

## System Components

### Server
- Manages task distribution
- Monitors worker health
- Handles task retries
- Maintains task state

### Worker
- Processes assigned tasks
- Sends regular heartbeats
- Reports task completion/failure
- Handles graceful shutdown

### Worker Manager
- Starts and stops worker processes
- Ensures system has required workers
- Monitors worker health
- Restarts failed workers

### Logger
- Records system events
- Maintains task history
- Provides audit trail

### Client
- Submits tasks
- Receives results
- Tracks task status

## Communication Flow

1. Client submits task → Server
2. Server assigns task → Worker
3. Worker processes task → Returns result
4. Server validates result → Forwards to Client
5. All events → Logger
6. Worker Manager ↔ Workers (health checks and management)

## Troubleshooting

### Common Issues

1. **Kafka Connection Problems**
   - Ensure Kafka is running and the broker address is correct in config.py

2. **Redis Connection Problems**
   - Verify Redis is running and the host/port settings are correct

3. **Worker Not Starting**
   - Check worker logs for errors
   - Ensure the worker manager is running properly

4. **Task Stuck in Queue**
   - Check for available workers
   - Verify task format is correct
   - Look for errors in server logs