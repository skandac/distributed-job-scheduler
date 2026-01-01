Design a distributed job scheduler that is at scale 

PENDING → SCHEDULED → RUNNING → SUCCESS | FAILED

1. Project Overview

Build a fault-tolerant, distributed job scheduler that can execute tasks across multiple workers, handle failures, and scale horizontally.

Core Features:

    Job submission and tracking
    Worker registration and heartbeat monitoring
    Fault-tolerant job scheduling
    Retry logic for failed jobs
    Kafka-based event communication
    Redis as source of truth

2. System Architecture
           ┌──────────┐
           │  Client  │
           └────┬─────┘
                │
         POST /jobs | GET /jobs/{id}
                │
         ┌──────▼──────┐
         │ Scheduler   │ // gRPC to be used for binary comm
         │    API      │
         └──────┬──────┘
                │
        ┌───────▼────────┐
        │     Kafka      │
        │ job-requests   │
        └───────┬────────┘
                │
        ┌───────▼────────┐
        │ Scheduler Loop │
        │   (Leader)     │
        └───────┬────────┘
                │
        ┌───────▼────────┐
        │     Kafka      │
        │ job-assignments│
        └───────┬────────┘
                │
        ┌───────▼────────┐
        │    Workers     │
        │  (heartbeats)  │
        └───────┬────────┘
                │
        ┌───────▼────────┐
        │     Kafka      │
        │  job-results   │
        └───────┬────────┘
                │
              Redis


Explanation:

Client → Scheduler API: Submit jobs and query status

The Scheduler API is exposed over HTTP for simplicity and interoperability, while scheduler–worker communication uses gRPC for efficiency and strong typing.

Client
  |
  |  HTTP (FastAPI)
  v
Scheduler API
  |
  |  Redis (state)
  |
  |  Kafka (events)
  |
  |  gRPC
  v
Worker

Scheduler API → Kafka: Publish job-requests events

Scheduler Loop: Leader-based scheduler that consumes requests, assigns jobs to workers, updates Redis

Workers: Consume assignments, execute jobs, report results, send heartbeats

Redis: Source of truth for job states, retries, worker liveness, leader metadata

Kafka Topics: job-requests, job-assignments, job-results

3. Components

Scheduler API:	Accepts jobs, validates input, persists to Redis, publishes job events to Kafka
Scheduler Loop:	Assigns jobs to workers, handles retries, leader election, consumes job-requests
Workers:        Consume job-assignments, execute tasks, report job-results, send heartbeats
Kafka:          Event transport backbone, decouples producers/consumers, ensures replayability
Redis:  	Authoritative state: job lifecycle, worker status, retries, leader lock

4. Data Models

Job

job_id: string
payload: JSON / command
status: PENDING | SCHEDULED | RUNNING | SUCCESS | FAILED
assigned_worker: string
retry_count: int
max_retries: int
created_at: timestamp
updated_at: timestamp


Worker

worker_id: string
status: ALIVE | DEAD
last_heartbeat: timestamp
current_jobs: list


Kafka Events

JobCreated
JobAssigned
JobStarted
JobCompleted
JobFailed
WorkerRegistered
WorkerDead

5. Reliability & Fault Tolerance

Leader Election: Only one scheduler instance active at a time

At-Least-Once Execution: Jobs retried on failure

Worker Crash Recovery: Requeue running jobs if heartbeat lost

Idempotency: Workers must safely handle duplicate events

Kafka Replay: Can reprocess events after failure

6. Failure Scenarios

Duplicate Kafka messages → handle idempotently

Worker crash mid-job → requeue job

Scheduler crash → leader election + replay events

Redis unavailable → queue events in memory temporarily

Network partition → eventually consistent state

7. Optional Enhancements

Metrics collection (latency, retries, failures)

Job timeouts & cancellation

Exponential backoff on retries

Multiple scheduling strategies (round-robin, least-loaded)

8. Non-Goals (Important)

DAG workflow orchestration (like Airflow)

Exactly-once execution

UI/dashboard

Autoscaling logic

Container orchestration

9. Deployment & Operations

All components containerized (Docker)

Config-driven deployment (env variables)

Logging & monitoring endpoints

Graceful shutdown handling

10. Summary (One Sentence)

Kafka-driven, fault-tolerant distributed job scheduler with leader-based scheduling, worker heartbeats, retries, and Redis-backed state management.


python -m grpc_tools.protoc \
  -I . \
  --python_out=. \
  --grpc_python_out=. \
  internal/proto/scheduler.proto

docker run -d --name zookeeper -p 2181:2181 zookeeper:3.8

docker run -d \
  --name kafka \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=host.docker.internal:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  wurstmeister/kafka



docker exec -it distributed-scheduler-kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic jobs \
  --bootstrap-server localhost:9092


to run 

you should have kafka and redis installed and pulled from docker 

docker start kafka 
docker start redis 

run the flask api app using uvicorn 

uvicorn internal.api.main:app --reload

python -m internal.worker.worker

this starts the worker thread 

to add the jobs u can use curl or swagger ui /docs -> POST 


