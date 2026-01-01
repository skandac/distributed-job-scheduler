{
  "Job": {
    "job_id": "string",
    "payload": {
      "type": "object",
      "description": "Job-specific data"
    },
    "status": "PENDING", 
    "assigned_worker": "string | null",
    "retry_count": 0,
    "max_retries": 3,
    "timestamps": {
      "created_at": "2025-12-29T00:00:00Z",
      "assigned_at": "2025-12-29T00:05:00Z",
      "started_at": "2025-12-29T00:10:00Z",
      "completed_at": "2025-12-29T00:20:00Z"
    }
  },
  "Worker": {
    "worker_id": "string",
    "status": "ALIVE",
    "last_heartbeat": "2025-12-29T00:15:00Z",
    "current_jobs": ["job_id_1", "job_id_2"]
  },
  "KafkaEvents": {
    "JobCreated": {
      "job_id": "string",
      "timestamp": "2025-12-29T00:00:00Z"
    },
    "JobAssigned": {
      "job_id": "string",
      "worker_id": "string",
      "timestamp": "2025-12-29T00:05:00Z"
    },
    "JobStarted": {
      "job_id": "string",
      "worker_id": "string",
      "timestamp": "2025-12-29T00:10:00Z"
    },
    "JobCompleted": {
      "job_id": "string",
      "worker_id": "string",
      "timestamp": "2025-12-29T00:20:00Z"
    },
    "JobFailed": {
      "job_id": "string",
      "worker_id": "string",
      "error_message": "string",
      "timestamp": "2025-12-29T00:18:00Z"
    },
    "WorkerRegistered": {
      "worker_id": "string",
      "timestamp": "2025-12-29T00:00:00Z"
    },
    "WorkerDead": {
      "worker_id": "string",
      "timestamp": "2025-12-29T00:30:00Z"
    }
  }
}
