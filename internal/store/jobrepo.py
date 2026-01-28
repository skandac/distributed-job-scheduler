from internal.models.job import JobType, JobStatus, JobTimestamps
from datetime import datetime
import redis
import json
import uuid
from typing import Optional, Dict, Any, List, Tuple
import os

# Worker heartbeat TTL in seconds
WORKER_HEARTBEAT_TTL = 15
# Job lock TTL for processing (should be longer than expected processing time)
JOB_LOCK_TTL = 60
# Idempotency key TTL (how long to remember processed events)
IDEMPOTENCY_TTL = 86400  # 24 hours

class JobRepository:
    def __init__(self, redis_url: Optional[str] = None):
        self.redis_client = redis.Redis.from_url(
            redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True  # CRITICAL: Ensures Redis returns strings, not bytes
        )

    def _get_key(self, job_id: str) -> str:
        """Helper to ensure consistent key naming"""
        return f"job:{job_id}"

    def _get_worker_heartbeat_key(self, worker_id: str) -> str:
        return f"worker:{worker_id}:heartbeat"

    def _get_worker_jobs_key(self, worker_id: str) -> str:
        return f"worker:{worker_id}:jobs"

    def _get_job_lock_key(self, job_id: str) -> str:
        return f"job:{job_id}:lock"

    def _get_idempotency_key(self, event_id: str) -> str:
        return f"idempotency:{event_id}"

    def save_job(self, job: JobType) -> None:
        job_data = {
            "job_id": job.job_id,
            "payload": job.payload,
            "status": job.status.value,
            "assigned_worker": job.assigned_worker,
            "retry_count": job.retry_count,
            "max_retries": job.max_retries,
            "version": job.version,
            "processing_token": job.processing_token,
            "timestamps": {
                "created_at": job.timestamps.created_at.isoformat(),
                "assigned_at": job.timestamps.assigned_at.isoformat() if job.timestamps.assigned_at else None,
                "started_at": job.timestamps.started_at.isoformat() if job.timestamps.started_at else None,
                "completed_at": job.timestamps.completed_at.isoformat() if job.timestamps.completed_at else None,
            }
        }
        key = self._get_key(job.job_id)
        self.redis_client.set(key, json.dumps(job_data))

    def update_job_status(self, job_id: str, new_status: JobStatus) -> None:
        key = self._get_key(job_id)
        job = self.get_job(job_id)
        if not job:
            raise ValueError(f"Job with ID {job_id} not found")
        
        job.update_status(new_status)
        self.save_job(job)
        
    def get_job(self, job_id: str) -> Optional[JobType]:
        job_data_json = self.redis_client.get(self._get_key(job_id))
        if not job_data_json:
            return None

        job_data = json.loads(job_data_json)
        timestamps = JobTimestamps(
            created_at=datetime.fromisoformat(job_data["timestamps"]["created_at"]),
            assigned_at=datetime.fromisoformat(job_data["timestamps"]["assigned_at"]) if job_data["timestamps"]["assigned_at"] else None,
            started_at=datetime.fromisoformat(job_data["timestamps"]["started_at"]) if job_data["timestamps"]["started_at"] else None,
            completed_at=datetime.fromisoformat(job_data["timestamps"]["completed_at"]) if job_data["timestamps"]["completed_at"] else None,
        )

        return JobType(
            job_id=job_data["job_id"],
            payload=job_data["payload"],
            status=JobStatus(job_data["status"]),
            assigned_worker=job_data["assigned_worker"],
            retry_count=job_data["retry_count"],
            max_retries=job_data["max_retries"],
            version=job_data.get("version", 0),
            processing_token=job_data.get("processing_token"),
            timestamps=timestamps
        )

    # ==================== Worker Heartbeat Methods ====================

    def register_worker(self, worker_id: str) -> None:
        """Register a worker and start its heartbeat"""
        self.send_heartbeat(worker_id)
        # Initialize empty job set for this worker
        self.redis_client.delete(self._get_worker_jobs_key(worker_id))

    def send_heartbeat(self, worker_id: str) -> None:
        """Update worker heartbeat with TTL - auto-expires if worker dies"""
        heartbeat_data = json.dumps({
            "worker_id": worker_id,
            "last_seen": datetime.utcnow().isoformat(),
            "status": "alive"
        })
        self.redis_client.setex(
            self._get_worker_heartbeat_key(worker_id),
            WORKER_HEARTBEAT_TTL,
            heartbeat_data
        )

    def is_worker_alive(self, worker_id: str) -> bool:
        """Check if worker heartbeat exists (not expired)"""
        return self.redis_client.exists(self._get_worker_heartbeat_key(worker_id)) > 0

    def get_all_workers(self) -> List[str]:
        """Get all registered workers (alive ones have valid heartbeats)"""
        keys = self.redis_client.keys("worker:*:heartbeat")
        return [key.split(":")[1] for key in keys]

    def get_dead_workers(self) -> List[str]:
        """Get workers whose heartbeats have expired but still have jobs assigned"""
        dead_workers = []
        # Get all workers that have jobs assigned
        job_keys = self.redis_client.keys("worker:*:jobs")
        for key in job_keys:
            worker_id = key.split(":")[1]
            if not self.is_worker_alive(worker_id):
                dead_workers.append(worker_id)
        return dead_workers

    def add_job_to_worker(self, worker_id: str, job_id: str) -> None:
        """Track which jobs a worker is processing"""
        self.redis_client.sadd(self._get_worker_jobs_key(worker_id), job_id)

    def remove_job_from_worker(self, worker_id: str, job_id: str) -> None:
        """Remove job from worker's job set"""
        self.redis_client.srem(self._get_worker_jobs_key(worker_id), job_id)

    def get_worker_jobs(self, worker_id: str) -> List[str]:
        """Get all jobs assigned to a worker"""
        jobs = self.redis_client.smembers(self._get_worker_jobs_key(worker_id))
        return list(jobs) if jobs else []

    def cleanup_worker(self, worker_id: str) -> None:
        """Clean up worker data after it's declared dead"""
        self.redis_client.delete(self._get_worker_heartbeat_key(worker_id))
        self.redis_client.delete(self._get_worker_jobs_key(worker_id))

    # ==================== Job Locking Methods ====================

    def try_acquire_job_lock(self, job_id: str, worker_id: str) -> Optional[str]:
        """
        Try to acquire an exclusive lock on a job for processing.
        Returns a processing token if successful, None if job is already locked.
        Uses Redis SETNX for atomic lock acquisition.
        """
        lock_key = self._get_job_lock_key(job_id)
        processing_token = str(uuid.uuid4())
        lock_data = json.dumps({
            "worker_id": worker_id,
            "token": processing_token,
            "acquired_at": datetime.utcnow().isoformat()
        })

        # SETNX - only set if not exists
        acquired = self.redis_client.set(lock_key, lock_data, nx=True, ex=JOB_LOCK_TTL)
        if acquired:
            return processing_token
        return None

    def release_job_lock(self, job_id: str, processing_token: str) -> bool:
        """
        Release the lock on a job. Only releases if token matches.
        Returns True if lock was released, False otherwise.
        """
        lock_key = self._get_job_lock_key(job_id)
        lock_data = self.redis_client.get(lock_key)

        if not lock_data:
            return False

        lock_info = json.loads(lock_data)
        if lock_info.get("token") == processing_token:
            self.redis_client.delete(lock_key)
            return True
        return False

    def extend_job_lock(self, job_id: str, processing_token: str) -> bool:
        """Extend the TTL on a job lock (for long-running jobs)"""
        lock_key = self._get_job_lock_key(job_id)
        lock_data = self.redis_client.get(lock_key)

        if not lock_data:
            return False

        lock_info = json.loads(lock_data)
        if lock_info.get("token") == processing_token:
            self.redis_client.expire(lock_key, JOB_LOCK_TTL)
            return True
        return False

    def get_job_lock_info(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get information about who holds the lock on a job"""
        lock_data = self.redis_client.get(self._get_job_lock_key(job_id))
        if lock_data:
            return json.loads(lock_data)
        return None

    # ==================== Idempotency Methods ====================

    def is_event_processed(self, event_id: str) -> bool:
        """Check if an event has already been processed (for Kafka replay safety)"""
        return self.redis_client.exists(self._get_idempotency_key(event_id)) > 0

    def mark_event_processed(self, event_id: str, result: str = "success") -> None:
        """Mark an event as processed to prevent duplicate processing on replay"""
        self.redis_client.setex(
            self._get_idempotency_key(event_id),
            IDEMPOTENCY_TTL,
            json.dumps({
                "processed_at": datetime.utcnow().isoformat(),
                "result": result
            })
        )

    def get_event_processing_result(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get the result of a previously processed event"""
        data = self.redis_client.get(self._get_idempotency_key(event_id))
        if data:
            return json.loads(data)
        return None

    # ==================== Job Query Methods ====================

    def get_running_jobs_for_worker(self, worker_id: str) -> List[JobType]:
        """Get all RUNNING jobs assigned to a specific worker"""
        jobs = []
        for key in self.redis_client.keys("job:*"):
            if key.endswith(":lock"):
                continue
            job = self.get_job(key.replace("job:", ""))
            if job and job.status == JobStatus.RUNNING and job.assigned_worker == worker_id:
                jobs.append(job)
        return jobs

    def get_stale_running_jobs(self, timeout_seconds: int = 300) -> List[JobType]:
        """Get jobs that have been RUNNING for too long (potential orphans)"""
        stale_jobs = []
        cutoff = datetime.utcnow()
        for key in self.redis_client.keys("job:*"):
            if key.endswith(":lock"):
                continue
            job = self.get_job(key.replace("job:", ""))
            if job and job.status == JobStatus.RUNNING and job.timestamps.started_at:
                running_time = (cutoff - job.timestamps.started_at).total_seconds()
                if running_time > timeout_seconds:
                    stale_jobs.append(job)
        return stale_jobs