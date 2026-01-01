from internal.models.job import JobType, JobStatus, JobTimestamps
from datetime import datetime
import redis
import json
from typing import Optional, Dict, Any
import os

class JobRepository:
    def __init__(self, redis_url: Optional[str] = None):
        self.redis_client = redis.Redis.from_url(
            redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True  # CRITICAL: Ensures Redis returns strings, not bytes
        )
    
    def _get_key(self, job_id: str) -> str:
        """Helper to ensure consistent key naming"""
        return f"job:{job_id}"

    def save_job(self, job: JobType) -> None:
        job_data = {
            "job_id": job.job_id,
            "payload": job.payload,
            "status": job.status.value,
            "assigned_worker": job.assigned_worker,
            "retry_count": job.retry_count,
            "max_retries": job.max_retries,
            "timestamps": {
                "created_at": job.timestamps.created_at.isoformat(),
                "assigned_at": job.timestamps.assigned_at.isoformat() if job.timestamps.assigned_at else None,
                "started_at": job.timestamps.started_at.isoformat() if job.timestamps.started_at else None,
                "completed_at": job.timestamps.completed_at.isoformat() if job.timestamps.completed_at else None,
            }
        }
        key = self._get_key(job.job_id) # Use consistent key naming and prevent collisions
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
            timestamps=timestamps
        )