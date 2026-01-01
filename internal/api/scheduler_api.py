from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from internal.store.jobrepo import JobRepository
from internal.models.job import JobType, JobStatus
from typing import Dict, Any
from datetime import datetime
from fastapi import APIRouter
from internal.kafka.producer import KafkaEventProducer

router = APIRouter()
job_repo = JobRepository()
kafka_producer = KafkaEventProducer(
    brokers="localhost:9092",
    topic="jobs"
)

class CreateJobRequest(BaseModel):
    payload: Dict[str, Any]
    max_retries: int = 3


@router.post("/jobs/", response_model=Dict[str, Any])
def create_job(request: CreateJobRequest):
    job_id = f"job_{int(datetime.utcnow().timestamp() * 1000)}"
    job = JobType.create_new(
        job_id=job_id,
        payload=request.payload,
        max_retries=request.max_retries
    )
    job_repo.save_job(job)
    
    kafka_producer.send_event({
    "event_type": "JobCreated",
    "job_id": job_id,
    "payload": request.payload
}, key=job_id)

    return {
        "job_id": job_id,
        "status": JobStatus.PENDING.value
    }

@router.get("/jobs/{job_id}", response_model=Dict[str, Any])
def get_job(job_id: str):
    job = job_repo.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
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