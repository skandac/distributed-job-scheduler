from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from internal.store.jobrepo import JobRepository
from internal.models.job import JobType, JobStatus
from typing import Dict, Any, List, Optional
from datetime import datetime
from fastapi import APIRouter
from internal.kafka.producer import KafkaEventProducer
import json

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
        "version": job.version,
        "timestamps": {
            "created_at": job.timestamps.created_at.isoformat(),
            "assigned_at": job.timestamps.assigned_at.isoformat() if job.timestamps.assigned_at else None,
            "started_at": job.timestamps.started_at.isoformat() if job.timestamps.started_at else None,
            "completed_at": job.timestamps.completed_at.isoformat() if job.timestamps.completed_at else None,
        }
    }


# ==================== Worker Monitoring Endpoints ====================

@router.get("/workers/", response_model=Dict[str, Any])
def list_workers():
    """List all workers and their status"""
    workers = job_repo.get_all_workers()
    worker_info = []

    for worker_id in workers:
        heartbeat_key = f"worker:{worker_id}:heartbeat"
        heartbeat_data = job_repo.redis_client.get(heartbeat_key)
        jobs = job_repo.get_worker_jobs(worker_id)

        info = {
            "worker_id": worker_id,
            "alive": job_repo.is_worker_alive(worker_id),
            "active_jobs": jobs,
            "job_count": len(jobs)
        }

        if heartbeat_data:
            try:
                hb = json.loads(heartbeat_data)
                info["last_seen"] = hb.get("last_seen")
            except (json.JSONDecodeError, TypeError):
                pass

        worker_info.append(info)

    return {
        "workers": worker_info,
        "total": len(worker_info),
        "alive": sum(1 for w in worker_info if w["alive"])
    }


@router.get("/workers/{worker_id}", response_model=Dict[str, Any])
def get_worker(worker_id: str):
    """Get details about a specific worker"""
    alive = job_repo.is_worker_alive(worker_id)
    jobs = job_repo.get_worker_jobs(worker_id)

    heartbeat_key = f"worker:{worker_id}:heartbeat"
    heartbeat_data = job_repo.redis_client.get(heartbeat_key)

    result = {
        "worker_id": worker_id,
        "alive": alive,
        "active_jobs": jobs,
        "job_count": len(jobs)
    }

    if heartbeat_data:
        try:
            hb = json.loads(heartbeat_data)
            result["last_seen"] = hb.get("last_seen")
            result["status"] = hb.get("status")
        except (json.JSONDecodeError, TypeError):
            pass

    # Get job details
    job_details = []
    for job_id in jobs:
        job = job_repo.get_job(job_id)
        if job:
            job_details.append({
                "job_id": job.job_id,
                "status": job.status.value,
                "started_at": job.timestamps.started_at.isoformat() if job.timestamps.started_at else None
            })
    result["job_details"] = job_details

    return result


# ==================== Job Management Endpoints ====================

@router.get("/jobs/", response_model=Dict[str, Any])
def list_jobs(status: Optional[str] = None, limit: int = 100):
    """List jobs with optional status filter"""
    jobs = []
    count = 0

    for key in job_repo.redis_client.keys("job:*"):
        if key.endswith(":lock"):
            continue
        if count >= limit:
            break

        job = job_repo.get_job(key.replace("job:", ""))
        if job:
            if status and job.status.value != status.upper():
                continue

            jobs.append({
                "job_id": job.job_id,
                "status": job.status.value,
                "assigned_worker": job.assigned_worker,
                "retry_count": job.retry_count,
                "created_at": job.timestamps.created_at.isoformat()
            })
            count += 1

    return {
        "jobs": jobs,
        "count": len(jobs),
        "filter": {"status": status} if status else None
    }


@router.post("/jobs/{job_id}/cancel", response_model=Dict[str, Any])
def cancel_job(job_id: str):
    """Cancel a pending or running job"""
    job = job_repo.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status in [JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELED]:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job in {job.status.value} status"
        )

    try:
        job.update_status(JobStatus.CANCELED)
        job_repo.save_job(job)

        kafka_producer.send_event({
            "event_type": "JobCanceled",
            "job_id": job_id,
            "previous_status": job.status.value,
            "timestamp": datetime.utcnow().isoformat()
        }, key=job_id)

        return {
            "job_id": job_id,
            "status": JobStatus.CANCELED.value,
            "message": "Job canceled successfully"
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/jobs/{job_id}/retry", response_model=Dict[str, Any])
def retry_job(job_id: str):
    """Manually retry a failed job"""
    job = job_repo.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != JobStatus.FAILED:
        raise HTTPException(
            status_code=400,
            detail=f"Can only retry FAILED jobs, current status: {job.status.value}"
        )

    # Reset retry count and requeue
    job.retry_count = 0
    job.update_status(JobStatus.PENDING)
    job_repo.save_job(job)

    kafka_producer.send_event({
        "event_type": "JobRetried",
        "job_id": job_id,
        "timestamp": datetime.utcnow().isoformat()
    }, key=job_id)

    return {
        "job_id": job_id,
        "status": JobStatus.PENDING.value,
        "message": "Job requeued for retry"
    }


# ==================== Health Check Endpoints ====================

@router.get("/health", response_model=Dict[str, Any])
def health_check():
    """System health check"""
    try:
        # Check Redis connectivity
        job_repo.redis_client.ping()
        redis_ok = True
    except Exception:
        redis_ok = False

    # Count jobs by status
    job_counts = {
        "PENDING": 0,
        "RUNNING": 0,
        "SUCCESS": 0,
        "FAILED": 0,
        "CANCELED": 0
    }

    for key in job_repo.redis_client.keys("job:*"):
        if key.endswith(":lock"):
            continue
        job = job_repo.get_job(key.replace("job:", ""))
        if job:
            job_counts[job.status.value] = job_counts.get(job.status.value, 0) + 1

    # Count workers
    workers = job_repo.get_all_workers()
    alive_workers = sum(1 for w in workers if job_repo.is_worker_alive(w))

    return {
        "status": "healthy" if redis_ok else "degraded",
        "redis": "connected" if redis_ok else "disconnected",
        "workers": {
            "total": len(workers),
            "alive": alive_workers
        },
        "jobs": job_counts,
        "timestamp": datetime.utcnow().isoformat()
    }