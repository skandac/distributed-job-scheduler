from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
import enum

class JobStatus(str, enum.Enum):

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"
    
@dataclass
class JobTimestamps:
    created_at: datetime
    assigned_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

@dataclass
class JobType:
    job_id: str
    payload: Dict[str, Any]
    status: JobStatus = JobStatus.PENDING
    assigned_worker: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    timestamps: JobTimestamps = field(
        default_factory=lambda: JobTimestamps(created_at=datetime.utcnow())
    )

    @staticmethod
    def create_new(job_id: str, payload: Dict[str, Any], max_retries: int = 3) -> 'JobType':
        return JobType(
            job_id=job_id,
            payload=payload,
            max_retries=max_retries
        )
    def can_transition_to(self, new_status: JobStatus) -> bool:
        valid_transitions = {
            JobStatus.PENDING: [JobStatus.RUNNING, JobStatus.CANCELED],
            JobStatus.RUNNING: [JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELED],
            JobStatus.FAILED: [JobStatus.PENDING] if self.retry_count < self.max_retries else [],
            JobStatus.SUCCESS: [],
            JobStatus.CANCELED: [],
            JobStatus.UNKNOWN: [JobStatus.PENDING],
        }
        return new_status in valid_transitions[self.status]
    
    def update_status(self, new_status: JobStatus):

        if not self.can_transition_to(new_status):
            raise ValueError(f"Invalid transition {self.status} → {new_status}")
        
        #self.status = new_status
        #If anything fails after this line (DB write, Kafka emit, crash), 
        # the job is now in a half-updated state:
        now = datetime.utcnow()

        if new_status == JobStatus.RUNNING:
            if self.timestamps.started_at is None:
                self.timestamps.started_at = now
            self.timestamps.started_at = now

        elif new_status in (JobStatus.SUCCESS, JobStatus.FAILED):
            self.timestamps.completed_at = now
            if new_status == JobStatus.FAILED:
                self.retry_count += 1

        elif new_status == JobStatus.CANCELED:
            self.timestamps.completed_at = now

        elif new_status == JobStatus.PENDING:
            self.timestamps.assigned_at = None
            self.timestamps.started_at = None
            self.timestamps.completed_at = None
            self.assigned_worker = None

        self.status = new_status
    



