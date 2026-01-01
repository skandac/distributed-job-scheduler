import grpc
import time
from concurrent import futures
from google.protobuf.struct_pb2 import Struct

from internal.proto import scheduler_pb2_grpc, scheduler_pb2
from internal.store.jobrepo import JobRepository
from internal.models.job import JobStatus, JobType


class SchedulerServicer(scheduler_pb2_grpc.SchedulerServiceServicer):
    def __init__(self):
        self.job_repo = JobRepository()

    def CreateJob(self, request, context):
        job_id = f"job_{int(time.time())}"

        payload_dict = dict(request.payload)

        job = JobType.create_new(
            job_id=job_id,
            payload=payload_dict,
            max_retries=request.max_retries,
        )

        self.job_repo.save_job(job)

        return scheduler_pb2.CreateJobResponse(
            job_id=job.job_id,
            status=scheduler_pb2.JobStatus.Value(
                f"JOB_STATUS_{job.status.value}"
            ),
        )

    def GetJob(self, request, context):
        job = self.job_repo.get_job(request.job_id)

        if not job:
            context.abort(grpc.StatusCode.NOT_FOUND, "Job not found")

        payload_struct = Struct()
        payload_struct.update(job.payload)

        return scheduler_pb2.GetJobResponse(
            job_id=job.job_id,
            payload=payload_struct,
            status=scheduler_pb2.JobStatus.Value(
                f"JOB_STATUS_{job.status.value}"
            ),
            assigned_worker=job.assigned_worker or "",
            retry_count=job.retry_count,
            max_retries=job.max_retries,
            timestamps=scheduler_pb2.JobTimestamps(
                created_at=int(job.timestamps.created_at.timestamp()),
                assigned_at=int(job.timestamps.assigned_at.timestamp())
                if job.timestamps.assigned_at else 0,
                started_at=int(job.timestamps.started_at.timestamp())
                if job.timestamps.started_at else 0,
                completed_at=int(job.timestamps.completed_at.timestamp())
                if job.timestamps.completed_at else 0,
            ),
        )
