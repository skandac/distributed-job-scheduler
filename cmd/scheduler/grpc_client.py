import grpc 
from internal.proto import scheduler_pb2_grpc, scheduler_pb2
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict

class SchedulerClient:
    def __init__(self, host='localhost', port=50051):
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = scheduler_pb2_grpc.SchedulerServiceStub(self.channel)

    def create_job(self, payload: dict, max_retries: int = 3):
        from google.protobuf.struct_pb2 import Struct
        payload_struct = Struct()
        payload_struct.update(payload)

        request = scheduler_pb2.CreateJobRequest(
            payload=payload_struct,
            max_retries=max_retries
        )
        response = self.stub.CreateJob(request)
        return {
            "job_id": response.job_id,
            "status": scheduler_pb2.JobStatus.Name(response.status)
        }

    def get_job(self, job_id: str):
        request = scheduler_pb2.GetJobRequest(job_id=job_id)
        response = self.stub.GetJob(request)

        payload_dict = MessageToDict(response.payload)

        return {
            "job_id": response.job_id,
            "payload": payload_dict,
            "status": scheduler_pb2.JobStatus.Name(response.status),
            "assigned_worker": response.assigned_worker,
            "retry_count": response.retry_count,
            "max_retries": response.max_retries,
            "timestamps": {
                "created_at": response.timestamps.created_at,
                "assigned_at": response.timestamps.assigned_at,
                "started_at": response.timestamps.started_at,
                "completed_at": response.timestamps.completed_at,
            }
        }
    def close(self):
        self.channel.close()