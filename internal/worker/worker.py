import time
import logging
import json
from datetime import datetime
from internal.kafka.consumer import KafkaEventConsumer
from internal.kafka.producer import KafkaEventProducer
from internal.store.jobrepo import JobRepository
from internal.models.job import JobStatus
from internal.models.job import JobType
from typing import Optional

HEARTBEAT_INTERVAL = 5 
WORKER_TIMEOUT = 15

logging.basicConfig(level=logging.INFO)

class Worker:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.job_repo = JobRepository()

        self.consumer = KafkaEventConsumer(
            brokers="localhost:9092",
            topic="jobs",
            group_id="workers"
        )

        # Kafka producer to send job status updates
        self.producer = KafkaEventProducer(
            brokers="localhost:9092",
            topic="jobs"
        )
    def send_heartbeat(self) -> None:
        self.job_repo.redis_client.set(f"worker:{self.worker_id}:heartbeat", datetime.utcnow().isoformat())

    def fetch_job(self) -> Optional[str]:
        for _, event in self.consumer.consume_events():
            if event.get("event_type") == "JobCreated" and "job_id" in event:
                logging.info(f"Received JobCreated event from Kafka: {event['job_id']}")
                return event["job_id"]
        return None
    
    def fetch_pending_jobs(self) -> Optional[str]:
        for key in self.job_repo.redis_client.keys("job:*"):
            job_data = self.job_repo.redis_client.get(key)
            if job_data:
                job_json = job_data.decode() if isinstance(job_data, bytes) else job_data
                job_dict = json.loads(job_json)
                if job_dict.get("status") == "PENDING":
                    yield job_dict["job_id"]

    def process_job(self, job_id: str):

        job = self.job_repo.get_job(job_id)
        if not job:
            logging.warning(f"Job {job_id} not found in Redis")
            return
        # idempotency check
        if job.status in [JobStatus.SUCCESS, JobStatus.FAILED]:
            logging.info(f"Skipping job {job_id}, already {job.status}")
            return

        try:
            # Assign worker and mark RUNNING
            logging.info(f"Picked up job {job_id}, current status: {job.status}")
            job.assigned_worker = self.worker_id
            job.update_status(JobStatus.RUNNING)
            self.job_repo.save_job(job)

            # Notify job started
            self.producer.send_event({
                "event_type": "JobStarted",
                "job_id": job_id,
                "worker_id": self.worker_id
            }, key=job_id)

            # Simulate job processing
            time.sleep(2)

            # Mark job SUCCESS
            job.update_status(JobStatus.SUCCESS)
            self.job_repo.save_job(job)

            # Notify job completed
            self.producer.send_event({
                "event_type": "JobCompleted",
                "job_id": job_id
            }, key=job_id)
            logging.info(f"Completed job {job_id}")

        except Exception as e:
            # Handle retries
            job.retry_count += 1
            status = (
                JobStatus.FAILED
                if job.retry_count > job.max_retries
                else JobStatus.PENDING
            )
            job.update_status(status)
            self.job_repo.save_job(job)

            # Notify job failed
            self.producer.send_event({
                "event_type": "JobFailed",
                "job_id": job_id,
                "error": str(e),
                "retry_count": job.retry_count
            }, key=job_id)
            logging.error(f"Job {job_id} failed with error: {e}. Retry count: {job.retry_count}")
    
    def run(self) -> None:
        logging.info(f"Worker {self.worker_id} starting...")
        for job_id in self.fetch_pending_jobs():
            self.process_job(job_id)

        while True:
            self.send_heartbeat()
            job = self.fetch_job()
            if job:
                self.process_job(job)
            else:
                time.sleep(HEARTBEAT_INTERVAL)  # Wait before checking for new jobs again

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    worker = Worker(worker_id="worker_1")
    worker.run()