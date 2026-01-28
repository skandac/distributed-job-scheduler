import time
import logging
import json
import threading
import signal
import sys
from datetime import datetime
from internal.kafka.consumer import KafkaEventConsumer
from internal.kafka.producer import KafkaEventProducer
from internal.store.jobrepo import JobRepository
from internal.models.job import JobStatus, JobType
from typing import Optional

HEARTBEAT_INTERVAL = 5  # Send heartbeat every 5 seconds
WORKER_TIMEOUT = 15     # Worker considered dead after 15 seconds without heartbeat

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.job_repo = JobRepository()
        self.running = True
        self._current_job_id: Optional[str] = None
        self._current_processing_token: Optional[str] = None
        self._heartbeat_thread: Optional[threading.Thread] = None

        self.consumer = KafkaEventConsumer(
            brokers="localhost:9092",
            topic="jobs",
            group_id="workers",
            auto_commit=True  # Auto-commit for simplicity; use manual for stricter guarantees
        )

        self.producer = KafkaEventProducer(
            brokers="localhost:9092",
            topic="jobs"
        )

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Worker {self.worker_id} received shutdown signal")
        self.shutdown()

    def shutdown(self):
        """Graceful shutdown - release current job back to pending"""
        self.running = False

        # If we're processing a job, release the lock and requeue it
        if self._current_job_id and self._current_processing_token:
            logger.info(f"Releasing job {self._current_job_id} during shutdown")
            try:
                job = self.job_repo.get_job(self._current_job_id)
                if job and job.status == JobStatus.RUNNING:
                    # Requeue the job for another worker
                    job.update_status(JobStatus.PENDING)
                    self.job_repo.save_job(job)
                    logger.info(f"Job {self._current_job_id} requeued to PENDING")

                # Release the lock
                self.job_repo.release_job_lock(self._current_job_id, self._current_processing_token)
                self.job_repo.remove_job_from_worker(self.worker_id, self._current_job_id)
            except Exception as e:
                logger.error(f"Error during shutdown cleanup: {e}")

        # Clean up worker registration
        self.job_repo.cleanup_worker(self.worker_id)
        logger.info(f"Worker {self.worker_id} shutdown complete")

    def _start_heartbeat_thread(self):
        """Start background thread to send heartbeats"""
        def heartbeat_loop():
            while self.running:
                try:
                    self.send_heartbeat()
                except Exception as e:
                    logger.error(f"Heartbeat error: {e}")
                time.sleep(HEARTBEAT_INTERVAL)

        self._heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

    def send_heartbeat(self) -> None:
        """Send heartbeat to Redis with TTL"""
        self.job_repo.send_heartbeat(self.worker_id)
        logger.debug(f"Worker {self.worker_id} heartbeat sent")

    def _generate_event_id(self, event: dict) -> str:
        """Generate a unique event ID for idempotency tracking"""
        # Use job_id + event_type + timestamp for uniqueness
        job_id = event.get("job_id", "unknown")
        event_type = event.get("event_type", "unknown")
        # For Kafka replay, we need consistent IDs, so use job_id and event_type
        # Add retry_count for failed events to allow reprocessing
        retry_count = event.get("retry_count", 0)
        return f"{job_id}:{event_type}:{retry_count}"

    def fetch_job(self) -> Optional[dict]:
        """Fetch job events from Kafka with idempotency check"""
        for _, event in self.consumer.consume_events():
            if not self.running:
                return None

            if event.get("event_type") == "JobCreated" and "job_id" in event:
                event_id = self._generate_event_id(event)

                # Idempotency check - skip if already processed
                if self.job_repo.is_event_processed(event_id):
                    logger.info(f"Skipping already processed event: {event_id}")
                    continue

                logger.info(f"Received JobCreated event from Kafka: {event['job_id']}")
                return {"event_id": event_id, "job_id": event["job_id"]}

        return None

    def fetch_pending_jobs(self):
        """Fetch existing pending jobs from Redis"""
        for key in self.job_repo.redis_client.keys("job:*"):
            if key.endswith(":lock"):
                continue
            job_data = self.job_repo.redis_client.get(key)
            if job_data:
                job_json = job_data if isinstance(job_data, str) else job_data.decode()
                job_dict = json.loads(job_json)
                if job_dict.get("status") == "PENDING":
                    yield job_dict["job_id"]

    def process_job(self, job_id: str, event_id: Optional[str] = None) -> bool:
        """
        Process a job with proper locking and idempotency.
        Returns True if job was processed successfully, False otherwise.
        """
        job = self.job_repo.get_job(job_id)
        if not job:
            logger.warning(f"Job {job_id} not found in Redis")
            if event_id:
                self.job_repo.mark_event_processed(event_id, "not_found")
            return False

        # Idempotency check - skip if already completed or failed terminally
        if job.status == JobStatus.SUCCESS:
            logger.info(f"Skipping job {job_id}, already SUCCESS")
            if event_id:
                self.job_repo.mark_event_processed(event_id, "already_completed")
            return True

        if job.status == JobStatus.FAILED and job.retry_count >= job.max_retries:
            logger.info(f"Skipping job {job_id}, already FAILED with max retries")
            if event_id:
                self.job_repo.mark_event_processed(event_id, "already_failed")
            return True

        # Skip if job is already running (being processed by another worker)
        if job.status == JobStatus.RUNNING:
            # Check if the lock is still valid
            lock_info = self.job_repo.get_job_lock_info(job_id)
            if lock_info and lock_info.get("worker_id") != self.worker_id:
                logger.info(f"Skipping job {job_id}, already being processed by {lock_info.get('worker_id')}")
                return False

        # Try to acquire exclusive lock on the job
        processing_token = self.job_repo.try_acquire_job_lock(job_id, self.worker_id)
        if not processing_token:
            logger.info(f"Could not acquire lock for job {job_id}, another worker is processing it")
            return False

        # Track current job for graceful shutdown
        self._current_job_id = job_id
        self._current_processing_token = processing_token

        try:
            # Re-fetch job after acquiring lock (check for race conditions)
            job = self.job_repo.get_job(job_id)
            if not job or job.status not in [JobStatus.PENDING, JobStatus.UNKNOWN]:
                logger.info(f"Job {job_id} state changed, skipping (status: {job.status if job else 'None'})")
                return False

            # Assign worker and mark RUNNING
            logger.info(f"Picked up job {job_id}, current status: {job.status}")
            job.assigned_worker = self.worker_id
            job.processing_token = processing_token
            job.timestamps.assigned_at = datetime.utcnow()
            job.update_status(JobStatus.RUNNING)
            self.job_repo.save_job(job)

            # Track job assignment for crash recovery
            self.job_repo.add_job_to_worker(self.worker_id, job_id)

            # Notify job started
            self.producer.send_event({
                "event_type": "JobStarted",
                "job_id": job_id,
                "worker_id": self.worker_id,
                "processing_token": processing_token
            }, key=job_id)

            # Simulate job processing (replace with actual job execution)
            self._execute_job(job)

            # Mark job SUCCESS
            job.update_status(JobStatus.SUCCESS)
            job.processing_token = None
            self.job_repo.save_job(job)

            # Notify job completed
            self.producer.send_event({
                "event_type": "JobCompleted",
                "job_id": job_id,
                "worker_id": self.worker_id
            }, key=job_id)

            # Mark event as processed for idempotency
            if event_id:
                self.job_repo.mark_event_processed(event_id, "success")

            logger.info(f"Completed job {job_id}")
            return True

        except Exception as e:
            logger.error(f"Job {job_id} failed with error: {e}")
            self._handle_job_failure(job, str(e), event_id)
            return False

        finally:
            # Clean up
            self.job_repo.release_job_lock(job_id, processing_token)
            self.job_repo.remove_job_from_worker(self.worker_id, job_id)
            self._current_job_id = None
            self._current_processing_token = None

    def _execute_job(self, job: JobType) -> None:
        """
        Execute the actual job logic.
        Override this method for custom job execution.
        """
        # Simulate job processing - replace with actual work
        work_duration = job.payload.get("duration", 2)
        for i in range(work_duration):
            if not self.running:
                raise Exception("Worker shutdown requested")
            time.sleep(1)
            # Extend lock for long-running jobs
            if self._current_processing_token:
                self.job_repo.extend_job_lock(job.job_id, self._current_processing_token)

    def _handle_job_failure(self, job: JobType, error: str, event_id: Optional[str] = None) -> None:
        """Handle job failure with retry logic"""
        try:
            # Re-fetch to get latest state
            job = self.job_repo.get_job(job.job_id)
            if not job:
                return

            # First, transition to FAILED (this increments retry_count)
            job.update_status(JobStatus.FAILED)

            # Check if we have retries remaining
            if job.retry_count < job.max_retries:
                # Requeue for retry (FAILED -> PENDING)
                job.update_status(JobStatus.PENDING)
                self.job_repo.save_job(job)
                logger.info(f"Job {job.job_id} requeued for retry ({job.retry_count}/{job.max_retries})")
            else:
                # Max retries exceeded, stay in FAILED state
                self.job_repo.save_job(job)
                logger.error(f"Job {job.job_id} permanently failed after {job.retry_count} retries")

            # Notify job failed
            self.producer.send_event({
                "event_type": "JobFailed",
                "job_id": job.job_id,
                "error": error,
                "retry_count": job.retry_count,
                "will_retry": job.status == JobStatus.PENDING
            }, key=job.job_id)

            # Mark event as processed (even failures should be tracked)
            if event_id:
                self.job_repo.mark_event_processed(event_id, f"failed:{error}")

        except Exception as e:
            logger.error(f"Error handling job failure: {e}")

    def run(self) -> None:
        """Main worker loop"""
        logger.info(f"Worker {self.worker_id} starting...")

        # Register worker and start heartbeat
        self.job_repo.register_worker(self.worker_id)
        self._start_heartbeat_thread()

        # First, process any existing pending jobs
        logger.info(f"Checking for existing pending jobs...")
        for job_id in self.fetch_pending_jobs():
            if not self.running:
                break
            self.process_job(job_id)

        # Main event loop
        logger.info(f"Worker {self.worker_id} entering main loop...")
        while self.running:
            try:
                # Send heartbeat in main loop as backup
                self.send_heartbeat()

                # Fetch and process jobs from Kafka
                event_data = self.fetch_job()
                if event_data:
                    self.process_job(event_data["job_id"], event_data.get("event_id"))
                else:
                    time.sleep(HEARTBEAT_INTERVAL)

            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(1)

        logger.info(f"Worker {self.worker_id} stopped")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Distributed Job Worker")
    parser.add_argument("--worker-id", default="worker_1", help="Unique worker ID")
    args = parser.parse_args()

    worker = Worker(worker_id=args.worker_id)
    worker.run()
