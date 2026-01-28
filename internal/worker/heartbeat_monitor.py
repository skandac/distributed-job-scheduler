"""
Heartbeat Monitor Service

This service monitors worker heartbeats and handles crash recovery:
1. Detects workers whose heartbeats have expired (TTL timeout)
2. Requeues RUNNING jobs from dead workers back to PENDING
3. Emits WorkerDead events to Kafka for observability
4. Cleans up stale job locks

Run this as a separate service alongside workers.
"""

import time
import logging
import threading
from datetime import datetime
from typing import List, Set
from internal.store.jobrepo import JobRepository, WORKER_HEARTBEAT_TTL
from internal.kafka.producer import KafkaEventProducer
from internal.models.job import JobStatus, JobType

# How often to check for dead workers (in seconds)
MONITOR_INTERVAL = 10

# How long a job can be RUNNING before considered stale (in seconds)
STALE_JOB_TIMEOUT = 300  # 5 minutes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HeartbeatMonitor:
    """
    Monitors worker heartbeats and handles crash recovery.

    This service should run as a singleton or with leader election
    to avoid duplicate recovery actions.
    """

    def __init__(self):
        self.job_repo = JobRepository()
        self.producer = KafkaEventProducer(
            brokers="localhost:9092",
            topic="jobs"
        )
        self.running = True
        self._known_workers: Set[str] = set()
        self._lock = threading.Lock()

    def _get_all_workers_with_jobs(self) -> List[str]:
        """Get all workers that have jobs assigned (from worker:*:jobs keys)"""
        workers = []
        keys = self.job_repo.redis_client.keys("worker:*:jobs")
        for key in keys:
            worker_id = key.split(":")[1]
            workers.append(worker_id)
        return workers

    def _get_running_jobs_for_worker(self, worker_id: str) -> List[str]:
        """Get job IDs assigned to a worker"""
        return self.job_repo.get_worker_jobs(worker_id)

    def check_worker_health(self, worker_id: str) -> bool:
        """Check if a worker is still alive based on heartbeat TTL"""
        return self.job_repo.is_worker_alive(worker_id)

    def recover_jobs_from_dead_worker(self, worker_id: str) -> int:
        """
        Recover all running jobs from a dead worker.
        Returns the number of jobs requeued.
        """
        recovered_count = 0

        # Get jobs assigned to this worker
        job_ids = self._get_running_jobs_for_worker(worker_id)
        logger.info(f"Found {len(job_ids)} jobs assigned to dead worker {worker_id}")

        for job_id in job_ids:
            try:
                job = self.job_repo.get_job(job_id)
                if not job:
                    logger.warning(f"Job {job_id} not found, skipping")
                    continue

                # Only recover RUNNING jobs
                if job.status != JobStatus.RUNNING:
                    logger.debug(f"Job {job_id} is {job.status}, not RUNNING, skipping")
                    continue

                # Verify this job was assigned to the dead worker
                if job.assigned_worker != worker_id:
                    logger.debug(f"Job {job_id} assigned to {job.assigned_worker}, not {worker_id}")
                    continue

                # Requeue the job
                logger.info(f"Requeuing job {job_id} from dead worker {worker_id}")

                # Clear the job lock if it exists
                lock_info = self.job_repo.get_job_lock_info(job_id)
                if lock_info:
                    # Force release the lock by deleting it
                    self.job_repo.redis_client.delete(self.job_repo._get_job_lock_key(job_id))

                # Transition job back to PENDING
                job.update_status(JobStatus.PENDING)
                self.job_repo.save_job(job)

                # Emit JobRequeued event
                self.producer.send_event({
                    "event_type": "JobRequeued",
                    "job_id": job_id,
                    "reason": "worker_dead",
                    "previous_worker": worker_id,
                    "retry_count": job.retry_count,
                    "timestamp": datetime.utcnow().isoformat()
                }, key=job_id)

                recovered_count += 1
                logger.info(f"Successfully requeued job {job_id}")

            except Exception as e:
                logger.error(f"Failed to recover job {job_id}: {e}")

        return recovered_count

    def handle_dead_worker(self, worker_id: str) -> None:
        """Handle a worker that has been detected as dead"""
        logger.warning(f"Worker {worker_id} detected as DEAD (heartbeat expired)")

        # Emit WorkerDead event
        self.producer.send_event({
            "event_type": "WorkerDead",
            "worker_id": worker_id,
            "detected_at": datetime.utcnow().isoformat(),
            "reason": "heartbeat_timeout"
        }, key=worker_id)

        # Recover jobs from the dead worker
        recovered_count = self.recover_jobs_from_dead_worker(worker_id)
        logger.info(f"Recovered {recovered_count} jobs from dead worker {worker_id}")

        # Clean up worker data
        self.job_repo.cleanup_worker(worker_id)

        # Remove from known workers set
        with self._lock:
            self._known_workers.discard(worker_id)

    def check_stale_jobs(self) -> int:
        """
        Check for jobs that have been RUNNING for too long without progress.
        These may be orphaned due to various failure modes.
        Returns the number of stale jobs recovered.
        """
        recovered_count = 0
        stale_jobs = self.job_repo.get_stale_running_jobs(STALE_JOB_TIMEOUT)

        for job in stale_jobs:
            try:
                logger.warning(f"Found stale job {job.job_id} (running since {job.timestamps.started_at})")

                # Check if the assigned worker is still alive
                if job.assigned_worker and self.check_worker_health(job.assigned_worker):
                    # Worker is alive, check if it still has the lock
                    lock_info = self.job_repo.get_job_lock_info(job.job_id)
                    if lock_info and lock_info.get("worker_id") == job.assigned_worker:
                        logger.info(f"Job {job.job_id} still locked by alive worker {job.assigned_worker}, skipping")
                        continue

                # Worker is dead or lock expired, recover the job
                logger.info(f"Recovering stale job {job.job_id}")

                # Clear the lock
                self.job_repo.redis_client.delete(self.job_repo._get_job_lock_key(job.job_id))

                # Requeue the job
                job.update_status(JobStatus.PENDING)
                self.job_repo.save_job(job)

                # Emit event
                self.producer.send_event({
                    "event_type": "JobRequeued",
                    "job_id": job.job_id,
                    "reason": "stale_timeout",
                    "previous_worker": job.assigned_worker,
                    "timestamp": datetime.utcnow().isoformat()
                }, key=job.job_id)

                recovered_count += 1

            except Exception as e:
                logger.error(f"Failed to recover stale job {job.job_id}: {e}")

        return recovered_count

    def monitor_cycle(self) -> None:
        """Perform one monitoring cycle"""
        try:
            # Get all workers that have jobs assigned
            workers_with_jobs = self._get_all_workers_with_jobs()

            # Track new workers
            with self._lock:
                for worker_id in workers_with_jobs:
                    if worker_id not in self._known_workers:
                        self._known_workers.add(worker_id)
                        logger.info(f"Discovered worker: {worker_id}")

            # Check for dead workers
            dead_workers = []
            for worker_id in workers_with_jobs:
                if not self.check_worker_health(worker_id):
                    dead_workers.append(worker_id)

            # Handle dead workers
            for worker_id in dead_workers:
                self.handle_dead_worker(worker_id)

            # Also check for stale jobs (additional safety net)
            stale_recovered = self.check_stale_jobs()
            if stale_recovered > 0:
                logger.info(f"Recovered {stale_recovered} stale jobs")

        except Exception as e:
            logger.error(f"Error in monitor cycle: {e}")

    def run(self) -> None:
        """Main monitoring loop"""
        logger.info("Heartbeat Monitor starting...")
        logger.info(f"Monitoring interval: {MONITOR_INTERVAL}s")
        logger.info(f"Worker heartbeat TTL: {WORKER_HEARTBEAT_TTL}s")
        logger.info(f"Stale job timeout: {STALE_JOB_TIMEOUT}s")

        while self.running:
            self.monitor_cycle()
            time.sleep(MONITOR_INTERVAL)

        logger.info("Heartbeat Monitor stopped")

    def stop(self) -> None:
        """Stop the monitor"""
        self.running = False


class HeartbeatMonitorWithLeaderElection(HeartbeatMonitor):
    """
    Heartbeat monitor with Redis-based leader election.
    Only the leader performs monitoring to avoid duplicate recovery actions.
    """

    LEADER_KEY = "heartbeat_monitor:leader"
    LEADER_TTL = 30  # Leader lease duration in seconds
    LEADER_RENEW_INTERVAL = 10  # How often to renew leadership

    def __init__(self, monitor_id: str = None):
        super().__init__()
        self.monitor_id = monitor_id or f"monitor_{datetime.utcnow().timestamp()}"
        self._is_leader = False
        self._leader_thread: threading.Thread = None

    def _try_acquire_leadership(self) -> bool:
        """Try to become the leader using Redis SETNX"""
        acquired = self.job_repo.redis_client.set(
            self.LEADER_KEY,
            self.monitor_id,
            nx=True,
            ex=self.LEADER_TTL
        )
        if acquired:
            self._is_leader = True
            logger.info(f"Monitor {self.monitor_id} acquired leadership")
        return acquired

    def _renew_leadership(self) -> bool:
        """Renew leadership lease"""
        current_leader = self.job_repo.redis_client.get(self.LEADER_KEY)
        if current_leader == self.monitor_id:
            self.job_repo.redis_client.expire(self.LEADER_KEY, self.LEADER_TTL)
            return True
        self._is_leader = False
        return False

    def _release_leadership(self) -> None:
        """Release leadership"""
        current_leader = self.job_repo.redis_client.get(self.LEADER_KEY)
        if current_leader == self.monitor_id:
            self.job_repo.redis_client.delete(self.LEADER_KEY)
            logger.info(f"Monitor {self.monitor_id} released leadership")
        self._is_leader = False

    def _leader_loop(self) -> None:
        """Background thread to maintain leadership"""
        while self.running:
            try:
                if self._is_leader:
                    if not self._renew_leadership():
                        logger.warning(f"Monitor {self.monitor_id} lost leadership")
                else:
                    self._try_acquire_leadership()
            except Exception as e:
                logger.error(f"Leader election error: {e}")
            time.sleep(self.LEADER_RENEW_INTERVAL)

    def run(self) -> None:
        """Main monitoring loop with leader election"""
        logger.info(f"Heartbeat Monitor {self.monitor_id} starting with leader election...")

        # Start leader election thread
        self._leader_thread = threading.Thread(target=self._leader_loop, daemon=True)
        self._leader_thread.start()

        while self.running:
            if self._is_leader:
                self.monitor_cycle()
            else:
                logger.debug(f"Monitor {self.monitor_id} is not leader, waiting...")
            time.sleep(MONITOR_INTERVAL)

        # Release leadership on shutdown
        self._release_leadership()
        logger.info(f"Heartbeat Monitor {self.monitor_id} stopped")


if __name__ == "__main__":
    import argparse
    import signal
    import sys

    parser = argparse.ArgumentParser(description="Heartbeat Monitor Service")
    parser.add_argument(
        "--leader-election",
        action="store_true",
        help="Enable leader election for HA deployment"
    )
    parser.add_argument(
        "--monitor-id",
        default=None,
        help="Unique monitor ID (for leader election)"
    )
    args = parser.parse_args()

    if args.leader_election:
        monitor = HeartbeatMonitorWithLeaderElection(monitor_id=args.monitor_id)
    else:
        monitor = HeartbeatMonitor()

    def signal_handler(signum, frame):
        logger.info("Shutdown signal received")
        monitor.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    monitor.run()
