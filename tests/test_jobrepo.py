"""
Tests for JobRepository including heartbeat, locking, and idempotency.
"""

import pytest
import time
import json
from datetime import datetime
from internal.models.job import JobType, JobStatus
from internal.store.jobrepo import (
    JobRepository,
    WORKER_HEARTBEAT_TTL,
    JOB_LOCK_TTL,
    IDEMPOTENCY_TTL
)


class TestJobPersistence:
    """Tests for basic job CRUD operations"""

    def test_save_and_get_job(self, job_repo, sample_job):
        """Test saving and retrieving a job"""
        job_repo.save_job(sample_job)
        retrieved = job_repo.get_job(sample_job.job_id)

        assert retrieved is not None
        assert retrieved.job_id == sample_job.job_id
        assert retrieved.payload == sample_job.payload
        assert retrieved.status == sample_job.status
        assert retrieved.max_retries == sample_job.max_retries

    def test_get_nonexistent_job(self, job_repo):
        """Test getting a job that doesn't exist"""
        result = job_repo.get_job("nonexistent_job")
        assert result is None

    def test_save_job_with_version(self, job_repo, sample_job):
        """Test that version is persisted"""
        sample_job.update_status(JobStatus.RUNNING)  # version becomes 1
        job_repo.save_job(sample_job)

        retrieved = job_repo.get_job(sample_job.job_id)
        assert retrieved.version == 1

    def test_save_job_with_processing_token(self, job_repo, sample_job):
        """Test that processing_token is persisted"""
        sample_job.processing_token = "token_123"
        job_repo.save_job(sample_job)

        retrieved = job_repo.get_job(sample_job.job_id)
        assert retrieved.processing_token == "token_123"

    def test_update_job_status(self, job_repo, sample_job):
        """Test updating job status through repository"""
        job_repo.save_job(sample_job)
        job_repo.update_job_status(sample_job.job_id, JobStatus.RUNNING)

        retrieved = job_repo.get_job(sample_job.job_id)
        assert retrieved.status == JobStatus.RUNNING


class TestWorkerHeartbeat:
    """Tests for worker heartbeat with TTL"""

    def test_register_worker(self, job_repo):
        """Test worker registration creates heartbeat"""
        job_repo.register_worker("worker_1")

        assert job_repo.is_worker_alive("worker_1")

    def test_send_heartbeat(self, job_repo):
        """Test sending heartbeat"""
        job_repo.send_heartbeat("worker_1")

        assert job_repo.is_worker_alive("worker_1")

        # Check heartbeat data
        heartbeat_key = job_repo._get_worker_heartbeat_key("worker_1")
        data = json.loads(job_repo.redis_client.get(heartbeat_key))
        assert data["worker_id"] == "worker_1"
        assert data["status"] == "alive"

    def test_heartbeat_ttl_expiry(self, job_repo, fake_redis):
        """Test that heartbeat expires after TTL"""
        job_repo.send_heartbeat("worker_1")
        assert job_repo.is_worker_alive("worker_1")

        # Simulate time passing beyond TTL
        heartbeat_key = job_repo._get_worker_heartbeat_key("worker_1")
        fake_redis._expiry[heartbeat_key] = time.time() - 1  # Expired

        assert not job_repo.is_worker_alive("worker_1")

    def test_get_all_workers(self, job_repo):
        """Test getting all registered workers"""
        job_repo.send_heartbeat("worker_1")
        job_repo.send_heartbeat("worker_2")
        job_repo.send_heartbeat("worker_3")

        workers = job_repo.get_all_workers()
        assert len(workers) == 3
        assert "worker_1" in workers
        assert "worker_2" in workers
        assert "worker_3" in workers

    def test_get_dead_workers(self, job_repo, fake_redis):
        """Test detecting dead workers"""
        # Register workers
        job_repo.send_heartbeat("worker_1")
        job_repo.send_heartbeat("worker_2")

        # Assign jobs to workers
        job_repo.add_job_to_worker("worker_1", "job_1")
        job_repo.add_job_to_worker("worker_2", "job_2")

        # Kill worker_1's heartbeat
        heartbeat_key = job_repo._get_worker_heartbeat_key("worker_1")
        fake_redis._expiry[heartbeat_key] = time.time() - 1

        dead = job_repo.get_dead_workers()
        assert "worker_1" in dead
        assert "worker_2" not in dead

    def test_cleanup_worker(self, job_repo):
        """Test worker cleanup removes heartbeat and job tracking"""
        job_repo.send_heartbeat("worker_1")
        job_repo.add_job_to_worker("worker_1", "job_1")

        job_repo.cleanup_worker("worker_1")

        assert not job_repo.is_worker_alive("worker_1")
        assert job_repo.get_worker_jobs("worker_1") == []


class TestWorkerJobTracking:
    """Tests for tracking jobs assigned to workers"""

    def test_add_job_to_worker(self, job_repo):
        """Test adding job to worker's job set"""
        job_repo.add_job_to_worker("worker_1", "job_1")
        job_repo.add_job_to_worker("worker_1", "job_2")

        jobs = job_repo.get_worker_jobs("worker_1")
        assert "job_1" in jobs
        assert "job_2" in jobs

    def test_remove_job_from_worker(self, job_repo):
        """Test removing job from worker's job set"""
        job_repo.add_job_to_worker("worker_1", "job_1")
        job_repo.add_job_to_worker("worker_1", "job_2")

        job_repo.remove_job_from_worker("worker_1", "job_1")

        jobs = job_repo.get_worker_jobs("worker_1")
        assert "job_1" not in jobs
        assert "job_2" in jobs

    def test_get_worker_jobs_empty(self, job_repo):
        """Test getting jobs for worker with no jobs"""
        jobs = job_repo.get_worker_jobs("worker_nonexistent")
        assert jobs == []


class TestJobLocking:
    """Tests for distributed job locking"""

    def test_acquire_job_lock(self, job_repo):
        """Test acquiring a job lock"""
        token = job_repo.try_acquire_job_lock("job_1", "worker_1")

        assert token is not None
        assert len(token) > 0  # UUID

    def test_acquire_lock_fails_if_locked(self, job_repo):
        """Test that second lock attempt fails"""
        token1 = job_repo.try_acquire_job_lock("job_1", "worker_1")
        token2 = job_repo.try_acquire_job_lock("job_1", "worker_2")

        assert token1 is not None
        assert token2 is None

    def test_release_job_lock(self, job_repo):
        """Test releasing a job lock"""
        token = job_repo.try_acquire_job_lock("job_1", "worker_1")
        released = job_repo.release_job_lock("job_1", token)

        assert released is True

        # Now another worker can acquire
        token2 = job_repo.try_acquire_job_lock("job_1", "worker_2")
        assert token2 is not None

    def test_release_lock_wrong_token(self, job_repo):
        """Test release fails with wrong token"""
        job_repo.try_acquire_job_lock("job_1", "worker_1")
        released = job_repo.release_job_lock("job_1", "wrong_token")

        assert released is False

    def test_extend_job_lock(self, job_repo, fake_redis):
        """Test extending job lock TTL"""
        token = job_repo.try_acquire_job_lock("job_1", "worker_1")
        lock_key = job_repo._get_job_lock_key("job_1")

        # Record initial expiry
        initial_expiry = fake_redis._expiry.get(lock_key)

        # Extend lock
        extended = job_repo.extend_job_lock("job_1", token)
        assert extended is True

        # Verify TTL was extended
        new_expiry = fake_redis._expiry.get(lock_key)
        assert new_expiry >= initial_expiry

    def test_get_job_lock_info(self, job_repo):
        """Test getting lock information"""
        token = job_repo.try_acquire_job_lock("job_1", "worker_1")

        info = job_repo.get_job_lock_info("job_1")
        assert info is not None
        assert info["worker_id"] == "worker_1"
        assert info["token"] == token

    def test_lock_auto_expires(self, job_repo, fake_redis):
        """Test that lock expires after TTL"""
        token = job_repo.try_acquire_job_lock("job_1", "worker_1")
        lock_key = job_repo._get_job_lock_key("job_1")

        # Expire the lock
        fake_redis._expiry[lock_key] = time.time() - 1

        # Another worker should be able to acquire
        token2 = job_repo.try_acquire_job_lock("job_1", "worker_2")
        assert token2 is not None


class TestIdempotency:
    """Tests for idempotency tracking"""

    def test_mark_event_processed(self, job_repo):
        """Test marking an event as processed"""
        job_repo.mark_event_processed("event_123", "success")

        assert job_repo.is_event_processed("event_123")

    def test_event_not_processed_initially(self, job_repo):
        """Test that new events are not marked as processed"""
        assert not job_repo.is_event_processed("new_event")

    def test_get_event_processing_result(self, job_repo):
        """Test getting the result of a processed event"""
        job_repo.mark_event_processed("event_123", "success")

        result = job_repo.get_event_processing_result("event_123")
        assert result is not None
        assert result["result"] == "success"
        assert "processed_at" in result

    def test_idempotency_key_expires(self, job_repo, fake_redis):
        """Test that idempotency keys expire after TTL"""
        job_repo.mark_event_processed("event_123", "success")
        idempotency_key = job_repo._get_idempotency_key("event_123")

        # Expire the key
        fake_redis._expiry[idempotency_key] = time.time() - 1

        assert not job_repo.is_event_processed("event_123")


class TestJobQueries:
    """Tests for job query methods"""

    def test_get_running_jobs_for_worker(self, job_repo):
        """Test getting running jobs for a specific worker"""
        # Create and save jobs
        job1 = JobType.create_new("job_1", {})
        job1.assigned_worker = "worker_1"
        job1.update_status(JobStatus.RUNNING)

        job2 = JobType.create_new("job_2", {})
        job2.assigned_worker = "worker_1"
        job2.update_status(JobStatus.RUNNING)

        job3 = JobType.create_new("job_3", {})
        job3.assigned_worker = "worker_2"
        job3.update_status(JobStatus.RUNNING)

        job_repo.save_job(job1)
        job_repo.save_job(job2)
        job_repo.save_job(job3)

        # Query
        worker1_jobs = job_repo.get_running_jobs_for_worker("worker_1")
        assert len(worker1_jobs) == 2
        job_ids = [j.job_id for j in worker1_jobs]
        assert "job_1" in job_ids
        assert "job_2" in job_ids

    def test_get_stale_running_jobs(self, job_repo):
        """Test getting jobs that have been running too long"""
        from datetime import timedelta

        # Create a job that started a long time ago
        job = JobType.create_new("stale_job", {})
        job.update_status(JobStatus.RUNNING)
        # Backdate the started_at timestamp
        job.timestamps.started_at = datetime.utcnow() - timedelta(seconds=400)
        job_repo.save_job(job)

        # Create a recent job
        recent_job = JobType.create_new("recent_job", {})
        recent_job.update_status(JobStatus.RUNNING)
        job_repo.save_job(recent_job)

        # Query stale jobs (timeout = 300s)
        stale = job_repo.get_stale_running_jobs(timeout_seconds=300)

        assert len(stale) == 1
        assert stale[0].job_id == "stale_job"
