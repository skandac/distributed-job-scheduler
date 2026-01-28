"""
Tests for the Job model and state machine.
"""

import pytest
from datetime import datetime
from internal.models.job import JobType, JobStatus, JobTimestamps


class TestJobCreation:
    """Tests for job creation"""

    def test_create_new_job(self):
        """Test creating a new job with default values"""
        job = JobType.create_new(
            job_id="test_1",
            payload={"task": "process"},
            max_retries=3
        )

        assert job.job_id == "test_1"
        assert job.payload == {"task": "process"}
        assert job.status == JobStatus.PENDING
        assert job.assigned_worker is None
        assert job.retry_count == 0
        assert job.max_retries == 3
        assert job.version == 0
        assert job.processing_token is None
        assert job.timestamps.created_at is not None

    def test_create_job_with_custom_retries(self):
        """Test creating a job with custom max retries"""
        job = JobType.create_new(
            job_id="test_2",
            payload={},
            max_retries=5
        )
        assert job.max_retries == 5


class TestJobStateTransitions:
    """Tests for job state machine transitions"""

    def test_pending_to_running(self):
        """Test PENDING -> RUNNING transition"""
        job = JobType.create_new("j1", {})
        assert job.can_transition_to(JobStatus.RUNNING)

        job.update_status(JobStatus.RUNNING)
        assert job.status == JobStatus.RUNNING
        assert job.timestamps.started_at is not None
        assert job.version == 1

    def test_pending_to_canceled(self):
        """Test PENDING -> CANCELED transition"""
        job = JobType.create_new("j1", {})
        assert job.can_transition_to(JobStatus.CANCELED)

        job.update_status(JobStatus.CANCELED)
        assert job.status == JobStatus.CANCELED
        assert job.timestamps.completed_at is not None

    def test_running_to_success(self):
        """Test RUNNING -> SUCCESS transition"""
        job = JobType.create_new("j1", {})
        job.update_status(JobStatus.RUNNING)

        assert job.can_transition_to(JobStatus.SUCCESS)
        job.update_status(JobStatus.SUCCESS)

        assert job.status == JobStatus.SUCCESS
        assert job.timestamps.completed_at is not None

    def test_running_to_failed(self):
        """Test RUNNING -> FAILED transition increments retry_count"""
        job = JobType.create_new("j1", {})
        job.update_status(JobStatus.RUNNING)

        assert job.can_transition_to(JobStatus.FAILED)
        job.update_status(JobStatus.FAILED)

        assert job.status == JobStatus.FAILED
        assert job.retry_count == 1
        assert job.timestamps.completed_at is not None

    def test_running_to_pending_crash_recovery(self):
        """Test RUNNING -> PENDING for crash recovery"""
        job = JobType.create_new("j1", {})
        job.assigned_worker = "worker_1"
        job.processing_token = "token_123"
        job.update_status(JobStatus.RUNNING)

        # Crash recovery - requeue job
        assert job.can_transition_to(JobStatus.PENDING)
        job.update_status(JobStatus.PENDING)

        assert job.status == JobStatus.PENDING
        assert job.assigned_worker is None
        assert job.processing_token is None
        assert job.timestamps.started_at is None
        assert job.timestamps.assigned_at is None

    def test_failed_to_pending_retry(self):
        """Test FAILED -> PENDING when retries available"""
        job = JobType.create_new("j1", {}, max_retries=3)
        job.update_status(JobStatus.RUNNING)
        job.update_status(JobStatus.FAILED)

        # retry_count is now 1, which is < max_retries (3)
        assert job.retry_count == 1
        assert job.can_transition_to(JobStatus.PENDING)

        job.update_status(JobStatus.PENDING)
        assert job.status == JobStatus.PENDING

    def test_failed_no_retry_when_exhausted(self):
        """Test FAILED cannot go to PENDING when retries exhausted"""
        job = JobType.create_new("j1", {}, max_retries=1)
        job.update_status(JobStatus.RUNNING)
        job.update_status(JobStatus.FAILED)

        # retry_count is now 1, equal to max_retries (1)
        assert job.retry_count == 1
        assert not job.can_transition_to(JobStatus.PENDING)

    def test_success_is_terminal(self):
        """Test SUCCESS state has no valid transitions"""
        job = JobType.create_new("j1", {})
        job.update_status(JobStatus.RUNNING)
        job.update_status(JobStatus.SUCCESS)

        assert not job.can_transition_to(JobStatus.PENDING)
        assert not job.can_transition_to(JobStatus.RUNNING)
        assert not job.can_transition_to(JobStatus.FAILED)

    def test_canceled_is_terminal(self):
        """Test CANCELED state has no valid transitions"""
        job = JobType.create_new("j1", {})
        job.update_status(JobStatus.CANCELED)

        assert not job.can_transition_to(JobStatus.PENDING)
        assert not job.can_transition_to(JobStatus.RUNNING)

    def test_invalid_transition_raises_error(self):
        """Test invalid transitions raise ValueError"""
        job = JobType.create_new("j1", {})

        with pytest.raises(ValueError):
            job.update_status(JobStatus.SUCCESS)  # Can't go PENDING -> SUCCESS


class TestJobVersioning:
    """Tests for job versioning (optimistic locking)"""

    def test_version_increments_on_status_change(self):
        """Test version increments with each status change"""
        job = JobType.create_new("j1", {})
        assert job.version == 0

        job.update_status(JobStatus.RUNNING)
        assert job.version == 1

        job.update_status(JobStatus.SUCCESS)
        assert job.version == 2

    def test_multiple_transitions_track_version(self):
        """Test version tracks through retry cycle"""
        job = JobType.create_new("j1", {}, max_retries=3)

        job.update_status(JobStatus.RUNNING)  # v1
        job.update_status(JobStatus.FAILED)   # v2
        job.update_status(JobStatus.PENDING)  # v3
        job.update_status(JobStatus.RUNNING)  # v4
        job.update_status(JobStatus.SUCCESS)  # v5

        assert job.version == 5


class TestJobTimestamps:
    """Tests for job timestamp handling"""

    def test_created_at_set_on_creation(self):
        """Test created_at is set when job is created"""
        before = datetime.utcnow()
        job = JobType.create_new("j1", {})
        after = datetime.utcnow()

        assert before <= job.timestamps.created_at <= after

    def test_started_at_set_on_running(self):
        """Test started_at is set when transitioning to RUNNING"""
        job = JobType.create_new("j1", {})
        assert job.timestamps.started_at is None

        job.update_status(JobStatus.RUNNING)
        assert job.timestamps.started_at is not None

    def test_completed_at_set_on_terminal(self):
        """Test completed_at is set on terminal states"""
        job = JobType.create_new("j1", {})
        job.update_status(JobStatus.RUNNING)
        assert job.timestamps.completed_at is None

        job.update_status(JobStatus.SUCCESS)
        assert job.timestamps.completed_at is not None

    def test_timestamps_cleared_on_requeue(self):
        """Test timestamps are cleared when job is requeued"""
        job = JobType.create_new("j1", {}, max_retries=3)
        job.timestamps.assigned_at = datetime.utcnow()
        job.update_status(JobStatus.RUNNING)
        job.update_status(JobStatus.FAILED)

        # Requeue
        job.update_status(JobStatus.PENDING)

        assert job.timestamps.assigned_at is None
        assert job.timestamps.started_at is None
        assert job.timestamps.completed_at is None
