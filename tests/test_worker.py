"""
Tests for the Worker class.
"""

import pytest
import time
import threading
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime

from internal.models.job import JobType, JobStatus
from internal.worker.worker import Worker, HEARTBEAT_INTERVAL


class TestWorkerInitialization:
    """Tests for worker initialization"""

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    @patch('internal.worker.worker.JobRepository')
    def test_worker_creates_with_id(self, mock_repo, mock_producer, mock_consumer):
        """Test worker initializes with correct ID"""
        worker = Worker("worker_test_1")

        assert worker.worker_id == "worker_test_1"
        assert worker.running is True

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    @patch('internal.worker.worker.JobRepository')
    def test_worker_initializes_state(self, mock_repo, mock_producer, mock_consumer):
        """Test worker initializes internal state correctly"""
        worker = Worker("worker_1")

        assert worker._current_job_id is None
        assert worker._current_processing_token is None
        assert worker._heartbeat_thread is None


class TestWorkerHeartbeat:
    """Tests for worker heartbeat functionality"""

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_send_heartbeat(self, mock_producer, mock_consumer, job_repo):
        """Test worker sends heartbeat to repository"""
        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo

            worker.send_heartbeat()

            assert job_repo.is_worker_alive("worker_1")

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_heartbeat_thread_starts(self, mock_producer, mock_consumer, job_repo):
        """Test that heartbeat thread can be started"""
        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo

            worker._start_heartbeat_thread()

            assert worker._heartbeat_thread is not None
            assert worker._heartbeat_thread.is_alive()

            # Clean up
            worker.running = False
            worker._heartbeat_thread.join(timeout=1)


class TestEventIdGeneration:
    """Tests for event ID generation for idempotency"""

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    @patch('internal.worker.worker.JobRepository')
    def test_generate_event_id(self, mock_repo, mock_producer, mock_consumer):
        """Test event ID generation"""
        worker = Worker("worker_1")

        event = {
            "event_type": "JobCreated",
            "job_id": "job_123",
            "retry_count": 0
        }

        event_id = worker._generate_event_id(event)
        assert event_id == "job_123:JobCreated:0"

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    @patch('internal.worker.worker.JobRepository')
    def test_generate_event_id_with_retry(self, mock_repo, mock_producer, mock_consumer):
        """Test event ID includes retry count"""
        worker = Worker("worker_1")

        event = {
            "event_type": "JobCreated",
            "job_id": "job_123",
            "retry_count": 2
        }

        event_id = worker._generate_event_id(event)
        assert event_id == "job_123:JobCreated:2"


class TestJobProcessing:
    """Tests for job processing"""

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_process_job_skips_completed(self, mock_producer, mock_consumer, job_repo, sample_job):
        """Test that already completed jobs are skipped"""
        sample_job.update_status(JobStatus.RUNNING)
        sample_job.update_status(JobStatus.SUCCESS)
        job_repo.save_job(sample_job)

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            worker.producer = MagicMock()

            result = worker.process_job(sample_job.job_id)

            # Job was skipped but considered "processed" (idempotent)
            assert result is True

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_process_job_skips_failed_max_retries(self, mock_producer, mock_consumer, job_repo):
        """Test that failed jobs with max retries are skipped"""
        job = JobType.create_new("job_1", {}, max_retries=1)
        job.update_status(JobStatus.RUNNING)
        job.update_status(JobStatus.FAILED)  # retry_count = 1 = max_retries
        job_repo.save_job(job)

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            worker.producer = MagicMock()

            result = worker.process_job(job.job_id)
            assert result is True  # Skipped = idempotent success

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_process_job_acquires_lock(self, mock_producer, mock_consumer, job_repo, sample_job):
        """Test that processing acquires a lock"""
        job_repo.save_job(sample_job)

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            worker.producer = MagicMock()

            # Mock _execute_job to avoid actual processing
            worker._execute_job = MagicMock()

            result = worker.process_job(sample_job.job_id)

            assert result is True
            # Lock should be released after processing
            assert job_repo.get_job_lock_info(sample_job.job_id) is None

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_process_job_cannot_acquire_lock(self, mock_producer, mock_consumer, job_repo, sample_job):
        """Test that job is skipped if lock cannot be acquired"""
        job_repo.save_job(sample_job)

        # Another worker holds the lock
        job_repo.try_acquire_job_lock(sample_job.job_id, "other_worker")

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            worker.producer = MagicMock()

            result = worker.process_job(sample_job.job_id)

            assert result is False

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_process_job_updates_status(self, mock_producer, mock_consumer, job_repo, sample_job):
        """Test that job status is updated correctly"""
        job_repo.save_job(sample_job)

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            worker.producer = MagicMock()
            worker._execute_job = MagicMock()

            worker.process_job(sample_job.job_id)

            updated_job = job_repo.get_job(sample_job.job_id)
            assert updated_job.status == JobStatus.SUCCESS

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_process_job_sends_events(self, mock_producer, mock_consumer, job_repo, sample_job):
        """Test that Kafka events are sent during processing"""
        job_repo.save_job(sample_job)

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            mock_prod = MagicMock()
            worker.producer = mock_prod
            worker._execute_job = MagicMock()

            worker.process_job(sample_job.job_id)

            # Should send JobStarted and JobCompleted events
            assert mock_prod.send_event.call_count == 2

            calls = mock_prod.send_event.call_args_list
            event_types = [call[0][0]["event_type"] for call in calls]
            assert "JobStarted" in event_types
            assert "JobCompleted" in event_types

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_process_job_tracks_worker_assignment(self, mock_producer, mock_consumer, job_repo, sample_job):
        """Test that job is tracked in worker's job set"""
        job_repo.save_job(sample_job)

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            worker.producer = MagicMock()

            # Use a flag to check during execution
            job_tracked = []

            def mock_execute(job):
                jobs = job_repo.get_worker_jobs("worker_1")
                job_tracked.append(sample_job.job_id in jobs)

            worker._execute_job = mock_execute
            worker.process_job(sample_job.job_id)

            assert job_tracked[0] is True
            # After completion, job should be removed
            assert sample_job.job_id not in job_repo.get_worker_jobs("worker_1")


class TestJobFailureHandling:
    """Tests for job failure and retry handling"""

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_job_failure_requeues_with_retries(self, mock_producer, mock_consumer, job_repo):
        """Test that failed jobs are requeued when retries available"""
        job = JobType.create_new("job_1", {}, max_retries=3)
        job_repo.save_job(job)

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            worker.producer = MagicMock()

            # Make execution fail
            worker._execute_job = MagicMock(side_effect=Exception("Test error"))

            worker.process_job(job.job_id)

            updated_job = job_repo.get_job(job.job_id)
            assert updated_job.status == JobStatus.PENDING
            assert updated_job.retry_count == 1

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_job_failure_permanent_after_max_retries(self, mock_producer, mock_consumer, job_repo):
        """Test that jobs permanently fail after max retries"""
        job = JobType.create_new("job_1", {}, max_retries=1)
        job_repo.save_job(job)

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            worker.producer = MagicMock()
            worker._execute_job = MagicMock(side_effect=Exception("Test error"))

            worker.process_job(job.job_id)

            updated_job = job_repo.get_job(job.job_id)
            assert updated_job.status == JobStatus.FAILED
            assert updated_job.retry_count == 1

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_failure_sends_job_failed_event(self, mock_producer, mock_consumer, job_repo):
        """Test that JobFailed event is sent on failure"""
        job = JobType.create_new("job_1", {}, max_retries=3)
        job_repo.save_job(job)

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            mock_prod = MagicMock()
            worker.producer = mock_prod
            worker._execute_job = MagicMock(side_effect=Exception("Test error"))

            worker.process_job(job.job_id)

            # Find JobFailed event
            calls = mock_prod.send_event.call_args_list
            failed_events = [c for c in calls if c[0][0].get("event_type") == "JobFailed"]

            assert len(failed_events) == 1
            assert "error" in failed_events[0][0][0]
            assert failed_events[0][0][0]["will_retry"] is True


class TestWorkerShutdown:
    """Tests for graceful worker shutdown"""

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_shutdown_sets_running_false(self, mock_producer, mock_consumer, job_repo):
        """Test that shutdown sets running flag to False"""
        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo

            worker.shutdown()

            assert worker.running is False

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_shutdown_requeues_current_job(self, mock_producer, mock_consumer, job_repo):
        """Test that current job is requeued on shutdown"""
        job = JobType.create_new("job_1", {})
        job.update_status(JobStatus.RUNNING)
        job.assigned_worker = "worker_1"
        job_repo.save_job(job)

        # Acquire lock to simulate in-progress
        token = job_repo.try_acquire_job_lock("job_1", "worker_1")
        job_repo.add_job_to_worker("worker_1", "job_1")

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            worker._current_job_id = "job_1"
            worker._current_processing_token = token

            worker.shutdown()

            updated_job = job_repo.get_job("job_1")
            assert updated_job.status == JobStatus.PENDING

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_shutdown_releases_lock(self, mock_producer, mock_consumer, job_repo):
        """Test that job lock is released on shutdown"""
        job = JobType.create_new("job_1", {})
        job_repo.save_job(job)
        token = job_repo.try_acquire_job_lock("job_1", "worker_1")

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            worker._current_job_id = "job_1"
            worker._current_processing_token = token

            worker.shutdown()

            # Lock should be released
            assert job_repo.get_job_lock_info("job_1") is None

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_shutdown_cleans_up_worker(self, mock_producer, mock_consumer, job_repo):
        """Test that worker cleanup is called on shutdown"""
        job_repo.send_heartbeat("worker_1")

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo

            worker.shutdown()

            assert not job_repo.is_worker_alive("worker_1")


class TestIdempotency:
    """Tests for idempotent job processing"""

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_marks_event_processed_on_success(self, mock_producer, mock_consumer, job_repo, sample_job):
        """Test that event is marked processed on successful job completion"""
        job_repo.save_job(sample_job)

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            worker.producer = MagicMock()
            worker._execute_job = MagicMock()

            event_id = "job_1:JobCreated:0"
            worker.process_job(sample_job.job_id, event_id=event_id)

            assert job_repo.is_event_processed(event_id)

    @patch('internal.worker.worker.KafkaEventConsumer')
    @patch('internal.worker.worker.KafkaEventProducer')
    def test_marks_event_processed_on_skip(self, mock_producer, mock_consumer, job_repo, sample_job):
        """Test that event is marked processed when job is skipped"""
        sample_job.update_status(JobStatus.RUNNING)
        sample_job.update_status(JobStatus.SUCCESS)
        job_repo.save_job(sample_job)

        with patch('internal.worker.worker.JobRepository', return_value=job_repo):
            worker = Worker("worker_1")
            worker.job_repo = job_repo
            worker.producer = MagicMock()

            event_id = "job_1:JobCreated:0"
            worker.process_job(sample_job.job_id, event_id=event_id)

            assert job_repo.is_event_processed(event_id)
