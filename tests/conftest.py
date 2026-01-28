"""
Pytest fixtures and configuration for distributed scheduler tests.
"""

import pytest
import sys
import os
import time
import json
from datetime import datetime
from unittest.mock import MagicMock, patch
from typing import Dict, Any, List

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from internal.models.job import JobType, JobStatus, JobTimestamps
from internal.store.jobrepo import JobRepository


class FakeRedis:
    """
    Fake Redis client for testing without a real Redis server.
    Implements the subset of Redis commands used by the application.
    """

    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._expiry: Dict[str, float] = {}
        self._sets: Dict[str, set] = {}

    def _check_expiry(self, key: str) -> bool:
        """Check if key has expired and delete if so"""
        if key in self._expiry:
            if time.time() > self._expiry[key]:
                self._data.pop(key, None)
                self._expiry.pop(key, None)
                return True
        return False

    def set(self, key: str, value: str, nx: bool = False, ex: int = None) -> bool:
        """SET command with NX and EX options"""
        self._check_expiry(key)

        if nx and key in self._data:
            return False

        self._data[key] = value
        if ex:
            self._expiry[key] = time.time() + ex
        return True

    def setex(self, key: str, seconds: int, value: str) -> bool:
        """SETEX command"""
        self._data[key] = value
        self._expiry[key] = time.time() + seconds
        return True

    def get(self, key: str) -> str:
        """GET command"""
        self._check_expiry(key)
        return self._data.get(key)

    def delete(self, *keys: str) -> int:
        """DELETE command"""
        count = 0
        for key in keys:
            if key in self._data:
                del self._data[key]
                self._expiry.pop(key, None)
                count += 1
            if key in self._sets:
                del self._sets[key]
                count += 1
        return count

    def exists(self, key: str) -> int:
        """EXISTS command"""
        self._check_expiry(key)
        if key in self._data:
            return 1
        if key in self._sets:
            return 1
        return 0

    def expire(self, key: str, seconds: int) -> bool:
        """EXPIRE command"""
        if key in self._data or key in self._sets:
            self._expiry[key] = time.time() + seconds
            return True
        return False

    def ttl(self, key: str) -> int:
        """TTL command"""
        self._check_expiry(key)
        if key not in self._data and key not in self._sets:
            return -2
        if key not in self._expiry:
            return -1
        remaining = self._expiry[key] - time.time()
        return max(0, int(remaining))

    def keys(self, pattern: str) -> List[str]:
        """KEYS command with simple pattern matching"""
        # Remove expired keys first
        for key in list(self._data.keys()):
            self._check_expiry(key)

        # Simple pattern matching (only supports * wildcard)
        if pattern == "*":
            return list(self._data.keys()) + list(self._sets.keys())

        if pattern.endswith("*"):
            prefix = pattern[:-1]
            result = [k for k in self._data.keys() if k.startswith(prefix)]
            result += [k for k in self._sets.keys() if k.startswith(prefix)]
            return result

        if pattern.startswith("*"):
            suffix = pattern[1:]
            result = [k for k in self._data.keys() if k.endswith(suffix)]
            result += [k for k in self._sets.keys() if k.endswith(suffix)]
            return result

        # Exact match
        if pattern in self._data or pattern in self._sets:
            return [pattern]
        return []

    def sadd(self, key: str, *values: str) -> int:
        """SADD command"""
        if key not in self._sets:
            self._sets[key] = set()
        added = 0
        for v in values:
            if v not in self._sets[key]:
                self._sets[key].add(v)
                added += 1
        return added

    def srem(self, key: str, *values: str) -> int:
        """SREM command"""
        if key not in self._sets:
            return 0
        removed = 0
        for v in values:
            if v in self._sets[key]:
                self._sets[key].remove(v)
                removed += 1
        return removed

    def smembers(self, key: str) -> set:
        """SMEMBERS command"""
        return self._sets.get(key, set())

    def ping(self) -> bool:
        """PING command"""
        return True

    def flushall(self) -> None:
        """FLUSHALL command"""
        self._data.clear()
        self._expiry.clear()
        self._sets.clear()


class FakeKafkaProducer:
    """Fake Kafka producer for testing"""

    def __init__(self):
        self.messages: List[Dict[str, Any]] = []

    def send_event(self, event: Dict[str, Any], key: str = None) -> None:
        self.messages.append({"key": key, "value": event})

    def close(self) -> None:
        pass

    def get_messages(self, event_type: str = None) -> List[Dict[str, Any]]:
        """Get messages, optionally filtered by event type"""
        if event_type:
            return [m for m in self.messages if m["value"].get("event_type") == event_type]
        return self.messages

    def clear(self) -> None:
        self.messages.clear()


class FakeKafkaConsumer:
    """Fake Kafka consumer for testing"""

    def __init__(self):
        self.messages: List[Dict[str, Any]] = []
        self._index = 0

    def add_message(self, event: Dict[str, Any], key: str = None) -> None:
        """Add a message to be consumed"""
        self.messages.append({"key": key, "value": event})

    def consume_events(self):
        """Consume events (generator)"""
        while self._index < len(self.messages):
            msg = self.messages[self._index]
            self._index += 1
            yield msg["key"], msg["value"]

    def close(self) -> None:
        pass

    def reset(self) -> None:
        self._index = 0


@pytest.fixture
def fake_redis():
    """Provide a fake Redis client"""
    return FakeRedis()


@pytest.fixture
def job_repo(fake_redis):
    """Provide a JobRepository with fake Redis"""
    repo = JobRepository()
    repo.redis_client = fake_redis
    return repo


@pytest.fixture
def fake_producer():
    """Provide a fake Kafka producer"""
    return FakeKafkaProducer()


@pytest.fixture
def fake_consumer():
    """Provide a fake Kafka consumer"""
    return FakeKafkaConsumer()


@pytest.fixture
def sample_job():
    """Create a sample job for testing"""
    return JobType.create_new(
        job_id="test_job_1",
        payload={"task": "test", "duration": 1},
        max_retries=3
    )


@pytest.fixture
def sample_job_data():
    """Sample job data dict"""
    return {
        "job_id": "test_job_1",
        "payload": {"task": "test"},
        "max_retries": 3
    }


def create_test_job(job_id: str = None, status: JobStatus = JobStatus.PENDING) -> JobType:
    """Helper to create test jobs"""
    job = JobType.create_new(
        job_id=job_id or f"job_{int(time.time() * 1000)}",
        payload={"test": True},
        max_retries=3
    )
    if status != JobStatus.PENDING:
        # Force status for testing
        job.status = status
    return job
