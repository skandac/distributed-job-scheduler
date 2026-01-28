from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import json
import logging
from typing import Dict, Any, Optional, Generator, Tuple

logger = logging.getLogger(__name__)


class KafkaEventConsumer:
    """
    Kafka consumer with support for manual offset management.

    Features:
    - Manual commit for at-least-once delivery guarantees
    - Offset tracking for replay support
    - Graceful error handling
    """

    def __init__(
        self,
        brokers: str,
        topic: str,
        group_id: str,
        auto_commit: bool = False,  # Default to manual commit for replay safety
        auto_offset_reset: str = 'earliest'
    ):
        self.topic = topic
        self.group_id = group_id
        self._auto_commit = auto_commit

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=brokers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=auto_commit,
            # Consumer timeout for non-blocking checks
            consumer_timeout_ms=1000,
        )

    def consume_events(self) -> Generator[Tuple[Optional[str], Dict[str, Any]], None, None]:
        """
        Consume events from Kafka.
        Yields (key, event) tuples.
        """
        for message in self.consumer:
            yield message.key, message.value

    def consume_events_with_metadata(self) -> Generator[Dict[str, Any], None, None]:
        """
        Consume events with full metadata for offset tracking.
        Yields dict with key, value, topic, partition, offset.
        """
        for message in self.consumer:
            yield {
                "key": message.key,
                "value": message.value,
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "timestamp": message.timestamp
            }

    def commit(self) -> None:
        """Manually commit current offsets (for at-least-once delivery)"""
        if not self._auto_commit:
            try:
                self.consumer.commit()
                logger.debug("Committed offsets")
            except KafkaError as e:
                logger.error(f"Failed to commit offsets: {e}")
                raise

    def commit_message(self, message: Dict[str, Any]) -> None:
        """Commit offset for a specific message"""
        if not self._auto_commit:
            try:
                tp = TopicPartition(message["topic"], message["partition"])
                # Commit the next offset (current offset + 1)
                self.consumer.commit({tp: message["offset"] + 1})
                logger.debug(f"Committed offset {message['offset'] + 1} for {tp}")
            except KafkaError as e:
                logger.error(f"Failed to commit message offset: {e}")
                raise

    def seek_to_beginning(self, partitions: list = None) -> None:
        """
        Seek to the beginning of partitions for replay.
        If partitions is None, seeks all assigned partitions.
        """
        if partitions is None:
            partitions = self.consumer.assignment()
        self.consumer.seek_to_beginning(*partitions)
        logger.info(f"Seeked to beginning for partitions: {partitions}")

    def seek_to_end(self, partitions: list = None) -> None:
        """Seek to the end of partitions (skip all existing messages)"""
        if partitions is None:
            partitions = self.consumer.assignment()
        self.consumer.seek_to_end(*partitions)
        logger.info(f"Seeked to end for partitions: {partitions}")

    def seek_to_offset(self, partition: int, offset: int) -> None:
        """Seek to a specific offset for replay from a known point"""
        tp = TopicPartition(self.topic, partition)
        self.consumer.seek(tp, offset)
        logger.info(f"Seeked to offset {offset} for partition {partition}")

    def get_current_offsets(self) -> Dict[TopicPartition, int]:
        """Get current consumer offsets for all assigned partitions"""
        return {tp: self.consumer.position(tp) for tp in self.consumer.assignment()}

    def get_committed_offsets(self) -> Dict[TopicPartition, int]:
        """Get committed offsets for the consumer group"""
        partitions = self.consumer.assignment()
        committed = self.consumer.committed(partitions)
        return {tp: offset for tp, offset in zip(partitions, committed) if offset is not None}

    def pause(self, partitions: list = None) -> None:
        """Pause consumption from partitions"""
        if partitions is None:
            partitions = self.consumer.assignment()
        self.consumer.pause(*partitions)
        logger.info(f"Paused partitions: {partitions}")

    def resume(self, partitions: list = None) -> None:
        """Resume consumption from paused partitions"""
        if partitions is None:
            partitions = self.consumer.paused()
        self.consumer.resume(*partitions)
        logger.info(f"Resumed partitions: {partitions}")

    def close(self) -> None:
        """Close the consumer"""
        self.consumer.close()


class ReplayableKafkaConsumer(KafkaEventConsumer):
    """
    Kafka consumer optimized for replay scenarios.

    Supports:
    - Replay from beginning (full reprocessing)
    - Replay from specific offset
    - Idempotent processing with external tracking
    """

    def __init__(
        self,
        brokers: str,
        topic: str,
        group_id: str,
        job_repo=None  # Optional JobRepository for idempotency tracking
    ):
        super().__init__(
            brokers=brokers,
            topic=topic,
            group_id=group_id,
            auto_commit=False,  # Always manual for replay
            auto_offset_reset='earliest'
        )
        self.job_repo = job_repo

    def replay_from_beginning(self) -> Generator[Dict[str, Any], None, None]:
        """
        Replay all events from the beginning.
        Use with idempotency checks to safely reprocess events.
        """
        logger.info("Starting replay from beginning...")
        self.seek_to_beginning()

        for message in self.consume_events_with_metadata():
            event = message["value"]
            event_id = self._generate_event_id(event)

            # Check if already processed (if job_repo available)
            if self.job_repo and self.job_repo.is_event_processed(event_id):
                logger.debug(f"Skipping already processed event: {event_id}")
                continue

            yield {
                **message,
                "event_id": event_id
            }

    def _generate_event_id(self, event: dict) -> str:
        """Generate consistent event ID for idempotency"""
        job_id = event.get("job_id", "unknown")
        event_type = event.get("event_type", "unknown")
        retry_count = event.get("retry_count", 0)
        return f"{job_id}:{event_type}:{retry_count}"
