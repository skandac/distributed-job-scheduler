#!/usr/bin/env python3
"""
Worker Entry Point

Usage:
    python -m cmd.worker.main --worker-id worker_1

The worker:
1. Sends heartbeats to Redis with TTL (auto-expires on crash)
2. Processes jobs from Kafka with idempotency checks
3. Uses job locks to prevent duplicate processing
4. Handles graceful shutdown (requeues in-flight jobs)
"""

import argparse
import logging
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from internal.worker.worker import Worker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Distributed Job Worker")
    parser.add_argument(
        "--worker-id",
        required=True,
        help="Unique worker ID (e.g., worker_1, worker_2)"
    )
    parser.add_argument(
        "--kafka-brokers",
        default="localhost:9092",
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--redis-url",
        default=None,
        help="Redis URL (defaults to REDIS_URL env var or localhost:6379)"
    )
    args = parser.parse_args()

    # Set Redis URL if provided
    if args.redis_url:
        os.environ["REDIS_URL"] = args.redis_url

    logger.info(f"Starting worker {args.worker_id}")
    logger.info(f"Kafka brokers: {args.kafka_brokers}")

    worker = Worker(worker_id=args.worker_id)
    try:
        worker.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
        worker.shutdown()
    except Exception as e:
        logger.error(f"Worker crashed: {e}")
        worker.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    main()
