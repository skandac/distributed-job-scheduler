#!/usr/bin/env python3
"""
Heartbeat Monitor Entry Point

This service monitors worker heartbeats and handles crash recovery:
1. Detects workers whose heartbeats have expired (TTL timeout)
2. Requeues RUNNING jobs from dead workers back to PENDING
3. Emits WorkerDead events to Kafka for observability
4. Cleans up stale job locks

Usage:
    # Single instance mode
    python -m cmd.worker.monitor

    # HA mode with leader election (for multiple instances)
    python -m cmd.worker.monitor --leader-election --monitor-id monitor_1
"""

import argparse
import logging
import signal
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from internal.worker.heartbeat_monitor import HeartbeatMonitor, HeartbeatMonitorWithLeaderElection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Heartbeat Monitor Service")
    parser.add_argument(
        "--leader-election",
        action="store_true",
        help="Enable leader election for HA deployment"
    )
    parser.add_argument(
        "--monitor-id",
        default=None,
        help="Unique monitor ID (required for leader election)"
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

    # Create appropriate monitor instance
    if args.leader_election:
        if not args.monitor_id:
            import uuid
            args.monitor_id = f"monitor_{uuid.uuid4().hex[:8]}"
        logger.info(f"Starting heartbeat monitor with leader election (ID: {args.monitor_id})")
        monitor = HeartbeatMonitorWithLeaderElection(monitor_id=args.monitor_id)
    else:
        logger.info("Starting heartbeat monitor (single instance mode)")
        monitor = HeartbeatMonitor()

    # Signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info("Shutdown signal received")
        monitor.stop()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        monitor.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        monitor.stop()
    except Exception as e:
        logger.error(f"Monitor crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
