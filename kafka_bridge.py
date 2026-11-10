"""
Distributed Partitioned Streaming Bus (Apache Kafka Bridge).

Features:
- Publishes transactions to Apache Kafka topic 'payment_transactions_raw'
  using deterministic key-based partitioning (key = user_id.encode('utf-8')).
  This guarantees that all transactions for a given user land in the exact same
  partition, ensuring strict temporal order and enabling parallel stateful processing.
- Zero-Crash Graceful Degradation: If no Kafka broker is active on localhost:9092,
  it automatically and transparently falls back to an in-memory streaming bus
  while reporting status to the Streamlit UI and metrics collector.
"""

from enum import Enum
import json
import logging
import queue
import threading
import time
from typing import Callable, Dict, Optional

from models import Transaction

logger = logging.getLogger("streamguard.kafka")


class KafkaBusMode(str, Enum):
    DISTRIBUTED_KAFKA = "DISTRIBUTED_KAFKA"
    LOCAL_MEMORY_FALLBACK = "LOCAL_MEMORY_FALLBACK"


class DistributedKafkaBus:
    """
    Kafka Streaming Client with automatic, zero-crash in-memory fallback.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "payment_transactions_raw",
        consumer_group: str = "streamguard-scoring-group",
        on_message_consumed: Optional[Callable[[Transaction], None]] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer_group = consumer_group
        self.on_message_consumed = on_message_consumed

        self.mode = KafkaBusMode.LOCAL_MEMORY_FALLBACK
        self.is_connected = False
        self.error_reason: Optional[str] = None

        self.published_count = 0
        self.consumed_count = 0

        self._producer = None
        self._consumer = None
        self._consumer_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # In-memory fallback queue for zero-crash fallback
        self._memory_queue: queue.Queue = queue.Queue(maxsize=10000)

        # Attempt Kafka Broker Connection
        self._init_kafka()

    def _init_kafka(self) -> None:
        """Attempt connection to external Kafka cluster with low timeout."""
        import socket
        try:
            # Parse host and port for fast pre-flight socket probe
            parts = self.bootstrap_servers.split(":")
            host = parts[0]
            port = int(parts[1]) if len(parts) > 1 else 9092
            
            # Fast 0.25s socket probe so we don't stall on KafkaProducer retries if no broker is active
            with socket.create_connection((host, port), timeout=0.25):
                pass
        except OSError:
            self.is_connected = False
            self.mode = KafkaBusMode.LOCAL_MEMORY_FALLBACK
            self.error_reason = (
                f"No Kafka broker listening on {self.bootstrap_servers}. "
                "Operating in High-Throughput In-Memory Fallback Mode."
            )
            return

        try:
            # Dynamically test kafka-python-ng import
            from kafka import KafkaProducer, KafkaConsumer
            from kafka.errors import NoBrokersAvailable, KafkaError

            logger.info("Attempting handshake with Kafka broker at %s...", self.bootstrap_servers)
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                request_timeout_ms=2000,
                api_version_auto_timeout_ms=2000,
                max_block_ms=2000,
                acks=1,  # Leader acknowledgement for balance of throughput and safety
                retries=2,
            )
            self.is_connected = True
            self.mode = KafkaBusMode.DISTRIBUTED_KAFKA
            self.error_reason = None
            logger.info("Connected to Apache Kafka broker (%s)!", self.bootstrap_servers)

        except Exception as e:
            self.is_connected = False
            self.mode = KafkaBusMode.LOCAL_MEMORY_FALLBACK
            self.error_reason = (
                f"Kafka broker unavailable on {self.bootstrap_servers} ({type(e).__name__}). "
                "Active in High-Throughput In-Memory Fallback Mode."
            )
            logger.warning("Kafka unreachable. Operating in LOCAL_MEMORY_FALLBACK mode.")

    def publish(self, txn: Transaction) -> bool:
        """
        Publish transaction event to the stream bus.
        Key = user_id (guarantees deterministic partition hashing: hash(user_id) % partitions).
        """
        if self.mode == KafkaBusMode.DISTRIBUTED_KAFKA and self._producer:
            try:
                # Key-based partitioning in Kafka ensures per-user FIFO event ordering
                payload = txn.to_dict()
                future = self._producer.send(
                    topic=self.topic,
                    key=txn.user_id,
                    value=payload,
                )
                self.published_count += 1
                return True
            except Exception as ex:
                logger.error("Kafka send failed: %s. Degrading to fallback queue.", ex)
                self.mode = KafkaBusMode.LOCAL_MEMORY_FALLBACK
                self.is_connected = False
                self.error_reason = f"Kafka publish error: {ex}"

        # Local Memory Fallback: Enqueue directly into memory queue
        try:
            self._memory_queue.put_nowait(txn)
            self.published_count += 1
            if self.on_message_consumed:
                self.on_message_consumed(txn)
                self.consumed_count += 1
            return True
        except queue.Full:
            return False

    def start_consumer(self) -> None:
        """Start consumer daemon thread for Kafka streaming ingest."""
        if self.mode != KafkaBusMode.DISTRIBUTED_KAFKA:
            return

        def _consumer_loop():
            try:
                from kafka import KafkaConsumer
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.consumer_group,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=1000,
                )
                while not self._stop_event.is_set():
                    records = consumer.poll(timeout_ms=500)
                    for topic_data, messages in records.items():
                        for msg in messages:
                            self.consumed_count += 1
                            if self.on_message_consumed:
                                try:
                                    # Convert dict back to Transaction object
                                    d = msg.value
                                    txn = Transaction(
                                        transaction_id=d.get("transaction_id", ""),
                                        user_id=d.get("user_id", ""),
                                        amount=float(d.get("amount", 0.0)),
                                        timestamp=float(d.get("timestamp", time.time())),
                                        merchant_category=d.get("merchant_category", "unknown"),
                                        location=d.get("location", "US"),
                                        ip_address=d.get("ip_address", "127.0.0.1"),
                                        is_synthetic_fraud=bool(d.get("is_synthetic_fraud", False)),
                                    )
                                    self.on_message_consumed(txn)
                                except Exception as err:
                                    logger.error("Error consuming Kafka message: %s", err)
            except Exception as e:
                logger.debug("Kafka consumer listener ended: %s", e)

        self._consumer_thread = threading.Thread(target=_consumer_loop, daemon=True)
        self._consumer_thread.start()

    def get_status(self) -> Dict[str, object]:
        """Telemetry details for Streamlit monitoring card."""
        return {
            "mode": self.mode.value,
            "is_connected": self.is_connected,
            "bootstrap_servers": self.bootstrap_servers,
            "topic": self.topic,
            "partition_strategy": "Key-based hash partitioner: hash(user_id) % num_partitions",
            "published_count": self.published_count,
            "consumed_count": self.consumed_count,
            "error_reason": self.error_reason,
        }

    def close(self) -> None:
        self._stop_event.set()
        if self._producer:
            try:
                self._producer.flush(timeout=2.0)
                self._producer.close(timeout=2.0)
            except Exception:
                pass
