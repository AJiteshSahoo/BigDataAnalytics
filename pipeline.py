"""
Streaming Pipeline Orchestrator with Distributed Kafka Bus and PySpark Analytics.

Connects the Producer (TransactionSimulator) with the Consumer/Scoring Engine (FraudScoringEngine)
via the DistributedKafkaBus, while integrating the SparkAnalyticsEngine for partitioned Parquet data
lake ingestion, distributed window metrics, SQL aggregations, and MLlib K-Means clustering.
"""

from collections import deque
import threading
import time
from typing import Dict, List, Optional
import pandas as pd

from engine import FraudScoringEngine
from kafka_bridge import DistributedKafkaBus
from models import AnomalyType, RiskResult, RiskStatus, Transaction
from simulator import TransactionSimulator
from spark_analytics import SparkAnalyticsEngine


class FraudStreamingPipeline:
    """
    Thread-safe orchestrator managing continuous transaction generation,
    Kafka key-based partitioned streaming, real-time scoring, and Spark batch analytics.
    """

    def __init__(
        self,
        tps: float = 2.0,
        fraud_prob: float = 0.15,
        max_buffer_size: int = 500,
        max_alert_buffer_size: int = 100,
    ):
        self.engine = FraudScoringEngine()
        self.simulator = TransactionSimulator(
            transactions_per_sec=tps,
            fraud_probability=fraud_prob,
            on_transaction_generated=self.dispatch_transaction,
        )

        # Distributed Kafka Bus with graceful in-memory fallback
        self.kafka_bus = DistributedKafkaBus(
            bootstrap_servers="localhost:9092",
            topic="payment_transactions_raw",
            on_message_consumed=self.process_transaction,
        )
        self.kafka_bus.start_consumer()

        # Distributed PySpark SQL, Parquet Data Lake & MLlib Engine
        self.spark_engine = SparkAnalyticsEngine()
        self.latest_spark_results: Optional[Dict] = None

        self.max_buffer_size = max_buffer_size
        self.max_alert_buffer_size = max_alert_buffer_size

        # In-memory sliding rolling buffers
        self.recent_results: deque = deque(maxlen=max_buffer_size)
        self.fraud_alerts: deque = deque(maxlen=max_alert_buffer_size)

        # Running statistical metrics
        self.total_processed = 0
        self.total_approved = 0
        self.total_review = 0
        self.total_declined = 0
        self.total_amount_processed = 0.0
        self.total_amount_prevented = 0.0

        # Anomaly type breakdown counters
        self.anomaly_counts: Dict[str, int] = {
            "VELOCITY_SPIKE": 0,
            "GEO_ANOMALY": 0,
            "HIGH_VALUE_DRAIN": 0,
            "STATISTICAL_ZSCORE": 0,
            "HIGH_RISK_CATEGORY": 0,
        }

        # Confusion Matrix counters
        self.tp = 0
        self.fp = 0
        self.tn = 0
        self.fn = 0

        # Latency sliding window (ms)
        self.latency_buffer: deque = deque(maxlen=100)

        # Pipeline lock
        self._lock = threading.RLock()
        self.start_time = time.time()

    def dispatch_transaction(self, txn: Transaction) -> None:
        """
        Dispatches generated transaction to the real-time scoring engine
        and publishes to the Distributed Kafka Bus with key-based partition hashing.
        """
        # 1. Immediately evaluate transaction in the real-time scoring engine
        self.process_transaction(txn)
        # 2. Publish to Apache Kafka (or in-memory bus) keyed by user_id
        self.kafka_bus.publish(txn)

    def process_transaction(self, txn: Transaction) -> RiskResult:
        """
        Stream consumer execution: scores event and updates rolling buffers and metrics.
        """
        result = self.engine.evaluate_transaction(txn)

        with self._lock:
            self.total_processed += 1
            self.total_amount_processed += txn.amount
            self.latency_buffer.append(result.latency_ms)

            # Update status counters
            if result.status == RiskStatus.APPROVED:
                self.total_approved += 1
                if txn.is_synthetic_fraud:
                    self.fn += 1
                else:
                    self.tn += 1
            elif result.status == RiskStatus.REVIEW:
                self.total_review += 1
                if txn.is_synthetic_fraud:
                    self.tp += 1
                else:
                    self.fp += 1
            elif result.status == RiskStatus.DECLINED:
                self.total_declined += 1
                self.total_amount_prevented += txn.amount
                if txn.is_synthetic_fraud:
                    self.tp += 1
                else:
                    self.fp += 1

            # Track triggered rules breakdown
            for rule in result.rules_triggered:
                if rule.triggered and rule.rule_name in self.anomaly_counts:
                    self.anomaly_counts[rule.rule_name] += 1

            # Append to rolling feed
            self.recent_results.append(result)

            # Append to Fraud Alert Center if flagged
            if result.status in (RiskStatus.DECLINED, RiskStatus.REVIEW):
                self.fraud_alerts.appendleft(result)

        return result

    # -------------------------------------------------------------
    # Distributed PySpark Analytics Batch Trigger
    # -------------------------------------------------------------
    def run_spark_batch_job(self, target_sample_size: int = 500) -> Dict:
        """
        Executes the PySpark Big Data & MLlib pipeline:
        1. Materializes recent history (or seeds to target_sample_size for realistic scale).
        2. Writes partitioned Apache Parquet data lake files partitioned by location.
        3. Runs distributed Window aggregations (lag delta, rolling 5-txn spend average, surge ratio).
        4. Runs distributed SQL aggregations by country & category.
        5. Trains PySpark MLlib K-Means clustering model (k=3) for unsupervised behavioral anomaly profiling.
        """
        start_ts = time.time()
        with self._lock:
            current_items = list(self.recent_results)

        records = []
        for r in current_items:
            records.append({
                "transaction_id": r.transaction.transaction_id,
                "user_id": r.transaction.user_id,
                "amount": float(r.transaction.amount),
                "timestamp": float(r.transaction.timestamp),
                "merchant_category": str(r.transaction.merchant_category),
                "location": str(r.transaction.location),
                "ip_address": str(r.transaction.ip_address),
                "risk_score": float(r.risk_score),
                "status": str(r.status.value),
                "is_synthetic_fraud": bool(r.transaction.is_synthetic_fraud),
            })

        # Augment with synthetic samples if pipeline just started to ensure statistically sound K-Means
        if len(records) < target_sample_size:
            needed = target_sample_size - len(records)
            for _ in range(needed):
                synth_txn = self.simulator.step()
                eval_res = self.engine.evaluate_transaction(synth_txn)
                records.append({
                    "transaction_id": synth_txn.transaction_id,
                    "user_id": synth_txn.user_id,
                    "amount": float(synth_txn.amount),
                    "timestamp": float(synth_txn.timestamp),
                    "merchant_category": str(synth_txn.merchant_category),
                    "location": str(synth_txn.location),
                    "ip_address": str(synth_txn.ip_address),
                    "risk_score": float(eval_res.risk_score),
                    "status": str(eval_res.status.value),
                    "is_synthetic_fraud": bool(synth_txn.is_synthetic_fraud),
                })

        df = pd.DataFrame(records)

        # 1. Parquet Data Lake Sink
        parquet_dir, count_written = self.spark_engine.write_to_parquet(df)

        # 2. Distributed Window Analytics
        window_df = self.spark_engine.run_window_analytics(df)

        # 3. Distributed Regional SQL Aggregations
        regional_df = self.spark_engine.run_regional_sql_aggregations(df)

        # 4. Distributed Spark MLlib K-Means Clustering
        clustered_users_df, cluster_summary_df = self.spark_engine.run_mllib_kmeans_clustering(df)

        elapsed = time.time() - start_ts

        results = {
            "status": "COMPLETED",
            "engine_mode": self.spark_engine.mode,
            "parquet_dir": parquet_dir,
            "records_processed": len(df),
            "execution_time_s": round(elapsed, 3),
            "window_df": window_df.head(50),
            "regional_df": regional_df,
            "clustered_users_df": clustered_users_df,
            "cluster_summary_df": cluster_summary_df,
            "timestamp": time.strftime("%H:%M:%S", time.localtime()),
        }

        with self._lock:
            self.latest_spark_results = results

        return results

    # -------------------------------------------------------------
    # Pipeline Controls
    # -------------------------------------------------------------
    def start(self) -> None:
        self.simulator.start_streaming()

    def stop(self) -> None:
        self.simulator.stop_streaming()

    @property
    def is_running(self) -> bool:
        return self.simulator.is_running

    def set_speed(self, tps: float) -> None:
        self.simulator.transactions_per_sec = max(0.5, tps)

    def set_fraud_rate(self, prob: float) -> None:
        self.simulator.fraud_probability = max(0.0, min(1.0, prob))

    def update_thresholds(
        self,
        velocity_threshold: int,
        drain_multiplier: float,
        decline_threshold: float,
        review_threshold: float,
    ) -> None:
        with self._lock:
            self.engine.velocity_threshold = velocity_threshold
            self.engine.drain_multiplier = drain_multiplier
            self.engine.decline_score_threshold = decline_threshold
            self.engine.review_score_threshold = review_threshold

    def manual_inject_velocity(self) -> None:
        self.simulator.inject_velocity_burst()

    def manual_inject_geo(self) -> None:
        self.simulator.inject_geo_anomaly()

    def manual_inject_drain(self) -> None:
        self.simulator.inject_high_value_drain()

    def reset(self) -> None:
        with self._lock:
            self.engine.reset_state()
            self.recent_results.clear()
            self.fraud_alerts.clear()
            self.total_processed = 0
            self.total_approved = 0
            self.total_review = 0
            self.total_declined = 0
            self.total_amount_processed = 0.0
            self.total_amount_prevented = 0.0
            self.tp = 0
            self.fp = 0
            self.tn = 0
            self.fn = 0
            self.latency_buffer.clear()
            for k in self.anomaly_counts:
                self.anomaly_counts[k] = 0
            self.latest_spark_results = None
            self.start_time = time.time()

    # -------------------------------------------------------------
    # Metrics & Data Retrieval
    # -------------------------------------------------------------
    def get_metrics(self) -> dict:
        with self._lock:
            avg_latency = (
                sum(self.latency_buffer) / len(self.latency_buffer)
                if self.latency_buffer else 0.0
            )
            fraud_rate = (
                (self.total_declined / self.total_processed * 100.0)
                if self.total_processed > 0 else 0.0
            )
            precision = (
                (self.tp / (self.tp + self.fp) * 100.0)
                if (self.tp + self.fp) > 0 else 100.0
            )
            recall = (
                (self.tp / (self.tp + self.fn) * 100.0)
                if (self.tp + self.fn) > 0 else 100.0
            )
            f1 = (
                (2 * precision * recall / (precision + recall))
                if (precision + recall) > 0 else 100.0
            )

            return {
                "total_processed": self.total_processed,
                "total_approved": self.total_approved,
                "total_review": self.total_review,
                "total_declined": self.total_declined,
                "total_amount_processed": self.total_amount_processed,
                "total_amount_prevented": self.total_amount_prevented,
                "fraud_rate_pct": fraud_rate,
                "avg_latency_ms": avg_latency,
                "current_tps": self.simulator.transactions_per_sec,
                "is_running": self.simulator.is_running,
                "precision": precision,
                "recall": recall,
                "f1_score": f1,
                "tp": self.tp,
                "fp": self.fp,
                "tn": self.tn,
                "fn": self.fn,
                "anomaly_counts": dict(self.anomaly_counts),
                "kafka_status": self.kafka_bus.get_status(),
                "spark_mode": self.spark_engine.mode,
            }

    def get_recent_df(self) -> pd.DataFrame:
        with self._lock:
            items = list(self.recent_results)[-50:]
        
        if not items:
            return pd.DataFrame(columns=[
                "Time", "Txn ID", "User ID", "Amount ($)", "Category", "Country", "Score", "Status", "Reason"
            ])

        records = []
        for r in reversed(items):
            txn = r.transaction
            records.append({
                "Time": time.strftime("%H:%M:%S", time.localtime(txn.timestamp)),
                "Txn ID": txn.transaction_id,
                "User ID": txn.user_id,
                "Amount ($)": f"${txn.amount:.2f}",
                "Category": txn.merchant_category,
                "Country": txn.location,
                "Score": f"{r.risk_score:.0f}",
                "Status": r.status.value,
                "Reason": r.primary_reason,
            })
        return pd.DataFrame(records)

    def get_alerts_list(self) -> List[RiskResult]:
        with self._lock:
            return list(self.fraud_alerts)


# Global singleton instance for Streamlit multi-run consistency
_pipeline_instance: Optional[FraudStreamingPipeline] = None
_instance_lock = threading.Lock()


def get_pipeline() -> FraudStreamingPipeline:
    global _pipeline_instance
    with _instance_lock:
        if _pipeline_instance is None:
            _pipeline_instance = FraudStreamingPipeline()
            _pipeline_instance.start()
        elif not _pipeline_instance.is_running:
            _pipeline_instance.start()
        return _pipeline_instance
